/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;
use std::sync::Arc;

use miette::{bail, IntoDiagnostic, Result};
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};

use crate::data::tuple::{check_key_for_validity, Tuple};
use crate::data::value::ValidityTs;
use crate::runtime::relation::{decode_tuple_from_kv, extend_tuple_from_v};
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cozo");

/// Creates a redb database object.
pub fn new_cozo_redb(path: impl AsRef<Path>) -> Result<crate::Db<RedbStorage>> {
    let db = Database::create(path).into_diagnostic()?;
    {
        let tx = db.begin_write().into_diagnostic()?;
        tx.open_table(TABLE).into_diagnostic()?;
        tx.commit().into_diagnostic()?;
    }
    let ret = crate::Db::new(RedbStorage {
        db: Arc::new(db),
    })?;
    ret.initialize()?;
    Ok(ret)
}

/// Storage engine using redb
#[derive(Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

impl<'s> Storage<'s> for RedbStorage {
    type Tx = RedbTx;

    fn storage_kind(&self) -> &'static str {
        "redb"
    }

    fn transact(&'s self, write: bool) -> Result<Self::Tx> {
        if write {
            let tx = self.db.begin_write().into_diagnostic()?;
            Ok(RedbTx::Write(Some(tx)))
        } else {
            let tx = self.db.begin_read().into_diagnostic()?;
            Ok(RedbTx::Read(tx))
        }
    }

    fn range_compact(&'s self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        let tx = self.db.begin_write().into_diagnostic()?;
        {
            let mut table = tx.open_table(TABLE).into_diagnostic()?;
            for result in data {
                let (key, val) = result?;
                table
                    .insert(key.as_slice(), val.as_slice())
                    .into_diagnostic()?;
            }
        }
        tx.commit().into_diagnostic()?;
        Ok(())
    }
}

pub enum RedbTx {
    Read(ReadTransaction),
    Write(Option<WriteTransaction>),
}

unsafe impl Sync for RedbTx {}


impl<'s> StoreTx<'s> for RedbTx {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        match self {
            RedbTx::Read(tx) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                Ok(table.get(key).into_diagnostic()?.map(|v| v.value().to_vec()))
            }
            RedbTx::Write(Some(tx)) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                let result = table.get(key).into_diagnostic()?.map(|v| v.value().to_vec());
                Ok(result)
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        match self {
            RedbTx::Read(_) => bail!("write in read transaction"),
            RedbTx::Write(Some(tx)) => {
                let mut table = tx.open_table(TABLE).into_diagnostic()?;
                table.insert(key, val).into_diagnostic()?;
                Ok(())
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn supports_par_put(&self) -> bool {
        false
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        match self {
            RedbTx::Read(_) => bail!("write in read transaction"),
            RedbTx::Write(Some(tx)) => {
                let mut table = tx.open_table(TABLE).into_diagnostic()?;
                table.remove(key).into_diagnostic()?;
                Ok(())
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        match self {
            RedbTx::Read(_) => bail!("write in read transaction"),
            RedbTx::Write(Some(tx)) => {
                let mut table = tx.open_table(TABLE).into_diagnostic()?;
                table
                    .retain_in::<&[u8], _>(lower..upper, |_, _| false)
                    .into_diagnostic()?;
                Ok(())
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        match self {
            RedbTx::Read(tx) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                Ok(table.get(key).into_diagnostic()?.is_some())
            }
            RedbTx::Write(Some(tx)) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                let result = table.get(key).into_diagnostic()?.is_some();
                Ok(result)
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn commit(&mut self) -> Result<()> {
        match self {
            RedbTx::Read(_) => Ok(()),
            RedbTx::Write(tx) => {
                if let Some(tx) = tx.take() {
                    tx.commit().into_diagnostic()?;
                }
                Ok(())
            }
        }
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        match self {
            RedbTx::Read(tx) => {
                let table = match tx.open_table(TABLE) {
                    Ok(t) => t,
                    Err(e) => return Box::new(std::iter::once(Err(miette::miette!("{e}")))),
                };
                let iter = match table.range::<&[u8]>(lower..upper) {
                    Ok(i) => i,
                    Err(e) => return Box::new(std::iter::once(Err(miette::miette!("{e}")))),
                };
                // ReadOnlyTable::range returns Range<'static> — safe to stream
                Box::new(iter.map(|r| match r {
                    Ok(entry) => Ok((entry.0.value().to_vec(), entry.1.value().to_vec())),
                    Err(e) => Err(miette::miette!("{e}")),
                }))
            }
            RedbTx::Write(Some(tx)) => {
                let table = match tx.open_table(TABLE) {
                    Ok(t) => t,
                    Err(e) => return Box::new(std::iter::once(Err(miette::miette!("{e}")))),
                };
                let collected: Vec<_> = table
                    .range::<&[u8]>(lower..upper)
                    .into_iter()
                    .flatten()
                    .map(|r| match r {
                        Ok(entry) => Ok((entry.0.value().to_vec(), entry.1.value().to_vec())),
                        Err(e) => Err(miette::miette!("{e}")),
                    })
                    .collect();
                Box::new(collected.into_iter())
            }
            RedbTx::Write(None) => {
                Box::new(std::iter::once(Err(miette::miette!("transaction already committed"))))
            }
        }
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        Box::new(RedbSkipIterator {
            tx: self,
            upper: upper.to_vec(),
            valid_at,
            next_bound: lower.to_vec(),
        })
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        
        match self {
            RedbTx::Read(tx) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                Ok(table.range::<&[u8]>(lower..upper).into_diagnostic()?.count())
            }
            RedbTx::Write(Some(tx)) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                Ok(table.range::<&[u8]>(lower..upper).into_diagnostic()?.count())
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        self.range_scan(&[], &[u8::MAX])
    }
}

impl RedbTx {
    fn seek_one(&self, lower: &[u8], upper: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        
        match self {
            RedbTx::Read(tx) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                match table.range::<&[u8]>(lower..upper).into_diagnostic()?.next() {
                    None => Ok(None),
                    Some(r) => {
                        let entry = r.into_diagnostic()?;
                        Ok(Some((entry.0.value().to_vec(), entry.1.value().to_vec())))
                    }
                }
            }
            RedbTx::Write(Some(tx)) => {
                let table = tx.open_table(TABLE).into_diagnostic()?;
                let result = match table.range::<&[u8]>(lower..upper).into_diagnostic()?.next() {
                    None => None,
                    Some(r) => {
                        let entry = r.into_diagnostic()?;
                        Some((entry.0.value().to_vec(), entry.1.value().to_vec()))
                    }
                };
                Ok(result)
            }
            RedbTx::Write(None) => bail!("transaction already committed"),
        }
    }
}

struct RedbSkipIterator<'a> {
    tx: &'a RedbTx,
    upper: Vec<u8>,
    valid_at: ValidityTs,
    next_bound: Vec<u8>,
}

impl RedbSkipIterator<'_> {
    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.tx.seek_one(&self.next_bound, &self.upper)? {
                None => return Ok(None),
                Some((candidate_key, candidate_val)) => {
                    let (ret, nxt_bound) =
                        check_key_for_validity(&candidate_key, self.valid_at, None);
                    self.next_bound = nxt_bound;
                    if let Some(mut nk) = ret {
                        extend_tuple_from_v(&mut nk, &candidate_val);
                        return Ok(Some(nk));
                    }
                }
            }
        }
    }
}

impl Iterator for RedbSkipIterator<'_> {
    type Item = Result<Tuple>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::{DataValue, Validity};
    use crate::runtime::db::ScriptMutability;
    use miette::{IntoDiagnostic, Result};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    use super::*;

    fn setup_test_db() -> Result<(TempDir, crate::Db<RedbStorage>)> {
        let temp_dir = TempDir::new().into_diagnostic()?;
        let db = new_cozo_redb(temp_dir.path().join("test.redb"))?;
        db.run_script(
            r#"
            {:create plain {k: Int => v}}
            {:create tt {k: Int, vld: Validity => v}}
            "#,
            Default::default(),
            ScriptMutability::Mutable,
        )?;
        Ok((temp_dir, db))
    }

    fn run(db: &crate::Db<RedbStorage>, q: &str) -> Result<crate::NamedRows> {
        db.run_script(q, Default::default(), ScriptMutability::Mutable)
    }

    fn tt_row(k: i64, ts: i64, v: i64) -> Vec<DataValue> {
        vec![
            DataValue::from(k),
            DataValue::Validity(Validity::from((ts, true))),
            DataValue::from(v),
        ]
    }

    #[test]
    fn test_basic_operations() -> Result<()> {
        let (_tmp, db) = setup_test_db()?;

        run(&db, "?[k, v] <- [[1, 'a'], [2, 'b'], [3, 'c']] :put plain {k => v}")?;
        let result = run(&db, "?[k, v] := *plain{k, v}")?;
        assert_eq!(result.rows.len(), 3);

        run(&db, "?[k, v] <- [[2, 'updated']] :put plain {k => v}")?;
        let result = run(&db, "?[v] := *plain{k: 2, v}")?;
        assert_eq!(result.rows[0][0], DataValue::from("updated"));

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let (_tmp, db) = setup_test_db()?;

        run(&db, "?[k, v] <- [[1, 'a'], [2, 'b'], [3, 'c']] :put plain {k => v}")?;

        let result = run(&db, r#"
            {?[k] <- [[2]] :rm plain {k}}
            {?[k, v] := *plain{k, v}}
        "#)?;
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], DataValue::from(1));
        assert_eq!(result.rows[1][0], DataValue::from(3));

        Ok(())
    }

    #[test]
    fn test_time_travel() -> Result<()> {
        let (_tmp, db) = setup_test_db()?;

        let mut to_import = BTreeMap::new();
        to_import.insert(
            "tt".to_string(),
            crate::NamedRows {
                headers: vec!["k".into(), "vld".into(), "v".into()],
                rows: vec![
                    tt_row(1, 0, 10), tt_row(1, 5, 15),
                    tt_row(2, 0, 20), tt_row(2, 5, 25),
                ],
                next: None,
            },
        );
        db.import_relations(to_import)?;

        let r = run(&db, "?[k, v] := *tt{k, v @ 0}")?;
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0], vec![DataValue::from(1), DataValue::from(10)]);
        assert_eq!(r.rows[1], vec![DataValue::from(2), DataValue::from(20)]);

        let r = run(&db, "?[k, v] := *tt{k, v @ 5}")?;
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0], vec![DataValue::from(1), DataValue::from(15)]);
        assert_eq!(r.rows[1], vec![DataValue::from(2), DataValue::from(25)]);

        Ok(())
    }
}
