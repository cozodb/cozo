/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::iter::Fuse;
use std::path::Path;

use itertools::Itertools;
use miette::{IntoDiagnostic, Result};
use sled::{Batch, Config, Db, IVec, Iter, Mode};

use crate::data::tuple::{check_key_for_validity, Tuple};
use crate::data::value::ValidityTs;
use crate::runtime::relation::{decode_tuple_from_kv, extend_tuple_from_v};
use crate::storage::{Storage, StoreTx};
use crate::utils::{swap_option_result, TempCollector};

/// Creates a Sled database object. Experimental.
/// You should use [`new_cozo_rocksdb`](crate::new_cozo_rocksdb) or
/// [`new_cozo_sqlite`](crate::new_cozo_sqlite) instead.
pub fn new_cozo_sled(path: impl AsRef<Path>) -> Result<crate::Db<SledStorage>> {
    let db = sled::open(path).into_diagnostic()?;
    let ret = crate::Db::new(SledStorage { db })?;

    ret.initialize()?;
    Ok(ret)
}

/// Storage engine using Sled
#[derive(Clone)]
pub struct SledStorage {
    db: Db,
}

const PUT_MARKER: u8 = 1;
const DEL_MARKER: u8 = 0;

impl Storage<'_> for SledStorage {
    type Tx = SledTx;

    fn storage_kind(&self) -> &'static str {
        "sled"
    }

    fn transact(&self, _write: bool) -> Result<Self::Tx> {
        Ok(SledTx {
            db: self.db.clone(),
            changes: Default::default(),
        })
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        let mut tx = self.transact(true)?;
        for result in data {
            let (key, val) = result?;
            tx.put(&key, &val)?;
        }
        tx.commit()?;
        Ok(())
    }
}

pub struct SledTx {
    db: Db,
    changes: Option<Db>,
}

impl SledTx {
    #[inline]
    fn ensure_changes_db(&mut self) -> Result<()> {
        if self.changes.is_none() {
            let db = Config::new()
                .temporary(true)
                .mode(Mode::HighThroughput)
                .use_compression(false)
                .open()
                .into_diagnostic()?;
            self.changes = Some(db)
        }
        Ok(())
    }
}

impl<'s> StoreTx<'s> for SledTx {
    #[inline]
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        if let Some(changes) = &self.changes {
            if let Some(val) = changes.get(key).into_diagnostic()? {
                return if val[0] == DEL_MARKER {
                    Ok(None)
                } else {
                    let data = val[1..].to_vec();
                    Ok(Some(data))
                };
            }
        }
        let ret = self.db.get(key).into_diagnostic()?;
        Ok(ret.map(|v| v.to_vec()))
    }

    #[inline]
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.ensure_changes_db()?;
        let mut val_to_write = Vec::with_capacity(val.len() + 1);
        val_to_write.push(PUT_MARKER);
        val_to_write.extend_from_slice(val);
        self.changes
            .as_mut()
            .unwrap()
            .insert(key, val_to_write)
            .into_diagnostic()?;
        Ok(())
    }

    fn supports_par_put(&self) -> bool {
        false
    }

    #[inline]
    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.ensure_changes_db()?;
        let val_to_write = [DEL_MARKER];
        self.changes
            .as_mut()
            .unwrap()
            .insert(key, &val_to_write)
            .into_diagnostic()?;
        Ok(())
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let mut to_del = TempCollector::default();

        for pair in self.range_scan(lower, upper) {
            let (k, _) = pair?;
            to_del.push(k);
        }

        for k_res in to_del.into_iter() {
            self.db.remove(&k_res).into_diagnostic()?;
        }
        Ok(())
    }

    #[inline]
    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        if let Some(changes) = &self.changes {
            if let Some(val) = changes.get(key).into_diagnostic()? {
                return Ok(val[0] != DEL_MARKER);
            }
        }
        let ret = self.db.get(key).into_diagnostic()?;
        Ok(ret.is_some())
    }

    fn commit(&mut self) -> Result<()> {
        if let Some(changes) = &self.changes {
            let mut batch = Batch::default();
            for pair in changes.iter() {
                let (k, v) = pair.into_diagnostic()?;
                if v[0] == DEL_MARKER {
                    batch.remove(&k);
                } else {
                    batch.insert(&k, &v[1..]);
                }
            }
            self.db.apply_batch(batch).into_diagnostic()?;
        }
        Ok(())
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        if let Some(changes) = &self.changes {
            let change_iter = changes.range(lower.to_vec()..upper.to_vec()).fuse();
            let db_iter = self.db.range(lower.to_vec()..upper.to_vec()).fuse();
            Box::new(SledIter {
                change_iter,
                db_iter,
                change_cache: None,
                db_cache: None,
            })
        } else {
            Box::new(
                self.db
                    .range(lower.to_vec()..upper.to_vec())
                    .map(|d| d.into_diagnostic())
                    .map_ok(|(k, v)| decode_tuple_from_kv(&k, &v, None)),
            )
        }
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        if let Some(changes) = &self.changes {
            Box::new(SledSkipDualIterator {
                db: &self.db,
                changes,
                upper: upper.to_vec(),
                valid_at,
                next_bound: lower.to_vec(),
            })
        } else {
            Box::new(SledSkipIterator {
                db: &self.db,
                upper: upper.to_vec(),
                valid_at,
                next_bound: lower.to_vec(),
            })
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
        if let Some(changes) = &self.changes {
            let change_iter = changes.range(lower.to_vec()..upper.to_vec()).fuse();
            let db_iter = self.db.range(lower.to_vec()..upper.to_vec()).fuse();
            Box::new(SledIterRaw {
                change_iter,
                db_iter,
                change_cache: None,
                db_cache: None,
            })
        } else {
            Box::new(
                self.db
                    .range(lower.to_vec()..upper.to_vec())
                    .map(|d| d.into_diagnostic())
                    .map_ok(|(k, v)| (k.to_vec(), v.to_vec())),
            )
        }
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        Ok(if let Some(changes) = &self.changes {
            let change_iter = changes.range(lower.to_vec()..upper.to_vec()).fuse();
            let db_iter = self.db.range(lower.to_vec()..upper.to_vec()).fuse();
            (SledIterRaw {
                change_iter,
                db_iter,
                change_cache: None,
                db_cache: None,
            })
            .count()
        } else {
            self.db.range(lower.to_vec()..upper.to_vec()).count()
        })
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        self.range_scan(&[], &[u8::MAX])
    }
}

struct SledIterRaw {
    change_iter: Fuse<Iter>,
    db_iter: Fuse<Iter>,
    change_cache: Option<(IVec, IVec)>,
    db_cache: Option<(IVec, IVec)>,
}

impl SledIterRaw {
    #[inline]
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some(res) = self.change_iter.next() {
                self.change_cache = Some(res.into_diagnostic()?)
            }
        }

        if self.db_cache.is_none() {
            if let Some(res) = self.db_iter.next() {
                self.db_cache = Some(res.into_diagnostic()?);
            }
        }

        Ok(())
    }

    #[inline]
    fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some(_), None) => {
                    let (k, cv) = self.change_cache.take().unwrap();
                    if cv[0] == DEL_MARKER {
                        continue;
                    } else {
                        return Ok(Some((k.to_vec(), cv[1..].to_vec())));
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some((k.to_vec(), v.to_vec())));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        if sv[0] == DEL_MARKER {
                            continue;
                        } else {
                            return Ok(Some((k.to_vec(), sv[1..].to_vec())));
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some((k.to_vec(), v.to_vec())));
                    }
                    Ordering::Equal => {
                        self.db_cache.take();
                        continue;
                    }
                },
            }
        }
    }
}

impl Iterator for SledIterRaw {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

struct SledIter {
    change_iter: Fuse<Iter>,
    db_iter: Fuse<Iter>,
    change_cache: Option<(IVec, IVec)>,
    db_cache: Option<(IVec, IVec)>,
}

impl SledIter {
    #[inline]
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some(res) = self.change_iter.next() {
                self.change_cache = Some(res.into_diagnostic()?)
            }
        }

        if self.db_cache.is_none() {
            if let Some(res) = self.db_iter.next() {
                self.db_cache = Some(res.into_diagnostic()?);
            }
        }

        Ok(())
    }

    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some(_), None) => {
                    let (k, cv) = self.change_cache.take().unwrap();
                    if cv[0] == DEL_MARKER {
                        continue;
                    } else {
                        return Ok(Some(decode_tuple_from_kv(&k, &cv[1..], None)));
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some(decode_tuple_from_kv(&k, &v, None)));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        if sv[0] == DEL_MARKER {
                            continue;
                        } else {
                            return Ok(Some(decode_tuple_from_kv(&k, &sv[1..], None)));
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some(decode_tuple_from_kv(&k, &v, None)));
                    }
                    Ordering::Equal => {
                        self.db_cache.take();
                        continue;
                    }
                },
            }
        }
    }
}

impl Iterator for SledIter {
    type Item = Result<Tuple>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

struct SledSkipIterator<'a> {
    db: &'a Db,
    upper: Vec<u8>,
    valid_at: ValidityTs,
    next_bound: Vec<u8>,
}

impl SledSkipIterator<'_> {
    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            let nxt = self
                .db
                .range(self.next_bound.clone()..self.upper.clone())
                .next();
            match nxt {
                None => return Ok(None),
                Some(result) => {
                    let (candidate_key, candidate_val) = result.into_diagnostic()?;
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

impl Iterator for SledSkipIterator<'_> {
    type Item = Result<Tuple>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

struct SledSkipDualIterator<'a> {
    db: &'a Db,
    changes: &'a Db,
    upper: Vec<u8>,
    valid_at: ValidityTs,
    next_bound: Vec<u8>,
}

impl SledSkipDualIterator<'_> {
    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            let stored_nxt = self
                .db
                .range(self.next_bound.clone()..self.upper.clone())
                .next()
                .transpose()
                .into_diagnostic()?;
            let delta_nxt = self
                .changes
                .range(self.next_bound.clone()..self.upper.clone())
                .next()
                .transpose()
                .into_diagnostic()?;

            let (candidate_key, candidate_val): (Vec<u8>, Vec<u8>) =
                match (stored_nxt, delta_nxt) {
                    (None, None) => return Ok(None),
                    (None, Some((dk, dv))) => {
                        if dv[0] == DEL_MARKER {
                            let (_, nxt_seek) =
                                check_key_for_validity(&dk, self.valid_at, None);
                            self.next_bound = nxt_seek;
                            continue;
                        }
                        (dk.to_vec(), dv[1..].to_vec())
                    }
                    (Some((sk, sv)), None) => (sk.to_vec(), sv.to_vec()),
                    (Some((sk, sv)), Some((dk, dv))) => {
                        if sk < dk {
                            (sk.to_vec(), sv.to_vec())
                        } else {
                            if dv[0] == DEL_MARKER {
                                let (_, nxt_seek) =
                                    check_key_for_validity(&dk, self.valid_at, None);
                                self.next_bound = nxt_seek;
                                continue;
                            }
                            (dk.to_vec(), dv[1..].to_vec())
                        }
                    }
                };

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

impl Iterator for SledSkipDualIterator<'_> {
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

    fn setup_test_db() -> Result<(TempDir, crate::Db<SledStorage>)> {
        let temp_dir = TempDir::new().into_diagnostic()?;
        let db = new_cozo_sled(temp_dir.path())?;
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

    fn run(db: &crate::Db<SledStorage>, q: &str) -> Result<crate::NamedRows> {
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
    fn test_delete() -> Result<()> {
        let (_tmp, db) = setup_test_db()?;

        run(&db, "?[k, v] <- [[1, 'a'], [2, 'b'], [3, 'c']] :put plain {k => v}")?;
        assert_eq!(run(&db, "?[k, v] := *plain{k, v}")?.rows.len(), 3);

        // Delete + read in same imperative script (exercises uncommitted delta)
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

        // Two keys, each with versions at t=0 and t=5
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
