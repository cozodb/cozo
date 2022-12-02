/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Borrow;
use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::{iter, thread};

use clap::builder::TypedValueParser;
use itertools::Itertools;
use miette::{miette, IntoDiagnostic, Report, Result};
use ouroboros::self_referencing;
use redb::{
    Builder, Database, RangeIter, ReadOnlyTable, ReadTransaction, ReadableTable, Table,
    TableDefinition, WriteStrategy, WriteTransaction,
};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};

/// This currently does not work even after pulling in ouroboros: ReDB's lifetimes are really maddening

/// Creates a ReDB database object.
pub fn new_cozo_redb(path: impl AsRef<Path>) -> Result<crate::Db<ReStorage>> {
    let ret = crate::Db::new(ReStorage::new(path)?)?;
    ret.initialize()?;
    Ok(ret)
}

const TABLE: TableDefinition<'_, [u8], [u8]> = TableDefinition::new("cozo");

/// Storage engine based on ReDB
#[derive(Clone)]
pub struct ReStorage {
    db: Arc<Database>,
}

impl ReStorage {
    fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db: Arc<Database> = Arc::new(unsafe {
            match Database::open(path.as_ref()) {
                Ok(db) => db,
                Err(_) => Builder::new()
                    .set_write_strategy(WriteStrategy::Checksum)
                    .set_dynamic_growth(true)
                    .create(path, 1024 * 1024 * 1024 * 1024)
                    .into_diagnostic()?,
            }
        });
        {
            let tx = db.begin_write().into_diagnostic()?;
            {
                let _tbl = tx.open_table(TABLE).into_diagnostic()?;
            }
            tx.commit().into_diagnostic()?;
        }
        Ok(ReStorage { db })
    }
}

impl<'s> Storage<'s> for ReStorage {
    type Tx = ReTx<'s>;

    fn transact(&'s self, write: bool) -> Result<Self::Tx> {
        Ok(if write {
            let tx = self.db.begin_write().into_diagnostic()?;
            ReTx::Write(ReTxWrite {
                tx: Some(RefCell::new(tx)),
            })
        } else {
            let tx = self.db.begin_read().into_diagnostic()?;
            ReTx::Read(ReTxRead { tx })
        })
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let lower_b = lower.to_vec();
        let upper_b = upper.to_vec();
        thread::spawn(move || {
            let keys = {
                let tx = db.begin_read().unwrap();
                let tbl = tx.open_table(TABLE).unwrap();
                tbl.range(lower_b..upper_b)
                    .unwrap()
                    .map(|(k, _)| k.to_vec())
                    .collect_vec()
            };
            let tx = db.begin_write().unwrap();
            let mut tbl = tx.open_table(TABLE).unwrap();
            for k in &keys {
                tbl.remove(k).unwrap();
            }
        });
        Ok(())
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub enum ReTx<'s> {
    Read(ReTxRead<'s>),
    Write(ReTxWrite<'s>),
}

pub struct ReTxRead<'s> {
    tx: ReadTransaction<'s>,
}

pub struct ReTxWrite<'s> {
    tx: Option<RefCell<WriteTransaction<'s>>>,
}

impl<'s> StoreTx<'s> for ReTx<'s> {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        match self {
            ReTx::Read(inner) => {
                let tbl = inner.tx.open_table(TABLE).into_diagnostic()?;
                tbl.get(key)
                    .map(|op| op.map(|s| s.to_vec()))
                    .into_diagnostic()
            }
            ReTx::Write(inner) => {
                let tx = inner.tx.as_ref().unwrap().borrow_mut();
                let tbl = tx.open_table(TABLE).into_diagnostic()?;
                tbl.get(key)
                    .map(|op| op.map(|s| s.to_vec()))
                    .into_diagnostic()
            }
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        match self {
            ReTx::Read(_) => unreachable!(),
            ReTx::Write(inner) => {
                let tx = inner.tx.as_ref().unwrap().borrow_mut();
                let mut tbl = tx.open_table(TABLE).into_diagnostic()?;
                tbl.insert(key, val).into_diagnostic()?;
                Ok(())
            }
        }
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        match self {
            ReTx::Read(_) => unreachable!(),
            ReTx::Write(inner) => {
                let tx = inner.tx.as_ref().unwrap().borrow_mut();
                let mut tbl = tx.open_table(TABLE).into_diagnostic()?;
                tbl.remove(key).into_diagnostic()?;
                Ok(())
            }
        }
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        match self {
            ReTx::Read(inner) => {
                let tbl = inner.tx.open_table(TABLE).into_diagnostic()?;
                tbl.get(key).map(|op| op.is_some()).into_diagnostic()
            }
            ReTx::Write(inner) => {
                let tx = inner.tx.as_ref().unwrap().borrow_mut();
                let tbl = tx.open_table(TABLE).into_diagnostic()?;
                tbl.get(key).map(|op| op.is_some()).into_diagnostic()
            }
        }
    }

    fn commit(&mut self) -> Result<()> {
        match self {
            ReTx::Read(_) => Ok(()),
            ReTx::Write(inner) => {
                let tx_cell = inner.tx.take().unwrap();
                let tx = tx_cell.into_inner();
                tx.commit().into_diagnostic()?;
                Ok(())
            }
        }
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        match self {
            ReTx::Read(inner) => {
                let tbl = match inner.tx.open_table(TABLE).into_diagnostic() {
                    Ok(tbl) => tbl,
                    Err(err) => return Box::new(iter::once(Err(miette!(err)))),
                };
                let it = ReadTableIterBuilder {
                    tbl,
                    it_builder: |tbl| tbl.range(lower.to_vec()..upper.to_vec()).unwrap(),
                }
                .build();
                todo!()
                // match tbl.range(lower.to_vec()..upper.to_vec()) {
                //     Ok(it) => Box::new(it.map(|(k, v)| Ok(decode_tuple_from_kv(k, v)))),
                //     Err(err) => Box::new(iter::once(Err(miette!(err)))),
                // }
            }
            ReTx::Write(inner) => {
                let tx = inner.tx.as_ref().unwrap().borrow_mut();
                let tbl = match tx.open_table(TABLE) {
                    Ok(tbl) => tbl,
                    Err(err) => return Box::new(iter::once(Err(miette!(err)))),
                };

                let it = WriteTableIterBuilder {
                    tbl,
                    it_builder: |tbl| tbl.range(lower.to_vec()..upper.to_vec()).unwrap(),
                }
                .build();
                todo!()
                // let tbl = &*inner.tbl_ptr.unwrap();
                // match tbl.range(lower.to_vec()..upper.to_vec()) {
                //     Ok(it) => Box::new(it.map(|(k, v)| Ok(decode_tuple_from_kv(k, v)))),
                //     Err(err) => Box::new(iter::once(Err(miette!(err)))),
                // }
            }
        }
    }

    fn range_scan_raw<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        todo!()
        // match self {
        //     ReTx::Read(inner) => unsafe {
        //         let tbl = &*inner.tbl_ptr.unwrap();
        //         match tbl.range(lower.to_vec()..upper.to_vec()) {
        //             Ok(it) => Box::new(it.map(|(k, v)| Ok((k.to_vec(), v.to_vec())))),
        //             Err(err) => Box::new(iter::once(Err(miette!(err)))),
        //         }
        //     },
        //     ReTx::Write(inner) => unsafe {
        //         todo!()
        //         // let tbl = &*inner.tbl_ptr.unwrap();
        //         // match tbl.range(lower.to_vec()..upper.to_vec()) {
        //         //     Ok(it) => Box::new(it.map(|(k, v)| Ok((k.to_vec(), v.to_vec())))),
        //         //     Err(err) => Box::new(iter::once(Err(miette!(err)))),
        //         // }
        //     },
        // }
    }
}

#[self_referencing]
struct ReadTableIter<'txn> {
    tbl: ReadOnlyTable<'txn, [u8], [u8]>,
    #[borrows(tbl)]
    #[not_covariant]
    it: RangeIter<'this, [u8], [u8]>,
}

#[self_referencing]
struct WriteTableIter<'db, 'txn>
where
    'txn: 'db,
{
    tbl: Table<'db, 'txn, [u8], [u8]>,
    #[borrows(tbl)]
    #[not_covariant]
    it: RangeIter<'this, [u8], [u8]>,
}
