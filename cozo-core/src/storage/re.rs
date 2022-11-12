/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::path::Path;
use std::sync::Arc;
use std::{iter, thread};

use itertools::Itertools;
use miette::{miette, IntoDiagnostic, Result};
use redb::{
    Builder, Database, ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition,
    WriteStrategy, WriteTransaction,
};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};

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

impl Storage for ReStorage {
    type Tx = ReTx;

    fn transact(&self, write: bool) -> Result<Self::Tx> {
        Ok(if write {
            ReTx::Write(ReTxWrite::new(self.db.clone()))
        } else {
            ReTx::Read(ReTxRead::new(self.db.clone()))
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

pub enum ReTx {
    Read(ReTxRead),
    Write(ReTxWrite),
}

pub struct ReTxRead {
    db_ptr: Option<*const Database>,
    tx_ptr: Option<*mut ReadTransaction<'static>>,
    tbl_ptr: Option<*mut ReadOnlyTable<'static, [u8], [u8]>>,
}

impl ReTxRead {
    fn new(db_arc: Arc<Database>) -> Self {
        unsafe {
            let db_ptr = Arc::into_raw(db_arc);
            let tx_ptr = Box::into_raw(Box::new(
                (&*db_ptr)
                    .begin_read()
                    .expect("fatal: open read transaction failed"),
            ));
            let tbl_ptr = Box::into_raw(Box::new(
                (&*tx_ptr)
                    .open_table(TABLE)
                    .expect("fatal: open table failed"),
            ));
            ReTxRead {
                db_ptr: Some(db_ptr),
                tx_ptr: Some(tx_ptr),
                tbl_ptr: Some(tbl_ptr),
            }
        }
    }
}

impl Drop for ReTxRead {
    fn drop(&mut self) {
        unsafe {
            let db_ptr = self.db_ptr.take();
            let tx_ptr = self.tx_ptr.take();
            let tbl_ptr = self.tbl_ptr.take();
            let _db = Arc::from_raw(db_ptr.unwrap());
            let _tx = Box::from_raw(tx_ptr.unwrap());
            let _tbl = Box::from_raw(tbl_ptr.unwrap());
        }
    }
}

pub struct ReTxWrite {
    db_ptr: Option<*const Database>,
    tx_ptr: Option<*mut WriteTransaction<'static>>,
    tbl_ptr: Option<*mut Table<'static, 'static, [u8], [u8]>>,
}

impl ReTxWrite {
    fn new(db_arc: Arc<Database>) -> Self {
        unsafe {
            let db_ptr = Arc::into_raw(db_arc);
            let tx_ptr = Box::into_raw(Box::new(
                (&*db_ptr)
                    .begin_write()
                    .expect("fatal: open write transaction failed"),
            ));
            let tbl_ptr = Box::into_raw(Box::new(
                (&*tx_ptr)
                    .open_table(TABLE)
                    .expect("fatal: open table failed"),
            ));
            ReTxWrite {
                db_ptr: Some(db_ptr),
                tx_ptr: Some(tx_ptr),
                tbl_ptr: Some(tbl_ptr),
            }
        }
    }
}

impl Drop for ReTxWrite {
    fn drop(&mut self) {
        unsafe {
            let db_ptr = self.db_ptr.take();
            let _db = Arc::from_raw(db_ptr.unwrap());
            if self.tx_ptr.is_some() {
                let tx_ptr = self.tx_ptr.take();
                let tbl_ptr = self.tbl_ptr.take();

                let _tx = Box::from_raw(tx_ptr.unwrap());
                let _tbl = Box::from_raw(tbl_ptr.unwrap());
            }
        }
    }
}

impl StoreTx for ReTx {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        unsafe {
            match self {
                ReTx::Read(inner) => {
                    let tbl = &*inner.tbl_ptr.unwrap();
                    tbl.get(key)
                        .map(|op| op.map(|s| s.to_vec()))
                        .into_diagnostic()
                }
                ReTx::Write(inner) => {
                    let tbl = &*inner.tbl_ptr.unwrap();
                    tbl.get(key)
                        .map(|op| op.map(|s| s.to_vec()))
                        .into_diagnostic()
                }
            }
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        unsafe {
            match self {
                ReTx::Read(_) => unreachable!(),
                ReTx::Write(inner) => {
                    let tbl = &mut *inner.tbl_ptr.unwrap();
                    tbl.insert(key, val).into_diagnostic()?;
                    Ok(())
                }
            }
        }
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        unsafe {
            match self {
                ReTx::Read(_) => unreachable!(),
                ReTx::Write(inner) => {
                    let tbl = &mut *inner.tbl_ptr.unwrap();
                    tbl.remove(key).into_diagnostic()?;
                    Ok(())
                }
            }
        }
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        unsafe {
            match self {
                ReTx::Read(inner) => {
                    let tbl = &*inner.tbl_ptr.unwrap();
                    tbl.get(key).map(|op| op.is_some()).into_diagnostic()
                }
                ReTx::Write(inner) => {
                    let tbl = &*inner.tbl_ptr.unwrap();
                    tbl.get(key).map(|op| op.is_some()).into_diagnostic()
                }
            }
        }
    }

    fn commit(&mut self) -> Result<()> {
        match self {
            ReTx::Read(_) => Ok(()),
            ReTx::Write(inner) => unsafe {
                let tx_ptr = inner.tx_ptr.take();
                let tbl_ptr = inner.tbl_ptr.take();
                let _tbl = Box::from_raw(tbl_ptr.unwrap());
                let tx = Box::from_raw(tx_ptr.unwrap());
                tx.commit().into_diagnostic()
            },
        }
    }

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Box<dyn Iterator<Item = Result<Tuple>>> {
        match self {
            ReTx::Read(inner) => unsafe {
                let tbl = &*inner.tbl_ptr.unwrap();
                match tbl.range(lower.to_vec()..upper.to_vec()) {
                    Ok(it) => Box::new(it.map(|(k, v)| Ok(decode_tuple_from_kv(k, v)))),
                    Err(err) => Box::new(iter::once(Err(miette!(err)))),
                }
            },
            ReTx::Write(inner) => unsafe {
                let tbl = &*inner.tbl_ptr.unwrap();
                match tbl.range(lower.to_vec()..upper.to_vec()) {
                    Ok(it) => Box::new(it.map(|(k, v)| Ok(decode_tuple_from_kv(k, v)))),
                    Err(err) => Box::new(iter::once(Err(miette!(err)))),
                }
            },
        }
    }

    fn range_scan_raw(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> {
        match self {
            ReTx::Read(inner) => unsafe {
                let tbl = &*inner.tbl_ptr.unwrap();
                match tbl.range(lower.to_vec()..upper.to_vec()) {
                    Ok(it) => Box::new(it.map(|(k, v)| Ok((k.to_vec(), v.to_vec())))),
                    Err(err) => Box::new(iter::once(Err(miette!(err)))),
                }
            },
            ReTx::Write(inner) => unsafe {
                let tbl = &*inner.tbl_ptr.unwrap();
                match tbl.range(lower.to_vec()..upper.to_vec()) {
                    Ok(it) => Box::new(it.map(|(k, v)| Ok((k.to_vec(), v.to_vec())))),
                    Err(err) => Box::new(iter::once(Err(miette!(err)))),
                }
            },
        }
    }
}
