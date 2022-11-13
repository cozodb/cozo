/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use ::sqlite::Connection;
use either::{Either, Left, Right};
use miette::{bail, miette, IntoDiagnostic, Result};
use sqlite::{State, Statement};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};

/// The Sqlite storage engine
pub struct SqliteStorage {
    lock: Arc<RwLock<()>>,
    name: String,
}

/// Create a sqlite backed database.
/// This is slower than [`new_cozo_rocksdb`](crate::new_cozo_rocksdb)
/// but uses way less resources and is much easier to compile for exotic
/// environments.
/// Supports concurrent readers but only a single writer.
///
/// You must provide a disk-based path: `:memory:` is not OK.
/// If you want a pure memory storage, use [`new_cozo_mem`](crate::new_cozo_mem).
pub fn new_cozo_sqlite(path: String) -> Result<crate::Db<SqliteStorage>> {
    let conn = sqlite::open(&path).into_diagnostic()?;
    let query = r#"
        create table if not exists cozo
        (
            k BLOB primary key,
            v BLOB
        );
    "#;
    let mut statement = conn.prepare(query).unwrap();
    while statement.next().into_diagnostic()? != State::Done {}

    let ret = crate::Db::new(SqliteStorage {
        lock: Arc::new(Default::default()),
        name: path,
    })?;

    ret.initialize()?;
    Ok(ret)
}

impl<'s> Storage<'s> for SqliteStorage {
    type Tx = SqliteTx<'s>;

    fn transact(&'s self, write: bool) -> Result<Self::Tx> {
        let conn = sqlite::open(&self.name).into_diagnostic()?;
        let lock = if write {
            Right(self.lock.write().unwrap())
        } else {
            Left(self.lock.read().unwrap())
        };
        if write {
            let mut stmt = conn.prepare("begin;").into_diagnostic()?;
            while stmt.next().into_diagnostic()? != State::Done {}
        }
        Ok(SqliteTx {
            lock,
            conn,
            committed: false,
        })
    }

    fn del_range(&'_ self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let lower_b = lower.to_vec();
        let upper_b = upper.to_vec();
        let query = r#"
                delete from cozo where k >= ? and k < ?;
            "#;
        let lock = self.lock.clone();
        let name = self.name.clone();
        std::thread::spawn(move || {
            let _locked = lock.write().unwrap();
            let conn = sqlite::open(&name).unwrap();
            let mut statement = conn.prepare(query).unwrap();
            statement.bind((1, &lower_b as &[u8])).unwrap();
            statement.bind((2, &upper_b as &[u8])).unwrap();
            while statement.next().unwrap() != State::Done {}
        });
        Ok(())
    }

    fn range_compact(&'_ self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub struct SqliteTx<'a> {
    lock: Either<RwLockReadGuard<'a, ()>, RwLockWriteGuard<'a, ()>>,
    conn: Connection,
    committed: bool,
}

impl Drop for SqliteTx<'_> {
    fn drop(&mut self) {
        if let Right(RwLockWriteGuard { .. }) = self.lock {
            if !self.committed {
                let query = r#"rollback;"#;
                let _ = self.conn.execute(query);
            }
        }
    }
}

impl<'s> StoreTx<'s> for SqliteTx<'s> {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        let query = r#"
                select v from cozo where k = ?;
            "#;

        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, key)).unwrap();
        Ok(match statement.next().into_diagnostic()? {
            State::Row => {
                let res = statement.read::<Vec<u8>, _>(0).into_diagnostic()?;
                Some(res)
            }
            State::Done => None,
        })
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let query = r#"
                insert into cozo(k, v) values (?, ?)
                on conflict(k) do update set v=excluded.v;
            "#;

        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, key)).unwrap();
        statement.bind((2, val)).unwrap();
        while statement.next().into_diagnostic()? != State::Done {}
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        let query = r#"
                delete from cozo where k = ?;
            "#;
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, key)).unwrap();
        while statement.next().into_diagnostic()? != State::Done {}

        Ok(())
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        let query = r#"
                select 1 from cozo where k = ?;
            "#;
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, key)).unwrap();
        Ok(match statement.next().into_diagnostic()? {
            State::Row => true,
            State::Done => false,
        })
    }

    fn commit(&mut self) -> Result<()> {
        if let Right(RwLockWriteGuard { .. }) = self.lock {
            if !self.committed {
                let query = r#"commit;"#;
                let mut statement = self.conn.prepare(query).unwrap();
                while statement.next().into_diagnostic()? != State::Done {}
                self.committed = true;
            } else {
                bail!("multiple commits")
            }
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
        let query = r#"
                select k, v from cozo where k >= ? and k < ?
                order by k;
            "#;
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, lower)).unwrap();
        statement.bind((2, upper)).unwrap();
        Box::new(TupleIter(statement))
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        let query = r#"
                select k, v from cozo where k >= ? and k < ?
                order by k;
            "#;
        let mut statement = self.conn.prepare(query).unwrap();
        statement.bind((1, lower)).unwrap();
        statement.bind((2, upper)).unwrap();
        Box::new(RawIter(statement))
    }

    fn batch_put(
        &mut self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>,
    ) -> Result<()> {
        let query = r#"
                insert into cozo(k, v) values (?, ?)
                on conflict(k) do update set v=excluded.v;
            "#;

        let mut statement = self.conn.prepare(query).unwrap();
        for pair in data {
            let (key, val) = pair?;
            statement.bind((1, key.as_slice())).unwrap();
            statement.bind((2, val.as_slice())).unwrap();
            while statement.next().into_diagnostic()? != State::Done {}
            statement.reset().into_diagnostic()?;
        }
        Ok(())
    }
}

struct TupleIter<'l>(Statement<'l>);

impl<'l> Iterator for TupleIter<'l> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Ok(State::Done) => None,
            Ok(State::Row) => {
                let k = self.0.read::<Vec<u8>, _>(0).unwrap();
                let v = self.0.read::<Vec<u8>, _>(1).unwrap();
                let tuple = decode_tuple_from_kv(&k, &v);
                Some(Ok(tuple))
            }
            Err(err) => Some(Err(miette!(err))),
        }
    }
}

struct RawIter<'l>(Statement<'l>);

impl<'l> Iterator for RawIter<'l> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Ok(State::Done) => None,
            Ok(State::Row) => {
                let k = self.0.read::<Vec<u8>, _>(0).unwrap();
                let v = self.0.read::<Vec<u8>, _>(1).unwrap();
                Some(Ok((k, v)))
            }
            Err(err) => Some(Err(miette!(err))),
        }
    }
}
