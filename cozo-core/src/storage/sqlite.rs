/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use ::sqlite::Connection;
use miette::{miette, IntoDiagnostic, Result, WrapErr};
use sqlite::{State, Statement};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};

/// The Sqlite storage engine
pub struct SqliteStorage {
    path: String,
}

/// create a sqlite backed database. `:memory:` is not OK.
pub fn new_cozo_sqlite(path: String) -> Result<crate::Db<SqliteStorage>> {
    let connection = sqlite::open(&path).into_diagnostic()?;
    let query = r#"
        create table if not exists cozo
        (
            k BLOB primary key,
            v BLOB
        );
    "#;
    let mut statement = connection.prepare(query).unwrap();
    while statement.next().into_diagnostic()? != State::Done {}

    let ret = crate::Db::new(SqliteStorage { path })?;

    ret.initialize()?;
    Ok(ret)
}

impl Storage<'_> for SqliteStorage {
    type Tx = SqliteTx;

    fn transact(&'_ self, _write: bool) -> Result<Self::Tx> {
        let conn = sqlite::open(&self.path).into_diagnostic()?;
        {
            let query = r#"begin;"#;
            let mut statement = conn.prepare(query).unwrap();
            while statement.next().unwrap() != State::Done {}
        }

        Ok(SqliteTx { conn })
    }

    fn del_range(&'_ self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let lower_b = lower.to_vec();
        let upper_b = upper.to_vec();
        let path = self.path.clone();
        let connection = sqlite::open(path).unwrap();
        let query = r#"
                delete from cozo where k >= ? and k < ?;
            "#;
        let mut statement = connection.prepare(query).unwrap();
        statement.bind((1, &lower_b as &[u8])).unwrap();
        statement.bind((2, &upper_b as &[u8])).unwrap();
        while statement.next().unwrap() != State::Done {}
        Ok(())
    }

    fn range_compact(&'_ self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub struct SqliteTx {
    conn: Connection,
}

impl<'s> StoreTx<'s> for SqliteTx {
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
        while statement
            .next()
            .into_diagnostic()
            .with_context(|| format!("{:x?} {:?} {:x?}", key, val, Tuple::decode_from_key(key)))?
            != State::Done
        {}
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
        let query = r#"commit;"#;
        let mut statement = self.conn.prepare(query).unwrap();
        while statement.next().unwrap() != State::Done {}
        Ok(())
    }

    fn range_scan<'a>(
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

    fn range_scan_raw<'a>(
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
