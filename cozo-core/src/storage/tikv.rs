/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::cell::RefCell;
use std::sync::Arc;
use std::{iter, thread};

use lazy_static::lazy_static;
use miette::{miette, IntoDiagnostic, Result};
use tikv_client::{RawClient, Transaction, TransactionClient};
use tokio::runtime::Runtime;

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::Db;

/// connect to a Storage engine backed by TiKV
pub fn new_cozo_tikv(pd_endpoints: Vec<String>, optimistic: bool) -> Result<Db<TiKvStorage>> {
    let raw_client = RT
        .block_on(RawClient::new(pd_endpoints.clone()))
        .into_diagnostic()?;
    let client = RT
        .block_on(TransactionClient::new(pd_endpoints.clone()))
        .into_diagnostic()?;
    Db::new(TiKvStorage {
        client: Arc::new(client),
        raw_client: Arc::new(raw_client),
        optimistic,
    })
}

lazy_static! {
    static ref RT: Runtime = {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("starting Tokio runtime failed")
    };
}

/// Storage engine based on TiKV
#[derive(Clone)]
pub struct TiKvStorage {
    client: Arc<TransactionClient>,
    raw_client: Arc<RawClient>,
    optimistic: bool,
}

impl Storage for TiKvStorage {
    type Tx = TiKvTx;

    fn transact(&self, _write: bool) -> Result<Self::Tx> {
        let tx = if self.optimistic {
            RT.block_on(self.client.begin_optimistic())
                .into_diagnostic()?
        } else {
            RT.block_on(self.client.begin_pessimistic())
                .into_diagnostic()?
        };
        Ok(TiKvTx {
            tx: RefCell::new(tx),
        })
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let raw_client = self.raw_client.clone();
        let lower_b = lower.to_owned();
        let upper_b = upper.to_owned();
        thread::spawn(move || {
            RT.block_on(raw_client.delete_range(lower_b..upper_b))
                .unwrap();
        });
        Ok(())
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub struct TiKvTx {
    tx: RefCell<Transaction>,
}

impl StoreTx for TiKvTx {
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>> {
        if for_update {
            RT.block_on(self.tx.borrow_mut().get_for_update(key.to_owned()))
                .into_diagnostic()
        } else {
            RT.block_on(self.tx.borrow_mut().get(key.to_owned()))
                .into_diagnostic()
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        RT.block_on(self.tx.borrow_mut().put(key.to_owned(), val.to_owned()))
            .into_diagnostic()
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        RT.block_on(self.tx.borrow_mut().delete(key.to_owned()))
            .into_diagnostic()
    }

    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool> {
        if for_update {
            RT.block_on(self.tx.borrow_mut().get_for_update(key.to_owned()))
                .map(|v| v.is_some())
                .into_diagnostic()
        } else {
            RT.block_on(self.tx.borrow_mut().key_exists(key.to_owned()))
                .into_diagnostic()
        }
    }

    fn commit(&mut self) -> Result<()> {
        RT.block_on(self.tx.borrow_mut().commit())
            .into_diagnostic()?;
        Ok(())
    }

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Box<dyn Iterator<Item = Result<Tuple>>> {
        match RT.block_on(
            self.tx
                .borrow_mut()
                .scan(lower.to_owned()..upper.to_owned(), u32::MAX),
        ) {
            Ok(it) => Box::new(it.map(|pair| -> Result<Tuple> {
                let k: Vec<_> = pair.0.into();
                let v: Vec<_> = pair.1.into();
                let tuple = decode_tuple_from_kv(&k, &v);
                Ok(tuple)
            })),
            Err(err) => Box::new(iter::once(Err(miette!(err)))),
        }
    }

    fn range_scan_raw(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> {
        match RT.block_on(
            self.tx
                .borrow_mut()
                .scan(lower.to_owned()..upper.to_owned(), u32::MAX),
        ) {
            Ok(it) => Box::new(it.map(|pair| -> Result<(Vec<u8>, Vec<u8>)> {
                let k = pair.0.into();
                let v = pair.1.into();
                Ok((k, v))
            })),
            Err(err) => Box::new(iter::once(Err(miette!(err)))),
        }
    }
}
