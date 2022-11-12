/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};
use std::thread;

use itertools::Itertools;
use lazy_static::lazy_static;
use miette::{IntoDiagnostic, Result};
use tikv_client::{RawClient, Transaction, TransactionClient};
use tokio::runtime::Runtime;

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;
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
            tx: Arc::new(Mutex::new(tx)),
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
    tx: Arc<Mutex<Transaction>>,
}

impl StoreTx for TiKvTx {
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>> {
        if for_update {
            RT.block_on(self.tx.lock().unwrap().get_for_update(key.to_owned()))
                .into_diagnostic()
        } else {
            RT.block_on(self.tx.lock().unwrap().get(key.to_owned()))
                .into_diagnostic()
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        RT.block_on(self.tx.lock().unwrap().put(key.to_owned(), val.to_owned()))
            .into_diagnostic()
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        RT.block_on(self.tx.lock().unwrap().delete(key.to_owned()))
            .into_diagnostic()
    }

    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool> {
        if for_update {
            RT.block_on(self.tx.lock().unwrap().get_for_update(key.to_owned()))
                .map(|v| v.is_some())
                .into_diagnostic()
        } else {
            RT.block_on(self.tx.lock().unwrap().key_exists(key.to_owned()))
                .into_diagnostic()
        }
    }

    fn commit(&mut self) -> Result<()> {
        RT.block_on(self.tx.lock().unwrap().commit())
            .into_diagnostic()?;
        Ok(())
    }

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Box<dyn Iterator<Item = Result<Tuple>>> {
        Box::new(BatchScanner::new(self.tx.clone(), lower, upper))
    }

    fn range_scan_raw(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> {
        Box::new(BatchScannerRaw::new(self.tx.clone(), lower, upper))
    }
}

struct BatchScannerRaw {
    tx: Arc<Mutex<Transaction>>,
    lower: Vec<u8>,
    upper: Vec<u8>,
    fetched: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    iter_idx: usize,
}

impl BatchScannerRaw {
    fn new(tx: Arc<Mutex<Transaction>>, lower: &[u8], upper: &[u8]) -> Self {
        Self {
            tx,
            lower: lower.to_vec(),
            upper: upper.to_vec(),
            fetched: None,
            iter_idx: 0,
        }
    }
}

const BATCH_SIZE: u32 = 100;

impl BatchScannerRaw {
    fn get_batch(&mut self) -> Result<bool> {
        match &mut self.fetched {
            None => {
                self.iter_idx = 0;
                let mut tx = self.tx.lock().unwrap();
                let fut = tx.scan(
                    (Included(self.lower.clone()), Excluded(self.upper.clone())),
                    BATCH_SIZE,
                );
                let res = RT.block_on(fut).into_diagnostic()?;
                let res_vec = res
                    .map(|pair| -> (Vec<u8>, Vec<u8>) { (pair.0.into(), pair.1.into()) })
                    .collect_vec();
                let has_content = !res_vec.is_empty();
                if has_content {
                    self.fetched = Some(res_vec);
                }
                Ok(has_content)
            }
            Some(fetched) => {
                let l = fetched.len();
                if l as u32 == BATCH_SIZE && self.iter_idx == l {
                    let last_key = fetched.pop().unwrap().0;

                    let mut tx = self.tx.lock().unwrap();
                    let fut = tx.scan(
                        (Excluded(last_key), Excluded(self.upper.clone())),
                        BATCH_SIZE,
                    );
                    let res = RT.block_on(fut).into_diagnostic()?;
                    let res_vec = res
                        .map(|pair| -> (Vec<u8>, Vec<u8>) { (pair.0.into(), pair.1.into()) })
                        .collect_vec();
                    let has_content = !res_vec.is_empty();
                    if has_content {
                        self.iter_idx = 0;
                        self.fetched = Some(res_vec);
                    }
                    Ok(has_content)
                } else {
                    Ok(self.iter_idx < l)
                }
            }
        }
    }
    fn next_inner(&mut self) -> Result<Option<&(Vec<u8>, Vec<u8>)>> {
        Ok(if self.get_batch()? {
            Some({
                let item = &self.fetched.as_ref().unwrap()[self.iter_idx];
                self.iter_idx += 1;
                item
            })
        } else {
            None
        })
    }
}

impl Iterator for BatchScannerRaw {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner().map(|kv| kv.cloned()))
    }
}

struct BatchScanner {
    raw: BatchScannerRaw,
}

impl BatchScanner {
    fn new(tx: Arc<Mutex<Transaction>>, lower: &[u8], upper: &[u8]) -> Self {
        Self {
            raw: BatchScannerRaw::new(tx, lower, upper),
        }
    }
}

impl Iterator for BatchScanner {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(
            self.raw
                .next_inner()
                .map(|mkv| mkv.map(|(k, v)| decode_tuple_from_kv(k, v))),
        )
    }
}
