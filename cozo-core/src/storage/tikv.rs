/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::iter;
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use lazy_static::lazy_static;
use miette::{miette, IntoDiagnostic, Result};
use tikv_client::{RawClient, Transaction, TransactionClient};
use tokio::runtime::Runtime;

use crate::data::tuple::Tuple;
use crate::data::value::ValidityTs;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::{swap_option_result, TempCollector};
use crate::Db;

/// Connect to a Storage engine backed by TiKV.
/// Experimental and very slow.
pub fn new_cozo_tikv(pd_endpoints: Vec<String>, optimistic: bool) -> Result<Db<TiKvStorage>> {
    let client = RT
        .block_on(TransactionClient::new(pd_endpoints))
        .into_diagnostic()?;
    let ret = Db::new(TiKvStorage {
        client: Arc::new(client),
        optimistic,
    })?;
    ret.initialize()?;
    Ok(ret)
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
    optimistic: bool,
}

impl Storage<'_> for TiKvStorage {
    type Tx = TiKvTx;

    fn storage_kind(&self) -> &'static str {
        "tikv"
    }

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

pub struct TiKvTx {
    tx: Arc<Mutex<Transaction>>,
}

impl<'s> StoreTx<'s> for TiKvTx {
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
        self.par_put(key, val)
    }

    fn supports_par_put(&self) -> bool {
        true
    }

    fn par_put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        RT.block_on(self.tx.lock().unwrap().put(key.to_owned(), val.to_owned()))
            .into_diagnostic()
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.par_del(key)
    }

    fn par_del(&self, key: &[u8]) -> Result<()> {
        RT.block_on(self.tx.lock().unwrap().delete(key.to_owned()))
            .into_diagnostic()
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let mut to_del = TempCollector::default();
        for pair in self.range_scan(lower, upper) {
            to_del.push(pair?.0);
        }

        for key in to_del.into_iter() {
            self.del(&key)?;
        }
        Ok(())
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

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        Box::new(BatchScanner::new(self.tx.clone(), lower, upper))
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        _lower: &[u8],
        _upper: &[u8],
        _valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        Box::new(iter::once(Err(miette!(
            "TiKV backend does not support time travelling."
        ))))
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        Box::new(BatchScannerRaw::new(self.tx.clone(), lower, upper))
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        Ok(BatchScannerRaw::new(self.tx.clone(), lower, upper).count())
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        self.range_scan(&[], &[u8::MAX])
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
                    .map(|pair| -> (Vec<u8>, Vec<u8>) { (pair.0.into(), pair.1) })
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
                        .map(|pair| -> (Vec<u8>, Vec<u8>) { (pair.0.into(), pair.1) })
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
                .map(|mkv| mkv.map(|(k, v)| decode_tuple_from_kv(k, v, None))),
        )
    }
}
