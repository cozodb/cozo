// /*
//  * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
//  */
//
// use std::cmp::Ordering;
// use std::collections::btree_map::Range;
// use std::collections::BTreeMap;
// use std::iter::Fuse;
// use std::sync::{Arc, RwLock};
// use std::thread;
//
// use miette::{IntoDiagnostic, Result};
// use sled::transaction::{ConflictableTransactionError, TransactionalTree};
// use sled::{Db, IVec, Iter};
//
// use crate::data::tuple::Tuple;
// use crate::runtime::relation::decode_tuple_from_kv;
// use crate::storage::{Storage, StoreTx};
// use crate::utils::swap_option_result;
//
// #[derive(Clone)]
// struct SledStorage {
//     db: Db,
// }
//
// impl Storage for SledStorage {
//     type Tx = SledTx;
//
//     fn transact(&self) -> Result<Self::Tx> {
//         Ok(SledTx {
//             db: self.db.clone(),
//             changes: Default::default(),
//         })
//     }
//
//     fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
//         let db = self.db.clone();
//         let lower_v = lower.to_vec();
//         let upper_v = upper.to_vec();
//         thread::spawn(move || -> Result<()> {
//             for k_res in db.range(lower_v..upper_v).keys() {
//                 db.remove(k_res.into_diagnostic()?).into_diagnostic()?;
//             }
//             Ok(())
//         });
//         Ok(())
//     }
//
//     fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
//         Ok(())
//     }
// }
//
// struct SledTx {
//     db: Db,
//     changes: Arc<RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>>,
// }
//
// impl StoreTx for SledTx {
//     #[inline]
//     fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
//         Ok(match self.changes.read().unwrap().get(key) {
//             Some(Some(val)) => Some(val.clone()),
//             Some(None) => None,
//             None => {
//                 let ret = self.db.get(key).into_diagnostic()?;
//                 ret.map(|v| v.to_vec())
//             }
//         })
//     }
//
//     #[inline]
//     fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
//         self.changes.write().unwrap().insert(key.into(), Some(val.into()));
//         Ok(())
//     }
//
//     #[inline]
//     fn del(&mut self, key: &[u8]) -> Result<()> {
//         self.changes.write().unwrap().insert(key.into(), None);
//         Ok(())
//     }
//
//     #[inline]
//     fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
//         Ok(match self.changes.read().unwrap().get(key) {
//             Some(Some(_)) => true,
//             Some(None) => false,
//             None => self.db.get(key).into_diagnostic()?.is_some(),
//         })
//     }
//
//     fn commit(&mut self) -> Result<()> {
//         self.db
//             .transaction(
//                 |db: &TransactionalTree| -> Result<(), ConflictableTransactionError> {
//                     for (k, v) in self.changes.read().unwrap().iter() {
//                         match v {
//                             None => {
//                                 db.remove(k as &[u8])?;
//                             }
//                             Some(v) => {
//                                 db.insert(k as &[u8], v as &[u8])?;
//                             }
//                         }
//                     }
//                     Ok(())
//                 },
//             )
//             .into_diagnostic()?;
//         Ok(())
//     }
//
//     fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Box<dyn Iterator<Item = Result<Tuple>>> {
//         let change_iter = self.changes.read().unwrap().range(lower.to_vec()..upper.to_vec()).fuse();
//         let db_iter = self.db.range(lower..upper).fuse();
//         Box::new(SledIter {
//             change_iter,
//             db_iter,
//             change_cache: None,
//             db_cache: None,
//         })
//     }
//
//     fn range_scan_raw(
//         &self,
//         lower: &[u8],
//         upper: &[u8],
//     ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> {
//         let change_iter = self.changes.read().unwrap().range(lower.to_vec()..upper.to_vec()).fuse();
//         let db_iter = self.db.range(lower..upper).fuse();
//         Box::new(SledIterRaw {
//             change_iter,
//             db_iter,
//             change_cache: None,
//             db_cache: None,
//         })
//     }
// }
//
// struct SledIter<'a> {
//     change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
//     db_iter: Fuse<Iter>,
//     change_cache: Option<(Vec<u8>, Option<Vec<u8>>)>,
//     db_cache: Option<(IVec, IVec)>,
// }
//
// impl<'a> SledIter<'a> {
//     #[inline]
//     fn fill_cache(&mut self) -> Result<()> {
//         if self.change_cache.is_none() {
//             if let Some((k, v)) = self.change_iter.next() {
//                 self.change_cache = Some((k.to_vec(), v.clone()))
//             }
//         }
//
//         if self.db_cache.is_none() {
//             if let Some(res) = self.db_iter.next() {
//                 self.db_cache = Some(res.into_diagnostic()?);
//             }
//         }
//
//         Ok(())
//     }
//
//     #[inline]
//     fn next_inner(&mut self) -> Result<Option<Tuple>> {
//         loop {
//             self.fill_cache()?;
//             match (&self.change_cache, &self.db_cache) {
//                 (None, None) => return Ok(None),
//                 (Some((_, None)), None) => {
//                     self.change_cache.take();
//                     continue;
//                 }
//                 (Some((_, Some(_))), None) => {
//                     let (k, sv) = self.change_cache.take().unwrap();
//                     let v = sv.unwrap();
//                     return Ok(Some(decode_tuple_from_kv(&k, &v)));
//                 }
//                 (None, Some(_)) => {
//                     let (k, v) = self.db_cache.take().unwrap();
//                     return Ok(Some(decode_tuple_from_kv(&k, &v)));
//                 }
//                 (Some((ck, _)), Some((dk, _))) => match ck.as_slice().cmp(dk) {
//                     Ordering::Less => {
//                         let (k, sv) = self.change_cache.take().unwrap();
//                         match sv {
//                             None => continue,
//                             Some(v) => {
//                                 return Ok(Some(decode_tuple_from_kv(&k, &v)));
//                             }
//                         }
//                     }
//                     Ordering::Greater => {
//                         let (k, v) = self.db_cache.take().unwrap();
//                         return Ok(Some(decode_tuple_from_kv(&k, &v)));
//                     }
//                     Ordering::Equal => {
//                         self.db_cache.take();
//                         continue;
//                     }
//                 },
//             }
//         }
//     }
// }
//
// impl<'a> Iterator for SledIter<'a> {
//     type Item = Result<Tuple>;
//
//     #[inline]
//     fn next(&mut self) -> Option<Self::Item> {
//         swap_option_result(self.next_inner())
//     }
// }
//
// struct SledIterRaw<'a> {
//     change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
//     db_iter: Fuse<Iter>,
//     change_cache: Option<(Vec<u8>, Option<Vec<u8>>)>,
//     db_cache: Option<(IVec, IVec)>,
// }
//
// impl<'a> SledIterRaw<'a> {
//     #[inline]
//     fn fill_cache(&mut self) -> Result<()> {
//         if self.change_cache.is_none() {
//             if let Some((k, v)) = self.change_iter.next() {
//                 self.change_cache = Some((k.to_vec(), v.clone()))
//             }
//         }
//
//         if self.db_cache.is_none() {
//             if let Some(res) = self.db_iter.next() {
//                 self.db_cache = Some(res.into_diagnostic()?);
//             }
//         }
//
//         Ok(())
//     }
//
//     #[inline]
//     fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
//         loop {
//             self.fill_cache()?;
//             match (&self.change_cache, &self.db_cache) {
//                 (None, None) => return Ok(None),
//                 (Some((_, None)), None) => {
//                     self.change_cache.take();
//                     continue;
//                 }
//                 (Some((_, Some(_))), None) => {
//                     let (k, sv) = self.change_cache.take().unwrap();
//                     let v = sv.unwrap();
//                     return Ok(Some((k, v)));
//                 }
//                 (None, Some(_)) => {
//                     let (k, v) = self.db_cache.take().unwrap();
//                     return Ok(Some((k.to_vec(), v.to_vec())));
//                 }
//                 (Some((ck, _)), Some((dk, _))) => match ck.as_slice().cmp(dk) {
//                     Ordering::Less => {
//                         let (k, sv) = self.change_cache.take().unwrap();
//                         match sv {
//                             None => continue,
//                             Some(v) => return Ok(Some((k, v))),
//                         }
//                     }
//                     Ordering::Greater => {
//                         let (k, v) = self.db_cache.take().unwrap();
//                         return Ok(Some((k.to_vec(), v.to_vec())));
//                     }
//                     Ordering::Equal => {
//                         self.db_cache.take();
//                         continue;
//                     }
//                 },
//             }
//         }
//     }
// }
//
// impl<'a> Iterator for SledIterRaw<'a> {
//     type Item = Result<(Vec<u8>, Vec<u8>)>;
//
//     #[inline]
//     fn next(&mut self) -> Option<Self::Item> {
//         swap_option_result(self.next_inner())
//     }
// }
