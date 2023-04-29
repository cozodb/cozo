/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// Some ideas are from https://github.com/schelterlabs/rust-minhash

use crate::data::expr::{eval_bytecode, Bytecode, eval_bytecode_pred};
use crate::data::tuple::Tuple;
use crate::fts::tokenizer::TextAnalyzer;
use crate::fts::TokenizerConfig;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;
use crate::{DataValue, Expr, SourceSpan, Symbol};
use miette::{bail, miette, Result};
use quadrature::integrate;
use rand::{thread_rng, RngCore};
use rustc_hash::FxHashMap;
use smartstring::{LazyCompact, SmartString};
use std::cmp::min;
use std::hash::{Hash, Hasher};
use twox_hash::XxHash32;

impl<'a> SessionTx<'a> {
    pub(crate) fn del_lsh_index_item(
        &mut self,
        tuple: &[DataValue],
        bytes: Option<Vec<u8>>,
        idx_handle: &RelationHandle,
        inv_idx_handle: &RelationHandle,
        manifest: &MinHashLshIndexManifest,
    ) -> Result<()> {
        let bytes = match bytes {
            None => {
                if let Some(mut found) = inv_idx_handle.get_val_only(self, tuple)? {
                    let inv_key = inv_idx_handle.encode_key_for_store(tuple, Default::default())?;
                    self.store_tx.del(&inv_key)?;
                    match found.pop() {
                        Some(DataValue::Bytes(b)) => b,
                        _ => unreachable!(),
                    }
                } else {
                    return Ok(());
                }
            }
            Some(b) => b,
        };

        let mut key = Vec::with_capacity(bytes.len() + 2);
        key.push(DataValue::Bot);
        key.push(DataValue::Bot);
        key.extend_from_slice(tuple);
        for (i, chunk) in bytes
            .chunks_exact(manifest.r * std::mem::size_of::<u32>())
            .enumerate()
        {
            key[0] = DataValue::from(i as i64);
            key[1] = DataValue::Bytes(chunk.to_vec());
            let key_bytes = idx_handle.encode_key_for_store(&key, Default::default())?;
            self.store_tx.del(&key_bytes)?;
        }
        Ok(())
    }
    pub(crate) fn put_lsh_index_item(
        &mut self,
        tuple: &[DataValue],
        extractor: &[Bytecode],
        stack: &mut Vec<DataValue>,
        tokenizer: &TextAnalyzer,
        rel_handle: &RelationHandle,
        idx_handle: &RelationHandle,
        inv_idx_handle: &RelationHandle,
        manifest: &MinHashLshIndexManifest,
        hash_perms: &HashPermutations,
    ) -> Result<()> {
        if let Some(mut found) =
            inv_idx_handle.get_val_only(self, &tuple[..rel_handle.metadata.keys.len()])?
        {
            let bytes = match found.pop() {
                Some(DataValue::Bytes(b)) => b,
                _ => unreachable!(),
            };
            self.del_lsh_index_item(tuple, Some(bytes), idx_handle, inv_idx_handle, manifest)?;
        }
        let to_index = eval_bytecode(extractor, tuple, stack)?;
        let min_hash = match to_index {
            DataValue::Null => return Ok(()),
            DataValue::List(l) => HashValues::new(l.iter(), hash_perms),
            DataValue::Str(s) => {
                let n_grams = tokenizer.unique_ngrams(&s, manifest.n_gram);
                HashValues::new(n_grams.iter(), hash_perms)
            }
            _ => bail!("Cannot put value {:?} into a LSH index", to_index),
        };
        let bytes = min_hash.get_bytes();
        let inv_key_part = &tuple[..rel_handle.metadata.keys.len()];
        let inv_val_part = vec![DataValue::Bytes(bytes.to_vec())];
        let inv_key = inv_idx_handle.encode_key_for_store(inv_key_part, Default::default())?;
        let inv_val =
            inv_idx_handle.encode_val_only_for_store(&inv_val_part, Default::default())?;
        self.store_tx.put(&inv_key, &inv_val)?;

        let mut key = Vec::with_capacity(bytes.len() + 2);
        key.push(DataValue::Bot);
        key.push(DataValue::Bot);
        key.extend_from_slice(inv_key_part);
        let chunk_size = manifest.r * std::mem::size_of::<u32>();
        for i in 0..manifest.b {
            let byte_range = &bytes[i * chunk_size..(i + 1) * chunk_size];
            key[0] = DataValue::from(i as i64);
            key[1] = DataValue::Bytes(byte_range.to_vec());
            let key_bytes = idx_handle.encode_key_for_store(&key, Default::default())?;
            self.store_tx.put(&key_bytes, &[])?;
        }

        Ok(())
    }
    pub(crate) fn lsh_search(
        &self,
        tuple: &[DataValue],
        config: &LshSearch,
        stack: &mut Vec<DataValue>,
        filter_code: &Option<(Vec<Bytecode>, SourceSpan)>,
    ) -> Result<Vec<Tuple>> {
        let bytes = if let Some(mut found) = config
            .inv_idx_handle
            .get_val_only(self, &tuple[..config.base_handle.metadata.keys.len()])?
        {
            match found.pop() {
                Some(DataValue::Bytes(b)) => b,
                _ => unreachable!(),
            }
        } else {
            return Ok(vec![]);
        };
        let chunk_size = config.manifest.r * std::mem::size_of::<u32>();
        let mut key_prefix = Vec::with_capacity(2);
        let mut found_tuples: FxHashMap<_, usize> = FxHashMap::default();
        for (i, chunk) in bytes.chunks_exact(chunk_size).enumerate() {
            key_prefix.clear();
            key_prefix.push(DataValue::from(i as i64));
            key_prefix.push(DataValue::Bytes(chunk.to_vec()));
            for ks in config.idx_handle.scan_prefix(self, &key_prefix) {
                let ks = ks?;
                let key_part = &ks[2..];
                if key_part != tuple {
                    let found = found_tuples.entry(key_part.to_vec()).or_default();
                    *found += 1;
                }
            }
        }
        let mut ret = vec![];
        for (key, count) in found_tuples {
            let similarity = count as f64 / config.manifest.r as f64;
            if similarity < config.min_similarity {
                continue;
            }
            let mut orig_tuple = config
                .base_handle
                .get(self, &key)?
                .ok_or_else(|| miette!("Tuple not found in base LSH relation"))?;
            if let Some((filter_code, span)) = filter_code {
                if !eval_bytecode_pred(filter_code, &orig_tuple, stack, *span)? {
                    continue;
                }
            }
            if config.bind_similarity.is_some() {
                orig_tuple.push(DataValue::from(similarity));
            }
            ret.push(orig_tuple);
            if let Some(k) = config.k {
                if ret.len() >= k {
                    break;
                }
            }
        }
        Ok(ret)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LshSearch {
    pub(crate) base_handle: RelationHandle,
    pub(crate) idx_handle: RelationHandle,
    pub(crate) inv_idx_handle: RelationHandle,
    pub(crate) manifest: MinHashLshIndexManifest,
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) k: Option<usize>,
    pub(crate) query: Symbol,
    pub(crate) bind_similarity: Option<Symbol>,
    pub(crate) min_similarity: f64,
    // pub(crate) lax_mode: bool,
    pub(crate) filter: Option<Expr>,
    pub(crate) span: SourceSpan,
}

impl LshSearch {
    pub(crate) fn all_bindings(&self) -> impl Iterator<Item = &Symbol> {
        self.bindings.iter().chain(self.bind_similarity.iter())
    }
}

pub(crate) struct HashValues(pub(crate) Vec<u32>);
pub(crate) struct HashPermutations(pub(crate) Vec<u32>);

#[derive(Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct MinHashLshIndexManifest {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) extractor: String,
    pub(crate) n_gram: usize,
    pub(crate) tokenizer: TokenizerConfig,
    pub(crate) filters: Vec<TokenizerConfig>,

    pub(crate) num_perm: usize,
    pub(crate) b: usize,
    pub(crate) r: usize,
    pub(crate) threshold: f64,
    pub(crate) perms: Vec<u8>,
}

impl MinHashLshIndexManifest {
    pub(crate) fn get_hash_perms(&self) -> HashPermutations {
        HashPermutations::from_bytes(&self.perms)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LshParams {
    pub b: usize,
    pub r: usize,
}

#[derive(Clone)]
pub(crate) struct Weights(pub(crate) f64, pub(crate) f64);

const _ALLOWED_INTEGRATE_ERR: f64 = 0.001;

// code is mostly from https://github.com/schelterlabs/rust-minhash/blob/81ea3fec24fd888a330a71b6932623643346b591/src/minhash_lsh.rs
impl LshParams {
    pub fn find_optimal_params(threshold: f64, num_perm: usize, weights: &Weights) -> LshParams {
        let Weights(false_positive_weight, false_negative_weight) = weights;
        let mut min_error = f64::INFINITY;
        let mut opt = LshParams { b: 0, r: 0 };
        for b in 1..num_perm + 1 {
            let max_r = num_perm / b;
            for r in 1..max_r + 1 {
                let false_pos = LshParams::false_positive_probability(threshold, b, r);
                let false_neg = LshParams::false_negative_probability(threshold, b, r);
                let error = false_pos * false_positive_weight + false_neg * false_negative_weight;
                if error < min_error {
                    min_error = error;
                    opt = LshParams { b, r };
                }
            }
        }
        opt
    }

    fn false_positive_probability(threshold: f64, b: usize, r: usize) -> f64 {
        let _probability =
            |s| -> f64 { 1. - f64::powf(1. - f64::powi(s, r as i32), b as f64) };
        integrate(_probability, 0.0, threshold, _ALLOWED_INTEGRATE_ERR).integral
    }

    fn false_negative_probability(threshold: f64, b: usize, r: usize) -> f64 {
        let _probability =
            |s| -> f64 { 1. - (1. - f64::powf(1. - f64::powi(s, r as i32), b as f64)) };
        integrate(_probability, threshold, 1.0, _ALLOWED_INTEGRATE_ERR).integral
    }
}

impl HashPermutations {
    pub(crate) fn new(n_perms: usize) -> Self {
        let mut rng = thread_rng();
        let mut perms = Vec::with_capacity(n_perms);
        for _ in 0..n_perms {
            perms.push(rng.next_u32());
        }
        Self(perms)
    }
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const u8,
                self.0.len() * std::mem::size_of::<u32>(),
            )
        }
    }
    // this is the inverse of `as_bytes`
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        unsafe {
            let ptr = bytes.as_ptr() as *const u32;
            let len = bytes.len() / std::mem::size_of::<u32>();
            let perms = std::slice::from_raw_parts(ptr, len);
            Self(perms.to_vec())
        }
    }
}

impl HashValues {
    pub(crate) fn new<T: Hash>(values: impl Iterator<Item = T>, perms: &HashPermutations) -> Self {
        let mut ret = Self::init(perms);
        ret.update(values, perms);
        ret
    }
    pub(crate) fn init(perms: &HashPermutations) -> Self {
        Self(vec![u32::MAX; perms.0.len()])
    }
    pub(crate) fn update<T: Hash>(
        &mut self,
        values: impl Iterator<Item = T>,
        perms: &HashPermutations,
    ) {
        for v in values {
            for (i, seed) in perms.0.iter().enumerate() {
                let mut hasher = XxHash32::with_seed(*seed);
                v.hash(&mut hasher);
                let hash = hasher.finish() as u32;
                self.0[i] = min(self.0[i], hash);
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn jaccard(&self, other_minhash: &Self) -> f32 {
        let matches = self
            .0
            .iter()
            .zip_eq(&other_minhash.0)
            .filter(|(left, right)| left == right)
            .count();
        let result = matches as f32 / self.0.len() as f32;
        result
    }
    pub(crate) fn get_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const u8,
                self.0.len() * std::mem::size_of::<u32>(),
            )
        }
    }
    // pub(crate) fn get_byte_chunks(&self, n_chunks: usize) -> impl Iterator<Item = &[u8]> {
    //     let chunk_size = self.0.len() * std::mem::size_of::<u32>() / n_chunks;
    //     self.get_bytes().chunks_exact(chunk_size)
    // }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_minhash() {
        let perms = HashPermutations::new(20000);
        let mut m1 = HashValues::new([1, 2, 3, 4, 5, 6].iter(), &perms);
        let mut m2 = HashValues::new([4, 3, 2, 1, 5, 6].iter(), &perms);
        assert_eq!(m1.0, m2.0);
        // println!("{:?}", &m1.0);
        // println!("{:?}", &m2.0);
        assert_eq!(m1.jaccard(&m2), 1.0);
        m1.update([7, 8, 9].iter(), &perms);
        assert!(m1.jaccard(&m2) < 1.0);
        println!("{:?}", m1.jaccard(&m2));
        m2.update([17, 18, 19].iter(), &perms);
        assert!(m1.jaccard(&m2) < 1.0);
        println!("{:?}", m1.jaccard(&m2));
        // println!("{:?}", m2.get_byte_chunks(2).collect_vec());
        assert_eq!(perms.0, HashPermutations::from_bytes(perms.as_bytes()).0);
    }
}
