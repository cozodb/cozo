/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::expr::{eval_bytecode_pred, Bytecode};
use crate::data::relation::VecElementType;
use crate::data::tuple::{decode_tuple_from_key, Tuple};
use crate::data::value::Vector;
use crate::parse::sys::HnswDistance;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;
use crate::{decode_tuple_from_kv, DataValue, Symbol};
use miette::{bail, Result};
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use rand::Rng;
use smartstring::{LazyCompact, SmartString};
use std::cmp::{max, Reverse};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct HnswIndexManifest {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) vec_dim: usize,
    pub(crate) dtype: VecElementType,
    pub(crate) vec_fields: Vec<usize>,
    pub(crate) distance: HnswDistance,
    pub(crate) ef_construction: usize,
    pub(crate) m_neighbours: usize,
    pub(crate) m_max: usize,
    pub(crate) m_max0: usize,
    pub(crate) level_multiplier: f64,
    pub(crate) index_filter: Option<String>,
    pub(crate) extend_candidates: bool,
    pub(crate) keep_pruned_connections: bool,
}

pub(crate) struct HnswKnnQueryOptions {
    k: usize,
    ef: usize,
    max_distance: f64,
    min_margin: f64,
    auto_margin_factor: Option<f64>,
    bind_field: Option<Symbol>,
    bind_distance: Option<Symbol>,
    bind_vector: Option<Symbol>,
}

impl HnswIndexManifest {
    fn get_random_level(&self) -> i64 {
        let mut rng = rand::thread_rng();
        let uniform_num: f64 = rng.gen_range(0.0..1.0);
        let r = -uniform_num.ln() * self.level_multiplier;
        // the level is the largest integer smaller than r
        -(r.floor() as i64)
    }
    fn get_vector(&self, tuple: &Tuple, idx: usize, sub_idx: i32) -> Result<Vector> {
        let field = tuple.get(idx).unwrap();
        if sub_idx >= 0 {
            match field {
                DataValue::List(l) => match l.get(sub_idx as usize) {
                    Some(DataValue::Vec(v)) => Ok(v.clone()),
                    _ => bail!(
                        "Cannot extract vector from {} for sub index {}",
                        field,
                        sub_idx
                    ),
                },
                _ => bail!("Cannot interpret {} as list", field),
            }
        } else {
            match field {
                DataValue::Vec(v) => Ok(v.clone()),
                _ => bail!("Cannot interpret {} as vector", field),
            }
        }
    }
    fn get_distance(&self, q: &Vector, tuple: &Tuple, idx: usize, sub_idx: i32) -> Result<f64> {
        let field = tuple.get(idx).unwrap();
        let target = if sub_idx >= 0 {
            match field {
                DataValue::List(l) => match l.get(sub_idx as usize) {
                    Some(DataValue::Vec(v)) => v,
                    _ => bail!(
                        "Cannot extract vector from {} for sub index {}",
                        field,
                        sub_idx
                    ),
                },
                _ => bail!("Cannot interpret {} as list", field),
            }
        } else {
            match field {
                DataValue::Vec(v) => v,
                _ => bail!("Cannot interpret {} as vector", field),
            }
        };
        Ok(match self.distance {
            HnswDistance::L2 => match (q, target) {
                (Vector::F32(a), Vector::F32(b)) => {
                    let diff = a - b;
                    diff.dot(&diff) as f64
                }
                (Vector::F64(a), Vector::F64(b)) => {
                    let diff = a - b;
                    diff.dot(&diff)
                }
                _ => bail!(
                    "Cannot compute L2 distance between {:?} and {:?}",
                    q,
                    target
                ),
            },
            HnswDistance::Cosine => match (q, target) {
                (Vector::F32(a), Vector::F32(b)) => {
                    let a_norm = a.dot(a) as f64;
                    let b_norm = b.dot(b) as f64;
                    let dot = a.dot(b) as f64;
                    1.0 - dot / (a_norm * b_norm).sqrt()
                }
                (Vector::F64(a), Vector::F64(b)) => {
                    let a_norm = a.dot(a) as f64;
                    let b_norm = b.dot(b) as f64;
                    let dot = a.dot(b);
                    1.0 - dot / (a_norm * b_norm).sqrt()
                }
                _ => bail!(
                    "Cannot compute cosine distance between {:?} and {:?}",
                    q,
                    target
                ),
            },
            HnswDistance::InnerProduct => match (q, target) {
                (Vector::F32(a), Vector::F32(b)) => {
                    let dot = a.dot(b);
                    1. - dot as f64
                }
                (Vector::F64(a), Vector::F64(b)) => {
                    let dot = a.dot(b);
                    1. - dot as f64
                }
                _ => bail!(
                    "Cannot compute inner product between {:?} and {:?}",
                    q,
                    target
                ),
            },
        })
    }
}

impl<'a> SessionTx<'a> {
    fn hnsw_put_vector(
        &mut self,
        tuple: &Tuple,
        q: &Vector,
        idx: usize,
        subidx: i32,
        manifest: &HnswIndexManifest,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
    ) -> Result<()> {
        let start_tuple =
            idx_table.encode_key_for_store(&vec![DataValue::from(i64::MIN)], Default::default())?;
        let end_tuple =
            idx_table.encode_key_for_store(&vec![DataValue::from(1)], Default::default())?;
        let ep_res = self.store_tx.range_scan(&start_tuple, &end_tuple).next();
        if let Some(ep) = ep_res {
            let (ep_key_bytes, _) = ep?;
            let ep_key_tuple = decode_tuple_from_key(&ep_key_bytes);
            // bottom level since we are going up
            let bottom_level = ep_key_tuple[0].get_int().unwrap();
            let ep_key = ep_key_tuple[1..orig_table.metadata.keys.len() + 1].to_vec();
            let ep_idx = ep_key_tuple[orig_table.metadata.keys.len() + 1]
                .get_int()
                .unwrap() as usize;
            let ep_subidx = ep_key_tuple[orig_table.metadata.keys.len() + 2]
                .get_int()
                .unwrap() as i32;
            let ep_distance =
                self.hnsw_compare_vector(q, &ep_key, idx, subidx, manifest, orig_table)?;
            let mut found_nn = PriorityQueue::new();
            found_nn.push((ep_key, ep_idx, ep_subidx), OrderedFloat(ep_distance));
            let target_level = manifest.get_random_level();
            if target_level < bottom_level {
                // this becomes the entry point
                self.hnsw_put_fresh_at_levels(
                    tuple,
                    idx,
                    subidx,
                    orig_table,
                    idx_table,
                    target_level,
                    bottom_level - 1,
                )?;
            }
            for current_level in bottom_level..target_level {
                self.hnsw_search_level(
                    q,
                    1,
                    current_level,
                    manifest,
                    orig_table,
                    idx_table,
                    &mut found_nn,
                )?;
            }
            for current_level in max(target_level, bottom_level)..=0 {
                self.hnsw_search_level(
                    q,
                    manifest.ef_construction,
                    current_level,
                    manifest,
                    orig_table,
                    idx_table,
                    &mut found_nn,
                )?;
                // add bidirectional links to the nearest neighbors
                todo!();
                // shrink links if necessary
                todo!();
            }
        } else {
            // This is the first vector in the index.
            let level = manifest.get_random_level();
            self.hnsw_put_fresh_at_levels(tuple, idx, subidx, orig_table, idx_table, level, 0)?;
        }
        Ok(())
    }
    fn hnsw_compare_vector(
        &self,
        q: &Vector,
        target_key: &[DataValue],
        target_idx: usize,
        target_subidx: i32,
        manifest: &HnswIndexManifest,
        orig_table: &RelationHandle,
    ) -> Result<f64> {
        let target_key_bytes = orig_table.encode_key_for_store(target_key, Default::default())?;
        let bytes = match self.store_tx.get(&target_key_bytes, false)? {
            Some(bytes) => bytes,
            None => bail!("Indexed data not found, this signifies a bug in the index."),
        };
        let target_tuple = decode_tuple_from_kv(&target_key_bytes, &bytes);
        manifest.get_distance(q, &target_tuple, target_idx, target_subidx)
    }
    fn hnsw_select_neighbours_heuristic(&self) -> Result<()> {
        todo!()
    }
    fn hnsw_search_level(
        &self,
        q: &Vector,
        ef: usize,
        cur_level: i64,
        manifest: &HnswIndexManifest,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
        found_nn: &mut PriorityQueue<(Tuple, usize, i32), OrderedFloat<f64>>,
    ) -> Result<()> {
        let mut visited: BTreeSet<(Tuple, usize, i32)> = BTreeSet::new();
        let mut candidates: PriorityQueue<(Tuple, usize, i32), Reverse<OrderedFloat<f64>>> =
            PriorityQueue::new();

        for item in found_nn.iter() {
            visited.insert(item.0.clone());
            candidates.push(item.0.clone(), Reverse(*item.1));
        }

        while let Some((candidate, Reverse(OrderedFloat(candidate_dist)))) = candidates.pop() {
            let (_, OrderedFloat(furtherest_dist)) = found_nn.peek().unwrap();
            let furtherest_dist = *furtherest_dist;
            if candidate_dist > furtherest_dist {
                break;
            }
            // loop over each of the candidate's neighbors
            for neighbour_triple in self.hnsw_get_neighbours(
                candidate.0,
                candidate.1,
                candidate.2,
                cur_level,
                idx_table,
            )? {
                if visited.contains(&neighbour_triple) {
                    continue;
                }
                let neighbour_dist = self.hnsw_compare_vector(
                    q,
                    &neighbour_triple.0,
                    neighbour_triple.1,
                    neighbour_triple.2,
                    manifest,
                    orig_table,
                )?;
                let (_, OrderedFloat(cand_furtherest_dist)) = found_nn.peek().unwrap();
                if found_nn.len() < ef || neighbour_dist < *cand_furtherest_dist {
                    candidates.push(
                        neighbour_triple.clone(),
                        Reverse(OrderedFloat(neighbour_dist)),
                    );
                    found_nn.push(neighbour_triple.clone(), OrderedFloat(neighbour_dist));
                    if found_nn.len() > ef {
                        found_nn.pop();
                    }
                }
                visited.insert(neighbour_triple);
            }
        }

        Ok(())
    }
    fn hnsw_get_neighbours<'b>(
        &'b self,
        cand_key: Vec<DataValue>,
        cand_idx: usize,
        cand_sub_idx: i32,
        level: i64,
        idx_handle: &RelationHandle,
    ) -> Result<impl Iterator<Item = (Tuple, usize, i32)> + 'b> {
        let mut start_tuple = Vec::with_capacity(cand_key.len() + 3);
        start_tuple.push(DataValue::from(level));
        start_tuple.extend_from_slice(&cand_key);
        start_tuple.push(DataValue::from(cand_idx as i64));
        start_tuple.push(DataValue::from(cand_sub_idx as i64));
        let mut end_tuple = start_tuple.clone();
        end_tuple.push(DataValue::Bot);
        let start_bytes = idx_handle.encode_key_for_store(&start_tuple, Default::default())?;
        let end_bytes = idx_handle.encode_key_for_store(&end_tuple, Default::default())?;
        Ok(self
            .store_tx
            .range_scan(&start_bytes, &end_bytes)
            .filter_map(move |res| {
                let (key, _value) = res.unwrap();
                let key_tuple = decode_tuple_from_key(&key);
                let key_total_len = key_tuple.len();
                let key_idx = key_tuple[key_total_len - 2].get_int().unwrap() as usize;
                let key_subidx = key_tuple[key_total_len - 1].get_int().unwrap() as i32;
                let key_slice = key_tuple[cand_key.len() + 3..key_total_len - 2].to_vec();
                if key_slice == cand_key {
                    None
                } else {
                    Some((key_slice, key_idx, key_subidx))
                }
            }))
    }
    fn hnsw_put_fresh_at_levels(
        &mut self,
        tuple: &Tuple,
        idx: usize,
        subidx: i32,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
        bottom_level: i64,
        top_level: i64,
    ) -> Result<()> {
        let mut target_key = vec![DataValue::Null];
        let mut canary_key = vec![DataValue::from(1)];
        for _ in 0..2 {
            for i in 0..orig_table.metadata.keys.len() {
                target_key.push(tuple.get(i).unwrap().clone());
                canary_key.push(DataValue::Null);
            }
            target_key.push(DataValue::from(idx as i64));
            target_key.push(DataValue::from(subidx as i64));
            canary_key.push(DataValue::Null);
            canary_key.push(DataValue::Null);
        }
        let target_value = [DataValue::from(0.0), DataValue::Null];
        let target_key_bytes = idx_table.encode_key_for_store(&target_key, Default::default())?;

        // canary value is for conflict detection: prevent the scenario of disconnected graphs at all levels
        let canary_value = [
            DataValue::from(bottom_level),
            DataValue::Bytes(target_key_bytes),
        ];
        let canary_key_bytes = idx_table.encode_key_for_store(&canary_key, Default::default())?;
        let canary_value_bytes =
            idx_table.encode_val_for_store(&canary_value, Default::default())?;
        self.store_tx.put(&canary_key_bytes, &canary_value_bytes)?;

        for cur_level in bottom_level..=top_level {
            target_key[0] = DataValue::from(cur_level);
            let key = idx_table.encode_key_for_store(&target_key, Default::default())?;
            let val = idx_table.encode_val_for_store(&target_value, Default::default())?;
            self.store_tx.put(&key, &val)?;
        }
        Ok(())
    }
    pub(crate) fn hnsw_put(
        &'a mut self,
        manifest: &HnswIndexManifest,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
        filter: Option<(&[Bytecode], &mut Vec<DataValue>)>,
        tuple: &Tuple,
    ) -> Result<bool> {
        if let Some((code, stack)) = filter {
            if !eval_bytecode_pred(code, tuple, stack, Default::default())? {
                return Ok(false);
            }
        }
        let mut extracted_vectors = vec![];
        for idx in &manifest.vec_fields {
            let val = tuple.get(*idx).unwrap();
            if let DataValue::Vec(v) = val {
                extracted_vectors.push((v, *idx, -1 as i32));
            } else if let DataValue::List(l) = val {
                for (sidx, v) in l.iter().enumerate() {
                    if let DataValue::Vec(v) = v {
                        extracted_vectors.push((v, *idx, sidx as i32));
                    }
                }
            }
        }
        if extracted_vectors.is_empty() {
            return Ok(false);
        }
        for (vec, idx, sub) in extracted_vectors {
            self.hnsw_put_vector(&tuple, vec, idx, sub, manifest, orig_table, idx_table)?;
        }
        Ok(true)
    }
    pub(crate) fn hnsw_remove(&mut self) -> Result<()> {
        todo!()
    }
    pub(crate) fn hnsw_knn(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::collections::BTreeMap;

    #[test]
    fn test_random_level() {
        let m = 20;
        let mult = 1. / (m as f64).ln();
        let mut rng = rand::thread_rng();
        let mut collected = BTreeMap::new();
        for _ in 0..10000 {
            let uniform_num: f64 = rng.gen_range(0.0..1.0);
            let r = -uniform_num.ln() * mult;
            // the level is the largest integer smaller than r
            let level = -(r.floor() as i64);
            collected.entry(level).and_modify(|x| *x += 1).or_insert(1);
        }
        println!("{:?}", collected);
    }
}
