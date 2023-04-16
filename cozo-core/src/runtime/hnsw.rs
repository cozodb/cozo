/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::expr::{eval_bytecode_pred, Bytecode};
use crate::data::relation::VecElementType;
use crate::data::tuple::{Tuple, ENCODED_KEY_MIN_LEN};
use crate::data::value::Vector;
use crate::parse::sys::HnswDistance;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;
use crate::{decode_tuple_from_kv, DataValue, Symbol};
use miette::{bail, Result};
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use rand::Rng;
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
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
        // TODO check if this is an update!

        let tuple_key = &tuple[..orig_table.metadata.keys.len()];

        let ep_res = idx_table
            .scan_bounded_prefix(
                self,
                &[],
                &[DataValue::from(i64::MIN)],
                &[DataValue::from(1)],
            )
            .next();
        if let Some(ep) = ep_res {
            let ep = ep?;
            // bottom level since we are going up
            let bottom_level = ep[0].get_int().unwrap();
            let ep_key = ep[1..orig_table.metadata.keys.len() + 1].to_vec();
            let ep_idx = ep[orig_table.metadata.keys.len() + 1].get_int().unwrap() as usize;
            let ep_subidx = ep[orig_table.metadata.keys.len() + 2].get_int().unwrap() as i32;
            let ep_distance =
                self.hnsw_compare_vector(q, &ep_key, idx, subidx, manifest, orig_table)?;
            let mut found_nn = PriorityQueue::new();
            found_nn.push((ep_key, ep_idx, ep_subidx), OrderedFloat(ep_distance));
            let target_level = manifest.get_random_level();
            if target_level < bottom_level {
                // this becomes the entry point
                self.hnsw_put_fresh_at_levels(
                    tuple_key,
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
            let mut self_tuple_key = Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
            self_tuple_key.push(DataValue::from(0));
            for _ in 0..2 {
                self_tuple_key.extend_from_slice(tuple_key);
                self_tuple_key.push(DataValue::from(idx as i64));
                self_tuple_key.push(DataValue::from(subidx as i64));
            }
            let mut self_tuple_val = vec![
                DataValue::from(0.0),
                DataValue::Null,
                DataValue::from(false),
            ];
            for current_level in max(target_level, bottom_level)..=0 {
                let m_max = if current_level == 0 {
                    manifest.m_max0
                } else {
                    manifest.m_max
                };
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
                let neighbours = self.hnsw_select_neighbours_heuristic(
                    q,
                    &found_nn,
                    m_max,
                    current_level,
                    manifest,
                    idx_table,
                    orig_table,
                )?;
                // add self-link
                self_tuple_key[0] = DataValue::from(current_level);
                self_tuple_val[0] = DataValue::from(neighbours.len() as f64);

                // save hash in self-loops
                let mut hasher = Sha256::new();
                for (_, Reverse(OrderedFloat(dist))) in neighbours.iter() {
                    let dist_bs = dist.to_be_bytes();
                    Digest::update(&mut hasher, &dist_bs);
                }
                let hash = hasher.finalize_fixed();
                self_tuple_val[1] = DataValue::Bytes(hash.to_vec());

                let self_tuple_key_bytes =
                    idx_table.encode_key_for_store(&self_tuple_key, Default::default())?;
                let self_tuple_val_bytes =
                    idx_table.encode_val_only_for_store(&self_tuple_val, Default::default())?;
                self.store_tx
                    .put(&self_tuple_key_bytes, &self_tuple_val_bytes)?;

                // add bidirectional links
                for (neighbour, Reverse(OrderedFloat(dist))) in neighbours.iter() {
                    let mut out_key = Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
                    let out_val = vec![
                        DataValue::from(*dist),
                        DataValue::Null,
                        DataValue::from(false),
                    ];
                    out_key.push(DataValue::from(current_level));
                    out_key.extend_from_slice(tuple_key);
                    out_key.push(DataValue::from(idx as i64));
                    out_key.push(DataValue::from(subidx as i64));
                    out_key.extend_from_slice(&neighbour.0);
                    out_key.push(DataValue::from(neighbour.1 as i64));
                    out_key.push(DataValue::from(neighbour.2 as i64));
                    let out_key_bytes =
                        idx_table.encode_key_for_store(&out_key, Default::default())?;
                    let out_val_bytes =
                        idx_table.encode_val_only_for_store(&out_val, Default::default())?;
                    // println!("tuple: {:?}", tuple_key);
                    // println!("out_key: {:?}", out_key);
                    self.store_tx.put(&out_key_bytes, &out_val_bytes)?;

                    let mut in_key = Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
                    let in_val = vec![
                        DataValue::from(*dist),
                        DataValue::Null,
                        DataValue::from(false),
                    ];
                    in_key.push(DataValue::from(current_level));
                    in_key.extend_from_slice(&neighbour.0);
                    in_key.push(DataValue::from(neighbour.1 as i64));
                    in_key.push(DataValue::from(neighbour.2 as i64));
                    in_key.extend_from_slice(tuple_key);
                    in_key.push(DataValue::from(idx as i64));
                    in_key.push(DataValue::from(subidx as i64));
                    // println!("in_key: {:?}", in_key);
                    let in_key_bytes =
                        idx_table.encode_key_for_store(&in_key, Default::default())?;
                    let in_val_bytes =
                        idx_table.encode_val_only_for_store(&in_val, Default::default())?;
                    self.store_tx.put(&in_key_bytes, &in_val_bytes)?;

                    // shrink links if necessary
                    let mut target_self_key =
                        Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
                    target_self_key.push(DataValue::from(current_level));
                    for _ in 0..2 {
                        target_self_key.extend_from_slice(&neighbour.0);
                        target_self_key.push(DataValue::from(neighbour.1 as i64));
                        target_self_key.push(DataValue::from(neighbour.2 as i64));
                    }
                    let target_self_key_bytes =
                        idx_table.encode_key_for_store(&target_self_key, Default::default())?;
                    let target_self_val_bytes = match self.store_tx.get(&target_self_key_bytes, false)? {
                        Some(bytes) => bytes,
                        None => bail!("Indexed vector not found, this signifies a bug in the index implementation"),
                    };
                    let target_self_val: Vec<DataValue> =
                        rmp_serde::from_slice(&target_self_val_bytes[ENCODED_KEY_MIN_LEN..])
                            .unwrap();
                    let target_degree = target_self_val[0].get_float().unwrap() as usize;
                    if target_degree > m_max {
                        // shrink links
                        self.hnsw_shrink_neighbour(
                            &neighbour.0,
                            neighbour.1,
                            neighbour.2,
                            m_max,
                            current_level,
                            manifest,
                            idx_table,
                            orig_table,
                        )?;
                    }
                }
            }
        } else {
            // This is the first vector in the index.
            let level = manifest.get_random_level();
            self.hnsw_put_fresh_at_levels(tuple_key, idx, subidx, orig_table, idx_table, level, 0)?;
        }
        Ok(())
    }
    fn hnsw_shrink_neighbour(
        &mut self,
        target: &[DataValue],
        idx: usize,
        sub_idx: i32,
        m: usize,
        level: i64,
        manifest: &HnswIndexManifest,
        idx_table: &RelationHandle,
        orig_table: &RelationHandle,
    ) -> Result<()> {
        let orig_key = orig_table.encode_key_for_store(target, Default::default())?;
        let orig_val = match self.store_tx.get(&orig_key, false)? {
            Some(bytes) => bytes,
            None => {
                bail!("Indexed vector not found, this signifies a bug in the index implementation")
            }
        };
        let orig_tuple = decode_tuple_from_kv(&orig_key, &orig_val);
        let vec = manifest.get_vector(&orig_tuple, idx, sub_idx)?;
        let mut candidates = PriorityQueue::new();
        for neighbour in
            self.hnsw_get_neighbours(target.to_vec(), idx, sub_idx, level, idx_table)?
        {
            candidates.push(
                (neighbour.0, neighbour.1, neighbour.2),
                OrderedFloat(neighbour.3),
            );
        }
        let new_candidates = self.hnsw_select_neighbours_heuristic(
            &vec,
            &candidates,
            m,
            level,
            manifest,
            idx_table,
            orig_table,
        )?;
        let mut old_candidate_set = BTreeSet::new();
        for (old, _) in &candidates {
            old_candidate_set.insert(old.clone());
        }
        let mut new_candidate_set = BTreeSet::new();
        for (new, _) in &new_candidates {
            new_candidate_set.insert(new.clone());
        }
        for (new, Reverse(OrderedFloat(new_dist))) in new_candidates {
            if !old_candidate_set.contains(&new) {
                let mut new_key = Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
                let new_val = vec![
                    DataValue::from(new_dist),
                    DataValue::Null,
                    DataValue::from(false),
                ];
                new_key.push(DataValue::from(level));
                new_key.extend_from_slice(target);
                new_key.push(DataValue::from(idx as i64));
                new_key.push(DataValue::from(sub_idx as i64));
                new_key.extend_from_slice(&new.0);
                new_key.push(DataValue::from(new.1 as i64));
                new_key.push(DataValue::from(new.2 as i64));
                let new_key_bytes = idx_table.encode_key_for_store(&new_key, Default::default())?;
                let new_val_bytes =
                    idx_table.encode_val_only_for_store(&new_val, Default::default())?;
                self.store_tx.put(&new_key_bytes, &new_val_bytes)?;
            }
        }
        for (old, OrderedFloat(old_dist)) in candidates {
            if !new_candidate_set.contains(&old) {
                let mut old_key = Vec::with_capacity(orig_table.metadata.keys.len() * 2 + 5);
                old_key.push(DataValue::from(level));
                old_key.extend_from_slice(target);
                old_key.push(DataValue::from(idx as i64));
                old_key.push(DataValue::from(sub_idx as i64));
                old_key.extend_from_slice(&old.0);
                old_key.push(DataValue::from(old.1 as i64));
                old_key.push(DataValue::from(old.2 as i64));
                let old_key_bytes = idx_table.encode_key_for_store(&old_key, Default::default())?;
                let old_existing_val = match self.store_tx.get(&old_key_bytes, false)? {
                    Some(bytes) => bytes,
                    None => {
                        bail!("Indexed vector not found, this signifies a bug in the index implementation")
                    }
                };
                let old_existing_val: Vec<DataValue> =
                    rmp_serde::from_slice(&old_existing_val[ENCODED_KEY_MIN_LEN..]).unwrap();
                if old_existing_val[2].get_bool().unwrap() {
                    self.store_tx.del(&old_key_bytes)?;
                } else {
                    let old_val = vec![
                        DataValue::from(old_dist),
                        DataValue::Null,
                        DataValue::from(true),
                    ];
                    let old_val_bytes =
                        idx_table.encode_val_only_for_store(&old_val, Default::default())?;
                    self.store_tx.put(&old_key_bytes, &old_val_bytes)?;
                }
            }
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
    fn hnsw_select_neighbours_heuristic(
        &self,
        q: &Vector,
        found: &PriorityQueue<(Tuple, usize, i32), OrderedFloat<f64>>,
        m: usize,
        level: i64,
        manifest: &HnswIndexManifest,
        idx_table: &RelationHandle,
        orig_table: &RelationHandle,
    ) -> Result<PriorityQueue<(Tuple, usize, i32), Reverse<OrderedFloat<f64>>>> {
        let mut candidates = PriorityQueue::new();
        let mut ret: PriorityQueue<_, Reverse<OrderedFloat<_>>> = PriorityQueue::new();
        let mut discarded: PriorityQueue<_, Reverse<OrderedFloat<_>>> = PriorityQueue::new();
        for (item, dist) in found.iter() {
            // Add to candidates
            candidates.push(item.clone(), Reverse(*dist));
        }
        if manifest.extend_candidates {
            for (item, _) in found.iter() {
                // Extend by neighbours
                for neighbour in
                    self.hnsw_get_neighbours(item.0.clone(), item.1, item.2, level, idx_table)?
                {
                    let dist = self.hnsw_compare_vector(
                        q,
                        &neighbour.0,
                        neighbour.1,
                        neighbour.2,
                        manifest,
                        orig_table,
                    )?;
                    candidates.push(
                        (neighbour.0, neighbour.1, neighbour.2),
                        Reverse(OrderedFloat(dist)),
                    );
                }
            }
        }
        while !candidates.is_empty() && ret.len() < m {
            let (nearest_triple, Reverse(OrderedFloat(nearest_dist))) = candidates.pop().unwrap();
            match ret.peek() {
                Some((_, Reverse(OrderedFloat(dist)))) => {
                    if nearest_dist < *dist {
                        ret.push(nearest_triple, Reverse(OrderedFloat(nearest_dist)));
                    } else if manifest.keep_pruned_connections {
                        discarded.push(nearest_triple, Reverse(OrderedFloat(nearest_dist)));
                    }
                }
                None => {
                    ret.push(nearest_triple, Reverse(OrderedFloat(nearest_dist)));
                }
            }
        }
        if manifest.keep_pruned_connections {
            while !discarded.is_empty() && ret.len() < m {
                let (nearest_triple, Reverse(OrderedFloat(nearest_dist))) =
                    discarded.pop().unwrap();
                ret.push(nearest_triple, Reverse(OrderedFloat(nearest_dist)));
            }
        }
        Ok(ret)
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
            for neighbour_tetra in self.hnsw_get_neighbours(
                candidate.0,
                candidate.1,
                candidate.2,
                cur_level,
                idx_table,
            )? {
                let neighbour_triple = (neighbour_tetra.0, neighbour_tetra.1, neighbour_tetra.2);
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
    ) -> Result<impl Iterator<Item = (Tuple, usize, i32, f64)> + 'b> {
        let mut start_tuple = Vec::with_capacity(cand_key.len() + 3);
        start_tuple.push(DataValue::from(level));
        start_tuple.extend_from_slice(&cand_key);
        start_tuple.push(DataValue::from(cand_idx as i64));
        start_tuple.push(DataValue::from(cand_sub_idx as i64));
        let key_len = cand_key.len();
        Ok(idx_handle
            .scan_prefix(self, &start_tuple)
            .filter_map(move |res| {
                let tuple = res.unwrap();
                // println!("tuple: {:?}", tuple);
                // println!("key_len: {}", key_len);
                let key_idx = tuple[2 * key_len + 3].get_int().unwrap() as usize;
                let key_subidx = tuple[2 * key_len + 4].get_int().unwrap() as i32;
                let key_slice = tuple[key_len + 3..2 * key_len + 3].to_vec();
                if key_slice == cand_key {
                    None
                } else {
                    let is_deleted = tuple[2 * key_len + 7].get_bool().unwrap();
                    if is_deleted {
                        None
                    } else {
                        Some((
                            key_slice,
                            key_idx,
                            key_subidx,
                            tuple[2 * key_len + 5].get_float().unwrap(),
                        ))
                    }
                }
            }))
    }
    fn hnsw_put_fresh_at_levels(
        &mut self,
        tuple: &[DataValue],
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
        let target_value = [
            DataValue::from(0.0),
            DataValue::Null,
            DataValue::from(false),
        ];
        let target_key_bytes = idx_table.encode_key_for_store(&target_key, Default::default())?;

        // canary value is for conflict detection: prevent the scenario of disconnected graphs at all levels
        let canary_value = [
            DataValue::from(bottom_level),
            DataValue::Bytes(target_key_bytes),
            DataValue::from(false),
        ];
        let canary_key_bytes = idx_table.encode_key_for_store(&canary_key, Default::default())?;
        let canary_value_bytes =
            idx_table.encode_val_only_for_store(&canary_value, Default::default())?;
        self.store_tx.put(&canary_key_bytes, &canary_value_bytes)?;

        for cur_level in bottom_level..=top_level {
            target_key[0] = DataValue::from(cur_level);
            let key = idx_table.encode_key_for_store(&target_key, Default::default())?;
            let val = idx_table.encode_val_only_for_store(&target_value, Default::default())?;
            self.store_tx.put(&key, &val)?;
        }
        Ok(())
    }
    pub(crate) fn hnsw_put(
        &mut self,
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
