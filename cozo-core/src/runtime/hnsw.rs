/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::expr::{eval_bytecode_pred, Bytecode};
use crate::data::tuple::Tuple;
use crate::data::value::Vector;
use crate::parse::sys::HnswIndexManifest;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;
use crate::DataValue;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

impl<'a> SessionTx<'a> {
    fn hnsw_put_vector(
        &mut self,
        vec: &Vector,
        idx: usize,
        subidx: i32,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
        tags: &[SmartString<LazyCompact>]
    ) -> Result<()> {
        todo!()
    }
    pub(crate) fn hnsw_put(
        &mut self,
        config: &HnswIndexManifest,
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
        for idx in &config.vec_fields {
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
        let mut extracted_tags: Vec<SmartString<LazyCompact>> = vec![];
        for tag_idx in &config.tag_fields {
            let tag_field = tuple.get(*tag_idx).unwrap();
            if let Some(s) = tag_field.get_str() {
                extracted_tags.push(SmartString::from(s));
            } else if let DataValue::List(l) = tag_field {
                for tag in l {
                    if let Some(s) = tag.get_str() {
                        extracted_tags.push(SmartString::from(s));
                    }
                }
            }
        }
        for (vec, idx, sub) in extracted_vectors {
            self.hnsw_put_vector(vec, idx, sub, orig_table, idx_table, &extracted_tags)?;
        }
        Ok(true)
    }
    pub(crate) fn hnsw_remove(
        &mut self,
        config: &HnswIndexManifest,
        orig_table: &RelationHandle,
        idx_table: &RelationHandle,
        tuple: &Tuple,
    ) -> Result<()> {
        todo!()
    }
    pub(crate) fn hnsw_knn(&self, node: u64, k: usize) -> Vec<(u64, f32)> {
        todo!()
    }
}
