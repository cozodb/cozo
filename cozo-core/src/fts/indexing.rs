/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::expr::{eval_bytecode, eval_bytecode_pred, Bytecode};
use crate::data::program::{FtsScoreKind, FtsSearch};
use crate::data::tuple::{decode_tuple_from_key, Tuple, ENCODED_KEY_MIN_LEN};
use crate::data::value::LARGEST_UTF_CHAR;
use crate::fts::ast::{FtsExpr, FtsLiteral, FtsNear};
use crate::fts::tokenizer::TextAnalyzer;
use crate::parse::fts::parse_fts_query;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;
use crate::{DataValue, SourceSpan};
use itertools::Itertools;
use miette::{bail, miette, Diagnostic, Result};
use ordered_float::OrderedFloat;
use rustc_hash::{FxHashMap, FxHashSet};
use smartstring::{LazyCompact, SmartString};
use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Default)]
pub(crate) struct FtsCache {
    total_n_cache: FxHashMap<SmartString<LazyCompact>, usize>,
}

impl FtsCache {
    fn get_n_for_relation(&mut self, rel: &RelationHandle, tx: &SessionTx<'_>) -> Result<usize> {
        Ok(match self.total_n_cache.entry(rel.name.clone()) {
            Entry::Vacant(v) => {
                let start = rel.encode_partial_key_for_store(&[]);
                let end = rel.encode_partial_key_for_store(&[DataValue::Bot]);
                let val = tx.store_tx.range_count(&start, &end)?;
                v.insert(val);
                val
            }
            Entry::Occupied(o) => *o.get(),
        })
    }
}

struct PositionInfo {
    // from: u32,
    // to: u32,
    position: u32,
}

struct LiteralStats {
    key: Tuple,
    position_info: Vec<PositionInfo>,
    // doc_len: u32,
}

impl<'a> SessionTx<'a> {
    fn fts_search_literal(
        &self,
        literal: &FtsLiteral,
        idx_handle: &RelationHandle,
    ) -> Result<Vec<LiteralStats>> {
        let start_key_str = &literal.value as &str;
        let start_key = vec![DataValue::Str(SmartString::from(start_key_str))];
        let mut end_key_str = literal.value.clone();
        end_key_str.push(LARGEST_UTF_CHAR);
        let end_key = vec![DataValue::Str(end_key_str)];
        let start_key_bytes = idx_handle.encode_partial_key_for_store(&start_key);
        let end_key_bytes = idx_handle.encode_partial_key_for_store(&end_key);
        let mut results = vec![];
        for item in self.store_tx.range_scan(&start_key_bytes, &end_key_bytes) {
            let (kvec, vvec) = item?;
            let key_tuple = decode_tuple_from_key(&kvec, idx_handle.metadata.keys.len());
            let found_str_key = key_tuple[0].get_str().unwrap();
            if literal.is_prefix {
                if !found_str_key.starts_with(start_key_str) {
                    break;
                }
            } else if found_str_key != start_key_str {
                break;
            }

            let vals: Vec<DataValue> = rmp_serde::from_slice(&vvec[ENCODED_KEY_MIN_LEN..]).unwrap();
            let froms = vals[0].get_slice().unwrap();
            let tos = vals[1].get_slice().unwrap();
            let positions = vals[2].get_slice().unwrap();
            // let total_length = vals[3].get_int().unwrap();
            let position_info = froms
                .iter()
                .zip(tos.iter())
                .zip(positions.iter())
                .map(|(_, p)| PositionInfo {
                    // from: f.get_int().unwrap() as u32,
                    // to: t.get_int().unwrap() as u32,
                    position: p.get_int().unwrap() as u32,
                })
                .collect_vec();
            results.push(LiteralStats {
                key: key_tuple[1..].to_vec(),
                position_info,
                // doc_len: total_length as u32,
            });
        }
        Ok(results)
    }
    fn fts_search_impl(
        &self,
        ast: &FtsExpr,
        config: &FtsSearch,
        n: usize,
    ) -> Result<FxHashMap<Tuple, f64>> {
        Ok(match ast {
            FtsExpr::Literal(l) => {
                let mut res = FxHashMap::default();
                let found_docs = self.fts_search_literal(l, &config.idx_handle)?;
                let found_docs_len = found_docs.len();
                for el in found_docs {
                    let score = Self::fts_compute_score(
                        el.position_info.len(),
                        found_docs_len,
                        n,
                        l.booster.0,
                        config,
                    );
                    res.insert(el.key, score);
                }
                res
            }
            FtsExpr::And(ls) => {
                let mut l_iter = ls.iter();
                let mut res = self.fts_search_impl(
                    l_iter.next().unwrap(),
                    config,
                    n,
                )?;
                for nxt in l_iter {
                    let nxt_res = self.fts_search_impl(nxt, config, n)?;
                    res = res
                        .into_iter()
                        .filter_map(|(k, v)| nxt_res.get(&k).map(|nxt_v| (k, v + nxt_v)))
                        .collect();
                }
                res
            }
            FtsExpr::Or(ls) => {
                let mut res: FxHashMap<Tuple, f64> = FxHashMap::default();
                for nxt in ls {
                    let nxt_res = self.fts_search_impl(nxt, config, n)?;
                    for (k, v) in nxt_res {
                        if let Some(old_v) = res.get_mut(&k) {
                            *old_v = (*old_v).max(v);
                        } else {
                            res.insert(k, v);
                        }
                    }
                }
                res
            }
            FtsExpr::Near(FtsNear { literals, distance }) => {
                let mut l_it = literals.iter();
                let mut coll: FxHashMap<_, _> = FxHashMap::default();
                for first_el in self.fts_search_literal(l_it.next().unwrap(), &config.idx_handle)? {
                    coll.insert(
                        first_el.key,
                        first_el
                            .position_info
                            .into_iter()
                            .map(|el| el.position)
                            .collect_vec(),
                    );
                }
                for lit_nxt in literals {
                    let el_res = self.fts_search_literal(lit_nxt, &config.idx_handle)?;
                    coll = el_res
                        .into_iter()
                        .filter_map(|x| match coll.remove(&x.key) {
                            None => None,
                            Some(prev_pos) => {
                                let mut inner_coll = FxHashSet::default();
                                for p in prev_pos {
                                    for pi in x.position_info.iter() {
                                        let cur = pi.position;
                                        if cur > p {
                                            if cur - p <= *distance {
                                                inner_coll.insert(p);
                                            }
                                        } else if p - cur <= *distance {
                                            inner_coll.insert(cur);
                                        }
                                    }
                                }
                                if inner_coll.is_empty() {
                                    None
                                } else {
                                    Some((x.key, inner_coll.into_iter().collect_vec()))
                                }
                            }
                        })
                        .collect();
                }
                let mut booster = 0.0;
                for lit in literals {
                    booster += lit.booster.0;
                }
                let coll_len = coll.len();
                coll.into_iter()
                    .map(|(k, cands)| {
                        (
                            k,
                            Self::fts_compute_score(cands.len(), coll_len, n, booster, config),
                        )
                    })
                    .collect()
            }
            FtsExpr::Not(fst, snd) => {
                let mut res = self.fts_search_impl(fst, config, n)?;
                for el in self
                    .fts_search_impl(snd, config, n)?
                    .keys()
                {
                    res.remove(el);
                }
                res
            }
        })
    }
    fn fts_compute_score(
        tf: usize,
        n_found_docs: usize,
        n_total: usize,
        booster: f64,
        config: &FtsSearch,
    ) -> f64 {
        let tf = tf as f64;
        match config.score_kind {
            FtsScoreKind::Tf => tf * booster,
            FtsScoreKind::TfIdf => {
                let n_found_docs = n_found_docs as f64;
                let idf = (1.0 + (n_total as f64 - n_found_docs + 0.5) / (n_found_docs + 0.5)).ln();
                tf * idf * booster
            }
        }
    }
    pub(crate) fn fts_search(
        &self,
        q: &str,
        config: &FtsSearch,
        filter_code: &Option<(Vec<Bytecode>, SourceSpan)>,
        tokenizer: &TextAnalyzer,
        stack: &mut Vec<DataValue>,
        cache: &mut FtsCache,
    ) -> Result<Vec<Tuple>> {
        let ast = parse_fts_query(q)?.tokenize(tokenizer);
        if ast.is_empty() {
            return Ok(vec![]);
        }
        let n = if config.score_kind == FtsScoreKind::TfIdf {
            cache.get_n_for_relation(&config.base_handle, self)?
        } else {
            0
        };
        let mut result: Vec<_> = self
            .fts_search_impl(&ast, config, n)?
            .into_iter()
            .collect();
        result.sort_by_key(|(_, score)| Reverse(OrderedFloat(*score)));
        if config.filter.is_none() {
            result.truncate(config.k);
        }

        let mut ret = Vec::with_capacity(config.k);
        for (found_key, score) in result {
            let mut cand_tuple = config
                .base_handle
                .get(self, &found_key)?
                .ok_or_else(|| miette!("corrupted index"))?;

            if config.bind_score.is_some() {
                cand_tuple.push(DataValue::from(score));
            }

            if let Some((code, span)) = filter_code {
                if !eval_bytecode_pred(code, &cand_tuple, stack, *span)? {
                    continue;
                }
            }

            ret.push(cand_tuple);
            if ret.len() >= config.k {
                break;
            }
        }
        Ok(ret)
    }
    pub(crate) fn put_fts_index_item(
        &mut self,
        tuple: &[DataValue],
        extractor: &[Bytecode],
        stack: &mut Vec<DataValue>,
        tokenizer: &TextAnalyzer,
        rel_handle: &RelationHandle,
        idx_handle: &RelationHandle,
    ) -> Result<()> {
        let to_index = match eval_bytecode(extractor, tuple, stack)? {
            DataValue::Null => return Ok(()),
            DataValue::Str(s) => s,
            val => {
                #[derive(Debug, Diagnostic, Error)]
                #[error("FTS index extractor must return a string, got {0}")]
                #[diagnostic(code(eval::fts::extractor::invalid_return_type))]
                struct FtsExtractError(String);

                bail!(FtsExtractError(format!("{}", val)))
            }
        };
        let mut token_stream = tokenizer.token_stream(&to_index);
        let mut collector: HashMap<_, (Vec<_>, Vec<_>, Vec<_>), _> = FxHashMap::default();
        let mut count = 0i64;
        while let Some(token) = token_stream.next() {
            let text = SmartString::<LazyCompact>::from(&token.text);
            let (fr, to, position) = collector.entry(text).or_default();
            fr.push(DataValue::from(token.offset_from as i64));
            to.push(DataValue::from(token.offset_to as i64));
            position.push(DataValue::from(token.position as i64));
            count += 1;
        }
        let mut key = Vec::with_capacity(1 + rel_handle.metadata.keys.len());
        key.push(DataValue::Bot);
        for k in &tuple[..rel_handle.metadata.keys.len()] {
            key.push(k.clone());
        }
        let mut val = vec![
            DataValue::Bot,
            DataValue::Bot,
            DataValue::Bot,
            DataValue::from(count),
        ];
        for (text, (from, to, position)) in collector {
            key[0] = DataValue::Str(text);
            val[0] = DataValue::List(from);
            val[1] = DataValue::List(to);
            val[2] = DataValue::List(position);
            let key_bytes = idx_handle.encode_key_for_store(&key, Default::default())?;
            let val_bytes = idx_handle.encode_val_only_for_store(&val, Default::default())?;
            self.store_tx.put(&key_bytes, &val_bytes)?;
        }
        Ok(())
    }
    pub(crate) fn del_fts_index_item(
        &mut self,
        tuple: &[DataValue],
        extractor: &[Bytecode],
        stack: &mut Vec<DataValue>,
        tokenizer: &TextAnalyzer,
        rel_handle: &RelationHandle,
        idx_handle: &RelationHandle,
    ) -> Result<()> {
        let to_index = match eval_bytecode(extractor, tuple, stack)? {
            DataValue::Null => return Ok(()),
            DataValue::Str(s) => s,
            val => {
                #[derive(Debug, Diagnostic, Error)]
                #[error("FTS index extractor must return a string, got {0}")]
                #[diagnostic(code(eval::fts::extractor::invalid_return_type))]
                struct FtsExtractError(String);

                bail!(FtsExtractError(format!("{}", val)))
            }
        };
        let mut token_stream = tokenizer.token_stream(&to_index);
        let mut collector = FxHashSet::default();
        while let Some(token) = token_stream.next() {
            let text = SmartString::<LazyCompact>::from(&token.text);
            collector.insert(text);
        }
        let mut key = Vec::with_capacity(1 + rel_handle.metadata.keys.len());
        key.push(DataValue::Bot);
        for k in &tuple[..rel_handle.metadata.keys.len()] {
            key.push(k.clone());
        }
        for text in collector {
            key[0] = DataValue::Str(text);
            let key_bytes = idx_handle.encode_key_for_store(&key, Default::default())?;
            self.store_tx.del(&key_bytes)?;
        }
        Ok(())
    }
}
