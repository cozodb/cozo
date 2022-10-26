/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::cmp::Reverse;
use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;

use crate::data::program::SortDir;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn sort_and_collect(
        &mut self,
        original: InMemRelation,
        sorters: &[(Symbol, SortDir)],
        head: &[Symbol],
    ) -> Result<InMemRelation> {
        let head_indices: BTreeMap<_, _> = head.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let idx_sorters = sorters
            .iter()
            .map(|(k, dir)| (head_indices[k], *dir))
            .collect_vec();
        let ret = self.new_temp_store(original.rule_name.symbol().span);
        for (idx, tuple) in original.scan_all().enumerate() {
            let tuple = tuple?;
            let mut key = idx_sorters
                .iter()
                .map(|(idx, dir)| {
                    let mut val = tuple.0[*idx].clone();
                    if *dir == SortDir::Dsc {
                        val = DataValue::Rev(Reverse(Box::new(val)));
                    }
                    val
                })
                .collect_vec();
            key.push(DataValue::from(idx as i64));
            let key = Tuple(key);
            ret.put_kv(key, tuple, 0);
        }
        Ok(ret)
    }
}
