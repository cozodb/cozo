/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::cmp::Ordering;
use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;

use crate::data::program::SortDir;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn sort_and_collect(
        &mut self,
        original: InMemRelation,
        sorters: &[(Symbol, SortDir)],
        head: &[Symbol],
    ) -> Result<Vec<Tuple>> {
        let head_indices: BTreeMap<_, _> = head.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let idx_sorters = sorters
            .iter()
            .map(|(k, dir)| (head_indices[k], *dir))
            .collect_vec();

        let mut all_data: Vec<_> = original.scan_all().try_collect()?;
        all_data.sort_by(|a, b| {
            for (idx, dir) in &idx_sorters {
                match a.0[*idx].cmp(&b.0[*idx]) {
                    Ordering::Equal => {}
                    o => {
                        return match dir {
                            SortDir::Asc => o,
                            SortDir::Dsc => o.reverse(),
                        }
                    }
                }
            }
            Ordering::Equal
        });

        Ok(all_data)
    }
}
