/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct DegreeCentrality;

impl FixedRule for DegreeCentrality {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let it = payload.get_input(0)?.ensure_min_len(2)?.iter()?;
        let mut counter: BTreeMap<DataValue, (usize, usize, usize)> = BTreeMap::new();
        for tuple in it {
            let tuple = tuple?;
            let from = tuple[0].clone();
            let (from_total, from_out, _) = counter.entry(from).or_default();
            *from_total += 1;
            *from_out += 1;

            let to = tuple[1].clone();
            let (to_total, _, to_in) = counter.entry(to).or_default();
            *to_total += 1;
            *to_in += 1;
            poison.check()?;
        }
        if let Ok(nodes) = payload.get_input(1) {
            for tuple in nodes.iter()? {
                let tuple = tuple?;
                let id = &tuple[0];
                if !counter.contains_key(id) {
                    counter.insert(id.clone(), (0, 0, 0));
                }
                poison.check()?;
            }
        }
        for (k, (total_d, out_d, in_d)) in counter.into_iter() {
            let tuple = vec![
                k,
                DataValue::from(total_d as i64),
                DataValue::from(out_d as i64),
                DataValue::from(in_d as i64),
            ];
            out.put(tuple);
        }
        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(4)
    }
}
