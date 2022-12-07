/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{bail, Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::{AlgoImpl, CannotDetermineArity};
use crate::data::expr::Expr;
use crate::data::functions::OP_LIST;
use crate::data::program::{MagicAlgoApply, MagicSymbol, WrongAlgoOptionError};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, NormalTempStore};
use crate::runtime::transact::SessionTx;

pub(crate) struct ReorderSort;

impl AlgoImpl for ReorderSort {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx<'_>,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
        out: &'a mut NormalTempStore,
        poison: Poison,
    ) -> Result<()> {
        let in_rel = algo.relation(0)?;

        let mut out_list = match algo.expr_option("out", None)? {
            Expr::Const {
                val: DataValue::List(l),
                span,
            } => l
                .iter()
                .map(|d| Expr::Const {
                    val: d.clone(),
                    span,
                })
                .collect_vec(),
            Expr::Apply { op, args, .. } if *op == OP_LIST => args.to_vec(),
            _ => {
                bail!(WrongAlgoOptionError {
                    name: "out".to_string(),
                    span: algo.span,
                    algo_name: algo.algo.name.to_string(),
                    help: "This option must evaluate to a list".to_string()
                })
            }
        };

        let mut sort_by = algo.expr_option(
            "sort_by",
            Some(Expr::Const {
                val: DataValue::Null,
                span: SourceSpan(0, 0),
            }),
        )?;
        let sort_descending = algo.bool_option("descending", Some(false))?;
        let break_ties = algo.bool_option("break_ties", Some(false))?;
        let skip = algo.non_neg_integer_option("skip", Some(0))?;
        let take = algo.non_neg_integer_option("take", Some(0))?;

        let binding_map = in_rel.get_binding_map(0);
        sort_by.fill_binding_indices(&binding_map)?;
        for out in out_list.iter_mut() {
            out.fill_binding_indices(&binding_map)?;
        }

        let mut buffer = vec![];
        for tuple in in_rel.iter(tx, stores)? {
            let tuple = tuple?;
            let sorter = sort_by.eval(&tuple)?;
            let mut s_tuple: Vec<_> = out_list.iter().map(|ex| ex.eval(&tuple)).try_collect()?;
            s_tuple.push(sorter);
            buffer.push(s_tuple);
            poison.check()?;
        }
        if sort_descending {
            buffer.sort_by(|l, r| r.last().cmp(&l.last()));
        } else {
            buffer.sort_by(|l, r| l.last().cmp(&r.last()));
        }

        let mut count = 0usize;
        let mut rank = 0usize;
        let mut last = &DataValue::Bot;
        let skip = skip as usize;
        let take_plus_skip = (take as usize).saturating_add(skip);
        for val in &buffer {
            let sorter = val.last().unwrap();

            if sorter == last {
                count += 1;
            } else {
                count += 1;
                rank = count;
                last = sorter;
            }

            if count > take_plus_skip {
                break;
            }

            if count <= skip {
                continue;
            }
            let mut out_t = vec![DataValue::from(if break_ties { count } else { rank } as i64)];
            out_t.extend_from_slice(&val[0..val.len() - 1]);
            out.put(out_t);
            poison.check()?;
        }
        Ok(())
    }

    fn arity(
        &self,
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize> {
        let out_opts = opts.get("out").ok_or_else(|| {
            CannotDetermineArity(
                "ReorderSort".to_string(),
                "option 'out' not provided".to_string(),
                span,
            )
        })?;
        Ok(match out_opts {
            Expr::Const {
                val: DataValue::List(l),
                ..
            } => l.len() + 1,
            Expr::Apply { op, args, .. } if **op == OP_LIST => args.len() + 1,
            _ => bail!(CannotDetermineArity(
                "ReorderSort".to_string(),
                "invalid option 'out' given, expect a list".to_string(),
                span
            )),
        })
    }
}
