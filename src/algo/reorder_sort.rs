use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::{Expr, OP_LIST};
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct ReorderSort;

impl AlgoImpl for ReorderSort {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let in_rel = rels
            .get(0)
            .ok_or_else(|| anyhow!("'reorder_sort' requires an input relation"))?;

        let mut out_list = match opts
            .get("out")
            .ok_or_else(|| anyhow!("'reorder_sort' requires the option 'out'"))?
        {
            Expr::Const(DataValue::List(l)) => {
                l.iter().map(|d| Expr::Const(d.clone())).collect_vec()
            }
            Expr::Apply(op, args) if **op == OP_LIST => args.to_vec(),
            v => {
                bail!("option 'out' of 'reorder_sort' must be a list, got {:?}", v)
            }
        };

        let mut sort_by = opts
            .get("sort_by")
            .cloned()
            .unwrap_or(Expr::Const(DataValue::Null));
        let sort_descending = match opts.get("descending") {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'descending' of 'reorder_sort' must be a bool, got {:?}",
                v
            ),
        };
        let break_ties = match opts.get("break_ties") {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'break_ties' of 'reorder_sort' must be a bool, got {:?}",
                v
            ),
        };
        let skip = match opts.get("skip") {
            None => 0,
            Some(Expr::Const(v)) => v.get_int().ok_or_else(|| {
                anyhow!(
                    "option 'skip' of 'reorder_sort' must be an integer, got {:?}",
                    v
                )
            })?,
            Some(v) => bail!(
                "option 'skip' of 'reorder_sort' must be an integer, got {:?}",
                v
            ),
        };
        ensure!(
            skip >= 0,
            "option 'skip' of 'reorder_sort' must be non-negative, got {}",
            skip
        );
        let take = match opts.get("take") {
            None => i64::MAX,
            Some(Expr::Const(v)) => v.get_int().ok_or_else(|| {
                anyhow!(
                    "option 'take' of 'reorder_sort' must be an integer, got {:?}",
                    v
                )
            })?,
            Some(v) => bail!(
                "option 'take' of 'reorder_sort' must be an integer, got {:?}",
                v
            ),
        };
        ensure!(
            take >= 0,
            "option 'take' of 'reorder_sort' must be non-negative, got {}",
            take
        );

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
        }
        if sort_descending {
            buffer.sort_by(|l, r| r.last().cmp(&l.last()));
        } else {
            buffer.sort_by(|l, r| l.last().cmp(&r.last()));
        }

        let mut count = 0usize;
        let mut rank = 0usize;
        let mut last = &DataValue::Bottom;
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
            out.put(Tuple(out_t), 0);
        }
        Ok(())
    }
}
