use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{bail, ensure, miette, Result};

use crate::algo::{get_bool_option_required, AlgoImpl};
use crate::data::expr::Expr;
use crate::data::functions::OP_LIST;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct ReorderSort;

impl AlgoImpl for ReorderSort {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let rels = &algo.rule_args;
        let opts = &algo.options;
        let in_rel = rels
            .get(0)
            .ok_or_else(|| miette!("'reorder_sort' requires an input relation"))?;

        let mut out_list = match opts
            .get("out")
            .ok_or_else(|| miette!("'reorder_sort' requires the option 'out'"))?
        {
            Expr::Const {
                val: DataValue::List(l), span
            } => l
                .iter()
                .map(|d| Expr::Const { val: d.clone(), span: *span })
                .collect_vec(),
            Expr::Apply { op, args, .. } if **op == OP_LIST => args.to_vec(),
            v => {
                bail!("option 'out' of 'reorder_sort' must be a list, got {:?}", v)
            }
        };

        let mut sort_by = opts.get("sort_by").cloned().unwrap_or(Expr::Const {
            val: DataValue::Null,
            span: SourceSpan(0, 0)
        });
        let sort_descending =
            get_bool_option_required("descending", opts, Some(false), "reorder_sort")?;
        let break_ties = get_bool_option_required("break_ties", opts, Some(false), "reorder_sort")?;
        let skip = match opts.get("skip") {
            None => 0,
            Some(Expr::Const { val, .. }) => val.get_int().ok_or_else(|| {
                miette!(
                    "option 'skip' of 'reorder_sort' must be an integer, got {:?}",
                    val
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
            Some(Expr::Const { val, .. }) => val.get_int().ok_or_else(|| {
                miette!(
                    "option 'take' of 'reorder_sort' must be an integer, got {:?}",
                    val
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
            out.put(Tuple(out_t), 0);
            poison.check()?;
        }
        Ok(())
    }
}
