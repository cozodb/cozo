use std::collections::BTreeMap;

use miette::{miette, ensure};
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct DegreeCentrality;

impl AlgoImpl for DegreeCentrality {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        _opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> miette::Result<()> {
        let it = rels
            .get(0)
            .ok_or_else(|| miette!(
                "'degree_centrality' requires at least an edge relation to proceed"
            ))?
            .iter(tx, stores)?;
        let mut counter: BTreeMap<DataValue, (usize, usize, usize)> = BTreeMap::new();
        for tuple in it {
            let tuple = tuple?;
            ensure!(
                tuple.0.len() >= 2,
                "'degree_centrality' requires input relation to be a tuple of two elements"
            );
            let from = tuple.0[0].clone();
            let (from_total, from_out, _) = counter.entry(from).or_default();
            *from_total += 1;
            *from_out += 1;

            let to = tuple.0[1].clone();
            let (to_total, _, to_in) = counter.entry(to).or_default();
            *to_total += 1;
            *to_in += 1;
            poison.check()?;
        }
        if let Some(nodes) = rels.get(1) {
            for tuple in nodes.iter(tx, stores)? {
                let tuple = tuple?;
                let id = tuple
                    .0
                    .get(0)
                    .ok_or_else(|| miette!("nodes relation to 'degree_centrality' too short"))?;
                if !counter.contains_key(id) {
                    counter.insert(id.clone(), (0, 0, 0));
                }
                poison.check()?;
            }
        }
        for (k, (total_d, out_d, in_d)) in counter.into_iter() {
            let tuple = Tuple(vec![
                k,
                DataValue::from(total_d as i64),
                DataValue::from(out_d as i64),
                DataValue::from(in_d as i64),
            ]);
            out.put(tuple, 0);
            poison.check()?;
        }
        Ok(())
    }
}
