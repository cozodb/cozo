use std::collections::BTreeMap;

use miette::Result;

use crate::algo::AlgoImpl;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::stored::StoredRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct DegreeCentrality;

impl AlgoImpl for DegreeCentrality {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, StoredRelation>,
        out: &StoredRelation,
        poison: Poison,
    ) -> Result<()> {
        let it = algo
            .relation_with_min_len(0, 2, tx, stores)?
            .iter(tx, stores)?;
        let mut counter: BTreeMap<DataValue, (usize, usize, usize)> = BTreeMap::new();
        for tuple in it {
            let tuple = tuple?;
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
        if let Ok(nodes) = algo.relation(1) {
            for tuple in nodes.iter(tx, stores)? {
                let tuple = tuple?;
                let id = &tuple.0[0];
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
