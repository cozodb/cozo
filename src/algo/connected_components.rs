use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

#[derive(Default)]
struct ConnectedComponents {
    union_find: BTreeMap<DataValue, DataValue>,
}

impl ConnectedComponents {
    pub(crate) fn find(&mut self, data: &DataValue) -> DataValue {
        if !self.union_find.contains_key(data) {
            self.union_find.insert(data.clone(), data.clone());
            data.clone()
        } else {
            let mut root = data;
            while let Some(new_found) = self.union_find.get(root) {
                root = new_found;
            }

            let root = root.clone();

            let mut current = data.clone();
            while current != root {
                let found = self.union_find.get_mut(&current).unwrap();
                let next = found.clone();
                *found = root.clone();
                current = next;
            }

            root
        }
    }
}

impl AlgoImpl for ConnectedComponents {
    fn run(
        &mut self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<Symbol, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'connected_components' missing edges relation"))?;

        let reverse_mode = match opts.get(&Symbol::from("mode")) {
            None => false,
            Some(Expr::Const(DataValue::String(s))) => match s as &str {
                "group_first" => true,
                "key_first" => false,
                v => bail!("unexpected option 'mode' for 'connected_components': {}", v),
            },
            Some(v) => bail!(
                "unexpected option 'mode' for 'connected_components': {:?}",
                v
            ),
        };

        for tuple in edges.iter(tx, stores)? {
            let mut tuple = tuple?.0.into_iter();
            let from = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation for 'connected_components' too short"))?;
            let to = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation for 'connected_components' too short"))?;

            let to_root = self.find(&to);
            let from_root = self.find(&from);
            if to_root != from_root {
                let from_target = self.union_find.get_mut(&from_root).unwrap();
                *from_target = to_root
            }
        }

        let mut counter = 0i64;
        let mut seen: BTreeMap<&DataValue, i64> = Default::default();

        for (k, grp) in self.union_find.iter() {
            let grp_id = if let Some(grp_id) = seen.get(grp) {
                *grp_id
            } else {
                let old = counter;
                seen.insert(grp, old);
                counter += 1;
                old
            };
            let tuple = if reverse_mode {
                Tuple(vec![DataValue::from(grp_id), k.clone()])
            } else {
                Tuple(vec![k.clone(), DataValue::from(grp_id)])
            };
            out.put(tuple, 0);
        }

        if let Some(nodes) = rels.get(1) {
            for tuple in nodes.iter(tx, stores)? {
                let tuple = tuple?;
                let node = tuple.0.into_iter().next().ok_or_else(|| {
                    anyhow!("nodes relation for 'connected_components' too short")
                })?;
                if !seen.contains_key(&node) {
                    let tuple = if reverse_mode {
                        Tuple(vec![DataValue::from(counter), node])
                    } else {
                        Tuple(vec![node, DataValue::from(counter)])
                    };
                    out.put(tuple, 0);
                    counter += 1;
                }
            }
        }
        Ok(())
    }
}
