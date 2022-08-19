use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use itertools::Itertools;

use crate::algo::degree_centrality::DegreeCentrality;
use crate::data::expr::Expr;
use crate::data::id::Validity;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol, TripleDir};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

mod degree_centrality;
pub(crate) mod page_rank;

pub(crate) trait AlgoImpl {
    fn name(&self) -> Symbol;
    fn arity(&self) -> usize;
    fn run(
        &self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<Symbol, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()>;
}

pub(crate) fn get_algo(name: &str) -> Result<Arc<dyn AlgoImpl>> {
    Ok(match name {
        "degree_centrality" => Arc::new(DegreeCentrality),
        "page_rank" => todo!(),
        name => bail!("algorithm '{}' not found", name),
    })
}

impl MagicAlgoRuleArg {
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem(s) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                Box::new(store.scan_all())
            }
            MagicAlgoRuleArg::Stored(s) => {
                let view_rel = tx.get_view_rel(s)?;
                Box::new(view_rel.scan_all()?)
            }
            MagicAlgoRuleArg::Triple(attr, dir) => match dir {
                TripleDir::Fwd => {
                    if attr.with_history {
                        Box::new(
                            tx.triple_a_before_scan(attr.id, Validity::MAX)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(attr.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    }
                }
                TripleDir::Bwd => {
                    if attr.with_history {
                        Box::new(
                            tx.triple_a_before_scan(attr.id, Validity::MAX)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(attr.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                }
            },
        })
    }
}
