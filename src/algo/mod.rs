use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;

use crate::algo::bfs::Bfs;
use crate::algo::degree_centrality::DegreeCentrality;
use crate::algo::dfs::Dfs;
use crate::data::expr::Expr;
use crate::data::id::{EntityId, Validity};
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol, TripleDir};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) mod bfs;
mod degree_centrality;
pub(crate) mod dfs;
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
        "dfs" => Arc::new(Dfs),
        "bfs" => Arc::new(Bfs),
        "page_rank" => todo!(),
        name => bail!("algorithm '{}' not found", name),
    })
}

impl MagicAlgoRuleArg {
    pub(crate) fn prefix_iter<'a>(
        &'a self,
        prefix: &DataValue,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem(s, _) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(store.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Stored(s, _) => {
                let view_rel = tx.get_view_rel(s)?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(view_rel.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Triple(attr, _, dir) => {
                if *dir == TripleDir::Bwd && !attr.val_type.is_ref_type() {
                    ensure!(
                        attr.indexing.should_index(),
                        "reverse scanning of triple values requires indexing: {:?}",
                        attr.name
                    );
                    if attr.with_history {
                        Box::new(
                            tx.triple_av_before_scan(attr.id, prefix, Validity::MAX)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_av_scan(attr.id, prefix)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                } else {
                    let id = prefix.get_int().ok_or_else(|| {
                        anyhow!(
                            "prefix scanning of triple requires integer id, got {:?}",
                            prefix
                        )
                    })?;
                    let id = EntityId(id as u64);
                    match dir {
                        TripleDir::Fwd => {
                            if attr.with_history {
                                Box::new(
                                    tx.triple_ea_before_scan(id, attr.id, Validity::MAX)
                                        .map_ok(|(eid, _, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            } else {
                                Box::new(
                                    tx.triple_ea_scan(id, attr.id)
                                        .map_ok(|(eid, _, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            }
                        }
                        TripleDir::Bwd => {
                            if attr.with_history {
                                Box::new(
                                    tx.triple_vref_a_before_scan(id, attr.id, Validity::MAX)
                                        .map_ok(|(v, _, eid)| {
                                            Tuple(vec![v.as_datavalue(), eid.as_datavalue()])
                                        }),
                                )
                            } else {
                                Box::new(tx.triple_vref_a_scan(id, attr.id).map_ok(
                                    |(v, _, eid)| Tuple(vec![v.as_datavalue(), eid.as_datavalue()]),
                                ))
                            }
                        }
                    }
                }
            }
        })
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem(s, _) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                Box::new(store.scan_all())
            }
            MagicAlgoRuleArg::Stored(s, _) => {
                let view_rel = tx.get_view_rel(s)?;
                Box::new(view_rel.scan_all()?)
            }
            MagicAlgoRuleArg::Triple(attr, _, dir) => match dir {
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
