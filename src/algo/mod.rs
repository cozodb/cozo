use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;

use crate::algo::bfs::Bfs;
use crate::algo::connected_components::ConnectedComponents;
use crate::algo::degree_centrality::DegreeCentrality;
use crate::algo::dfs::Dfs;
use crate::algo::top_sort::TopSort;
use crate::data::expr::Expr;
use crate::data::id::{EntityId, Validity};
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol, TripleDir};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) mod bfs;
pub(crate) mod connected_components;
pub(crate) mod degree_centrality;
pub(crate) mod dfs;
pub(crate) mod page_rank;
pub(crate) mod top_sort;

pub(crate) trait AlgoImpl {
    fn run(
        &mut self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<Symbol, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct AlgoHandle {
    pub(crate) name: Symbol,
}

impl AlgoHandle {
    pub(crate) fn new(name: &str) -> Self {
        AlgoHandle {
            name: Symbol::from(name),
        }
    }
    pub(crate) fn arity(&self) -> Result<usize> {
        Ok(match &self.name.0 as &str {
            "degree_centrality" => 4,
            "depth_first_search" | "dfs" => 1,
            "breadth_first_search" | "bfs" => 1,
            "top_sort" => 2,
            "connected_components" => 2,
            "strongly_connected_components" | "scc" => 2,
            "page_rank" => todo!(),
            name => bail!("algorithm '{}' not found", name),
        })
    }

    pub(crate) fn get_impl(&self) -> Result<Box<dyn AlgoImpl>> {
        Ok(match &self.name.0 as &str {
            "degree_centrality" => Box::new(DegreeCentrality),
            "depth_first_search" | "dfs" => Box::new(Dfs),
            "breadth_first_search" | "bfs" => Box::new(Bfs),
            "top_sort" => Box::new(TopSort),
            "connected_components" => Box::new(ConnectedComponents::default()),
            "strongly_connected_components" | "scc" => todo!(),
            "page_rank" => todo!(),
            name => bail!("algorithm '{}' not found", name),
        })
    }
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
