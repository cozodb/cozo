use std::cmp::min;
use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct StronglyConnectedComponent {
    strong: bool,
}

impl StronglyConnectedComponent {
    pub(crate) fn new(strong: bool) -> Self {
        Self { strong }
    }
}

impl AlgoImpl for StronglyConnectedComponent {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'strongly_connected_components' missing edges relation"))?;

        let reverse_mode = match opts.get("mode") {
            None => false,
            Some(Expr::Const(DataValue::Str(s))) => match s as &str {
                "group_first" => true,
                "key_first" => false,
                v => bail!(
                    "unexpected option 'mode' for 'strongly_connected_components': {}",
                    v
                ),
            },
            Some(v) => bail!(
                "unexpected option 'mode' for 'strongly_connected_components': {:?}",
                v
            ),
        };

        let (graph, indices, mut inv_indices) =
            edges.convert_edge_to_graph(!self.strong, tx, stores)?;

        let tarjan = TarjanScc::new(&graph).run(poison)?;
        for (grp_id, cc) in tarjan.iter().enumerate() {
            for idx in cc {
                let val = indices.get(*idx).unwrap();
                let tuple = if reverse_mode {
                    Tuple(vec![DataValue::from(grp_id as i64), val.clone()])
                } else {
                    Tuple(vec![val.clone(), DataValue::from(grp_id as i64)])
                };
                out.put(tuple, 0);
            }
        }

        let mut counter = tarjan.len() as i64;

        if let Some(nodes) = rels.get(1) {
            for tuple in nodes.iter(tx, stores)? {
                let tuple = tuple?;
                let node = tuple.0.into_iter().next().ok_or_else(|| {
                    anyhow!("nodes relation for 'strongly_connected_components' too short")
                })?;
                if !inv_indices.contains_key(&node) {
                    inv_indices.insert(node.clone(), usize::MAX);
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

pub(crate) struct TarjanScc<'a> {
    graph: &'a [Vec<usize>],
    id: usize,
    ids: Vec<Option<usize>>,
    low: Vec<usize>,
    on_stack: Vec<bool>,
    stack: Vec<usize>,
}

impl<'a> TarjanScc<'a> {
    pub(crate) fn new(graph: &'a [Vec<usize>]) -> Self {
        Self {
            graph,
            id: 0,
            ids: vec![None; graph.len()],
            low: vec![0; graph.len()],
            on_stack: vec![false; graph.len()],
            stack: vec![],
        }
    }
    pub(crate) fn run(mut self, poison: Poison) -> Result<Vec<Vec<usize>>> {
        for i in 0..self.graph.len() {
            if self.ids[i].is_none() {
                self.dfs(i);
                poison.check()?;
            }
        }

        let mut low_map: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (idx, grp) in self.low.into_iter().enumerate() {
            low_map.entry(grp).or_default().push(idx);
        }

        Ok(low_map.into_iter().map(|(_, vs)| vs).collect_vec())
    }
    fn dfs(&mut self, at: usize) {
        self.stack.push(at);
        self.on_stack[at] = true;
        self.id += 1;
        self.ids[at] = Some(self.id);
        self.low[at] = self.id;
        for to in &self.graph[at] {
            let to = *to;
            if self.ids[to].is_none() {
                self.dfs(to);
            }
            if self.on_stack[to] {
                self.low[at] = min(self.low[at], self.low[to]);
            }
        }
        if self.ids[at].unwrap() == self.low[at] {
            while let Some(node) = self.stack.pop() {
                self.on_stack[node] = false;
                self.low[node] = self.ids[at].unwrap();
                if node == at {
                    break;
                }
            }
        }
    }
}
