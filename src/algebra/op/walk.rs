use crate::algebra::op::{
    build_binding_map_from_info, parse_chain, ChainPart, ChainPartEdgeDir, InterpretContext,
    JoinType, RelationalAlgebra, SortDirection,
};
use crate::algebra::parser::{AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::parser::parse_scoped_dict;
use crate::data::tuple_set::{merge_binding_maps, BindingMap, TupleSet};
use crate::ddl::reify::{AssocInfo, EdgeInfo, NodeInfo, TableInfo};
use crate::parser::{Pair, Pairs, Rule};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_WALK: &str = "Walk";

pub(crate) struct WalkOp<'a> {
    ctx: &'a TempDbContext<'a>,
    starting: StartingEl,
    hops: Vec<HoppingEls>,
    collector: Expr,
    binding: String,
    pivot: TableInfo,
    binding_map: BindingMap,
}

impl<'a> WalkOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        if !matches!(prev, None) {
            return Err(AlgebraParseError::Unchainable(NAME_WALK.to_string()).into());
        }
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_WALK.to_string());
        let arg = args.next().ok_or_else(not_enough_args)?;

        let chain = arg.into_inner().next().ok_or_else(not_enough_args)?;

        let chain = parse_chain(chain)?;

        if chain.is_empty() {
            return Err(not_enough_args().into());
        }

        if chain.first().unwrap().part != ChainPart::Node
            || chain.last().unwrap().part != ChainPart::Node
        {
            return Err(WalkError::Chain.into());
        }

        // check no dup binding

        let mut bindings: BTreeSet<&str> = BTreeSet::new();
        for el in &chain {
            if !bindings.insert(&el.binding) {
                return Err(AlgebraParseError::DuplicateBinding(el.binding.to_string()).into());
            }
        }

        // check the chain connects, and get the table infos

        let mut chain = chain.into_iter();
        let first_el = chain.next().unwrap();

        let (first_info, first_assocs) =
            get_chain_el_info(ctx, &first_el.target, &first_el.assocs)?;

        let bmap_inner = build_binding_map_from_info(ctx, &first_info, &first_assocs, true)?;
        let mut binding_map = BindingMap {
            inner_map: BTreeMap::from([(first_el.binding.clone(), bmap_inner)]),
            key_size: 1,
            val_size: 1 + first_el.assocs.len(),
        };

        let mut starting_el = StartingEl {
            node_info: first_info.into_node()?,
            assocs: first_assocs,
            binding: first_el.binding,
            pivot: false,
            filters: vec![],
        };

        let mut last_node_tid = starting_el.node_info.tid;
        let mut hops = vec![];

        loop {
            match chain.next() {
                None => break,
                Some(el) => {
                    let (edge_info, edge_assocs) = get_chain_el_info(ctx, &el.target, &el.assocs)?;

                    let bmap_inner =
                        build_binding_map_from_info(ctx, &edge_info, &edge_assocs, true)?;
                    let bmap = BindingMap {
                        inner_map: BTreeMap::from([(el.binding.clone(), bmap_inner)]),
                        key_size: 1,
                        val_size: 1 + el.assocs.len(),
                    };
                    binding_map = merge_binding_maps([binding_map, bmap].into_iter());

                    let edge_info = edge_info.into_edge()?;
                    let edge_binding = el.binding;
                    let direction = match el.part {
                        ChainPart::Edge { dir, join } => {
                            if join != JoinType::Inner {
                                return Err(WalkError::OuterJoin.into());
                            }
                            dir
                        }
                        _ => unreachable!(),
                    };
                    let el = chain.next().unwrap();
                    let (node_info, node_assocs) = get_chain_el_info(ctx, &el.target, &el.assocs)?;

                    let bmap_inner =
                        build_binding_map_from_info(ctx, &node_info, &node_assocs, true)?;
                    let bmap = BindingMap {
                        inner_map: BTreeMap::from([(el.binding.clone(), bmap_inner)]),
                        key_size: 1,
                        val_size: 1 + el.assocs.len(),
                    };

                    binding_map = merge_binding_maps([binding_map, bmap].into_iter());

                    let node_info = node_info.into_node()?;
                    let node_binding = el.binding;

                    match direction {
                        ChainPartEdgeDir::Fwd => {
                            if edge_info.src_id != last_node_tid
                                || edge_info.dst_id != node_info.tid
                            {
                                return Err(WalkError::Disconnect.into());
                            }
                        }
                        ChainPartEdgeDir::Bwd => {
                            if edge_info.dst_id != last_node_tid
                                || edge_info.src_id != node_info.tid
                            {
                                return Err(WalkError::Disconnect.into());
                            }
                        }
                    }

                    last_node_tid = node_info.tid;

                    let hop = HoppingEls {
                        node_info,
                        node_assocs,
                        node_binding,
                        edge_info,
                        edge_assocs,
                        edge_binding,
                        direction,
                        pivot: false,
                        filters: vec![],
                        sorters: vec![],
                    };
                    hops.push(hop);
                }
            }
        }

        let mut collectors = vec![];
        let mut bindings = vec![];
        let mut pivots = vec![];

        for arg in args {
            let arg = arg.into_inner().next().unwrap();
            match arg.as_rule() {
                Rule::walk_cond => {
                    let (binding, filters, sorters) = parse_walk_cond(arg)?;
                    let mut found = false;
                    if binding == starting_el.binding {
                        found = true;
                        if !sorters.is_empty() {
                            return Err(WalkError::SorterOnStart.into());
                        }
                        starting_el.filters.extend(filters);
                        pivots.push(TableInfo::Node(starting_el.node_info.clone()));
                    } else {
                        for hop in hops.iter_mut() {
                            if hop.node_binding == binding || hop.edge_binding == binding {
                                found = true;
                                hop.sorters.extend(sorters);
                                hop.filters.extend(filters);
                                break;
                            }
                        }
                    }
                    if !found {
                        return Err(WalkError::UnboundFilter.into());
                    }
                }
                Rule::scoped_dict => {
                    let (binding, keys, vals) = parse_scoped_dict(arg)?;
                    if !keys.is_empty() {
                        return Err(WalkError::Keyed.into());
                    }
                    let mut found = false;
                    if binding == starting_el.binding {
                        found = true;
                        starting_el.pivot = true;
                    } else {
                        for hop in hops.iter_mut() {
                            if hop.node_binding == binding || hop.edge_binding == binding {
                                if hop.node_binding == binding {
                                    pivots.push(TableInfo::Node(hop.node_info.clone()));
                                } else {
                                    pivots.push(TableInfo::Edge(hop.edge_info.clone()));
                                }
                                hop.pivot = true;
                                found = true;
                                break;
                            }
                        }
                    }
                    if !found {
                        return Err(WalkError::UnboundCollection.into());
                    } else {
                        collectors.push(vals);
                        bindings.push(binding);
                    }
                }
                _ => unreachable!(),
            }
        }

        if collectors.len() != 1 {
            return Err(WalkError::CollectorNumberMismatch.into());
        }

        let collector = collectors.pop().unwrap();

        Ok(Self {
            ctx,
            starting: starting_el,
            hops,
            collector,
            binding: bindings.pop().unwrap(),
            pivot: pivots.pop().unwrap(),
            binding_map,
        })
    }
}

#[derive(Debug)]
struct StartingEl {
    node_info: NodeInfo,
    assocs: Vec<AssocInfo>,
    binding: String,
    pivot: bool,
    filters: Vec<Expr>,
}

#[derive(Debug)]
struct HoppingEls {
    node_info: NodeInfo,
    node_assocs: Vec<AssocInfo>,
    node_binding: String,
    edge_info: EdgeInfo,
    edge_assocs: Vec<AssocInfo>,
    edge_binding: String,
    direction: ChainPartEdgeDir,
    pivot: bool,
    filters: Vec<Expr>,
    sorters: Vec<(Expr, SortDirection)>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WalkError {
    #[error("Walk chain must start and end with nodes, not edges")]
    Chain,
    #[error("Outer join not allowed in Walk")]
    OuterJoin,
    #[error("Walk chain does not connect")]
    Disconnect,
    #[error("Keyed collection not allowed")]
    Keyed,
    #[error("Unbound collection")]
    UnboundCollection,
    #[error("Unbound filter")]
    UnboundFilter,
    #[error("No/multiple collectors")]
    CollectorNumberMismatch,
    #[error("Starting el cannot have sorters")]
    SorterOnStart,
}

impl<'b> RelationalAlgebra for WalkOp<'b> {
    fn name(&self) -> &str {
        NAME_WALK
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        Ok(self.binding_map.clone())
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

fn get_chain_el_info(
    ctx: &TempDbContext,
    name: &str,
    assoc_names: &BTreeSet<String>,
) -> Result<(TableInfo, Vec<AssocInfo>)> {
    let tid = ctx
        .resolve_table(name)
        .ok_or_else(|| AlgebraParseError::TableNotFound(name.to_string()))?;
    let table = ctx.get_table_info(tid)?;
    let assocs = assoc_names
        .iter()
        .map(|a_name| -> Result<AssocInfo> {
            let a_tid = ctx
                .resolve_table(a_name)
                .ok_or_else(|| AlgebraParseError::TableNotFound(a_name.to_string()))?;
            let a_table = ctx.get_table_info(a_tid)?.into_assoc()?;
            if a_table.src_id != tid {
                Err(AlgebraParseError::NoAssociation(a_name.to_string(), name.to_string()).into())
            } else {
                Ok(a_table)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((table, assocs))
}

fn parse_walk_cond(pair: Pair) -> Result<(String, Vec<Expr>, Vec<(Expr, SortDirection)>)> {
    let mut pairs = pair.into_inner();
    let binding = pairs.next().unwrap().as_str().to_string();
    let mut conds = vec![];
    let mut ordering = vec![];
    for pair in pairs {
        match pair.as_rule() {
            Rule::expr => {
                conds.push(Expr::try_from(pair)?);
            }
            Rule::sort_arg => {
                let mut pairs = pair.into_inner();
                let expr = Expr::try_from(pairs.next().unwrap())?;
                let dir = match pairs.next().unwrap().as_rule() {
                    Rule::asc_dir => SortDirection::Asc,
                    Rule::desc_dir => SortDirection::Dsc,
                    _ => unreachable!(),
                };
                ordering.push((expr, dir))
            }
            _ => unreachable!(),
        }
    }
    Ok((binding, conds, ordering))
}
