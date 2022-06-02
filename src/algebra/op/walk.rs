use crate::algebra::op::{
    build_binding_map_from_info, parse_chain, unique_prefix_nested_loop, ChainPart,
    ChainPartEdgeDir, FilterError, InterpretContext, JoinType, NestLoopLeftPrefixIter, QueryError,
    RelationalAlgebra, SelectOpError, SortDirection, NAME_SKIP, NAME_SORT, NAME_TAKE, NAME_WHERE,
};
use crate::algebra::parser::{AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::parser::parse_scoped_dict;
use crate::data::tuple::{DataKind, OwnTuple, ReifiedTuple, Tuple};
use crate::data::tuple_set::{
    merge_binding_maps, BindingMap, BindingMapEvalContext, TableId, TupleSet, TupleSetEvalContext,
    TupleSetIdx,
};
use crate::data::value::Value;
use crate::ddl::reify::{AssocInfo, EdgeInfo, NodeInfo, TableInfo};
use crate::parser::{Pair, Pairs, Rule};
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::Result;
use cozorocks::{
    DbPtr, IteratorPtr, PrefixIterator, ReadOptionsPtr, RowIterator, TransactionPtr,
    WriteOptionsPtr,
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_WALK: &str = "Walk";

pub(crate) struct WalkOp<'a> {
    ctx: &'a TempDbContext<'a>,
    starting: StartingEl,
    hops: Vec<HoppingEls>,
    extraction_map: BTreeMap<String, Expr>,
    binding: String,
    pivot: TableInfo,
    binding_maps: Vec<BindingMap>,
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
        let mut binding_maps = vec![BindingMap {
            inner_map: BTreeMap::from([(first_el.binding.clone(), bmap_inner)]),
            key_size: 1,
            val_size: 1 + first_el.assocs.len(),
        }];

        let mut starting_el = StartingEl {
            node_info: first_info.into_node()?,
            assocs: first_assocs,
            binding: first_el.binding,
            pivot: false,
            ops: vec![],
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
                    let e_bmap = BindingMap {
                        inner_map: BTreeMap::from([(el.binding.clone(), bmap_inner)]),
                        key_size: 1,
                        val_size: 1 + el.assocs.len(),
                    };

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
                    let n_bmap = BindingMap {
                        inner_map: BTreeMap::from([(el.binding.clone(), bmap_inner)]),
                        key_size: 1,
                        val_size: 1 + el.assocs.len(),
                    };

                    binding_maps.push(merge_binding_maps(
                        [binding_maps.last().unwrap().clone(), e_bmap, n_bmap].into_iter(),
                    ));

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
                        ops: vec![],
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
                    let (binding, ops) = parse_walk_cond(arg)?;
                    let mut found = false;
                    if binding == starting_el.binding {
                        found = true;
                        starting_el.ops.extend(ops);
                        pivots.push(TableInfo::Node(starting_el.node_info.clone()));
                    } else {
                        for hop in hops.iter_mut() {
                            if hop.node_binding == binding || hop.edge_binding == binding {
                                found = true;
                                hop.ops.extend(ops);
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

        let source_map = binding_maps.last().unwrap();
        let binding_ctx = BindingMapEvalContext {
            map: source_map,
            parent: ctx,
        };
        let extraction_map = match collector.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d,
            Expr::Const(Value::Dict(d)) => d
                .into_iter()
                .map(|(k, v)| (k.to_string(), Expr::Const(v.clone())))
                .collect(),
            ex => return Err(SelectOpError::NeedsDict(ex).into()),
        };

        Ok(Self {
            ctx,
            starting: starting_el,
            hops,
            extraction_map,
            binding: bindings.pop().unwrap(),
            pivot: pivots.pop().unwrap(),
            binding_maps,
        })
    }
}

#[derive(Debug)]
struct StartingEl {
    node_info: NodeInfo,
    assocs: Vec<AssocInfo>,
    binding: String,
    pivot: bool,
    ops: Vec<WalkElOp>,
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
    ops: Vec<WalkElOp>,
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
    #[error("Unsupported operation {0} on walk element")]
    UnsupportedWalkOp(String),
    #[error("Wrong argument to walk op")]
    WalkOpWrongArg,
}

impl<'b> RelationalAlgebra for WalkOp<'b> {
    fn name(&self) -> &str {
        NAME_WALK
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        Ok(BindingMap {
            inner_map: BTreeMap::from([(
                self.binding.clone(),
                self.extraction_map
                    .keys()
                    .enumerate()
                    .map(|(i, k)| {
                        (
                            k.to_string(),
                            TupleSetIdx {
                                is_key: false,
                                t_set: 0,
                                col_idx: i,
                            },
                        )
                    })
                    .collect(),
            )]),
            key_size: 0,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let starting_tid = self.starting.node_info.tid;
        let it = if starting_tid.in_root {
            self.ctx.txn.iterator(&self.ctx.sess.r_opts_main)
        } else {
            self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp)
        };
        let key_tuple = OwnTuple::with_prefix(starting_tid.id);
        let it = it.iter_rows(&key_tuple);
        let mut it: Box<dyn Iterator<Item = Result<TupleSet>>> = Box::new(node_iterator(it));

        let first_binding_map = self.binding_maps.first().unwrap();

        let first_binding_ctx = BindingMapEvalContext {
            map: first_binding_map,
            parent: self.ctx,
        };

        let mut met_pivot = self.starting.pivot;

        for op in &self.starting.ops {
            match op {
                WalkElOp::Sort(_) => {
                    // TODO
                }
                WalkElOp::Filter(expr) => {
                    let expr = expr.clone().partial_eval(&first_binding_ctx)?;
                    it = Box::new(filter_iterator(it, expr));
                }
                WalkElOp::Take(n) => it = Box::new(it.take(*n)),
                WalkElOp::Skip(n) => it = Box::new(it.skip(*n)),
            }
        }

        let mut last_node_keys_extractors = self
            .starting
            .node_info
            .keys
            .iter()
            .map(|col| {
                Expr::FieldAcc(
                    col.name.clone(),
                    Expr::Variable(self.starting.binding.clone()).into(),
                )
                .partial_eval(&first_binding_ctx)
            })
            .collect::<Result<Vec<_>>>()?;

        for (hop, binding_map) in self.hops.iter().zip(self.binding_maps.iter().skip(1)) {
            // node to edge hop
            let mut key_extractors = vec![match hop.direction {
                ChainPartEdgeDir::Fwd => Expr::Const(Value::Bool(true)),
                ChainPartEdgeDir::Bwd => Expr::Const(Value::Bool(false)),
            }];
            key_extractors.extend_from_slice(&last_node_keys_extractors);

            let txn = self.ctx.txn.clone();
            let temp_db = self.ctx.sess.temp.clone();
            let w_opts = default_write_options();
            let r_opts = default_read_options();

            let right_iter = if hop.edge_info.tid.in_root {
                txn.iterator(&r_opts)
            } else {
                temp_db.iterator(&r_opts)
            };
            let right_iter = right_iter.iter_prefix(OwnTuple::empty_tuple());
            it = Box::new(NestLoopLeftPrefixIter {
                left_join: met_pivot,
                always_output_padded: false,
                left_iter: it,
                right_iter,
                right_table_id: hop.edge_info.tid,
                key_extractors,
                left_cache: None,
                left_cache_used: false,
                txn,
                temp_db,
                w_opts,
                r_opts,
            });

            // edge to node hop
            let key_prefix = match hop.direction {
                ChainPartEdgeDir::Fwd => "_dst_",
                ChainPartEdgeDir::Bwd => "_src_",
            };

            let binding_ctx = BindingMapEvalContext {
                map: binding_map,
                parent: self.ctx,
            };

            let key_extractors = hop
                .node_info
                .keys
                .iter()
                .map(|col| {
                    Expr::FieldAcc(
                        key_prefix.to_string() + &col.name,
                        Expr::Variable(hop.edge_binding.clone()).into(),
                    )
                    .partial_eval(&binding_ctx)
                })
                .collect::<Result<Vec<_>>>()?;

            last_node_keys_extractors = hop
                .node_info
                .keys
                .iter()
                .map(|col| {
                    Expr::FieldAcc(
                        col.name.clone(),
                        Expr::Variable(hop.node_binding.clone()).into(),
                    )
                    .partial_eval(&binding_ctx)
                })
                .collect::<Result<Vec<_>>>()?;

            let txn = self.ctx.txn.clone();
            let temp_db = self.ctx.sess.temp.clone();
            let w_opts = default_write_options();
            let r_opts = default_read_options();

            it = Box::new(unique_prefix_nested_loop(
                it,
                txn,
                temp_db,
                w_opts,
                r_opts,
                true,
                OwnTuple::with_prefix(hop.node_info.tid.id),
                key_extractors,
                hop.node_info.tid,
            ));

            met_pivot = met_pivot || hop.pivot;

            // todo add filters
        }

        let extraction_vec = self
            .extraction_map
            .values().cloned()
            .collect::<Vec<_>>();

        extraction_vec.iter().for_each(|ex| ex.aggr_reset());

        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let w_opts = default_write_options();

        let iter = it.map(move |tset| -> Result<TupleSet> {
            let tset = tset?;
            let eval_ctx = TupleSetEvalContext {
                tuple_set: &tset,
                txn: &txn,
                temp_db: &temp_db,
                write_options: &w_opts,
            };
            let mut tuple = OwnTuple::with_data_prefix(DataKind::Data);
            for expr in &extraction_vec {
                let value = expr.row_eval(&eval_ctx)?;
                tuple.push_value(&value);
            }
            let mut out = TupleSet::default();
            out.vals.push(tuple.into());
            Ok(out)
        });
        Ok(Box::new(iter))
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

#[derive(Debug)]
enum WalkElOp {
    Sort(Vec<(Expr, SortDirection)>),
    Filter(Expr),
    Take(usize),
    Skip(usize),
}

fn parse_walk_cond(pair: Pair) -> Result<(String, Vec<WalkElOp>)> {
    let mut pairs = pair.into_inner();
    let binding = pairs.next().unwrap().as_str().to_string();
    let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_WALK.to_string());
    let mut ops = vec![];
    for op_expr in pairs.next().unwrap().into_inner() {
        let mut pairs = op_expr.into_inner();
        match pairs.next().unwrap().as_str() {
            NAME_WHERE => {
                let mut exprs = vec![];
                for pair in pairs {
                    let mut arg = pair.into_inner().next().unwrap();
                    let expr = Expr::try_from(arg)?;
                    exprs.push(expr);
                }
                if exprs.is_empty() {
                    return Err(WalkError::WalkOpWrongArg.into());
                } else {
                    ops.push(WalkElOp::Filter(Expr::OpAnd(exprs)))
                }
            }
            NAME_SORT => {
                let mut sorters = vec![];
                for pair in pairs {
                    let mut arg = pair.into_inner().next().unwrap();
                    let mut dir = SortDirection::Asc;
                    if arg.as_rule() == Rule::sort_arg {
                        let mut pairs = arg.into_inner();
                        arg = pairs.next().unwrap();
                        if pairs.next().unwrap().as_rule() == Rule::desc_dir {
                            dir = SortDirection::Dsc
                        }
                    }
                    let expr = Expr::try_from(arg)?;
                    sorters.push((expr, dir));
                }
                if sorters.is_empty() {
                    return Err(WalkError::WalkOpWrongArg.into());
                } else {
                    ops.push(WalkElOp::Sort(sorters))
                }
            }
            NAME_TAKE => {
                let op_arg = pairs
                    .next()
                    .ok_or_else(not_enough_args)?
                    .into_inner()
                    .next()
                    .unwrap();
                let expr = Expr::try_from(op_arg)?;
                let n = match expr {
                    Expr::Const(Value::Int(n)) if n >= 0 => n as usize,
                    _ => return Err(WalkError::WalkOpWrongArg.into()),
                };
                ops.push(WalkElOp::Take(n))
            }
            NAME_SKIP => {
                let op_arg = pairs
                    .next()
                    .ok_or_else(not_enough_args)?
                    .into_inner()
                    .next()
                    .unwrap();
                let expr = Expr::try_from(op_arg)?;
                let n = match expr {
                    Expr::Const(Value::Int(n)) if n >= 0 => n as usize,
                    _ => return Err(WalkError::WalkOpWrongArg.into()),
                };
                ops.push(WalkElOp::Skip(n))
            }
            s => return Err(WalkError::UnsupportedWalkOp(s.to_string()).into()),
        }
    }
    Ok((binding, ops))
}

fn node_iterator(iter: RowIterator) -> impl Iterator<Item = Result<TupleSet>> {
    iter.map(move |(key_slice, val_slice)| -> Result<TupleSet> {
        let tset = TupleSet::from((
            [ReifiedTuple::from(Tuple::new(key_slice))],
            [ReifiedTuple::from(Tuple::new(val_slice))],
        ));
        Ok(tset)
    })
}

fn filter_iterator(
    iter: Box<dyn Iterator<Item = Result<TupleSet>>>,
    filter: Expr,
) -> impl Iterator<Item = Result<TupleSet>> {
    // TODO key extractors and assocs
    iter.filter_map(move |tset| -> Option<Result<TupleSet>> {
        match tset {
            Err(e) => Some(Err(e)),
            Ok(tset) => match filter.row_eval(&tset) {
                Ok(Value::Null) | Ok(Value::Bool(false)) => None,
                Ok(Value::Bool(true)) => Some(Ok(tset)),
                Ok(v) => Some(Err(FilterError::ExpectBoolean(v.into_static()).into())),
                Err(e) => Some(Err(e)),
            },
        }
    })
}

// fn node_edge_hop_iterator(
//     iter: Box<dyn Iterator<Item = Result<TupleSet>>>,
//     bridge_key_extractors: Vec<Expr>,
//     txn: TransactionPtr,
//     temp_db: DbPtr,
//     target_tid: TableId,
// ) -> impl Iterator<Item = Result<TupleSet>> {
//     iter.flat_map(|tset| {
//         match tset {
//             Err(e) => [Err(e)].into_iter(),
//             Ok(tset) => vec![Ok(tset)].into_iter()
//         }
//     })
// }

// fn edge_node_hop_iterator(
//
// )
