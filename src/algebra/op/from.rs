use crate::algebra::op::{
    CartesianJoin, InterpretContext, NestedLoopLeft, RelationalAlgebra, TableScan, WhereFilter,
};
use crate::algebra::parser::{assert_rule, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::{Expr, StaticExpr};
use crate::data::op::{OpAnd, OpEq};
use crate::data::uuid::random_uuid_v1;
use crate::data::value::Value;
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::{Pair, Pairs, Rule};
use anyhow::Result;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

pub(crate) const NAME_FROM: &str = "From";

pub(crate) fn build_from_clause<'a>(
    ctx: &'a TempDbContext<'a>,
    prev: Option<RaBox<'a>>,
    mut args: Pairs,
) -> Result<RaBox<'a>> {
    if !matches!(prev, None) {
        return Err(AlgebraParseError::Unchainable(NAME_FROM.to_string()).into());
    }
    let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_FROM.to_string());
    let arg = args.next().ok_or_else(not_enough_args)?;
    let mut ret = build_chain(ctx, arg)?;

    for arg in args {
        let nxt = build_chain(ctx, arg)?;
        let existing_bindings = ret.bindings()?;
        let new_bindings = nxt.bindings()?;
        if !existing_bindings.is_disjoint(&new_bindings) {
            let mut dups = existing_bindings.intersection(&new_bindings);
            return Err(AlgebraParseError::DuplicateBinding(dups.next().unwrap().clone()).into());
        }
        ret = RaBox::Cartesian(Box::new(CartesianJoin {
            left: ret,
            right: nxt,
        }))
    }

    Ok(ret)
}

pub(crate) fn build_chain<'a>(ctx: &'a TempDbContext<'a>, arg: Pair) -> Result<RaBox<'a>> {
    let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_FROM.to_string());

    let chain = arg.into_inner().next().ok_or_else(not_enough_args)?;

    let chain = parse_chain(chain)?;

    if chain.is_empty() {
        return Err(not_enough_args().into());
    }

    let mut seen_bindings = HashSet::new();
    let first_el = chain.first().unwrap();
    let mut ret = TableScan::build(ctx, first_el, true)?;

    if !seen_bindings.insert(first_el.binding.to_string()) {
        return Err(AlgebraParseError::DuplicateBinding(first_el.binding.to_string()).into());
    }

    if chain.len() == 1 {
        return Ok(ret);
    }

    let mut prev_el = first_el;
    let mut prev_tid = ctx
        .resolve_table(&prev_el.target)
        .ok_or_else(|| AlgebraParseError::TableNotFound(prev_el.target.clone()))?;
    let mut prev_info = ctx.get_table_info(prev_tid)?;

    for cur_el in chain.iter().skip(1) {
        match cur_el.part {
            ChainPart::Node => {
                // Edge to node
                let node_id = ctx
                    .resolve_table(&cur_el.target)
                    .ok_or_else(|| AlgebraParseError::TableNotFound(cur_el.target.clone()))?;
                let table_info = ctx.get_table_info(node_id)?;

                let (prev_dir, prev_join) = match prev_el.part {
                    ChainPart::Node => unreachable!(),
                    ChainPart::Edge { dir, join } => (dir, join),
                };
                let join_key_prefix = match prev_dir {
                    ChainPartEdgeDir::Fwd => "_dst_",
                    ChainPartEdgeDir::Bwd => "_src_",
                    ChainPartEdgeDir::Bidi => todo!(),
                };
                let left_join_keys: Vec<StaticExpr> = table_info
                    .as_node()?
                    .keys
                    .iter()
                    .map(|col| {
                        Expr::FieldAcc(
                            join_key_prefix.to_string() + &col.name,
                            Expr::Variable(prev_el.binding.clone()).into(),
                        )
                    })
                    .collect();

                ret = RaBox::NestedLoopLeft(Box::new(NestedLoopLeft {
                    ctx,
                    left: ret,
                    right: table_info.clone(),
                    right_binding: cur_el.binding.clone(),
                    left_outer_join: false,
                    join_key_extractor: left_join_keys,
                    key_is_prefix: false,
                }));

                prev_info = table_info;
                prev_tid = node_id;
            }
            ChainPart::Edge { dir, join } => {
                // Node to edge join
                let edge_id = ctx
                    .resolve_table(&cur_el.target)
                    .ok_or_else(|| AlgebraParseError::TableNotFound(cur_el.target.clone()))?;
                let table_info = ctx.get_table_info(edge_id)?;
                let mut left_join_keys: Vec<StaticExpr> = vec![Expr::Const(match dir {
                    ChainPartEdgeDir::Fwd => true.into(),
                    ChainPartEdgeDir::Bwd => false.into(),
                    ChainPartEdgeDir::Bidi => {
                        todo!()
                    }
                })];
                for key in prev_info.as_node()?.keys.iter() {
                    left_join_keys.push(Expr::FieldAcc(
                        key.name.to_string(),
                        Expr::Variable(prev_el.binding.clone()).into(),
                    ))
                }
                ret = RaBox::NestedLoopLeft(Box::new(NestedLoopLeft {
                    ctx,
                    left: ret,
                    right: table_info.clone(),
                    right_binding: cur_el.binding.clone(),
                    left_outer_join: match join {
                        JoinType::Inner => false,
                        JoinType::Left => true,
                        JoinType::Right => todo!(),
                    },
                    join_key_extractor: left_join_keys,
                    key_is_prefix: true,
                }));
                prev_info = table_info;
                prev_tid = edge_id;
            }
        }
        prev_el = cur_el;
    }
    Ok(ret)
}

fn build_join_conditions(
    ctx: &TempDbContext,
    node_to_edge: bool,
    is_outer: bool,
    dir: ChainPartEdgeDir,
    node_name: &str,
    node_binding: &str,
    edge_binding: &str,
) -> Result<StaticExpr> {
    let dir_prefix = if node_to_edge {
        match dir {
            ChainPartEdgeDir::Fwd => "_src_",
            ChainPartEdgeDir::Bwd => "_dst_",
            ChainPartEdgeDir::Bidi => todo!(),
        }
    } else {
        match dir {
            ChainPartEdgeDir::Fwd => "_dst_",
            ChainPartEdgeDir::Bwd => "_src_",
            ChainPartEdgeDir::Bidi => todo!(),
        }
    };
    let the_node = ctx.resolve_table(node_name).unwrap();
    let the_node = ctx.get_table_info(the_node).unwrap().into_node().unwrap();

    let conditions = the_node
        .keys
        .into_iter()
        .map(|k| {
            Expr::Apply(
                Arc::new(OpEq),
                vec![
                    Expr::FieldAcc(
                        dir_prefix.to_string() + &k.name,
                        Expr::Variable(edge_binding.to_string()).into(),
                    ),
                    Expr::FieldAcc(k.name, Expr::Variable(node_binding.to_string()).into()),
                ],
            )
        })
        .collect::<Vec<_>>();
    Ok(if conditions.len() == 1 {
        conditions.into_iter().next().unwrap()
    } else {
        Expr::Apply(Arc::new(OpAnd), conditions)
    })
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChainPartEdgeDir {
    Fwd,
    Bwd,
    Bidi,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    // FullOuter,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChainPart {
    Node,
    Edge {
        dir: ChainPartEdgeDir,
        join: JoinType,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ChainEl {
    pub(crate) part: ChainPart,
    pub(crate) binding: String,
    pub(crate) target: String,
    pub(crate) assocs: BTreeSet<String>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum JoinError {
    #[error("Cannot have both left and right join marker in a chain segment")]
    NoFullOuterInChain,
}

pub(crate) fn parse_chain(pair: Pair) -> Result<Vec<ChainEl>> {
    assert_rule(&pair, Rule::chain, NAME_FROM, 0)?;
    let mut collected = vec![];
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::node_part => {
                let (binding, target, assocs) = parse_node_part(pair)?;
                collected.push(ChainEl {
                    part: ChainPart::Node,
                    binding,
                    target,
                    assocs,
                });
            }
            Rule::edge_part => {
                let mut pairs = pair.into_inner();
                let src_marker = pairs.next().unwrap();
                let (is_bwd, src_outer) = parse_edge_marker(src_marker);
                let middle = pairs.next().unwrap();
                let (binding, target, assocs) = parse_node_part(middle)?;
                let dst_marker = pairs.next().unwrap();
                let (is_fwd, dst_outer) = parse_edge_marker(dst_marker);
                let dir = if (is_fwd && is_bwd) || (!is_fwd && !is_bwd) {
                    ChainPartEdgeDir::Bidi
                } else if is_fwd {
                    ChainPartEdgeDir::Fwd
                } else {
                    ChainPartEdgeDir::Bwd
                };
                let join = match (src_outer, dst_outer) {
                    (true, true) => return Err(JoinError::NoFullOuterInChain.into()),
                    (true, false) => JoinType::Right,
                    (false, true) => JoinType::Left,
                    (false, false) => JoinType::Inner,
                };
                collected.push(ChainEl {
                    part: ChainPart::Edge { dir, join },
                    binding,
                    target,
                    assocs,
                });
            }
            _ => unreachable!(),
        }
    }
    Ok(collected)
}

fn parse_node_part(pair: Pair) -> Result<(String, String, BTreeSet<String>)> {
    let mut pairs = pair.into_inner();
    let mut nxt = pairs.next().unwrap();
    let binding = if nxt.as_rule() == Rule::ident {
        let binding = nxt.as_str().to_string();
        nxt = pairs.next().unwrap();
        binding
    } else {
        let mut ret = "@".to_string();
        ret += &random_uuid_v1()?.to_string();
        ret
    };
    let mut pairs = nxt.into_inner();
    let table_name = build_name_in_def(pairs.next().unwrap(), true)?;
    let assoc_names = pairs
        .map(|v| build_name_in_def(v, true))
        .collect::<Result<BTreeSet<_>>>()?;
    Ok((binding, table_name, assoc_names))
}

fn parse_edge_marker(pair: Pair) -> (bool, bool) {
    let mut arrow_mark = false;
    let mut outer_mark = false;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::outer_marker => outer_mark = true,
            Rule::bwd_marker | Rule::fwd_marker => arrow_mark = true,
            _ => unreachable!(),
        }
    }
    (arrow_mark, outer_mark)
}
