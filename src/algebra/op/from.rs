use crate::algebra::op::{TableScan};
use crate::algebra::parser::{assert_rule, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::uuid::random_uuid_v1;
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::{Pair, Pairs, Rule};
use anyhow::Result;
use std::collections::BTreeSet;

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
    let chain = args
        .next()
        .ok_or_else(not_enough_args)?
        .into_inner()
        .next()
        .ok_or_else(not_enough_args)?;
    let mut chain = parse_chain(chain)?.into_iter();
    let mut last_el = chain.next().ok_or_else(not_enough_args)?;
    let mut ret = RaBox::TableScan(Box::new(TableScan::build(ctx, &last_el, true)?));
    for el in chain {
        todo!()
    }
    Ok(ret)
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
    FullOuter,
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
                    (true, true) => JoinType::FullOuter,
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
