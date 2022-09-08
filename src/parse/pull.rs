use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;

use crate::data::id::Validity;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::expr::build_expr;
use crate::parse::{ExtractSpan, Pair, Rule, SourceSpan};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct OutPullSpec {
    pub(crate) attr: Symbol,
    pub(crate) reverse: bool,
    pub(crate) subfields: Vec<OutPullSpec>,
    pub(crate) span: SourceSpan
}

pub(crate) fn parse_out_options(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, Option<Validity>, Vec<OutPullSpec>)> {
    let mut src = pair.into_inner();
    let sp = src.next().unwrap();
    let target = Symbol::new(sp.as_str(), sp.extract_span());
    let mut specs = src.next().unwrap();
    let mut at = None;

    if specs.as_rule() == Rule::expr {
        let vld = build_expr(specs, param_pool)?;
        let vld = Validity::try_from(vld)?;
        at = Some(vld);

        specs = src.next().unwrap();
    }

    Ok((
        target,
        at,
        specs.into_inner().map(parse_pull_field).try_collect()?,
    ))
}

fn parse_pull_field(pair: Pair<'_>) -> Result<OutPullSpec> {
    let span = pair.extract_span();
    let mut is_reverse = false;
    let mut src = pair.into_inner();
    let mut name_p = src.next().unwrap();
    if Rule::rev_pull_marker == name_p.as_rule() {
        is_reverse = true;
        name_p = src.next().unwrap();
    }
    let name = Symbol::new(name_p.as_str(), name_p.extract_span());
    let subfields = match src.next() {
        None => vec![],
        Some(p) => p.into_inner().map(parse_pull_field).try_collect()?,
    };
    Ok(OutPullSpec {
        attr: name,
        reverse: is_reverse,
        subfields,
        span
    })
}
