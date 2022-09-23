use itertools::Itertools;
use miette::{Diagnostic, ensure, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::relation::{ColType, ColumnDef, NullableColType, StoredRelationMetadata};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::{ExtractSpan, Pair, Rule, SourceSpan};
use crate::parse::expr::build_expr;

pub(crate) fn build_schema(pair: Pair<'_>) -> Result<(StoredRelationMetadata, Vec<Option<Symbol>>)> {
    assert_eq!(pair.as_rule(), Rule::table_schema);

    let mut src = pair.into_inner();
    let mut keys = vec![];
    let mut dependents = vec![];
    let mut idents = vec![];
    for p in src.next().unwrap().into_inner() {
        let (col, ident) = parse_col(p)?;
        keys.push(col);
        idents.push(ident)
    }
    for p in src.next().unwrap().into_inner() {
        let (col, ident) = parse_col(p)?;
        dependents.push(col);
        idents.push(ident)
    }

    Ok((StoredRelationMetadata {
        keys,
        dependents,
    }, idents))
}

fn parse_col(pair: Pair<'_>) -> Result<(ColumnDef, Option<Symbol>)> {
    let mut src = pair.into_inner();
    let name = SmartString::from(src.next().unwrap().as_str());
    let typing = parse_type(src.next().unwrap())?;
    let mut default_gen = None;
    let mut binding = None;
    for nxt in src {
        match nxt.as_rule() {
            Rule::expr => default_gen = Some(build_expr(nxt, &Default::default())?),
            Rule::ident => binding = Some(Symbol::new(nxt.as_str(), nxt.extract_span()))
        }
    }
    Ok((ColumnDef {
        name,
        typing,
        default_gen,
    }, binding))
}

fn parse_type(pair: Pair<'_>) -> Result<NullableColType> {
    let nullable = pair.as_str().ends_with('?');
    let coltype = parse_type_inner(pair.into_inner().next().unwrap())?;
    Ok(NullableColType {
        coltype,
        nullable,
    })
}

fn parse_type_inner(pair: Pair<'_>) -> Result<ColType> {
    Ok(match pair.as_rule() {
        Rule::any_type => ColType::Any,
        Rule::int_type => ColType::Int,
        Rule::float_type => ColType::Float,
        Rule::string_type => ColType::String,
        Rule::bytes_type => ColType::Bytes,
        Rule::uuid_type => ColType::Uuid,
        Rule::list_type => {
            let mut inner = pair.into_inner();
            let eltype = parse_type(inner.next().unwrap())?;
            let len = match inner.next() {
                None => None,
                Some(len_p) => {
                    let span = len_p.extract_span();
                    let expr = build_expr(len_p, &Default::default())?;
                    let dv = expr.eval_to_const()?;

                    #[derive(Debug, Error, Diagnostic)]
                    #[error("Bad specification of list length in type: {0:?}")]
                    #[diagnostic(code(parser::bad_list_len_in_type))]
                    struct BadListLenSpec(DataValue, #[label] SourceSpan);

                    let n = dv.get_int()
                        .ok_or_else(|| BadListLenSpec(dv, span))?;
                    ensure!(n >=0, BadListLenSpec(dv, span));
                    Some(n as usize)
                }
            };
            ColType::List { eltype: eltype.into(), len }
        }
        Rule::tuple_type => {
            ColType::Tuple(pair.into_inner().map(|p| parse_type(p)).try_collect()?)
        }
    })
}