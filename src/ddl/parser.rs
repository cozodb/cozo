use std::result;
use crate::data::expr::{Expr, StaticExpr};
use crate::data::parser::ExprParseError;
use crate::data::typing::{Typing, TypingError};
use crate::data::value::Value;
use crate::parser::{Pair, Rule};
use crate::parser::text_identifier::{build_name_in_def, TextParseError};

#[derive(thiserror::Error, Debug)]
pub(crate) enum DdlParseError {
    #[error(transparent)]
    TextParse(#[from] TextParseError),

    #[error(transparent)]
    Typing(#[from] TypingError),

    #[error(transparent)]
    ExprParse(#[from] ExprParseError)
}

type Result<T> = result::Result<T, DdlParseError>;

#[derive(Debug, Clone)]
pub(crate) struct ColSchema {
    name: String,
    typing: Typing,
    default: StaticExpr,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeSchema {
    name: String,
    keys: Vec<ColSchema>,
    vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct EdgeSchema {
    name: String,
    src_name: String,
    dst_name: String,
    keys: Vec<ColSchema>,
    vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct AssocSchema {
    name: String,
    src_name: String,
    vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) enum IndexCol {
    Simple(String),
    Computed(StaticExpr),
}

#[derive(Debug, Clone)]
pub(crate) struct IndexSchema {
    name: String,
    src_name: String,
    index: Vec<IndexCol>,
}

#[derive(Debug, Clone)]
pub(crate) struct SequenceSchema {
    name: String,
}

#[derive(Debug, Clone)]
pub(crate) enum DdlSchema {
    Node(NodeSchema),
    Edge(EdgeSchema),
    Assoc(AssocSchema),
    Index(IndexSchema),
    Sequence(SequenceSchema)
}

impl<'a> TryFrom<Pair<'a>> for DdlSchema {
    type Error = DdlParseError;

    fn try_from(pair: Pair<'a>) -> result::Result<Self, Self::Error> {
        Ok(match pair.as_rule() {
            Rule::node_def => DdlSchema::Node(pair.try_into()?),
            _ => todo!()
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for NodeSchema {
    type Error = DdlParseError;

    fn try_from(pair: Pair) -> Result<Self> {
        let mut pairs = pair.into_inner();
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let col_pair = pairs.next().unwrap();
        let (keys, vals) = parse_cols(col_pair)?;
        Ok(Self {
            name,
            keys,
            vals,
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for EdgeSchema {
    type Error = DdlParseError;
    fn try_from(value: Pair) -> Result<Self> {
        todo!()
    }
}

impl<'a> TryFrom<Pair<'a>> for AssocSchema {
    type Error = DdlParseError;
    fn try_from(value: Pair) -> Result<Self> {
        todo!()
    }
}

impl<'a> TryFrom<Pair<'a>> for IndexSchema {
    type Error = DdlParseError;
    fn try_from(value: Pair) -> Result<Self> {
        todo!()
    }
}

fn parse_cols(pair: Pair) -> Result<(Vec<ColSchema>, Vec<ColSchema>)> {
    let mut keys = vec![];
    let mut vals = vec![];
    for pair in pair.into_inner() {
        match parse_col_entry(pair)? {
            (true, res) => keys.push(res),
            (false, res) => vals.push(res)
        }
    }
    Ok((keys, vals))
}

fn parse_col_entry(pair: Pair) -> Result<(bool, ColSchema)> {
    let mut pairs = pair.into_inner();
    let (is_key, name) = parse_col_name(pairs.next().unwrap())?;
    let typing = Typing::try_from(pairs.next().unwrap())?;
    let default = match pairs.next() {
        None => Expr::Const(Value::Null),
        Some(pair) => Expr::try_from(pair)?.to_static(),
    };
    Ok((is_key, ColSchema {
        name,
        typing,
        default
    }))
}

fn parse_col_name(pair: Pair) -> Result<(bool, String)> {
    let mut pairs = pair.into_inner();
    let mut nxt = pairs.next().unwrap();
    let is_key = match nxt.as_rule() {
        Rule::key_marker => {
            nxt = pairs.next().unwrap();
            true
        },
        _ => false
    };
    let name = build_name_in_def(nxt, true)?;
    Ok((is_key, name))
}

#[cfg(test)]
mod tests {
    use crate::parser::CozoParser;
    use pest::Parser;
    use super::*;

    #[test]
    fn parse_ddl() -> Result<()> {
        let s = r#"
        node Job {
            *id: Int,
            title: Text,
            min_salary: Float = 0,
            max_salary: Float
        }
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);
        Ok(())
    }
}