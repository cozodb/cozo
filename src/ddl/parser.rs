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
    ExprParse(#[from] ExprParseError),

    #[error("definition error: {0}")]
    Definition(&'static str),
}

type Result<T> = result::Result<T, DdlParseError>;

#[derive(Debug, Clone)]
pub(crate) struct ColSchema {
    pub(crate) name: String,
    pub(crate) typing: Typing,
    pub(crate) default: StaticExpr,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeSchema {
    pub(crate) name: String,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct EdgeSchema {
    pub(crate) name: String,
    pub(crate) src_name: String,
    pub(crate) dst_name: String,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct AssocSchema {
    pub(crate) name: String,
    pub(crate) src_name: String,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexSchema {
    pub(crate) name: String,
    pub(crate) src_name: String,
    pub(crate) assoc_names: Vec<String>,
    pub(crate) index: Vec<StaticExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct SequenceSchema {
    pub(crate) name: String,
}

#[derive(Debug, Clone)]
pub(crate) enum DdlSchema {
    Node(NodeSchema),
    Edge(EdgeSchema),
    Assoc(AssocSchema),
    Index(IndexSchema),
    Sequence(SequenceSchema),
}

impl<'a> TryFrom<Pair<'a>> for DdlSchema {
    type Error = DdlParseError;

    fn try_from(pair: Pair<'a>) -> result::Result<Self, Self::Error> {
        Ok(match pair.as_rule() {
            Rule::node_def => DdlSchema::Node(pair.try_into()?),
            Rule::edge_def => DdlSchema::Edge(pair.try_into()?),
            Rule::assoc_def => DdlSchema::Assoc(pair.try_into()?),
            Rule::seq_def => DdlSchema::Sequence(pair.try_into()?),
            Rule::index_def => DdlSchema::Index(pair.try_into()?),
            _ => todo!()
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for NodeSchema {
    type Error = DdlParseError;

    fn try_from(pair: Pair) -> Result<Self> {
        let mut pairs = pair.into_inner();
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let cols_pair = pairs.next().unwrap();
        let (keys, vals) = parse_cols(cols_pair)?;
        Ok(Self {
            name,
            keys,
            vals,
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for EdgeSchema {
    type Error = DdlParseError;
    fn try_from(pair: Pair) -> Result<Self> {
        let mut pairs = pair.into_inner();
        let src_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let dst_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let (keys, vals) = match pairs.next() {
            Some(pair) => parse_cols(pair)?,
            None => (vec![], vec![])
        };
        Ok(EdgeSchema {
            name,
            src_name,
            dst_name,
            keys,
            vals,
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for AssocSchema {
    type Error = DdlParseError;
    fn try_from(pair: Pair) -> Result<Self> {
        let mut pairs = pair.into_inner();
        let src_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let name = build_name_in_def(pairs.next().unwrap(), true)?;

        let (keys, vals) = parse_cols(pairs.next().unwrap())?;
        if !keys.is_empty() {
            return Err(DdlParseError::Definition("assoc cannot have keys"));
        }
        if vals.is_empty() {
            return Err(DdlParseError::Definition("assoc has no values"));
        }
        Ok(AssocSchema {
            name,
            src_name,
            vals,
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for IndexSchema {
    type Error = DdlParseError;
    fn try_from(pair: Pair) -> Result<Self> {
        let mut pairs = pair.into_inner();
        let index_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let main_name = build_name_in_def(pairs.next().unwrap(), false)?;
        let mut associate_names = vec![];
        let mut indices = vec![];
        for pair in pairs {
            match pair.as_rule() {
                Rule::name_in_def => associate_names.push(build_name_in_def(pair, false)?),
                _ => indices.push(Expr::try_from(pair)?.to_static())
            }
        }
        if indices.is_empty() {
            return Err(DdlParseError::Definition("Empty indexed columns"));
        }
        Ok(IndexSchema {
            name: index_name,
            src_name: main_name,
            assoc_names: associate_names,
            index: indices
        })
    }
}

impl<'a> TryFrom<Pair<'a>> for SequenceSchema {
    type Error = DdlParseError;
    fn try_from(pair: Pair) -> Result<Self> {
        let name = build_name_in_def(pair.into_inner().next().unwrap(), true)?;
        Ok(SequenceSchema {
            name
        })
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
        default,
    }))
}

fn parse_col_name(pair: Pair) -> Result<(bool, String)> {
    let mut pairs = pair.into_inner();
    let mut nxt = pairs.next().unwrap();
    let is_key = match nxt.as_rule() {
        Rule::key_marker => {
            nxt = pairs.next().unwrap();
            true
        }
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

        let s = r#"
        edge (Department)-[InLocation]->(Location)
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);

        let s = r#"
        edge (Employee)-[HasDependent]->(Dependent) {
            relationship: Text
        }
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);

        let s = r#"
        assoc BankAccount: Person {
            balance: Float = 0
        }
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);

        let s = r#"
        sequence PersonId;
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);

        let s = r#"
        index bankaccountidx: Person + BankAccount [id, x, y, z]
        "#;
        let p = CozoParser::parse(Rule::definition_all, s).unwrap().next().unwrap();
        dbg!(DdlSchema::try_from(p)?);

        Ok(())
    }
}