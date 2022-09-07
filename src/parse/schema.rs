use miette::{bail, Result};

use crate::data::attr::{Attribute, AttributeCardinality, AttributeIndex, AttributeTyping};
use crate::data::triple::StoreOp;
use crate::parse::{Pair, Pairs, Rule};

pub(crate) fn parse_schema(src: Pairs<'_>) -> Result<Vec<AttrTxItem>> {
    let mut ret = vec![];
    for pair in src {
        if pair.as_rule() == Rule::EOI {
            break;
        }
        ret.extend(parse_schema_clause(pair)?);
    }
    Ok(ret)
}

fn parse_schema_clause(src: Pair<'_>) -> Result<Vec<AttrTxItem>> {
    let mut src = src.into_inner();
    let op = match src.next().unwrap().as_rule() {
        Rule::schema_put => StoreOp::Assert,
        Rule::schema_retract => StoreOp::Retract,
        _ => unreachable!(),
    };
    let ident = src.next().unwrap().into_inner().next().unwrap().as_str();
    let mut ret = vec![];
    let attr_def = src.next().unwrap();
    match attr_def.as_rule() {
        Rule::simple_schema_def => {
            let mut attr = parse_attr_defs(attr_def.into_inner())?;
            attr.name.0.push_str(ident);
            ret.push(AttrTxItem { op, attr });
        }
        Rule::nested_schema_def => {
            for clause in attr_def.into_inner() {
                let mut clause_row = clause.into_inner();
                let nested_ident = clause_row.next().unwrap().as_str();
                let combined_ident = format!("{}.{}", ident, nested_ident);
                let mut attr = parse_attr_defs(clause_row)?;
                attr.name.0.push_str(&combined_ident);
                ret.push(AttrTxItem { op, attr });
            }
        }
        _ => unreachable!(),
    }
    Ok(ret)
}

fn parse_attr_defs(src: Pairs<'_>) -> Result<Attribute> {
    let mut attr = Attribute::default();
    for pair in src {
        match pair.as_str() {
            "one" => attr.cardinality = AttributeCardinality::One,
            "many" => attr.cardinality = AttributeCardinality::Many,
            "history" => attr.with_history = true,
            "no_history" => attr.with_history = false,
            "identity" => attr.indexing = AttributeIndex::Identity,
            "index" => attr.indexing = AttributeIndex::Indexed,
            "no_index" => attr.indexing = AttributeIndex::None,
            "unique" => attr.indexing = AttributeIndex::Unique,
            "ref" => attr.val_type = AttributeTyping::Ref,
            "bool" => attr.val_type = AttributeTyping::Bool,
            "int" => attr.val_type = AttributeTyping::Int,
            "float" => attr.val_type = AttributeTyping::Float,
            "string" => attr.val_type = AttributeTyping::String,
            "bytes" => attr.val_type = AttributeTyping::Bytes,
            "list" => attr.val_type = AttributeTyping::List,
            v => bail!("cannot interpret {} as attribute property", v),
        };
    }
    Ok(attr)
}

#[derive(Debug)]
pub(crate) struct AttrTxItem {
    pub(crate) op: StoreOp,
    pub(crate) attr: Attribute,
}
