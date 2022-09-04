use miette::{bail, ensure, miette, Result};

use crate::data::attr::{Attribute, AttributeCardinality, AttributeIndex, AttributeTyping};
use crate::data::json::JsonValue;
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
            "component" => attr.val_type = AttributeTyping::Component,
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

// impl AttrTxItem {
    // pub(crate) fn parse_request(req: &JsonValue) -> Result<(Vec<AttrTxItem>, String)> {
    //     let map = req
    //         .as_object()
    //         .ok_or_else(|| miette!("expect object, got {}", req))?;
    //     let comment = match map.get("comment") {
    //         None => "".to_string(),
    //         Some(c) => c.to_string(),
    //     };
    //     let items = map
    //         .get("attrs")
    //         .ok_or_else(|| miette!("expect key 'attrs' in {:?}", map))?;
    //     let items = items
    //         .as_array()
    //         .ok_or_else(|| miette!("expect array for value of key 'attrs', got {:?}", items))?;
    //     ensure!(
    //         !items.is_empty(),
    //         "array for value of key 'attrs' must be non-empty"
    //     );
    //     let res = items.iter().map(AttrTxItem::try_from).try_collect()?;
    //     Ok((res, comment))
    // }
// }

impl TryFrom<&'_ JsonValue> for AttrTxItem {
    type Error = miette::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let map = value
            .as_object()
            .ok_or_else(|| miette!("expect object for attribute tx, got {}", value))?;
        ensure!(
            map.len() == 1,
            "attr definition must have exactly one pair, got {}",
            value
        );
        let (k, v) = map.into_iter().next().unwrap();
        let op = match k as &str {
            "put" => StoreOp::Assert,
            "retract" => StoreOp::Retract,
            _ => bail!("unknown op {} for attribute tx", k),
        };

        let attr = Attribute::try_from(v)?;

        Ok(AttrTxItem { op, attr })
    }
}
