use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;

use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::triple::StoreOp;

#[derive(Debug)]
pub struct AttrTxItem {
    pub(crate) op: StoreOp,
    pub(crate) attr: Attribute,
}

impl AttrTxItem {
    pub fn parse_request(req: &JsonValue) -> Result<(Vec<AttrTxItem>, String)> {
        let map = req
            .as_object()
            .ok_or_else(|| anyhow!("expect object, got {}", req))?;
        let comment = match map.get("comment") {
            None => "".to_string(),
            Some(c) => c.to_string(),
        };
        let items = map
            .get("attrs")
            .ok_or_else(|| anyhow!("expect key 'attrs' in {:?}", map))?;
        let items = items
            .as_array()
            .ok_or_else(|| anyhow!("expect array for value of key 'attrs', got {:?}", items))?;
        ensure!(
            !items.is_empty(),
            "array for value of key 'attrs' must be non-empty"
        );
        let res = items.iter().map(AttrTxItem::try_from).try_collect()?;
        Ok((res, comment))
    }
}

impl TryFrom<&'_ JsonValue> for AttrTxItem {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let map = value
            .as_object()
            .ok_or_else(|| anyhow!("expect object for attribute tx, got {}", value))?;
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
