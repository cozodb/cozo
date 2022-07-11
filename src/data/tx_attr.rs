use crate::data::attr::Attribute;
use crate::data::triple::StoreOp;
use anyhow::Result;
use itertools::Itertools;

#[derive(Debug)]
pub struct AttrTxItem {
    pub(crate) op: StoreOp,
    pub(crate) attr: Attribute,
}

impl AttrTxItem {
    pub fn parse_request(req: &serde_json::Value) -> Result<(Vec<AttrTxItem>, String)> {
        let map = req
            .as_object()
            .ok_or_else(|| AttrTxItemError::Decoding(req.clone(), "expected object".to_string()))?;
        let comment = match map.get("comment") {
            None => "".to_string(),
            Some(c) => c.to_string(),
        };
        let items = map.get("attrs").ok_or_else(|| {
            AttrTxItemError::Decoding(req.clone(), "expected key 'attrs'".to_string())
        })?;
        let items = items.as_array().ok_or_else(|| {
            AttrTxItemError::Decoding(items.clone(), "expected array".to_string())
        })?;
        if items.is_empty() {
            return Err(AttrTxItemError::Decoding(
                req.clone(),
                "'attrs' cannot be empty".to_string(),
            )
            .into());
        }
        let res = items.iter().map(AttrTxItem::try_from).try_collect()?;
        Ok((res, comment))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AttrTxItemError {
    #[error("Error decoding {0}: {1}")]
    Decoding(serde_json::Value, String),
}

impl TryFrom<&'_ serde_json::Value> for AttrTxItem {
    type Error = anyhow::Error;

    fn try_from(value: &'_ serde_json::Value) -> Result<Self, Self::Error> {
        let map = value.as_object().ok_or_else(|| {
            AttrTxItemError::Decoding(value.clone(), "expected object".to_string())
        })?;
        if map.len() != 1 {
            return Err(AttrTxItemError::Decoding(
                value.clone(),
                "object must have exactly one field".to_string(),
            )
            .into());
        }
        let (k, v) = map.into_iter().next().unwrap();
        let op = match k as &str {
            "put" => StoreOp::Assert,
            "retract" => StoreOp::Retract,
            _ => {
                return Err(
                    AttrTxItemError::Decoding(value.clone(), format!("unknown op {}", k)).into(),
                )
            }
        };

        let attr = Attribute::try_from(v)?;

        Ok(AttrTxItem { op, attr })
    }
}
