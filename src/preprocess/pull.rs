use std::cmp::max;

use anyhow::Result;
use itertools::Itertools;
use serde_json::Map;

use crate::data::attr::AttributeCardinality;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::value::Value;
use crate::preprocess::triple::TxError;
use crate::runtime::transact::SessionTx;
use crate::transact::pull::{AttrPullSpec, PullSpec, PullSpecs};

#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("cannot parse pull format {0}: {1}")]
    InvalidFormat(JsonValue, String),
}

impl SessionTx {
    pub(crate) fn parse_pull(&mut self, desc: &JsonValue, depth: usize) -> Result<PullSpecs> {
        if let Some(inner) = desc.as_array() {
            inner
                .iter()
                .map(|v| self.parse_pull_element(v, depth))
                .try_collect()
        } else {
            Err(PullError::InvalidFormat(desc.clone(), "expect array".to_string()).into())
        }
    }
    pub(crate) fn parse_pull_element(
        &mut self,
        desc: &JsonValue,
        depth: usize,
    ) -> Result<PullSpec> {
        match desc {
            JsonValue::String(s) if s == "*" => Ok(PullSpec::PullAll),
            JsonValue::String(s) if s == "_id" => Ok(PullSpec::PullId("_id".into())),
            JsonValue::String(s) => {
                let input_kw = Keyword::from(s.as_ref());
                let reverse = input_kw.0.starts_with('<');
                let kw = if reverse {
                    Keyword::from(input_kw.0.strip_prefix('<').unwrap())
                } else {
                    input_kw.clone()
                };
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
                let cardinality = attr.cardinality;
                Ok(PullSpec::Attr(AttrPullSpec {
                    attr,
                    default_val: Value::Null,
                    reverse,
                    name: input_kw,
                    cardinality,
                    take: None,
                    nested: vec![],
                    recursive: false,
                    recursion_limit: None,
                    recursion_depth: 0,
                }))
            }
            JsonValue::Object(m) => self.parse_pull_obj(m, depth),
            v => Err(
                PullError::InvalidFormat(v.clone(), "expect string or object".to_string()).into(),
            ),
        }
    }
    pub(crate) fn parse_pull_obj(
        &mut self,
        desc: &Map<String, JsonValue>,
        depth: usize,
    ) -> Result<PullSpec> {
        let mut default_val = Value::Null;
        let mut as_override = None;
        let mut take = None;
        let mut cardinality_override = None;
        let mut input_kw = None;
        let mut sub_target = vec![];
        let mut recursive = false;
        let mut recursion_limit = None;
        let mut pull_id = false;
        let mut recursion_depth = 0;

        for (k, v) in desc {
            match k as &str {
                "as" => {
                    as_override = Some(Keyword::from(v.as_str().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect string".to_string())
                    })?))
                }
                "limit" => {
                    take = Some(v.as_u64().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect limit".to_string())
                    })? as usize)
                }
                "cardinality" => {
                    cardinality_override =
                        Some(AttributeCardinality::try_from(v.as_str().ok_or_else(
                            || PullError::InvalidFormat(v.clone(), "expect string".to_string()),
                        )?)?)
                }
                "default" => default_val = Value::from(v).to_static(),
                "pull" => {
                    let v = v.as_str().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect string".to_string())
                    })?;
                    if v == "_id" {
                        pull_id = true
                    } else {
                        input_kw = Some(Keyword::from(v));
                    }
                }
                "recurse" => {
                    if let Some(u) = v.as_u64() {
                        recursion_limit = Some(u as usize);
                    } else if let Some(b) = v.as_bool() {
                        if !b {
                            continue;
                        }
                    } else {
                        return Err(PullError::InvalidFormat(
                            JsonValue::Object(desc.clone()),
                            "expect boolean or number".to_string(),
                        )
                        .into());
                    }
                    recursive = true;
                }
                "depth" => {
                    recursion_depth = v.as_u64().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect depth".to_string())
                    })? as usize
                }
                "spec" => {
                    sub_target = {
                        if let Some(arr) = v.as_array() {
                            arr.clone()
                        } else {
                            return Err(PullError::InvalidFormat(
                                JsonValue::Object(desc.clone()),
                                "expect array".to_string(),
                            )
                            .into());
                        }
                    };
                }
                v => {
                    return Err(PullError::InvalidFormat(
                        v.into(),
                        "unexpected spec key".to_string(),
                    )
                    .into())
                }
            }
        }

        if pull_id {
            return Ok(PullSpec::PullId(
                as_override.unwrap_or_else(|| "_id".into()),
            ));
        }

        if input_kw.is_none() {
            return Err(PullError::InvalidFormat(
                JsonValue::Object(desc.clone()),
                "expect target key".to_string(),
            )
            .into());
        }

        let input_kw = input_kw.unwrap();

        let reverse = input_kw.0.starts_with('<');
        let kw = if reverse {
            Keyword::from(input_kw.0.strip_prefix('<').unwrap())
        } else {
            input_kw.clone()
        };
        let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
        let cardinality = cardinality_override.unwrap_or(attr.cardinality);
        let nested = self.parse_pull(&JsonValue::Array(sub_target), depth + 1)?;

        if recursive {
            recursion_depth = max(recursion_depth, 1);
        }

        Ok(PullSpec::Attr(AttrPullSpec {
            attr,
            default_val,
            reverse,
            name: as_override.unwrap_or(input_kw),
            cardinality,
            take,
            nested,
            recursive,
            recursion_limit,
            recursion_depth,
        }))
    }
}
