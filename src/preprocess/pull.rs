use anyhow::Result;
use itertools::Itertools;
use serde_json::{json, Map};

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
    pub(crate) fn parse_pull(&mut self, desc: &JsonValue, no_parent: bool) -> Result<PullSpecs> {
        if let Some(inner) = desc.as_array() {
            inner
                .iter()
                .map(|v| self.parse_pull_element(v, if no_parent { None } else { Some(inner) }))
                .try_collect()
        } else {
            Err(PullError::InvalidFormat(desc.clone(), "expect array".to_string()).into())
        }
    }
    pub(crate) fn parse_pull_element(
        &mut self,
        desc: &JsonValue,
        parent: Option<&Vec<JsonValue>>,
    ) -> Result<PullSpec> {
        match desc {
            JsonValue::String(s) if s == "*" => Ok(PullSpec::PullAll),
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
                }))
            }
            JsonValue::Object(m) => self.parse_pull_obj(m, parent),
            v => Err(
                PullError::InvalidFormat(v.clone(), "expect string or object".to_string()).into(),
            ),
        }
    }
    pub(crate) fn parse_pull_obj(
        &mut self,
        desc: &Map<String, JsonValue>,
        parent: Option<&Vec<JsonValue>>,
    ) -> Result<PullSpec> {
        let mut default_val = Value::Null;
        let mut as_override = None;
        let mut take = None;
        let mut cardinality_override = None;
        let mut input_kw = None;
        let mut sub_target = vec![];
        let mut recursive = false;
        let mut recursion_limit = None;

        for (k, v) in desc {
            match k as &str {
                "_as" => {
                    as_override = Some(Keyword::from(v.as_str().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect string".to_string())
                    })?))
                }
                "_limit" => {
                    take = Some(v.as_u64().ok_or_else(|| {
                        PullError::InvalidFormat(v.clone(), "expect limit".to_string())
                    })? as usize)
                }
                "_cardinality" => {
                    cardinality_override =
                        Some(AttributeCardinality::try_from(v.as_str().ok_or_else(
                            || PullError::InvalidFormat(v.clone(), "expect string".to_string()),
                        )?)?)
                }
                "_default" => default_val = Value::from(v).to_static(),
                k if !k.starts_with('_') => {
                    if input_kw.is_some() {
                        return Err(PullError::InvalidFormat(
                            JsonValue::Object(desc.clone()),
                            "only one sublevel target expected".to_string(),
                        )
                        .into());
                    }
                    input_kw = Some(Keyword::from(k));
                    sub_target = {
                        if let Some(arr) = v.as_array() {
                            arr.clone()
                        } else {
                            if let Some(u) = v.as_u64() {
                                recursion_limit = Some(u as usize);
                            } else if *v != json!("...") {
                                return Err(PullError::InvalidFormat(
                                    v.clone(),
                                    "expect array".to_string(),
                                )
                                .into());
                            }
                            let parent = parent.ok_or_else(|| {
                                PullError::InvalidFormat(
                                    JsonValue::Object(desc.clone()),
                                    "cannot recurse at top level".to_string(),
                                )
                            })?;
                            // not clear what two recursions would do
                            if recursive {
                                return Err(PullError::InvalidFormat(
                                    JsonValue::Object(desc.clone()),
                                    "cannot have two recursions".to_string(),
                                )
                                .into());
                            }
                            recursive = true;
                            // remove self to prevent infinite recursion
                            parent
                                .iter()
                                .filter(|p| {
                                    if let Some(o) = p.as_object() {
                                        o != desc
                                    } else {
                                        true
                                    }
                                })
                                .cloned()
                                .collect_vec()
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

        if input_kw.is_none() {
            return Err(PullError::InvalidFormat(
                JsonValue::Object(desc.clone()),
                "expect target key".to_string(),
            )
            .into());
        }

        let input_kw = input_kw.unwrap();
        // let recurse_target = sub_target.unwrap();

        let reverse = input_kw.0.starts_with('<');
        let kw = if reverse {
            Keyword::from(input_kw.0.strip_prefix('<').unwrap())
        } else {
            input_kw.clone()
        };
        let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
        let cardinality = cardinality_override.unwrap_or(attr.cardinality);
        let nested = self.parse_pull(&JsonValue::Array(sub_target), recursive)?;

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
        }))
    }
}
