use crate::data::attr::{Attribute, AttributeIndex, AttributeTyping};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::keyword::Keyword;
use crate::data::value::Value;
use crate::runtime::transact::SessionTx;
use anyhow::Result;
use serde_json::Map;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

pub(crate) struct Triple<'a> {
    id: EntityId,
    attr: AttrId,
    value: Value<'a>,
}

pub struct Quintuple<'a> {
    triple: Triple<'a>,
    action: TxAction,
    validity: Validity,
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum TxAction {
    Put,
    RetractEAV,
    RetractEA,
    RetractE,
    Erase,
    Ensure,
}

impl Display for TxAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TxError {
    #[error("Error decoding {0}: {1}")]
    Decoding(serde_json::Value, String),
    #[error("triple length error")]
    TripleLength,
    #[error("attribute not found: {0}")]
    AttrNotFound(Keyword),
    #[error("wrong specification of entity id {0}: {}")]
    EntityId(u64, String),
    #[error("invalid action {0:?}: {}")]
    InvalidAction(TxAction, String),
}

impl SessionTx {
    /// Requests are like these
    /// ```json
    /// {"tx": [...], "comment": "a comment", "since": timestamp}
    /// ```
    /// each line in `tx` is `{"put: ...}`, `{"retract": ...}`, `{"erase": ...}` or `{"ensure": ...}`
    /// these can also have a `since` field, overriding the timestamp
    /// the dots can be triples
    /// ```json
    /// [12345, ":x/y", 12345]
    /// ```
    /// triples with tempid
    /// ```json
    /// ["tempid1", ":x/y", 12345]
    /// ```
    /// objects format
    /// ```json
    /// {
    ///     "_id": 12345,
    ///     "_tempid": "xyzwf",
    ///     "ns/fieldname": 111
    /// }
    /// ```
    /// nesting is allowed for values of type `ref` and `component`
    pub fn parse_tx_requests<'a>(
        &mut self,
        req: &'a serde_json::Value,
    ) -> Result<(Vec<Quintuple<'a>>, String)> {
        let map = req
            .as_object()
            .ok_or_else(|| TxError::Decoding(req.clone(), "expected object".to_string()))?;
        let items = map
            .get("tx")
            .ok_or_else(|| TxError::Decoding(req.clone(), "expected field 'tx'".to_string()))?
            .as_array()
            .ok_or_else(|| {
                TxError::Decoding(
                    req.clone(),
                    "expected field 'tx' to be an array".to_string(),
                )
            })?;
        let default_since = match map.get("since") {
            None => Validity::current(),
            Some(v) => v.try_into()?,
        };
        let comment = match map.get("comment") {
            None => "".to_string(),
            Some(v) => v.to_string(),
        };
        let mut collected = Vec::with_capacity(items.len());
        let mut cur_temp_eid = 1;
        let mut str2tempid = BTreeMap::default();
        for item in items {
            self.parse_tx_request_item(
                item,
                default_since,
                &mut cur_temp_eid,
                &mut str2tempid,
                &mut collected,
            )?;
        }
        Ok((collected, comment))
    }
    fn parse_tx_request_item<'a>(
        &mut self,
        item: &'a serde_json::Value,
        default_since: Validity,
        cur_temp_eid: &mut u64,
        str2tempid: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        let item = item
            .as_object()
            .ok_or_else(|| TxError::Decoding(item.clone(), "expected object".to_string()))?;
        let (inner, action) = {
            if let Some(inner) = item.get("put") {
                (inner, TxAction::Put)
            } else if let Some(inner) = item.get("retract") {
                (inner, TxAction::RetractEAV)
            } else if let Some(inner) = item.get("erase") {
                (inner, TxAction::Erase)
            } else if let Some(inner) = item.get("ensure") {
                (inner, TxAction::Ensure)
            } else {
                return Err(TxError::Decoding(
                    serde_json::Value::Object(item.clone()),
                    "expect any of the keys 'put', 'retract', 'erase', 'ensure'".to_string(),
                )
                .into());
            }
        };
        let since = match item.get("since") {
            None => default_since,
            Some(v) => v.try_into()?,
        };
        if let Some(arr) = inner.as_array() {
            return self.parse_tx_request_arr(
                arr,
                action,
                since,
                cur_temp_eid,
                str2tempid,
                collected,
            );
        }

        if let Some(obj) = inner.as_object() {
            return self.parse_tx_request_obj(
                obj,
                action,
                since,
                cur_temp_eid,
                str2tempid,
                collected,
            );
        }

        Err(TxError::Decoding(inner.clone(), "expected object or array".to_string()).into())
    }
    fn parse_tx_request_inner<'a>(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        val: &'a serde_json::Value,
        action: TxAction,
        since: Validity,
        cur_temp_eid: &mut u64,
        str2tempid: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        if !eid.is_perm() && action != TxAction::Put {
            return Err(TxError::InvalidAction(
                action,
                "using temp id instead of perm id".to_string(),
            )
            .into());
        }
        if val.is_object() && attr.val_type.is_ref_type() {
            let ref_id = self.parse_tx_submap(
                attr.val_type == AttributeTyping::Component,
                val.as_object().unwrap(),
                action,
                since,
                cur_temp_eid,
                str2tempid,
                collected,
            )?;
            collected.push(Quintuple {
                triple: Triple {
                    id: eid,
                    attr: attr.id,
                    value: Value::EnId(ref_id),
                },
                action,
                validity: since,
            });
        } else {
            collected.push(Quintuple {
                triple: Triple {
                    id: eid,
                    attr: attr.id,
                    value: attr.val_type.coerce_value(val.into())?,
                },
                action,
                validity: since,
            });
        }

        Ok(())
    }
    fn parse_tx_submap<'a>(
        &mut self,
        is_component: bool,
        item: &'a Map<String, serde_json::Value>,
        action: TxAction,
        since: Validity,
        cur_temp_eid: &mut u64,
        str2tempid: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<EntityId> {
        todo!()
    }
    fn parse_tx_request_arr<'a>(
        &mut self,
        item: &'a [serde_json::Value],
        action: TxAction,
        since: Validity,
        cur_temp_eid: &mut u64,
        str2tempid: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        match item {
            [eid, attr_kw, val] => {
                let kw: Keyword = attr_kw.try_into()?;
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;

                let id = if attr.indexing == AttributeIndex::Identity {
                    let value: Value = val.into();
                    let value = attr.val_type.coerce_value(value)?;
                    let existing = self.eid_by_unique_av(&attr, &value, since)?;
                    match existing {
                        None => {
                            if let Some(i) = eid.as_u64() {
                                let id = EntityId(i);
                                if !id.is_perm() {
                                    return Err(TxError::EntityId(
                                        id.0,
                                        "temp id specified".into(),
                                    )
                                    .into());
                                }
                                id
                            } else if let Some(s) = eid.as_str() {
                                match str2tempid.entry(s.to_string()) {
                                    Entry::Vacant(e) => {
                                        let id = EntityId(*cur_temp_eid);
                                        *cur_temp_eid += 1;
                                        e.insert(id);
                                        id
                                    }
                                    Entry::Occupied(e) => *e.get(),
                                }
                            } else {
                                let id = EntityId(*cur_temp_eid);
                                *cur_temp_eid += 1;
                                id
                            }
                        }
                        Some(existing_id) => {
                            if let Some(i) = eid.as_u64() {
                                let id = EntityId(i);
                                if !id.is_perm() {
                                    return Err(TxError::EntityId(
                                        id.0,
                                        "temp id specified".into(),
                                    )
                                    .into());
                                }
                                if existing_id != id {
                                    return Err(TxError::EntityId(
                                        id.0,
                                        "conflicting id for identity value".into(),
                                    )
                                    .into());
                                }
                                id
                            } else if let Some(_) = eid.as_str() {
                                return Err(TxError::EntityId(
                                    existing_id.0,
                                    "specifying tempid string together with unique constraint"
                                        .into(),
                                )
                                .into());
                            } else {
                                existing_id
                            }
                        }
                    }
                } else if let Some(i) = eid.as_u64() {
                    let id = EntityId(i);
                    if !id.is_perm() {
                        return Err(TxError::EntityId(id.0, "temp id specified".into()).into());
                    }
                    id
                } else if let Some(s) = eid.as_str() {
                    match str2tempid.entry(s.to_string()) {
                        Entry::Vacant(e) => {
                            let id = EntityId(*cur_temp_eid);
                            *cur_temp_eid += 1;
                            e.insert(id);
                            id
                        }
                        Entry::Occupied(e) => *e.get(),
                    }
                } else {
                    let id = EntityId(*cur_temp_eid);
                    *cur_temp_eid += 1;
                    id
                };

                if attr.val_type != AttributeTyping::Tuple && val.is_array() {
                    let vals = val.as_array().unwrap();
                    for val in vals {
                        self.parse_tx_request_inner(
                            id,
                            &attr,
                            val,
                            action,
                            since,
                            cur_temp_eid,
                            str2tempid,
                            collected,
                        )?;
                    }
                    Ok(())
                } else {
                    self.parse_tx_request_inner(
                        id,
                        &attr,
                        val,
                        action,
                        since,
                        cur_temp_eid,
                        str2tempid,
                        collected,
                    )
                }
            }
            _ => Err(TxError::TripleLength.into()),
        }
    }
    fn parse_tx_request_obj<'a>(
        &mut self,
        item: &'a Map<String, serde_json::Value>,
        action: TxAction,
        since: Validity,
        cur_temp_eid: &mut u64,
        str2tempid: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        // let eid = match (item.get("_id"), item.get("_temp_id")) {
        //     (None, None) =>
        // }
        todo!()
    }
}

fn assert_absence_of_keys(m: &Map<String, serde_json::Value>, keys: &[&str]) -> Result<()> {
    for k in keys {
        if m.contains_key(*k) {
            return Err(TxError::Decoding(
                serde_json::Value::Object(m.clone()),
                format!("object must not contain key {}", k),
            )
            .into());
        }
    }
    Ok(())
}
