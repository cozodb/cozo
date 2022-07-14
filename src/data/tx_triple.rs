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

#[derive(Debug)]
pub(crate) struct Triple<'a> {
    pub(crate) id: EntityId,
    pub(crate) attr: AttrId,
    pub(crate) value: Value<'a>,
}

#[derive(Debug)]
pub struct Quintuple<'a> {
    pub(crate) triple: Triple<'a>,
    pub(crate) action: TxAction,
    pub(crate) validity: Validity,
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum TxAction {
    Put,
    Retract,
    RetractAllEA,
    RetractAllE,
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
    /// each line in `tx` is `{"put: ...}`, `{"retract": ...}` or `{"ensure": ...}`
    /// these can also have a `since` field, overriding the timestamp
    /// the dots can be triples
    /// ```json
    /// [12345, ":x/y", 12345]
    /// ```
    /// triples with temp_id
    /// ```json
    /// ["temp_id1", ":x/y", 12345]
    /// ```
    /// objects format
    /// ```json
    /// {
    ///     "_id": 12345,
    ///     "_temp_id": "xyzwf",
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
        let mut str2temp_id = BTreeMap::default();
        for item in items {
            self.parse_tx_request_item(
                item,
                default_since,
                &mut cur_temp_eid,
                &mut str2temp_id,
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
        str2temp_id: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        let item = item
            .as_object()
            .ok_or_else(|| TxError::Decoding(item.clone(), "expected object".to_string()))?;
        let (inner, action) = {
            if let Some(inner) = item.get("put") {
                (inner, TxAction::Put)
            } else if let Some(inner) = item.get("retract") {
                (inner, TxAction::Retract)
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
                str2temp_id,
                collected,
            );
        }

        if let Some(obj) = inner.as_object() {
            return self.parse_tx_request_obj(
                obj,
                action,
                since,
                cur_temp_eid,
                str2temp_id,
                collected,
            );
        }

        Err(TxError::Decoding(inner.clone(), "expected object or array".to_string()).into())
    }
    fn parse_tx_request_inner<'a>(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        value: Value<'a>,
        action: TxAction,
        since: Validity,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        if !eid.is_perm() && action != TxAction::Put {
            return Err(TxError::InvalidAction(
                action,
                "using temp id instead of perm id".to_string(),
            )
            .into());
        }

        collected.push(Quintuple {
            triple: Triple {
                id: eid,
                attr: attr.id,
                value,
            },
            action,
            validity: since,
        });

        Ok(())
    }
    fn parse_tx_request_arr<'a>(
        &mut self,
        item: &'a [serde_json::Value],
        action: TxAction,
        since: Validity,
        cur_temp_eid: &mut u64,
        str2temp_id: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        match item {
            [eid] => {
                if action != TxAction::Retract {
                    return Err(TxError::InvalidAction(
                        action,
                        "singlet only allowed for 'retract'".to_string(),
                    )
                    .into());
                }
                let eid = eid.as_u64().ok_or_else(|| {
                    TxError::Decoding(eid.clone(), "cannot parse as entity id".to_string())
                })?;
                let eid = EntityId(eid);
                if !eid.is_perm() {
                    return Err(
                        TxError::EntityId(eid.0, "expected perm entity id".to_string()).into(),
                    );
                }
                collected.push(Quintuple {
                    triple: Triple {
                        id: eid,
                        attr: AttrId(0),
                        value: Value::Null,
                    },
                    action: TxAction::RetractAllE,
                    validity: since,
                });
                Ok(())
            }
            [eid, attr] => {
                if action != TxAction::Retract {
                    return Err(TxError::InvalidAction(
                        action,
                        "doublet only allowed for 'retract'".to_string(),
                    )
                    .into());
                }
                let kw: Keyword = attr.try_into()?;
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;

                let eid = eid.as_u64().ok_or_else(|| {
                    TxError::Decoding(eid.clone(), "cannot parse as entity id".to_string())
                })?;
                let eid = EntityId(eid);
                if !eid.is_perm() {
                    return Err(
                        TxError::EntityId(eid.0, "expected perm entity id".to_string()).into(),
                    );
                }
                collected.push(Quintuple {
                    triple: Triple {
                        id: eid,
                        attr: attr.id,
                        value: Value::Null,
                    },
                    action: TxAction::RetractAllEA,
                    validity: since,
                });
                Ok(())
            }
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
                                match str2temp_id.entry(s.to_string()) {
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
                                    "specifying temp_id string together with unique constraint"
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
                    match str2temp_id.entry(s.to_string()) {
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
                            attr.val_type.coerce_value(val.into())?,
                            action,
                            since,
                            collected,
                        )?;
                    }
                    Ok(())
                } else {
                    self.parse_tx_request_inner(
                        id,
                        &attr,
                        attr.val_type.coerce_value(val.into())?,
                        action,
                        since,
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
        str2temp_id: &mut BTreeMap<String, EntityId>,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        let mut pairs = Vec::with_capacity(item.len());
        let mut eid = None;
        for (k, v) in item {
            if k != "_id" && k != "_temp_id" {
                let kw = (k as &str).try_into()?;
                let attr = self
                    .attr_by_kw(&kw)?
                    .ok_or_else(|| TxError::AttrNotFound(kw.clone()))?;
                let value = attr.val_type.coerce_value(v.into())?;
                if attr.indexing == AttributeIndex::Identity {
                    let existing_id = self.eid_by_unique_av(&attr, &value, since)?;
                    match existing_id {
                        None => {}
                        Some(existing_eid) => {
                            if let Some(prev_eid) = eid {
                                if existing_eid != prev_eid {
                                    return Err(TxError::EntityId(
                                        existing_eid.0,
                                        "conflicting entity id given".to_string(),
                                    )
                                    .into());
                                }
                            }
                            eid = Some(existing_eid)
                        }
                    }
                }
                pairs.push((attr, value));
            }
        }
        if let Some(given_id) = item.get("_id") {
            let given_id = given_id.as_u64().ok_or_else(|| {
                TxError::Decoding(
                    given_id.clone(),
                    "unable to interpret as entity id".to_string(),
                )
            })?;
            let given_id = EntityId(given_id);
            if !given_id.is_perm() {
                return Err(TxError::EntityId(
                    given_id.0,
                    "temp id given where perm id is required".to_string(),
                )
                .into());
            }
            if let Some(prev_id) = eid {
                if prev_id != given_id {
                    return Err(TxError::EntityId(
                        given_id.0,
                        "conflicting entity id given".to_string(),
                    )
                    .into());
                }
            }
            eid = Some(given_id);
        }
        if let Some(temp_id) = item.get("_temp_id") {
            if let Some(eid_inner) = eid {
                return Err(TxError::EntityId(
                    eid_inner.0,
                    "conflicting entity id given".to_string(),
                )
                .into());
            }
            let temp_id_str = temp_id.as_str().ok_or_else(|| {
                TxError::Decoding(
                    temp_id.clone(),
                    "unable to interpret as temp id".to_string(),
                )
            })?;
            match str2temp_id.entry(temp_id_str.to_string()) {
                Entry::Vacant(e) => {
                    let newid = EntityId(*cur_temp_eid);
                    *cur_temp_eid += 1;
                    e.insert(newid);
                    eid = Some(newid);
                }
                Entry::Occupied(e) => {
                    eid = Some(*e.get());
                }
            }
        }
        let eid = match eid {
            Some(eid) => eid,
            None => {
                let newid = EntityId(*cur_temp_eid);
                *cur_temp_eid += 1;
                newid
            }
        };
        if action != TxAction::Put && !eid.is_perm() {
            return Err(TxError::InvalidAction(action, "temp id not allowed".to_string()).into());
        }
        for (attr, v) in pairs {
            self.parse_tx_request_inner(eid, &attr, v, action, since, collected)?;
        }
        Ok(())
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
