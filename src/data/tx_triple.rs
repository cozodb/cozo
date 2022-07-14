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
    #[error("wrong specification of entity id {0}: {1}")]
    EntityId(u64, String),
    #[error("invalid action {0:?}: {1}")]
    InvalidAction(TxAction, String),
    #[error("temp id does not occur in head position: {0}")]
    TempIdNoHead(String),
}

#[derive(Default)]
pub(crate) struct TempIdCtx {
    store: BTreeMap<String, (EntityId, bool)>,
    prev_id: u64,
}

impl TempIdCtx {
    fn validate_usage(&self) -> Result<()> {
        for (k, (_, b)) in self.store.iter() {
            if !*b {
                return Err(TxError::TempIdNoHead(k.to_string()).into());
            }
        }
        Ok(())
    }
    pub(crate) fn str2tempid(&mut self, key: &str, in_head: bool) -> EntityId {
        match self.store.entry(key.to_string()) {
            Entry::Vacant(e) => {
                self.prev_id += 1;
                let eid = EntityId(self.prev_id);
                e.insert((eid, in_head));
                eid
            }
            Entry::Occupied(mut e) => {
                let (eid, prev_in_head) = e.get();
                let (eid, prev_in_head) = (*eid, *prev_in_head);
                if !prev_in_head && in_head {
                    e.insert((eid, true));
                }
                eid
            }
        }
    }
    fn unnamed_tempid(&mut self) -> EntityId {
        self.prev_id += 1;
        EntityId(self.prev_id)
    }
}

const TEMP_ID_FIELD: &str = "_temp_id";
const PERM_ID_FIELD: &str = "_id";

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
        let mut temp_id_ctx = TempIdCtx::default();
        for item in items {
            self.parse_tx_request_item(item, default_since, &mut temp_id_ctx, &mut collected)?;
        }
        temp_id_ctx.validate_usage()?;
        Ok((collected, comment))
    }
    fn parse_tx_request_item<'a>(
        &mut self,
        item: &'a serde_json::Value,
        default_since: Validity,
        temp_id_ctx: &mut TempIdCtx,
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
            return self.parse_tx_request_arr(arr, action, since, temp_id_ctx, collected);
        }

        if let Some(obj) = inner.as_object() {
            return self
                .parse_tx_request_obj(obj, false, action, since, temp_id_ctx, collected)
                .map(|_| ());
        }

        Err(TxError::Decoding(inner.clone(), "expected object or array".to_string()).into())
    }
    fn parse_tx_request_inner<'a>(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        value: &'a serde_json::Value,
        action: TxAction,
        since: Validity,
        temp_id_ctx: &mut TempIdCtx,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        if attr.cardinality.is_many() && attr.val_type != AttributeTyping::Tuple && value.is_array()
        {
            for cur_val in value.as_array().unwrap() {
                self.parse_tx_request_inner(
                    eid,
                    attr,
                    cur_val,
                    action,
                    since,
                    temp_id_ctx,
                    collected,
                )?;
            }
            return Ok(());
        }

        if !eid.is_perm() && action != TxAction::Put {
            return Err(TxError::InvalidAction(
                action,
                "using temp id instead of perm id".to_string(),
            )
            .into());
        }

        let v = if let serde_json::Value::Object(inner) = value {
            self.parse_tx_component(&attr, inner, action, since, temp_id_ctx, collected)?
        } else {
            attr.coerce_value(Value::from(value), temp_id_ctx)?
        };

        collected.push(Quintuple {
            triple: Triple {
                id: eid,
                attr: attr.id,
                value: v,
            },
            action,
            validity: since,
        });

        Ok(())
    }
    fn parse_tx_component<'a>(
        &mut self,
        parent_attr: &Attribute,
        comp: &'a Map<String, serde_json::Value>,
        action: TxAction,
        since: Validity,
        temp_id_ctx: &mut TempIdCtx,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<Value<'a>> {
        if action != TxAction::Put {
            return Err(TxError::InvalidAction(
                action,
                "component shorthand cannot be used".to_string(),
            )
            .into());
        }
        let (eid, has_unique_attr) =
            self.parse_tx_request_obj(comp, true, action, since, temp_id_ctx, collected)?;
        if !has_unique_attr && parent_attr.val_type != AttributeTyping::Component {
            return Err(TxError::InvalidAction(action,
            "component shorthand must contain at least one unique/identity field for non-component refs".to_string()).into());
        }
        Ok(Value::EnId(eid))
    }
    fn parse_tx_request_arr<'a>(
        &mut self,
        item: &'a [serde_json::Value],
        action: TxAction,
        since: Validity,
        temp_id_ctx: &mut TempIdCtx,
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
                self.parse_tx_triple(eid, attr_kw, val, action, since, temp_id_ctx, collected)
            }
            _ => Err(TxError::TripleLength.into()),
        }
    }
    fn parse_tx_triple<'a>(
        &mut self,
        eid: &serde_json::Value,
        attr_kw: &serde_json::Value,
        val: &'a serde_json::Value,
        action: TxAction,
        since: Validity,
        temp_id_ctx: &mut TempIdCtx,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<()> {
        let kw: Keyword = attr_kw.try_into()?;
        let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
        if attr.cardinality.is_many() && attr.val_type != AttributeTyping::Tuple && val.is_array() {
            for cur_val in val.as_array().unwrap() {
                self.parse_tx_triple(eid, attr_kw, cur_val, action, since, temp_id_ctx, collected)?;
            }
            return Ok(());
        }

        let id = if attr.indexing.is_unique_index() {
            let value = if let serde_json::Value::Object(inner) = val {
                self.parse_tx_component(&attr, inner, action, since, temp_id_ctx, collected)?
            } else {
                attr.coerce_value(val.into(), temp_id_ctx)?
            };
            let existing = self.eid_by_unique_av(&attr, &value, since)?;
            match existing {
                None => {
                    if let Some(i) = eid.as_u64() {
                        let id = EntityId(i);
                        if !id.is_perm() {
                            return Err(TxError::EntityId(id.0, "temp id specified".into()).into());
                        }
                        id
                    } else if let Some(s) = eid.as_str() {
                        temp_id_ctx.str2tempid(s, true)
                    } else {
                        temp_id_ctx.unnamed_tempid()
                    }
                }
                Some(existing_id) => {
                    if let Some(i) = eid.as_u64() {
                        let id = EntityId(i);
                        if !id.is_perm() {
                            return Err(TxError::EntityId(id.0, "temp id specified".into()).into());
                        }
                        if existing_id != id {
                            return Err(TxError::EntityId(
                                id.0,
                                "conflicting id for identity value".into(),
                            )
                            .into());
                        }
                        id
                    } else if eid.is_string() {
                        return Err(TxError::EntityId(
                            existing_id.0,
                            "specifying temp_id string together with unique constraint".into(),
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
            temp_id_ctx.str2tempid(s, true)
        } else {
            temp_id_ctx.unnamed_tempid()
        };

        if attr.val_type != AttributeTyping::Tuple && val.is_array() {
            let vals = val.as_array().unwrap();
            for val in vals {
                self.parse_tx_request_inner(id, &attr, val, action, since, temp_id_ctx, collected)?;
            }
            Ok(())
        } else {
            self.parse_tx_request_inner(id, &attr, val, action, since, temp_id_ctx, collected)
        }
    }
    fn parse_tx_request_obj<'a>(
        &mut self,
        item: &'a Map<String, serde_json::Value>,
        is_sub_component: bool,
        action: TxAction,
        since: Validity,
        temp_id_ctx: &mut TempIdCtx,
        collected: &mut Vec<Quintuple<'a>>,
    ) -> Result<(EntityId, bool)> {
        let mut pairs = Vec::with_capacity(item.len());
        let mut eid = None;
        let mut has_unique_attr = false;
        let mut has_identity_attr = false;
        for (k, v) in item {
            if k != PERM_ID_FIELD && k != TEMP_ID_FIELD {
                let kw = (k as &str).try_into()?;
                let attr = self
                    .attr_by_kw(&kw)?
                    .ok_or_else(|| TxError::AttrNotFound(kw.clone()))?;
                has_unique_attr = has_unique_attr || attr.indexing.is_unique_index();
                has_identity_attr = has_identity_attr || attr.indexing == AttributeIndex::Identity;
                if attr.indexing == AttributeIndex::Identity {
                    let value = if let serde_json::Value::Object(inner) = v {
                        self.parse_tx_component(
                            &attr,
                            inner,
                            action,
                            since,
                            temp_id_ctx,
                            collected,
                        )?
                    } else {
                        attr.coerce_value(v.into(), temp_id_ctx)?
                    };
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
                pairs.push((attr, v));
            }
        }
        if let Some(given_id) = item.get(PERM_ID_FIELD) {
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
        if let Some(temp_id) = item.get(TEMP_ID_FIELD) {
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
            eid = Some(temp_id_ctx.str2tempid(temp_id_str, true));
        }
        let eid = match eid {
            Some(eid) => eid,
            None => temp_id_ctx.unnamed_tempid(),
        };
        if action != TxAction::Put && !eid.is_perm() {
            return Err(TxError::InvalidAction(action, "temp id not allowed".to_string()).into());
        }
        if !is_sub_component {
            if action == TxAction::Put && eid.is_perm() && !has_identity_attr {
                return Err(TxError::InvalidAction(
                    action,
                    "upsert requires identity attribute present".to_string(),
                )
                .into());
            }
            for (attr, v) in pairs {
                self.parse_tx_request_inner(eid, &attr, v, action, since, temp_id_ctx, collected)?;
            }
        }
        Ok((eid, has_unique_attr))
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
