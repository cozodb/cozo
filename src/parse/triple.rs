// use std::collections::btree_map::Entry;
// use std::collections::BTreeMap;
// use std::fmt::{Display, Formatter};
//
// use miette::{miette, bail, ensure, Context, Result};
// use serde_json::{json, Map};
//
// use crate::data::attr::{Attribute, AttributeIndex, AttributeTyping};
// use crate::data::id::{AttrId, EntityId, Validity};
// use crate::data::json::JsonValue;
// use crate::data::symb::Symbol;
// use crate::data::value::DataValue;
// use crate::runtime::transact::SessionTx;
//
// #[derive(Debug)]
// pub(crate) struct Triple {
//     pub(crate) id: EntityId,
//     pub(crate) attr: AttrId,
//     pub(crate) value: DataValue,
// }
//
// #[derive(Debug)]
// pub(crate) struct Quintuple {
//     pub(crate) triple: Triple,
//     pub(crate) action: TxAction,
//     pub(crate) validity: Validity,
// }
//
// #[repr(u8)]
// #[derive(Debug, Eq, PartialEq, Copy, Clone)]
// pub(crate) enum TxAction {
//     Put,
//     Retract,
//     RetractAllEA,
//     RetractAllE,
//     Ensure,
// }
//
// impl Display for TxAction {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", self)
//     }
// }
//
// #[derive(Default)]
// pub(crate) struct TempIdCtx {
//     store: BTreeMap<String, (EntityId, bool)>,
//     prev_id: u64,
// }
//
// impl TempIdCtx {
//     fn validate_usage(&self) -> Result<()> {
//         for (k, (_, b)) in self.store.iter() {
//             ensure!(
//                 *b,
//                 "defining temp id {} in non-head position is not allowed",
//                 k
//             );
//         }
//         Ok(())
//     }
//     pub(crate) fn str2tempid(&mut self, key: &str, in_head: bool) -> EntityId {
//         match self.store.entry(key.to_string()) {
//             Entry::Vacant(e) => {
//                 self.prev_id += 1;
//                 let eid = EntityId(self.prev_id);
//                 e.insert((eid, in_head));
//                 eid
//             }
//             Entry::Occupied(mut e) => {
//                 let (eid, prev_in_head) = e.get();
//                 let (eid, prev_in_head) = (*eid, *prev_in_head);
//                 if !prev_in_head && in_head {
//                     e.insert((eid, true));
//                 }
//                 eid
//             }
//         }
//     }
//     fn unnamed_tempid(&mut self) -> EntityId {
//         self.prev_id += 1;
//         EntityId(self.prev_id)
//     }
// }
//
// const TEMP_ID_FIELD: &str = "_temp_id";
// const PERM_ID_FIELD: &str = "_id";
//
// impl SessionTx {
//     /// Requests are like these
//     /// ```json
//     /// {"tx": [...], "comment": "a comment", "since": timestamp}
//     /// ```
//     /// each line in `tx` is `{"put: ...}`, `{"retract": ...}` or `{"ensure": ...}`
//     /// these can also have a `since` field, overriding the timestamp
//     /// the dots can be triples
//     /// ```json
//     /// [12345, ":x/y", 12345]
//     /// ```
//     /// triples with temp_id
//     /// ```json
//     /// ["temp_id1", ":x/y", 12345]
//     /// ```
//     /// objects format
//     /// ```json
//     /// {
//     ///     "_id": 12345,
//     ///     "_temp_id": "xyzwf",
//     ///     "ns/fieldname": 111
//     /// }
//     /// ```
//     /// nesting is allowed for values of type `ref` and `component`
//     pub(crate) fn parse_tx_requests(
//         &mut self,
//         req: &JsonValue,
//     ) -> Result<(Vec<Quintuple>, String)> {
//         let map = req
//             .as_object()
//             .ok_or_else(|| miette!("expect tx request to be an object, got {}", req))?;
//         let items = map
//             .get("tx")
//             .ok_or_else(|| miette!("expect field 'tx' in tx request object {}", req))?
//             .as_array()
//             .ok_or_else(|| miette!("expect field 'tx' to be an array in {}", req))?;
//         let default_since = match map.get("since") {
//             None => Validity::current(),
//             Some(v) => v.try_into()?,
//         };
//         let comment = match map.get("comment") {
//             None => "".to_string(),
//             Some(v) => v.to_string(),
//         };
//         let mut collected = Vec::with_capacity(items.len());
//         let mut temp_id_ctx = TempIdCtx::default();
//         for item in items {
//             self.parse_tx_request_item(item, default_since, &mut temp_id_ctx, &mut collected)?;
//         }
//         temp_id_ctx.validate_usage()?;
//         Ok((collected, comment))
//     }
//     fn parse_tx_request_item<'a>(
//         &mut self,
//         item: &'a JsonValue,
//         default_since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<()> {
//         let item = item
//             .as_object()
//             .ok_or_else(|| miette!("expect tx request item to be an object, got {}", item))?;
//         let (inner, action) = {
//             if let Some(inner) = item.get("put") {
//                 (inner, TxAction::Put)
//             } else if let Some(inner) = item.get("retract") {
//                 (inner, TxAction::Retract)
//             } else if let Some(inner) = item.get("ensure") {
//                 (inner, TxAction::Ensure)
//             } else {
//                 bail!(
//                     "expect key 'put', 'retract', 'erase' or 'ensure' in tx request object, got {:?}",
//                     item
//                 );
//             }
//         };
//         let since = match item.get("since") {
//             None => default_since,
//             Some(v) => v.try_into()?,
//         };
//         if let Some(arr) = inner.as_array() {
//             return self.parse_tx_request_arr(arr, action, since, temp_id_ctx, collected);
//         }
//
//         if let Some(obj) = inner.as_object() {
//             return self
//                 .parse_tx_request_obj(obj, false, action, since, temp_id_ctx, collected)
//                 .map(|_| ());
//         }
//
//         bail!("expect object or array for tx object item, got {}", inner);
//     }
//     fn parse_tx_request_inner<'a>(
//         &mut self,
//         eid: EntityId,
//         attr: &Attribute,
//         value: &'a JsonValue,
//         action: TxAction,
//         since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<()> {
//         if attr.cardinality.is_many() && attr.val_type != AttributeTyping::List && value.is_array()
//         {
//             for cur_val in value.as_array().unwrap() {
//                 self.parse_tx_request_inner(
//                     eid,
//                     attr,
//                     cur_val,
//                     action,
//                     since,
//                     temp_id_ctx,
//                     collected,
//                 )?;
//             }
//             return Ok(());
//         }
//
//         ensure!(
//             action == TxAction::Put || eid.is_perm(),
//             "using temp id instead of perm id for op {} is not allow",
//             action
//         );
//
//         let v = if let JsonValue::Object(inner) = value {
//             self.parse_tx_component(attr, inner, action, since, temp_id_ctx, collected)?
//         } else {
//             attr.coerce_value(DataValue::from(value), temp_id_ctx)?
//         };
//
//         collected.push(Quintuple {
//             triple: Triple {
//                 id: eid,
//                 attr: attr.id,
//                 value: v,
//             },
//             action,
//             validity: since,
//         });
//
//         Ok(())
//     }
//     fn parse_tx_component<'a>(
//         &mut self,
//         parent_attr: &Attribute,
//         comp: &'a Map<String, JsonValue>,
//         action: TxAction,
//         since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<DataValue> {
//         ensure!(
//             action == TxAction::Put,
//             "component shorthand can only be use for 'put', got {}",
//             action
//         );
//         let (eid, has_unique_attr) =
//             self.parse_tx_request_obj(comp, true, action, since, temp_id_ctx, collected)?;
//         ensure!(has_unique_attr || parent_attr.val_type == AttributeTyping::Component,
//             "component shorthand must contain at least one unique/identity field for non-component refs");
//         Ok(eid.as_datavalue())
//     }
//     fn parse_tx_request_arr<'a>(
//         &mut self,
//         item: &'a [JsonValue],
//         action: TxAction,
//         since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<()> {
//         match item {
//             [eid] => {
//                 ensure!(
//                     action == TxAction::Retract,
//                     "singlet action only allowed for 'retract', got {}",
//                     action
//                 );
//                 let eid = eid
//                     .as_u64()
//                     .ok_or_else(|| miette!("cannot parse {} as entity id", eid))?;
//                 let eid = EntityId(eid);
//                 ensure!(eid.is_perm(), "expected perm entity id, got {:?}", eid);
//                 collected.push(Quintuple {
//                     triple: Triple {
//                         id: eid,
//                         attr: AttrId(0),
//                         value: DataValue::Null,
//                     },
//                     action: TxAction::RetractAllE,
//                     validity: since,
//                 });
//                 Ok(())
//             }
//             [eid, attr] => {
//                 ensure!(
//                     action == TxAction::Retract,
//                     "double only allowed for 'retract', got {}",
//                     action
//                 );
//                 let kw: Symbol = attr.try_into()?;
//                 let attr = self
//                     .attr_by_name(&kw)?
//                     .ok_or_else(|| miette!("attribute not found {}", kw))?;
//
//                 let eid = eid
//                     .as_u64()
//                     .ok_or_else(|| miette!("cannot parse {} as entity id", eid))?;
//                 let eid = EntityId(eid);
//                 ensure!(eid.is_perm(), "expect perm entity id, got {:?}", eid);
//                 collected.push(Quintuple {
//                     triple: Triple {
//                         id: eid,
//                         attr: attr.id,
//                         value: DataValue::Null,
//                     },
//                     action: TxAction::RetractAllEA,
//                     validity: since,
//                 });
//                 Ok(())
//             }
//             [eid, attr_kw, val] => {
//                 self.parse_tx_triple(eid, attr_kw, val, action, since, temp_id_ctx, collected)
//             }
//             arr => bail!("bad triple in tx: {:?}", arr),
//         }
//     }
//     fn parse_tx_triple<'a>(
//         &mut self,
//         eid: &JsonValue,
//         attr_kw: &JsonValue,
//         val: &'a JsonValue,
//         action: TxAction,
//         since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<()> {
//         let kw: Symbol = attr_kw.try_into()?;
//         let attr = self
//             .attr_by_name(&kw)?
//             .ok_or_else(|| miette!("attribute not found: {}", kw))?;
//         if attr.cardinality.is_many() && attr.val_type != AttributeTyping::List && val.is_array() {
//             for cur_val in val.as_array().unwrap() {
//                 self.parse_tx_triple(eid, attr_kw, cur_val, action, since, temp_id_ctx, collected)?;
//             }
//             return Ok(());
//         }
//
//         let id = if attr.indexing.is_unique_index() {
//             let value = if let JsonValue::Object(inner) = val {
//                 self.parse_tx_component(&attr, inner, action, since, temp_id_ctx, collected)?
//             } else {
//                 attr.coerce_value(val.into(), temp_id_ctx)?
//             };
//             let existing = self.eid_by_unique_av(&attr, &value, since)?;
//             match existing {
//                 None => {
//                     if let Some(i) = eid.as_u64() {
//                         let id = EntityId(i);
//                         ensure!(id.is_perm(), "temp id not allowed here, found {:?}", id);
//                         id
//                     } else if let Some(s) = eid.as_str() {
//                         temp_id_ctx.str2tempid(s, true)
//                     } else {
//                         temp_id_ctx.unnamed_tempid()
//                     }
//                 }
//                 Some(existing_id) => {
//                     if let Some(i) = eid.as_u64() {
//                         let id = EntityId(i);
//                         ensure!(id.is_perm(), "temp id not allowed here, found {:?}", id);
//                         ensure!(
//                             existing_id == id,
//                             "conflicting id for identity value: {:?} vs {:?}",
//                             existing_id,
//                             id
//                         );
//                         id
//                     } else if eid.is_string() {
//                         bail!(
//                             "specifying temp_id string {} together with unique constraint",
//                             eid
//                         );
//                     } else {
//                         existing_id
//                     }
//                 }
//             }
//         } else if let Some(i) = eid.as_u64() {
//             let id = EntityId(i);
//             ensure!(id.is_perm(), "temp id not allowed here, found {:?}", id);
//             id
//         } else if let Some(s) = eid.as_str() {
//             temp_id_ctx.str2tempid(s, true)
//         } else {
//             temp_id_ctx.unnamed_tempid()
//         };
//
//         if attr.val_type != AttributeTyping::List && val.is_array() {
//             let vals = val.as_array().unwrap();
//             for val in vals {
//                 self.parse_tx_request_inner(id, &attr, val, action, since, temp_id_ctx, collected)?;
//             }
//             Ok(())
//         } else {
//             self.parse_tx_request_inner(id, &attr, val, action, since, temp_id_ctx, collected)
//         }
//     }
//     fn parse_tx_request_obj<'a>(
//         &mut self,
//         item: &'a Map<String, JsonValue>,
//         is_sub_component: bool,
//         action: TxAction,
//         since: Validity,
//         temp_id_ctx: &mut TempIdCtx,
//         collected: &mut Vec<Quintuple>,
//     ) -> Result<(EntityId, bool)> {
//         let mut pairs = Vec::with_capacity(item.len());
//         let mut eid = None;
//         let mut has_unique_attr = false;
//         let mut has_identity_attr = false;
//         for (k, v) in item {
//             if k != PERM_ID_FIELD && k != TEMP_ID_FIELD {
//                 let kw = (k as &str).into();
//                 let attr = self
//                     .attr_by_name(&kw)?
//                     .ok_or_else(|| miette!("attribute '{}' not found", kw))
//                     .with_context(|| format!("cannot process {}", json!(item)))?;
//                 has_unique_attr = has_unique_attr || attr.indexing.is_unique_index();
//                 has_identity_attr = has_identity_attr || attr.indexing == AttributeIndex::Identity;
//                 if attr.indexing == AttributeIndex::Identity {
//                     let value = if let JsonValue::Object(inner) = v {
//                         self.parse_tx_component(
//                             &attr,
//                             inner,
//                             action,
//                             since,
//                             temp_id_ctx,
//                             collected,
//                         )?
//                     } else {
//                         attr.coerce_value(v.into(), temp_id_ctx)?
//                     };
//                     let existing_id = self.eid_by_unique_av(&attr, &value, since)?;
//                     match existing_id {
//                         None => {}
//                         Some(existing_eid) => {
//                             if let Some(prev_eid) = eid {
//                                 ensure!(
//                                     existing_eid == prev_eid,
//                                     "conflicting entity id: {:?} vs {:?}",
//                                     existing_eid,
//                                     prev_eid
//                                 );
//                             }
//                             eid = Some(existing_eid)
//                         }
//                     }
//                 }
//                 pairs.push((attr, v));
//             }
//         }
//         if let Some(given_id) = item.get(PERM_ID_FIELD) {
//             let given_id = given_id
//                 .as_u64()
//                 .ok_or_else(|| miette!("unable to interpret {} as entity id", given_id))?;
//             let given_id = EntityId(given_id);
//             ensure!(
//                 given_id.is_perm(),
//                 "temp id not allowed here, found {:?}",
//                 given_id
//             );
//             if let Some(prev_id) = eid {
//                 ensure!(
//                     prev_id == given_id,
//                     "conflicting entity id give: {:?} vs {:?}",
//                     prev_id,
//                     given_id
//                 );
//             }
//             eid = Some(given_id);
//         }
//         if let Some(temp_id) = item.get(TEMP_ID_FIELD) {
//             ensure!(
//                 eid.is_none(),
//                 "conflicting entity id given: {:?} vs {}",
//                 eid.unwrap(),
//                 temp_id
//             );
//             let temp_id_str = temp_id
//                 .as_str()
//                 .ok_or_else(|| miette!("unable to interpret {} as temp id", temp_id))?;
//             eid = Some(temp_id_ctx.str2tempid(temp_id_str, true));
//         }
//         let eid = match eid {
//             Some(eid) => eid,
//             None => temp_id_ctx.unnamed_tempid(),
//         };
//         ensure!(
//             action == TxAction::Put || eid.is_perm(),
//             "temp id {:?} not allowed for {}",
//             eid,
//             action
//         );
//         if !is_sub_component {
//             ensure!(
//                 action != TxAction::Put || !eid.is_perm() || has_identity_attr,
//                 "upsert requires identity attribute present"
//             );
//             for (attr, v) in pairs {
//                 self.parse_tx_request_inner(eid, &attr, v, action, since, temp_id_ctx, collected)?;
//             }
//         } else if !eid.is_perm() {
//             for (attr, v) in pairs {
//                 self.parse_tx_request_inner(eid, &attr, v, action, since, temp_id_ctx, collected)?;
//             }
//         } else {
//             for (attr, _v) in pairs {
//                 ensure!(
//                     attr.indexing.is_unique_index(),
//                     "cannot use non-unique attribute {} to specify entity",
//                     attr.name
//                 );
//             }
//         }
//         Ok((eid, has_unique_attr))
//     }
// }
