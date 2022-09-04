// use std::cmp::max;
//
// use miette::{miette, bail, Result};
// use itertools::Itertools;
// use serde_json::Map;
//
// use crate::data::attr::AttributeCardinality;
// use crate::data::json::JsonValue;
// use crate::data::symb::Symbol;
// use crate::data::value::DataValue;
// use crate::query::pull::{AttrPullSpec, PullSpec, PullSpecs};
// use crate::runtime::transact::SessionTx;
//
// impl SessionTx {
//     pub(crate) fn parse_pull(&mut self, desc: &JsonValue, depth: usize) -> Result<PullSpecs> {
//         if let Some(inner) = desc.as_array() {
//             let mut ret: PullSpecs = inner
//                 .iter()
//                 .map(|v| self.parse_pull_element(v, depth))
//                 .try_collect()?;
//             // the sort is necessary to put recursive queries last
//             ret.sort();
//             Ok(ret)
//         } else {
//             bail!("pull definition: expect array, got {}", desc);
//         }
//     }
//     pub(crate) fn parse_pull_element(
//         &mut self,
//         desc: &JsonValue,
//         depth: usize,
//     ) -> Result<PullSpec> {
//         match desc {
//             JsonValue::String(s) if s == "*" => Ok(PullSpec::PullAll),
//             JsonValue::String(s) if s == "_id" => Ok(PullSpec::PullId("_id".into())),
//             JsonValue::String(s) => {
//                 let input_symb = Symbol::from(s.as_ref());
//                 let reverse = input_symb.0.starts_with('<');
//                 let symb = if reverse {
//                     Symbol::from(input_symb.0.strip_prefix('<').unwrap())
//                 } else {
//                     input_symb.clone()
//                 };
//                 let attr = self
//                     .attr_by_name(&symb)?
//                     .ok_or_else(|| miette!("attribute {} not found", symb))?;
//                 let cardinality = attr.cardinality;
//                 Ok(PullSpec::Attr(AttrPullSpec {
//                     attr,
//                     default_val: DataValue::Null,
//                     reverse,
//                     name: input_symb,
//                     cardinality,
//                     take: None,
//                     nested: vec![],
//                     recursive: false,
//                     recursion_limit: None,
//                     recursion_depth: 0,
//                 }))
//             }
//             JsonValue::Object(m) => self.parse_pull_obj(m, depth),
//             v => bail!("pull element: expect string or object, got {}", v),
//         }
//     }
//     pub(crate) fn parse_pull_obj(
//         &mut self,
//         desc: &Map<String, JsonValue>,
//         depth: usize,
//     ) -> Result<PullSpec> {
//         let mut default_val = DataValue::Null;
//         let mut as_override = None;
//         let mut take = None;
//         let mut cardinality_override = None;
//         let mut input_symb = None;
//         let mut sub_target = vec![];
//         let mut recursive = false;
//         let mut recursion_limit = None;
//         let mut pull_id = false;
//         let mut recursion_depth = 0;
//
//         for (k, v) in desc {
//             match k as &str {
//                 "as" => {
//                     as_override =
//                         Some(Symbol::from(v.as_str().ok_or_else(|| {
//                             miette!("expect 'as' field to be string, got {}", v)
//                         })?))
//                 }
//                 "limit" => {
//                     take = Some(v.as_u64().ok_or_else(|| {
//                         miette!("expect 'limit field to be non-negative integer, got {}", v)
//                     })? as usize)
//                 }
//                 "cardinality" => {
//                     cardinality_override =
//                         Some(AttributeCardinality::try_from(v.as_str().ok_or_else(
//                             || miette!("expect 'cardinality' field to be string, got {}", v),
//                         )?)?)
//                 }
//                 "default" => default_val = DataValue::from(v),
//                 "pull" => {
//                     let v = v
//                         .as_str()
//                         .ok_or_else(|| miette!("expect 'pull' field to be string, got {}", v))?;
//                     if v == "_id" {
//                         pull_id = true
//                     } else {
//                         input_symb = Some(Symbol::from(v));
//                     }
//                 }
//                 "recurse" => {
//                     if let Some(u) = v.as_u64() {
//                         recursion_limit = Some(u as usize);
//                     } else if let Some(b) = v.as_bool() {
//                         if !b {
//                             continue;
//                         }
//                     } else {
//                         bail!(
//                             "expect 'recurse' field to be non-negative integer or boolean, got {}",
//                             v
//                         );
//                     }
//                     recursive = true;
//                 }
//                 "depth" => {
//                     recursion_depth = v.as_u64().ok_or_else(|| {
//                         miette!("expect 'depth' field to be non-negative integer, got {}", v)
//                     })? as usize
//                 }
//                 "spec" => {
//                     sub_target = {
//                         if let Some(arr) = v.as_array() {
//                             arr.clone()
//                         } else {
//                             bail!("expect 'spec' field to be an array, got {}", v);
//                         }
//                     };
//                 }
//                 v => {
//                     bail!("unexpected pull spec key {}", v);
//                 }
//             }
//         }
//
//         if pull_id {
//             return Ok(PullSpec::PullId(
//                 as_override.unwrap_or_else(|| "_id".into()),
//             ));
//         }
//
//         if input_symb.is_none() {
//             bail!("no target key in pull definition");
//         }
//
//         let input_symb = input_symb.unwrap();
//
//         let reverse = input_symb.0.starts_with('<');
//         let symb = if reverse {
//             Symbol::from(input_symb.0.strip_prefix('<').unwrap())
//         } else {
//             input_symb.clone()
//         };
//         let attr = self
//             .attr_by_name(&symb)?
//             .ok_or_else(|| miette!("attribute not found: {}", symb))?;
//         let cardinality = cardinality_override.unwrap_or(attr.cardinality);
//         let nested = self.parse_pull(&JsonValue::Array(sub_target), depth + 1)?;
//
//         if recursive {
//             recursion_depth = max(recursion_depth, 1);
//         }
//
//         let default_val = if default_val == DataValue::Null {
//             default_val
//         } else {
//             attr.val_type.coerce_value(default_val)?
//         };
//
//         Ok(PullSpec::Attr(AttrPullSpec {
//             attr,
//             default_val,
//             reverse,
//             name: as_override.unwrap_or(input_symb),
//             cardinality,
//             take,
//             nested,
//             recursive,
//             recursion_limit,
//             recursion_depth,
//         }))
//     }
// }
