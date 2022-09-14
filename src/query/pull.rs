use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{ensure, Diagnostic, Result};
use serde_json::{json, Map};
use thiserror::Error;

use cozorocks::CfHandle::Snd;

use crate::data::attr::Attribute;
use crate::data::id::{EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::program::RelationOp;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::pull::OutPullSpec;
use crate::parse::SourceSpan;
use crate::runtime::relation::RelationMetadata;
use crate::runtime::transact::SessionTx;
use crate::transact::meta::AttrNotFoundError;

struct OutPullSpecWithAttr {
    attr: Attribute,
    reverse: bool,
    subfields: Vec<OutPullSpecWithAttr>,
    vld: Validity,
    span: SourceSpan,
}

impl OutPullSpec {
    fn hydrate(&self, tx: &SessionTx, vld: Validity) -> Result<OutPullSpecWithAttr> {
        let attr = tx
            .attr_by_name(&self.attr.name)?
            .ok_or_else(|| AttrNotFoundError(self.attr.to_string()))?;
        Ok(OutPullSpecWithAttr {
            attr,
            reverse: self.reverse,
            subfields: self
                .subfields
                .iter()
                .map(|v| v.hydrate(tx, vld))
                .try_collect()?,
            vld,
            span: self.span,
        })
    }
}

impl SessionTx {
    pub(crate) fn execute_relation<'a>(
        &'a mut self,
        res_iter: impl Iterator<Item = Result<Tuple>> + 'a,
        op: RelationOp,
        meta: &RelationMetadata,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut to_clear = None;
        if op == RelationOp::ReDerive {
            if let Ok(c) = self.destroy_relation(&meta.name) {
                to_clear = Some(c);
            }
        }
        let relation_store = if op == RelationOp::ReDerive || op == RelationOp::Create {
            self.create_relation(meta.clone())?
        } else {
            let found = self.get_relation(&meta.name)?;

            #[derive(Debug, Error, Diagnostic)]
            #[error("Attempting to write into relation {0} of arity {1} with data of arity {2}")]
            #[diagnostic(code(eval::relation_arity_mismatch))]
            struct RelationArityMismatch(String, usize, usize);

            ensure!(
                found.arity == meta.arity,
                RelationArityMismatch(found.name.to_string(), found.arity, meta.arity)
            );
            found
        };
        if op == RelationOp::Retract {
            for data in res_iter {
                let data = data?;
                let encoded = data.encode_as_key(relation_store.id);
                self.tx.del(&encoded, Snd)?;
            }
        } else {
            for data in res_iter {
                let data = data?;
                let encoded = data.encode_as_key(relation_store.id);
                self.tx.put(&encoded, &[], Snd)?;
            }
        }
        Ok(to_clear)
    }
    fn run_pull_on_item(&self, id: EntityId, specs: &[OutPullSpecWithAttr]) -> Result<JsonValue> {
        let mut ret_map = Map::default();
        ret_map.insert("_id".to_string(), json!(id.0));
        for spec in specs {
            self.run_pull_spec_on_item(id, spec, &mut ret_map)?;
        }

        Ok(json!(ret_map))
    }
    fn run_pull_spec_on_item(
        &self,
        id: EntityId,
        spec: &OutPullSpecWithAttr,
        coll: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Cannot pull on {0}: attribute is not of type 'ref'")]
        #[diagnostic(code(eval::reverse_pull_on_non_ref))]
        struct PullOnNonRef(String, #[label] SourceSpan);

        if spec.reverse {
            ensure!(
                spec.attr.val_type.is_ref_type(),
                PullOnNonRef(spec.attr.name.to_string(), spec.span)
            );
            let back_res: Vec<_> = if spec.attr.with_history {
                self.triple_vref_a_before_scan(spec.attr.id,id,  spec.vld)
                    .map_ok(|(_, _, e)| e)
                    .try_collect()?
            } else {
                self.triple_vref_a_scan(spec.attr.id, id)
                    .map_ok(|(_, _, e)| e)
                    .try_collect()?
            };
            let name = format!("<{}", spec.attr.name);
            if spec.subfields.is_empty() {
                coll.insert(
                    name,
                    back_res
                        .into_iter()
                        .map(|v| JsonValue::from(v.as_datavalue()))
                        .collect(),
                );
            } else {
                let maps: JsonValue = back_res
                    .iter()
                    .map(|eid| self.run_pull_on_item(*eid, &spec.subfields))
                    .try_collect()?;
                coll.insert(name, maps);
            }
        } else {
            let res: Vec<_> = if spec.attr.with_history {
                self.triple_ae_before_scan(spec.attr.id, id, spec.vld)
                    .map_ok(|(_, _, e)| e)
                    .try_collect()?
            } else {
                self.triple_ae_scan(spec.attr.id, id)
                    .map_ok(|(_, _, e)| e)
                    .try_collect()?
            };
            let name = format!("{}", spec.attr.name);
            if spec.subfields.is_empty() {
                if spec.attr.cardinality.is_many() {
                    coll.insert(name, res.into_iter().map(|v| JsonValue::from(v)).collect());
                } else {
                    coll.insert(
                        name,
                        res.into_iter()
                            .map(|v| JsonValue::from(v))
                            .next()
                            .unwrap_or(JsonValue::Null),
                    );
                }
            } else {
                ensure!(
                    spec.attr.val_type.is_ref_type(),
                    PullOnNonRef(spec.attr.name.to_string(), spec.span)
                );
                let maps: Vec<_> = res
                    .iter()
                    .map(|dv| -> Result<_> {
                        let id = dv.get_entity_id().ok_or_else(|| {
                            #[derive(Debug, Error, Diagnostic)]
                            #[error("Cannot pull: {0:?} is not an entity ID")]
                            #[diagnostic(code(eval::non_id_for_pull))]
                            struct UnacceptableIdForPull(DataValue, #[label] SourceSpan);

                            UnacceptableIdForPull(dv.clone(), spec.span)
                        })?;
                        self.run_pull_on_item(id, &spec.subfields)
                    })
                    .try_collect()?;
                if spec.attr.cardinality.is_many() {
                    coll.insert(name, JsonValue::Array(maps));
                } else {
                    coll.insert(name, maps.into_iter().next().unwrap_or(JsonValue::Null));
                }
            }
        }

        Ok(())
    }
    pub(crate) fn run_pull_on_query_results(
        &self,
        res_iter: impl Iterator<Item = Result<Tuple>>,
        headers: Option<&[Symbol]>,
        out_spec: &BTreeMap<Symbol, (Vec<OutPullSpec>, Option<Validity>)>,
        default_vld: Validity,
    ) -> Result<Vec<JsonValue>> {
        if out_spec.is_empty() {
            Ok(res_iter
                .map_ok(|tuple| tuple.0.into_iter().map(JsonValue::from).collect())
                .try_collect()?)
        } else {
            let headers = headers.unwrap_or(&[]);
            let mut idx2pull: Vec<Option<(Vec<_>, _)>> = Vec::with_capacity(headers.len());
            for head in headers.iter() {
                match out_spec.get(head) {
                    None => idx2pull.push(None),
                    Some((os, vld)) => idx2pull.push(Some((
                        os.iter()
                            .map(|o| o.hydrate(self, vld.unwrap_or(default_vld)))
                            .try_collect()?,
                        head.clone(),
                    ))),
                }
            }
            let mut collected = vec![];
            for tuple in res_iter {
                let tuple = tuple?.0;
                let mut row_collected = Vec::with_capacity(tuple.len());
                for (idx, item) in tuple.into_iter().enumerate() {
                    if let Some((specs, symb)) = &idx2pull[idx] {
                        #[derive(Debug, Diagnostic, Error)]
                        #[error("Cannot interpret {0:?} as an entity")]
                        #[diagnostic(code(eval::bad_pull_id))]
                        #[diagnostic(help(
                            "You specified pull operation for the variable '{1}', but the value in the output \
                        stream cannot be interpreted as an entity ID (must be an integer)."
                        ))]
                        struct BadPullInputError(DataValue, String, #[label] SourceSpan);

                        let id =
                            EntityId(item.get_int().ok_or_else(|| {
                                BadPullInputError(item, symb.to_string(), symb.span)
                            })? as u64);
                        let res = self.run_pull_on_item(id, specs)?;
                        row_collected.push(res);
                    } else {
                        row_collected.push(JsonValue::from(item));
                    }
                }
                collected.push(JsonValue::Array(row_collected));
            }
            Ok(collected)
        }
    }
}
