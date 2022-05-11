use crate::db::engine::Session;
use crate::db::iterator::{ExecPlan, IteratorSlot, OutputItPlan};
use crate::db::query::{FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::error::Result;
use crate::parser::Rule;
use crate::relation::value::{StaticValue, Value};
use cozorocks::IteratorPtr;
use pest::iterators::Pair;
use std::collections::BTreeMap;
use crate::error::CozoError::LogicError;
use crate::relation::data::DataKind;

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum OuterJoinType {
    LeftJoin,
    RightJoin,
    FullOuterJoin,
}

pub type AccessorMap = BTreeMap<String, BTreeMap<String, (TableId, ColId)>>;

impl<'a> Session<'a> {
    pub fn reify_intermediate_plan(&'a self, plan: ExecPlan<'a>) -> Result<ExecPlan<'a>> {
        self.do_reify_intermediate_plan(plan).map(|v| v.0)
    }
    fn convert_to_relative_amap(&self, amap: AccessorMap) -> AccessorMap {
        // TODO this only handles the simplest case
        fn convert_inner(inner: BTreeMap<String, (TableId, ColId)>) -> BTreeMap<String, (TableId, ColId)> {
            inner.into_iter().map(|(k, (_tid, cid))| {
                (k, (TableId::new(false, 0), cid))
            }).collect()
        }
        amap.into_iter().map(|(k, v)| (k, convert_inner(v))).collect()
    }
    fn do_reify_intermediate_plan(&'a self, plan: ExecPlan<'a>) -> Result<(ExecPlan<'a>, AccessorMap)> {
        let res = match plan {
            ExecPlan::NodeItPlan { info, binding, .. } => {
                let amap = self.base_relation_to_accessor_map(&binding, &info);
                let amap = self.convert_to_relative_amap(amap);
                let it = if info.table_id.in_root {
                    self.txn.iterator(true, &self.perm_cf)
                } else {
                    self.txn.iterator(true, &self.temp_cf)
                };
                let it = IteratorSlot::Reified(it);
                let plan = ExecPlan::NodeItPlan {
                    it,
                    info,
                    binding,
                };
                (plan, amap)
            }
            ExecPlan::EdgeItPlan { .. } => todo!(),
            ExecPlan::EdgeKeyOnlyBwdItPlan { .. } => todo!(),
            ExecPlan::KeySortedWithAssocItPlan { .. } => todo!(),
            ExecPlan::CartesianProdItPlan { .. } => todo!(),
            ExecPlan::MergeJoinItPlan { .. } => todo!(),
            ExecPlan::OuterMergeJoinItPlan { .. } => todo!(),
            ExecPlan::KeyedUnionItPlan { .. } => todo!(),
            ExecPlan::KeyedDifferenceItPlan { .. } => todo!(),
            ExecPlan::FilterItPlan { source: it, filter } => {
                let (inner, amap) = self.do_reify_intermediate_plan(*it)?;
                let (_, filter) = self.partial_eval(filter, &Default::default(), &amap)?;
                let plan = ExecPlan::FilterItPlan {
                    source: inner.into(),
                    filter,
                };
                (plan, amap)
            }
            ExecPlan::EvalItPlan { source: it, keys, vals } => {
                let (inner, amap) = self.do_reify_intermediate_plan(*it)?;
                let keys = keys.into_iter().map(|(k, v)| -> Result<_> {
                    let (_, v) = self.partial_eval(v, &Default::default(), &amap)?;
                    Ok((k, v))
                }).collect::<Result<Vec<_>>>()?;
                let vals = vals.into_iter().map(|(k, v)| -> Result<_> {
                    let (_, v) = self.partial_eval(v, &Default::default(), &amap)?;
                    Ok((k, v))
                }).collect::<Result<Vec<_>>>()?;
                let plan = ExecPlan::EvalItPlan {
                    source: inner.into(),
                    keys,
                    vals,
                };
                (plan, amap)
            }
            ExecPlan::BagsUnionIt { .. } => todo!(),
        };
        Ok(res)
    }
    pub fn reify_output_plan(&'a self, plan: ExecPlan<'a>) -> Result<OutputItPlan<'a>> {
        let plan = self.reify_intermediate_plan(plan)?;
        let plan = match plan {
            ExecPlan::EvalItPlan { source: it, mut keys, vals } => {
                keys.extend(vals);
                let filter = Value::Dict(keys.into_iter().map(|(k, v)| (k.into(), v)).collect());
                OutputItPlan {
                    source: *it,
                    value: filter,
                }
            }
            _plan => {
                todo!()
            }
        };
        Ok(plan)
    }
    pub fn query_to_plan(&self, pair: Pair<Rule>) -> Result<ExecPlan> {
        let mut pairs = pair.into_inner();
        let from_data = self.parse_from_pattern(pairs.next().unwrap())?;
        let mut nxt = pairs.next().unwrap();
        let where_data = match nxt.as_rule() {
            Rule::where_pattern => {
                let r = self.parse_where_pattern(nxt)?.to_static();
                nxt = pairs.next().unwrap();
                r
            }
            _ => true.into(),
        };
        let select_data = self.parse_select_pattern(nxt)?;
        let plan = self.convert_from_data_to_plan(from_data)?;
        let plan = self.convert_where_data_to_plan(plan, where_data)?;
        self.convert_select_data_to_plan(plan, select_data)
    }
    fn convert_from_data_to_plan(&self, mut from_data: Vec<FromEl>) -> Result<ExecPlan> {
        let res = match from_data.pop().unwrap() {
            FromEl::Simple(el) => {
                // println!(
                //     "{:#?}",
                //     self.base_relation_to_accessor_map(&el.table, &el.binding, &el.info)
                // );
                match el.info.kind {
                    DataKind::Node => {
                        ExecPlan::NodeItPlan {
                            it: IteratorSlot::Dummy,
                            info: el.info,
                            binding: el.binding,
                        }
                    }
                    DataKind::Edge => {
                        ExecPlan::EdgeItPlan {
                            it: IteratorSlot::Dummy,
                            info: el.info,
                            binding: el.binding,
                        }
                    }
                    _ => return Err(LogicError("Wrong type for table binding".to_string()))
                }
            }
            FromEl::Chain(_) => todo!(),
        };
        Ok(res)
    }
    pub(crate) fn base_relation_to_accessor_map(
        &self,
        binding: &str,
        info: &TableInfo,
    ) -> AccessorMap {
        let mut ret = BTreeMap::new();
        for (i, (k, _)) in info.key_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (true, i).into()));
        }
        for (i, (k, _)) in info.val_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (false, i).into()));
        }
        for assoc in &info.associates {
            for (i, (k, _)) in assoc.key_typing.iter().enumerate() {
                ret.insert(k.into(), (assoc.table_id, (true, i).into()));
            }
            for (i, (k, _)) in assoc.val_typing.iter().enumerate() {
                ret.insert(k.into(), (assoc.table_id, (false, i).into()));
            }
        }
        BTreeMap::from([(binding.to_string(), ret)])
    }
    fn convert_where_data_to_plan<'b>(
        &self,
        plan: ExecPlan<'b>,
        where_data: StaticValue,
    ) -> Result<ExecPlan<'b>> {
        let where_data = self.partial_eval(where_data, &Default::default(), &Default::default());
        let plan = match where_data?.1 {
            Value::Bool(true) => plan,
            v => {
                ExecPlan::FilterItPlan {
                    source: Box::new(plan),
                    filter: v,
                }
            }
        };
        Ok(plan)
    }
    fn convert_select_data_to_plan<'b>(
        &self,
        plan: ExecPlan<'b>,
        select_data: Selection,
    ) -> Result<ExecPlan<'b>> {
        Ok(ExecPlan::EvalItPlan {
            source: Box::new(plan),
            keys: select_data.keys,
            vals: select_data.vals,
        })
    }

    pub fn raw_iterator(&self, in_root: bool) -> IteratorPtr {
        if in_root {
            self.txn.iterator(true, &self.perm_cf)
        } else {
            self.txn.iterator(false, &self.temp_cf)
        }
    }

    // internal temp use
    pub fn iter_node(&self, tid: TableId) -> ExecPlan {
        let it = self.raw_iterator(tid.in_root);
        ExecPlan::NodeItPlan {
            it: IteratorSlot::Reified(it),
            info: TableInfo {
                kind: DataKind::Data,
                table_id: tid,
                src_table_id: Default::default(),
                dst_table_id: Default::default(),
                data_keys: Default::default(),
                key_typing: vec![],
                val_typing: vec![],
                src_key_typing: vec![],
                dst_key_typing: vec![],
                associates: vec![],
            },
            binding: "".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn from_data() {}
}