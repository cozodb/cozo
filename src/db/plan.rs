use std::collections::btree_map::Entry;
use crate::db::engine::Session;
use crate::db::iterator::{ChainJoinKind, ExecPlan, IteratorSlot, OutputItPlan, TableRowGetter};
use crate::db::query::{EdgeOrNodeEl, EdgeOrNodeKind, FromEl, Selection};
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

fn merge_accessor_map(mut left: AccessorMap, right: AccessorMap) -> AccessorMap {
    // println!("Before {:?} {:?}", left, right);
    for (k, vs) in right.into_iter() {
        let entry = left.entry(k);
        match &entry {
            Entry::Vacant(_) => {
                entry.or_insert(vs);
            }
            Entry::Occupied(_) => {
                entry.and_modify(|existing| existing.extend(vs));
            }
        }
    }
    // println!("After {:?}", left);
    left
}

fn convert_to_relative_accessor_map(amap: AccessorMap) -> AccessorMap {
    // TODO this only handles the simplest case
    fn convert_inner(inner: BTreeMap<String, (TableId, ColId)>) -> BTreeMap<String, (TableId, ColId)> {
        inner.into_iter().map(|(k, (_tid, cid))| {
            (k, (TableId::new(false, 0), cid))
        }).collect()
    }
    amap.into_iter().map(|(k, v)| (k, convert_inner(v))).collect()
}

fn shift_accessor_map(amap: AccessorMap, (keyshift, valshift): (usize, usize)) -> AccessorMap {
    let shift_inner = |inner: BTreeMap<String, (TableId, ColId)>| -> BTreeMap<String, (TableId, ColId)> {
        inner.into_iter().map(|(k, (tid, cid))| {
            (k, (TableId::new(tid.in_root, tid.id + if cid.is_key { keyshift as i64 } else { valshift as i64 }), cid))
        }).collect()
    };
    amap.into_iter().map(|(k, v)| (k, shift_inner(v))).collect()
}

impl<'a> Session<'a> {
    pub fn reify_intermediate_plan(&'a self, plan: ExecPlan<'a>) -> Result<ExecPlan<'a>> {
        self.do_reify_intermediate_plan(plan).map(|v| {
            // println!("Accessor map {:#?}", v.1);
            v.0
        })
    }
    fn do_reify_intermediate_plan(&'a self, plan: ExecPlan<'a>) -> Result<(ExecPlan<'a>, AccessorMap)> {
        let res = match plan {
            ExecPlan::NodeItPlan { info, binding, .. } => {
                let amap = match &binding {
                    None => Default::default(),
                    Some(binding) => {
                        let amap = self.node_accessor_map(binding, &info);
                        convert_to_relative_accessor_map(amap)
                    }
                };
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
            ExecPlan::EdgeItPlan { info, binding, .. } => {
                let amap = match &binding {
                    None => Default::default(),
                    Some(binding) => {
                        let amap = self.edge_accessor_map(&binding, &info);
                        convert_to_relative_accessor_map(amap)
                    }
                };
                let it = if info.table_id.in_root {
                    self.txn.iterator(true, &self.perm_cf)
                } else {
                    self.txn.iterator(true, &self.temp_cf)
                };
                let it = IteratorSlot::Reified(it);
                let plan = ExecPlan::EdgeItPlan {
                    it,
                    info,
                    binding,
                };
                (plan, amap)
            }
            ExecPlan::EdgeBwdItPlan { .. } => {
                todo!()
            }
            ExecPlan::EdgeKeyOnlyBwdItPlan { .. } => todo!(),
            ExecPlan::KeySortedWithAssocItPlan { .. } => todo!(),
            ExecPlan::CartesianProdItPlan { left, right } => {
                let (l_plan, l_map) = self.do_reify_intermediate_plan(*left)?;
                let (r_plan, r_map) = self.do_reify_intermediate_plan(*right)?;
                let r_map = shift_accessor_map(r_map, l_plan.tuple_widths());
                let plan = ExecPlan::CartesianProdItPlan {
                    left: l_plan.into(),
                    right: r_plan.into(),
                };
                (plan, merge_accessor_map(l_map, r_map))
            }
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
            ExecPlan::ChainJoinItPlan { .. } => {
                todo!()
            }
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
    fn convert_from_data_to_plan(&self, from_data: Vec<FromEl>) -> Result<ExecPlan> {
        let convert_el = |el|
            match el {
                FromEl::Simple(el) => {
                    match el.info.kind {
                        DataKind::Node => {
                            Ok(ExecPlan::NodeItPlan {
                                it: IteratorSlot::Dummy,
                                info: el.info,
                                binding: Some(el.binding),
                            })
                        }
                        DataKind::Edge => {
                            Ok(ExecPlan::EdgeItPlan {
                                it: IteratorSlot::Dummy,
                                info: el.info,
                                binding: Some(el.binding),
                            })
                        }
                        _ => Err(LogicError("Wrong type for table binding".to_string()))
                    }
                }
                FromEl::Chain(ch) => {
                    let mut it = ch.into_iter();
                    let nxt = it.next().ok_or_else(|| LogicError("Empty chain not allowed".to_string()))?;
                    let mut prev_kind = nxt.kind;
                    let mut prev_left_outer = nxt.left_outer_marker;
                    let mut plan = match prev_kind {
                        EdgeOrNodeKind::Node => {
                            ExecPlan::NodeItPlan {
                                it: IteratorSlot::Dummy,
                                info: nxt.info,
                                binding: nxt.binding,
                            }
                        }
                        EdgeOrNodeKind::FwdEdge => {
                            ExecPlan::EdgeItPlan {
                                it: IteratorSlot::Dummy,
                                info: nxt.info,
                                binding: nxt.binding,
                            }
                        }
                        EdgeOrNodeKind::BwdEdge => {
                            ExecPlan::EdgeBwdItPlan {
                                it: IteratorSlot::Dummy,
                                info: nxt.info,
                                binding: nxt.binding,
                                getter: TableRowGetter::Dummy
                            }
                        }
                    };
                    for el in it {
                        plan = ExecPlan::ChainJoinItPlan {
                            left: plan.into(),
                            right: TableRowGetter::Dummy,
                            right_info: el.info,
                            kind: match (prev_kind, el.kind) {
                                (EdgeOrNodeKind::Node, EdgeOrNodeKind::FwdEdge) => ChainJoinKind::NodeToFwdEdge,
                                (EdgeOrNodeKind::Node, EdgeOrNodeKind::BwdEdge) => ChainJoinKind::NodeToBwdEdge,
                                (EdgeOrNodeKind::FwdEdge, EdgeOrNodeKind::Node) => ChainJoinKind::FwdEdgeToNode,
                                (EdgeOrNodeKind::BwdEdge, EdgeOrNodeKind::Node) => ChainJoinKind::BwdEdgeToNode,
                                _ => unreachable!()
                            },
                            left_outer: prev_left_outer,
                            right_outer: el.right_outer_marker
                        };

                        prev_kind = el.kind;
                        prev_left_outer = el.left_outer_marker;
                    }
                    println!("{:#?}", plan);
                    Ok(plan)
                }
            };
        let mut from_data = from_data.into_iter();
        let fst = from_data.next().ok_or_else(||
            LogicError("Empty from clause".to_string()))?;
        let mut res = convert_el(fst)?;
        for nxt in from_data {
            let nxt = convert_el(nxt)?;
            res = ExecPlan::CartesianProdItPlan {
                left: res.into(),
                right: nxt.into(),
            };
        }
        Ok(res)
    }
    pub(crate) fn node_accessor_map(
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
    pub(crate) fn edge_accessor_map(
        &self,
        binding: &str,
        info: &TableInfo,
    ) -> AccessorMap {
        let mut ret = BTreeMap::new();
        let src_key_len = info.src_key_typing.len();
        let dst_key_len = info.dst_key_typing.len();
        for (i, (k, _)) in info.src_key_typing.iter().enumerate() {
            ret.insert("_src_".to_string() + k, (info.table_id, (true, 1 + i).into()));
        }
        for (i, (k, _)) in info.dst_key_typing.iter().enumerate() {
            ret.insert("_dst_".to_string() + k, (info.table_id, (true, 2 + src_key_len + i).into()));
        }
        for (i, (k, _)) in info.key_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (true, 2 + src_key_len + dst_key_len + i).into()));
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
            binding: None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn from_data() {}
}