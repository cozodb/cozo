use crate::db::engine::Session;
use crate::db::iterator::{
    BagsUnionIterator, CartesianProdIterator, EdgeIterator, EdgeKeyOnlyBwdIterator,
    EdgeToNodeChainJoinIterator, EvalIterator, FilterIterator, KeySortedWithAssocIterator,
    KeyedDifferenceIterator, KeyedUnionIterator, LimiterIterator, MergeJoinIterator,
    NodeEdgeChainKind, NodeIterator, NodeToEdgeChainJoinIterator, OuterMergeJoinIterator,
    OutputIterator, SortingMaterialization,
};
use crate::db::query::{EdgeOrNodeKind, FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::error::CozoError::LogicError;
use crate::error::Result;
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::table::MegaTuple;
use crate::relation::tuple::{OwnTuple, SliceTuple, Tuple};
use crate::relation::value::{StaticValue, Value};
use cozorocks::IteratorPtr;
use pest::iterators::Pair;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::iter;

pub enum SessionSlot<'a> {
    Dummy,
    Reified(&'a Session<'a>),
}

impl<'a> Debug for SessionSlot<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionSlot::Dummy => {
                write!(f, "DummySession")
            }
            SessionSlot::Reified(_) => {
                write!(f, "Session")
            }
        }
    }
}

pub enum IteratorSlot<'a> {
    Dummy,
    Reified(IteratorPtr<'a>),
}

impl<'a> Debug for IteratorSlot<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IteratorSlot::Dummy { .. } => write!(f, "DummyIterator"),
            IteratorSlot::Reified(_) => write!(f, "BaseIterator"),
        }
    }
}

pub enum TableRowGetterSlot<'a> {
    Dummy,
    Reified(TableRowGetter<'a>),
}

#[derive(Clone)]
pub struct TableRowGetter<'a> {
    pub sess: &'a Session<'a>,
    pub key_cache: OwnTuple,
    pub in_root: bool,
}

impl<'a> TableRowGetter<'a> {
    pub fn reset(&mut self) {
        self.key_cache.truncate_all();
    }
    pub fn get_with_tuple<T: AsRef<[u8]>>(&self, t: &T) -> Result<Option<SliceTuple>> {
        let res = self
            .sess
            .txn
            .get(
                self.in_root,
                if self.in_root {
                    &self.sess.perm_cf
                } else {
                    &self.sess.temp_cf
                },
                t,
            )?
            .map(Tuple::new);
        Ok(res)
    }
    pub fn get_with_iter<'b, T: Iterator<Item = Value<'b>>>(
        &mut self,
        vals: T,
    ) -> Result<Option<SliceTuple>> {
        for val in vals {
            self.key_cache.push_value(&val);
        }
        let val = self.sess.txn.get(
            self.in_root,
            if self.in_root {
                &self.sess.perm_cf
            } else {
                &self.sess.temp_cf
            },
            &self.key_cache,
        )?;
        Ok(val.map(Tuple::new))
    }
}

impl<'a> Debug for TableRowGetterSlot<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableRowGetterSlot::Dummy => write!(f, "DummyRowGetter"),
            TableRowGetterSlot::Reified { .. } => write!(f, "TableRowGetter"),
        }
    }
}

impl<'a> From<IteratorPtr<'a>> for IteratorSlot<'a> {
    fn from(it: IteratorPtr<'a>) -> Self {
        Self::Reified(it)
    }
}

impl<'a> IteratorSlot<'a> {
    pub fn try_get(&self) -> Result<&IteratorPtr<'a>> {
        match self {
            IteratorSlot::Dummy => Err(LogicError("Cannot iter over dummy".to_string())),
            IteratorSlot::Reified(r) => Ok(r),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ChainJoinKind {
    NodeToFwdEdge,
    NodeToBwdEdge,
    FwdEdgeToNode,
    BwdEdgeToNode,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ExecPlan<'a> {
    NodeItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: Option<String>,
    },
    EdgeItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: Option<String>,
    },
    EdgeKeyOnlyBwdItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: Option<String>,
    },
    EdgeBwdItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: Option<String>,
        getter: TableRowGetterSlot<'a>,
    },
    ChainJoinItPlan {
        left: Box<ExecPlan<'a>>,
        left_info: TableInfo,
        right: TableRowGetterSlot<'a>,
        right_info: TableInfo,
        right_binding: Option<String>,
        right_associates: Vec<(TableInfo, TableRowGetterSlot<'a>)>,
        kind: ChainJoinKind,
        left_outer: bool,
    },
    // IndexIt {it: ..}
    KeySortedWithAssocItPlan {
        main: Box<ExecPlan<'a>>,
        associates: Vec<(TableInfo, IteratorSlot<'a>)>,
        binding: Option<String>,
    },
    CartesianProdItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    MergeJoinItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
        left_keys: Vec<(TableId, ColId)>,
        right_keys: Vec<(TableId, ColId)>,
    },
    OuterMergeJoinItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
        left_keys: Vec<(TableId, ColId)>,
        right_keys: Vec<(TableId, ColId)>,
        left_outer: bool,
        right_outer: bool,
        left_len: (usize, usize),
        right_len: (usize, usize),
    },
    KeyedUnionItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    KeyedDifferenceItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    FilterItPlan {
        source: Box<ExecPlan<'a>>,
        filter: Value<'a>,
    },
    EvalItPlan {
        source: Box<ExecPlan<'a>>,
        keys: Vec<(String, Value<'a>)>,
        vals: Vec<(String, Value<'a>)>,
    },
    BagsUnionItPlan {
        bags: Vec<ExecPlan<'a>>,
    },
    LimiterItPlan {
        source: Box<ExecPlan<'a>>,
        offset: usize,
        limit: usize,
    },
    SortingMatPlan {
        source: Box<ExecPlan<'a>>,
        ordering: Vec<(bool, StaticValue)>,
        sess: SessionSlot<'a>,
    },
}

impl<'a> ExecPlan<'a> {
    pub fn tuple_widths(&self) -> (usize, usize) {
        match self {
            ExecPlan::NodeItPlan { .. } => (1, 1),
            ExecPlan::EdgeItPlan { .. } => (1, 1),
            ExecPlan::EdgeKeyOnlyBwdItPlan { .. } => (1, 0),
            ExecPlan::KeySortedWithAssocItPlan {
                main, associates, ..
            } => {
                let (k, v) = main.tuple_widths();
                (k, v + associates.len())
            }
            ExecPlan::CartesianProdItPlan { left, right } => {
                let (l1, l2) = left.tuple_widths();
                let (r1, r2) = right.tuple_widths();
                (l1 + r1, l2 + r2)
            }
            ExecPlan::MergeJoinItPlan { left, right, .. } => {
                let (l1, l2) = left.tuple_widths();
                let (r1, r2) = right.tuple_widths();
                (l1 + r1, l2 + r2)
            }
            ExecPlan::OuterMergeJoinItPlan { left, right, .. } => {
                let (l1, l2) = left.tuple_widths();
                let (r1, r2) = right.tuple_widths();
                (l1 + r1, l2 + r2)
            }
            ExecPlan::KeyedUnionItPlan { left, .. } => left.tuple_widths(),
            ExecPlan::KeyedDifferenceItPlan { left, .. } => left.tuple_widths(),
            ExecPlan::FilterItPlan { source, .. } => source.tuple_widths(),
            ExecPlan::EvalItPlan { source, .. } => source.tuple_widths(),
            ExecPlan::BagsUnionItPlan { bags } => {
                if bags.is_empty() {
                    (0, 0)
                } else {
                    bags.get(0).unwrap().tuple_widths()
                }
            }
            ExecPlan::EdgeBwdItPlan { .. } => {
                todo!()
            }
            ExecPlan::ChainJoinItPlan {
                left,
                right_associates,
                ..
            } => {
                let (l1, l2) = left.tuple_widths();
                (l1 + 1, l2 + 1 + right_associates.len())
            }
            ExecPlan::LimiterItPlan { source, .. } => source.tuple_widths(),
            ExecPlan::SortingMatPlan { source, .. } => source.tuple_widths(),
        }
    }
}

#[derive(Debug)]
pub struct OutputItPlan<'a> {
    pub source: ExecPlan<'a>,
    pub value: Value<'a>,
}

impl<'a> OutputItPlan<'a> {
    pub fn iter(&self) -> Result<OutputIterator> {
        Ok(OutputIterator {
            it: self.source.iter()?,
            transform: &self.value,
        })
    }
}

impl<'a> ExecPlan<'a> {
    pub fn iter(&'a self) -> Result<Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>> {
        match self {
            ExecPlan::NodeItPlan { it, info, .. } => {
                let it = it.try_get()?;
                let prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                it.seek(prefix_tuple);

                Ok(Box::new(NodeIterator { it, started: false }))
            }
            ExecPlan::EdgeItPlan { it, info, .. } => {
                let it = it.try_get()?;
                let mut prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                prefix_tuple.push_int(info.src_table_id.id);
                it.seek(prefix_tuple);

                Ok(Box::new(EdgeIterator {
                    it,
                    started: false,
                    src_table_id: info.src_table_id.id,
                }))
            }
            ExecPlan::EdgeKeyOnlyBwdItPlan { it, info, .. } => {
                let it = it.try_get()?;
                let mut prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                prefix_tuple.push_int(info.dst_table_id.id);
                it.seek(prefix_tuple);

                Ok(Box::new(EdgeKeyOnlyBwdIterator {
                    it,
                    started: false,
                    dst_table_id: info.dst_table_id.id,
                }))
            }
            ExecPlan::KeySortedWithAssocItPlan {
                main, associates, ..
            } => {
                let buffer = iter::repeat_with(|| None).take(associates.len()).collect();
                let associates = associates
                    .iter()
                    .map(|(tid, it)| {
                        it.try_get().map(|it| {
                            let prefix_tuple = OwnTuple::with_prefix(tid.table_id.id as u32);
                            it.seek(prefix_tuple);

                            NodeIterator { it, started: false }
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(KeySortedWithAssocIterator {
                    main: main.iter()?,
                    associates,
                    buffer,
                }))
            }
            ExecPlan::CartesianProdItPlan { left, right } => Ok(Box::new(CartesianProdIterator {
                left: left.iter()?,
                left_cache: MegaTuple::empty_tuple(),
                right_source: right.as_ref(),
                right: right.as_ref().iter()?,
            })),
            ExecPlan::FilterItPlan { source: it, filter } => Ok(Box::new(FilterIterator {
                it: it.iter()?,
                filter,
            })),
            ExecPlan::EvalItPlan {
                source: it,
                keys,
                vals,
            } => Ok(Box::new(EvalIterator {
                it: it.iter()?,
                keys,
                vals,
            })),
            ExecPlan::MergeJoinItPlan {
                left,
                right,
                left_keys,
                right_keys,
            } => Ok(Box::new(MergeJoinIterator {
                left: left.iter()?,
                right: right.iter()?,
                left_keys,
                right_keys,
            })),
            ExecPlan::OuterMergeJoinItPlan {
                left,
                right,
                left_keys,
                right_keys,
                left_outer,
                right_outer,
                left_len,
                right_len,
            } => Ok(Box::new(OuterMergeJoinIterator {
                left: left.iter()?,
                right: right.iter()?,
                left_outer: *left_outer,
                right_outer: *right_outer,
                left_keys,
                right_keys,
                left_len: *left_len,
                right_len: *right_len,
                left_cache: None,
                right_cache: None,
                pull_left: true,
                pull_right: true,
            })),
            ExecPlan::KeyedUnionItPlan { left, right } => Ok(Box::new(KeyedUnionIterator {
                left: left.iter()?,
                right: right.iter()?,
            })),
            ExecPlan::KeyedDifferenceItPlan { left, right } => {
                Ok(Box::new(KeyedDifferenceIterator {
                    left: left.iter()?,
                    right: right.iter()?,
                    right_cache: None,
                    started: false,
                }))
            }
            ExecPlan::BagsUnionItPlan { bags } => {
                let bags = bags.iter().map(|i| i.iter()).collect::<Result<Vec<_>>>()?;
                Ok(Box::new(BagsUnionIterator { bags, current: 0 }))
            }
            ExecPlan::EdgeBwdItPlan { .. } => {
                todo!()
            }
            ExecPlan::ChainJoinItPlan {
                left,
                right,
                left_outer,
                kind,
                left_info,
                right_info,
                ..
            } => match right {
                TableRowGetterSlot::Dummy => {
                    Err(LogicError("Uninitialized chain join".to_string()))
                }
                TableRowGetterSlot::Reified(right) => Ok(match kind {
                    ChainJoinKind::NodeToFwdEdge | ChainJoinKind::NodeToBwdEdge => {
                        let chain_kind = match kind {
                            ChainJoinKind::NodeToFwdEdge => NodeEdgeChainKind::Fwd,
                            ChainJoinKind::NodeToBwdEdge => NodeEdgeChainKind::Bwd,
                            _ => unreachable!(),
                        };
                        let (edge_front_table_id, left_key_len) =
                            if *kind == ChainJoinKind::NodeToBwdEdge {
                                (
                                    right_info.dst_table_id.id as u32,
                                    right_info.dst_key_typing.len(),
                                )
                            } else {
                                (
                                    right_info.src_table_id.id as u32,
                                    right_info.src_key_typing.len(),
                                )
                            };
                        let right_iter = if right.in_root {
                            right.sess.txn.iterator(true, &right.sess.perm_cf)
                        } else {
                            right.sess.txn.iterator(false, &right.sess.temp_cf)
                        };
                        Box::new(NodeToEdgeChainJoinIterator {
                            left: left.iter()?,
                            right_it: right_iter,
                            right_getter: right.clone(),
                            kind: chain_kind,
                            right_key_cache: None,
                            edge_front_table_id,
                            left_key_len,
                            left_cache: None,
                            last_right_key_cache: None,
                        })
                    }
                    ChainJoinKind::FwdEdgeToNode => Box::new(EdgeToNodeChainJoinIterator {
                        left: left.iter()?,
                        right: right.clone(),
                        left_outer: *left_outer,
                        key_start_idx: left_info.src_key_typing.len() + 2,
                        key_end_idx: left_info.src_key_typing.len()
                            + left_info.dst_key_typing.len()
                            + 2,
                    }),
                    ChainJoinKind::BwdEdgeToNode => Box::new(EdgeToNodeChainJoinIterator {
                        left: left.iter()?,
                        right: right.clone(),
                        left_outer: *left_outer,
                        key_start_idx: 1,
                        key_end_idx: left_info.src_key_typing.len() + 1,
                    }),
                }),
            },
            ExecPlan::LimiterItPlan {
                source,
                limit,
                offset,
            } => Ok(Box::new(LimiterIterator {
                source: source.iter()?,
                limit: *limit,
                offset: *offset,
                current: 0,
            })),
            ExecPlan::SortingMatPlan {
                source,
                ordering,
                sess,
            } => match sess {
                SessionSlot::Dummy => Err(LogicError("Uninitialized session data".to_string())),
                SessionSlot::Reified(sess) => Ok(Box::new(SortingMaterialization {
                    source: source.iter()?,
                    ordering,
                    sess,
                    sorted: false,
                    temp_table_id: 0,
                    skv_len: (0, 0, 0),
                    sorted_it: sess.raw_iterator(false),
                })),
            },
        }
    }
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum OuterJoinType {
    LeftJoin,
    RightJoin,
    FullOuterJoin,
}

pub type AccessorMap = BTreeMap<String, BTreeMap<String, (TableId, ColId)>>;

fn merge_accessor_map(left: &mut AccessorMap, right: AccessorMap) {
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
}

fn convert_to_relative_accessor_map(amap: AccessorMap) -> AccessorMap {
    // TODO this only handles the simplest case
    fn convert_inner(
        inner: BTreeMap<String, (TableId, ColId)>,
    ) -> BTreeMap<String, (TableId, ColId)> {
        inner
            .into_iter()
            .map(|(k, (_tid, cid))| (k, (TableId::new(false, 0), cid)))
            .collect()
    }
    amap.into_iter()
        .map(|(k, v)| (k, convert_inner(v)))
        .collect()
}

fn shift_accessor_map(amap: AccessorMap, (keyshift, valshift): (usize, usize)) -> AccessorMap {
    let shift_inner =
        |inner: BTreeMap<String, (TableId, ColId)>| -> BTreeMap<String, (TableId, ColId)> {
            inner
                .into_iter()
                .map(|(k, (tid, cid))| {
                    (
                        k,
                        (
                            TableId::new(
                                tid.in_root,
                                tid.id
                                    + if cid.is_key {
                                        keyshift as i64
                                    } else {
                                        valshift as i64
                                    },
                            ),
                            cid,
                        ),
                    )
                })
                .collect()
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
    fn do_reify_intermediate_plan(
        &'a self,
        plan: ExecPlan<'a>,
    ) -> Result<(ExecPlan<'a>, AccessorMap)> {
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
                let plan = ExecPlan::NodeItPlan { it, info, binding };
                (plan, amap)
            }
            ExecPlan::EdgeItPlan { info, binding, .. } => {
                let amap = match &binding {
                    None => Default::default(),
                    Some(binding) => {
                        let amap = self.edge_accessor_map(binding, &info);
                        convert_to_relative_accessor_map(amap)
                    }
                };
                let it = if info.table_id.in_root {
                    self.txn.iterator(true, &self.perm_cf)
                } else {
                    self.txn.iterator(true, &self.temp_cf)
                };
                let it = IteratorSlot::Reified(it);
                let plan = ExecPlan::EdgeItPlan { it, info, binding };
                (plan, amap)
            }
            ExecPlan::EdgeBwdItPlan { .. } => {
                todo!()
            }
            ExecPlan::EdgeKeyOnlyBwdItPlan { .. } => todo!(),
            ExecPlan::KeySortedWithAssocItPlan {
                main,
                associates,
                binding,
            } => {
                let (main_plan, mut amap) = self.do_reify_intermediate_plan(*main)?;
                let associates = associates
                    .into_iter()
                    .enumerate()
                    .map(|(i, (info, _))| {
                        if let Some(binding) = &binding {
                            let assoc_amap = self.assoc_accessor_map(binding, &info);
                            let (key_shift, val_shift) = main_plan.tuple_widths();
                            let assoc_amap =
                                shift_accessor_map(assoc_amap, (key_shift, val_shift + i));
                            merge_accessor_map(&mut amap, assoc_amap);
                        }
                        let it = self.raw_iterator(info.table_id.in_root);
                        (info, IteratorSlot::Reified(it))
                    })
                    .collect();
                (
                    ExecPlan::KeySortedWithAssocItPlan {
                        main: main_plan.into(),
                        associates,
                        binding,
                    },
                    amap,
                )
            }
            ExecPlan::CartesianProdItPlan { left, right } => {
                let (l_plan, mut l_map) = self.do_reify_intermediate_plan(*left)?;
                let (r_plan, r_map) = self.do_reify_intermediate_plan(*right)?;
                let r_map = shift_accessor_map(r_map, l_plan.tuple_widths());
                let plan = ExecPlan::CartesianProdItPlan {
                    left: l_plan.into(),
                    right: r_plan.into(),
                };
                merge_accessor_map(&mut l_map, r_map);
                (plan, l_map)
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
            ExecPlan::EvalItPlan {
                source: it,
                keys,
                vals,
            } => {
                let (inner, amap) = self.do_reify_intermediate_plan(*it)?;
                let keys = keys
                    .into_iter()
                    .map(|(k, v)| -> Result<_> {
                        let (_, v) = self.partial_eval(v, &Default::default(), &amap)?;
                        Ok((k, v))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let vals = vals
                    .into_iter()
                    .map(|(k, v)| -> Result<_> {
                        let (_, v) = self.partial_eval(v, &Default::default(), &amap)?;
                        Ok((k, v))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let plan = ExecPlan::EvalItPlan {
                    source: inner.into(),
                    keys,
                    vals,
                };
                (plan, amap)
            }
            ExecPlan::BagsUnionItPlan { .. } => todo!(),
            ExecPlan::ChainJoinItPlan {
                left,
                left_info,
                right_info,
                kind,
                left_outer,
                right_binding,
                right_associates,
                ..
            } => {
                let (l_plan, mut l_map) = self.do_reify_intermediate_plan(*left)?;
                let r_map = match &right_binding {
                    None => Default::default(),
                    Some(binding) => match kind {
                        ChainJoinKind::NodeToFwdEdge | ChainJoinKind::NodeToBwdEdge => {
                            convert_to_relative_accessor_map(
                                self.edge_accessor_map(binding, &right_info),
                            )
                        }
                        ChainJoinKind::FwdEdgeToNode | ChainJoinKind::BwdEdgeToNode => {
                            convert_to_relative_accessor_map(
                                self.node_accessor_map(binding, &right_info),
                            )
                        }
                    },
                };
                let r_map = shift_accessor_map(r_map, l_plan.tuple_widths());
                merge_accessor_map(&mut l_map, r_map);

                let right_associates = right_associates
                    .into_iter()
                    .enumerate()
                    .map(|(i, (tinfo, _))| {
                        if let Some(binding) = &right_binding {
                            let assoc_amap = self.assoc_accessor_map(binding, &tinfo);
                            let (key_shift, val_shift) = l_plan.tuple_widths();
                            let assoc_amap =
                                shift_accessor_map(assoc_amap, (key_shift, val_shift + 1 + i));
                            merge_accessor_map(&mut l_map, assoc_amap);
                        }
                        let getter = TableRowGetter {
                            sess: self,
                            key_cache: OwnTuple::with_prefix(tinfo.table_id.id as u32),
                            in_root: tinfo.table_id.in_root,
                        };
                        (tinfo, TableRowGetterSlot::Reified(getter))
                    })
                    .collect();
                let plan = ExecPlan::ChainJoinItPlan {
                    left: l_plan.into(),
                    left_info,
                    right: TableRowGetterSlot::Reified(TableRowGetter {
                        sess: self,
                        key_cache: OwnTuple::with_prefix(right_info.table_id.id as u32),
                        in_root: right_info.table_id.in_root,
                    }),
                    right_info,
                    right_binding,
                    kind,
                    left_outer,
                    right_associates,
                };
                (plan, l_map)
            }
            ExecPlan::LimiterItPlan {
                source,
                limit,
                offset,
            } => {
                let (source, amap) = self.do_reify_intermediate_plan(*source)?;
                (
                    ExecPlan::LimiterItPlan {
                        source: source.into(),
                        limit,
                        offset,
                    },
                    amap,
                )
            }
            ExecPlan::SortingMatPlan {
                source, ordering, ..
            } => {
                let (source, amap) = self.do_reify_intermediate_plan(*source)?;
                let ordering = ordering
                    .into_iter()
                    .map(|(is_asc, val)| -> Result<(bool, StaticValue)> {
                        let (_, val) = self.partial_eval(val, &Default::default(), &amap)?;
                        Ok((is_asc, val))
                    })
                    .collect::<Result<Vec<_>>>()?;
                (
                    ExecPlan::SortingMatPlan {
                        source: source.into(),
                        ordering,
                        sess: SessionSlot::Reified(self),
                    },
                    amap,
                )
            }
        };
        Ok(res)
    }
    pub fn reify_output_plan(&'a self, plan: ExecPlan<'a>) -> Result<OutputItPlan<'a>> {
        let plan = self.reify_intermediate_plan(plan)?;
        let plan = match plan {
            ExecPlan::EvalItPlan {
                source: it,
                mut keys,
                vals,
            } => {
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
        let convert_el = |el| match el {
            FromEl::Simple(el) => {
                let mut ret = match el.info.kind {
                    DataKind::Node => ExecPlan::NodeItPlan {
                        it: IteratorSlot::Dummy,
                        info: el.info,
                        binding: Some(el.binding.clone()),
                    },
                    DataKind::Edge => ExecPlan::EdgeItPlan {
                        it: IteratorSlot::Dummy,
                        info: el.info,
                        binding: Some(el.binding.clone()),
                    },
                    _ => return Err(LogicError("Wrong type for table binding".to_string())),
                };
                if !el.associates.is_empty() {
                    ret = ExecPlan::KeySortedWithAssocItPlan {
                        main: ret.into(),
                        associates: el
                            .associates
                            .into_iter()
                            .map(|(_, info)| (info, IteratorSlot::Dummy))
                            .collect(),
                        binding: Some(el.binding),
                    }
                }
                Ok(ret)
            }
            FromEl::Chain(ch) => {
                let mut it = ch.into_iter();
                let nxt = it
                    .next()
                    .ok_or_else(|| LogicError("Empty chain not allowed".to_string()))?;
                let mut prev_kind = nxt.kind;
                let mut prev_left_outer = nxt.left_outer_marker;
                let mut last_info = nxt.info.clone();
                let mut plan = match prev_kind {
                    EdgeOrNodeKind::Node => ExecPlan::NodeItPlan {
                        it: IteratorSlot::Dummy,
                        info: nxt.info,
                        binding: nxt.binding,
                    },
                    EdgeOrNodeKind::FwdEdge => ExecPlan::EdgeItPlan {
                        it: IteratorSlot::Dummy,
                        info: nxt.info,
                        binding: nxt.binding,
                    },
                    EdgeOrNodeKind::BwdEdge => ExecPlan::EdgeBwdItPlan {
                        it: IteratorSlot::Dummy,
                        info: nxt.info,
                        binding: nxt.binding,
                        getter: TableRowGetterSlot::Dummy,
                    },
                };
                for el in it {
                    plan = ExecPlan::ChainJoinItPlan {
                        left: plan.into(),
                        left_info: last_info,
                        right: TableRowGetterSlot::Dummy,
                        right_info: el.info.clone(),
                        kind: match (prev_kind, el.kind) {
                            (EdgeOrNodeKind::Node, EdgeOrNodeKind::FwdEdge) => {
                                ChainJoinKind::NodeToFwdEdge
                            }
                            (EdgeOrNodeKind::Node, EdgeOrNodeKind::BwdEdge) => {
                                ChainJoinKind::NodeToBwdEdge
                            }
                            (EdgeOrNodeKind::FwdEdge, EdgeOrNodeKind::Node) => {
                                ChainJoinKind::FwdEdgeToNode
                            }
                            (EdgeOrNodeKind::BwdEdge, EdgeOrNodeKind::Node) => {
                                ChainJoinKind::BwdEdgeToNode
                            }
                            _ => unreachable!(),
                        },
                        left_outer: prev_left_outer,
                        right_binding: el.binding,
                        right_associates: el
                            .associates
                            .into_iter()
                            .map(|(_, tinfo)| (tinfo, TableRowGetterSlot::Dummy))
                            .collect(),
                    };

                    prev_kind = el.kind;
                    prev_left_outer = el.left_outer_marker;
                    last_info = el.info;
                }
                Ok(plan)
            }
        };
        let mut from_data = from_data.into_iter();
        let fst = from_data
            .next()
            .ok_or_else(|| LogicError("Empty from clause".to_string()))?;
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
    pub(crate) fn assoc_accessor_map(&self, binding: &str, info: &TableInfo) -> AccessorMap {
        let mut ret = BTreeMap::new();
        for (i, (k, _)) in info.val_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (false, i).into()));
        }
        BTreeMap::from([(binding.to_string(), ret)])
    }
    pub(crate) fn node_accessor_map(&self, binding: &str, info: &TableInfo) -> AccessorMap {
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
    pub(crate) fn edge_accessor_map(&self, binding: &str, info: &TableInfo) -> AccessorMap {
        let mut ret = BTreeMap::new();
        let src_key_len = info.src_key_typing.len();
        let dst_key_len = info.dst_key_typing.len();
        for (i, (k, _)) in info.src_key_typing.iter().enumerate() {
            ret.insert(
                "_src_".to_string() + k,
                (info.table_id, (true, 1 + i).into()),
            );
        }
        for (i, (k, _)) in info.dst_key_typing.iter().enumerate() {
            ret.insert(
                "_dst_".to_string() + k,
                (info.table_id, (true, 2 + src_key_len + i).into()),
            );
        }
        for (i, (k, _)) in info.key_typing.iter().enumerate() {
            ret.insert(
                k.into(),
                (
                    info.table_id,
                    (true, 2 + src_key_len + dst_key_len + i).into(),
                ),
            );
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
            v => ExecPlan::FilterItPlan {
                source: Box::new(plan),
                filter: v,
            },
        };
        Ok(plan)
    }
    fn convert_select_data_to_plan<'b>(
        &self,
        mut plan: ExecPlan<'b>,
        select_data: Selection,
    ) -> Result<ExecPlan<'b>> {
        if !select_data.ordering.is_empty() {
            plan = ExecPlan::SortingMatPlan {
                source: plan.into(),
                ordering: select_data.ordering,
                sess: SessionSlot::Dummy,
            };
        }
        if select_data.limit.is_some() || select_data.offset.is_some() {
            let limit = select_data.limit.unwrap_or(0) as usize;
            let offset = select_data.offset.unwrap_or(0) as usize;
            plan = ExecPlan::LimiterItPlan {
                source: plan.into(),
                offset,
                limit,
            }
        }
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
