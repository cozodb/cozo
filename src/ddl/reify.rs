use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::result;
use chrono::format::Item;
use cozorocks::TransactionPtr;
use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::{Expr, StaticExpr};
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use crate::ddl::parser::{AssocSchema, ColSchema, DdlParseError, DdlSchema, EdgeSchema, IndexSchema, NodeSchema, SequenceSchema};
use crate::runtime::instance::DbInstanceError;
use crate::runtime::instance::DbInstanceError::NameConflict;
use crate::runtime::options::default_read_options;
use crate::runtime::session::{Session, SessionDefinable};

#[derive(thiserror::Error, Debug)]
pub(crate) enum DdlReifyError {
    #[error("Name clash: {0}")]
    NameClash(String),

    #[error(transparent)]
    Eval(#[from] EvalError),

    #[error(transparent)]
    Instance(#[from] DbInstanceError),

    #[error(transparent)]
    Bridge(#[from] cozorocks::BridgeError),

    #[error(transparent)]
    Ddl(#[from] DdlParseError)
}

type Result<T> = result::Result<T, DdlReifyError>;

#[derive(Debug, Copy, Clone)]
pub(crate) enum TableKind {
    Node,
    Edge,
    Assoc,
    Index,
    Sequence,
}

#[derive(Debug, Clone)]
pub(crate) enum TableInfo {
    Node(NodeInfo),
    Edge(EdgeInfo),
    Assoc(AssocInfo),
    Index(IndexInfo),
    Sequence(SequenceInfo),
}

impl TableInfo {
    pub(crate) fn table_id(&self) -> TableId {
        match self {
            TableInfo::Node(n) => n.tid,
            TableInfo::Edge(e) => e.tid,
            TableInfo::Assoc(a) => a.tid,
            TableInfo::Index(i) => i.tid,
            TableInfo::Sequence(s) => s.tid
        }
    }
    pub(crate) fn table_name(&self) -> &str {
        match self {
            TableInfo::Node(t) => &t.name,
            TableInfo::Edge(t) => &t.name,
            TableInfo::Assoc(t) => &t.name,
            TableInfo::Index(t) => &t.name,
            TableInfo::Sequence(t) => &t.name,
        }
    }
}

impl From<&TableInfo> for OwnTuple {
    fn from(ti: &TableInfo) -> Self {
        match ti {
            TableInfo::Node(NodeInfo { name, tid, keys, vals }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Node);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let keys = keys.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(keys);
                let vals = vals.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(vals);
                target
            }
            TableInfo::Edge(EdgeInfo { name, tid, src_id, dst_id, keys, vals, }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Edge);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let keys = keys.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(keys);
                let vals = vals.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(vals);
                target.push_value(&Value::from(*src_id));
                target.push_value(&Value::from(*dst_id));
                target
            }
            TableInfo::Assoc(AssocInfo { name, tid, src_id, vals }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Assoc);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let vals = vals.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(vals);
                target.push_value(&Value::from(*src_id));
                target
            }
            TableInfo::Index(IndexInfo { name, tid, src_id, assoc_ids, index }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Index);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let indices = index.iter().map(|i| Value::from(i.clone()));
                target.push_values_as_list(indices);
                target.push_value(&Value::from(*src_id));
                let assoc_ids = assoc_ids.iter().map(|v| Value::from(*v));
                target.push_values_as_list(assoc_ids);
                target
            }
            TableInfo::Sequence(SequenceInfo { name, tid }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Sequence);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                target
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NodeInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}


#[derive(Debug, Clone)]
pub(crate) struct EdgeInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) dst_id: TableId,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct AssocInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) assoc_ids: Vec<TableId>,
    pub(crate) index: Vec<StaticExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct SequenceInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
}

pub(crate) trait DdlContext {
    fn gen_table_id(&mut self) -> Result<TableId>;
    fn table_id_by_name<I: IntoIterator<Item=TableKind>>(&self, name: &str, kind: I, for_derivation: bool) -> Result<TableId> {
        todo!()
    }
    fn table_by_name<I: IntoIterator<Item=TableKind>>(&self, name: &str, kind: I, for_derivation: bool) -> Result<TableInfo> {
        todo!()
    }
    fn table_by_id(&self, tid: TableId) -> Result<TableInfo> {
        todo!()
    }
    fn assocs_by_main_id(&self, id: TableId) -> Vec<AssocInfo> {
        todo!()
    }
    fn edges_by_main_id(&self, id: TableId) -> Vec<EdgeInfo> {
        todo!()
    }
    fn indices_by_main_id(&self, id: TableId) -> Vec<IndexInfo> {
        todo!()
    }
    fn build_table(&mut self, schema: DdlSchema) -> Result<()> {
        match schema {
            DdlSchema::Node(n) => self.build_node(n)?,
            DdlSchema::Edge(e) => self.build_edge(e)?,
            DdlSchema::Assoc(a) => self.build_assoc(a)?,
            DdlSchema::Index(i) => self.build_index(i)?,
            DdlSchema::Sequence(s) => self.build_sequence(s)?
        };
        Ok(())
    }
    fn build_node(&mut self, schema: NodeSchema) -> Result<()> {
        check_name_clash([&schema.keys, &schema.vals])?;
        let info = NodeInfo {
            name: schema.name,
            tid: self.gen_table_id()?,
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Node(info))
    }
    fn build_edge(&mut self, schema: EdgeSchema) -> Result<()> {
        check_name_clash([&schema.keys, &schema.vals])?;
        let info = EdgeInfo {
            name: schema.name,
            tid: self.gen_table_id()?,
            src_id: self.table_id_by_name(&schema.src_name, [TableKind::Node], true)?,
            dst_id: self.table_id_by_name(&schema.dst_name, [TableKind::Node], true)?,
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Edge(info))
    }
    fn build_assoc(&mut self, schema: AssocSchema) -> Result<()> {
        let src_info = self.table_by_name(&schema.src_name, [TableKind::Node, TableKind::Edge], true)?;
        let src_id = src_info.table_id();
        let associates = self.assocs_by_main_id(src_id);
        let mut names_to_check: Vec<_> = associates.iter().map(|ai| &ai.vals).collect();
        names_to_check.push(&schema.vals);
        check_name_clash(names_to_check)?;
        let info = AssocInfo {
            name: schema.name,
            tid: self.gen_table_id()?,
            src_id,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Assoc(info))
    }
    fn build_index(&mut self, schema: IndexSchema) -> Result<()> {
        let src_schema = self.table_by_name(&schema.src_name, [TableKind::Node, TableKind::Edge], true)?;
        let associates = self.assocs_by_main_id(src_schema.table_id());
        let assoc_vals = associates.iter().map(|v| v.vals.as_slice()).collect::<Vec<_>>();
        let index_exprs = match &src_schema {
            TableInfo::Node(node_info) => {
                let ctx = NodeDefEvalCtx {
                    keys: &node_info.keys,
                    vals: &node_info.vals,
                    assoc_vals: &assoc_vals,
                };
                schema.index.into_iter().map(|ex|
                    ex.partial_eval(&ctx).map(|ex| ex.to_static()))
                    .collect::<result::Result<Vec<_>, _>>()?
            }
            TableInfo::Edge(edge_info) => {
                let src_info = self.table_by_id(edge_info.src_id)?;
                let src_keys = match &src_info {
                    TableInfo::Node(n) => &n.keys,
                    _ => unreachable!()
                };
                let dst_info = self.table_by_id(edge_info.dst_id)?;
                let dst_keys = match &dst_info {
                    TableInfo::Node(n) => &n.keys,
                    _ => unreachable!()
                };
                let ctx = EdgeDefEvalCtx {
                    keys: &edge_info.keys,
                    vals: &edge_info.vals,
                    src_keys,
                    dst_keys,
                    assoc_vals: &assoc_vals,
                };
                schema.index.into_iter().map(|ex|
                    ex.partial_eval(&ctx).map(|ex| ex.to_static()))
                    .collect::<result::Result<Vec<_>, _>>()?
            }
            _ => unreachable!()
        };

        let info = IndexInfo {
            name: schema.name,
            tid: self.gen_table_id()?,
            src_id: src_schema.table_id(),
            assoc_ids: schema.assoc_names.iter().map(|n|
                self.table_id_by_name(n, [TableKind::Assoc], true))
                .collect::<Result<Vec<_>>>()?,
            index: index_exprs,
        };
        self.store_table(TableInfo::Index(info))
    }
    fn build_sequence(&mut self, schema: SequenceSchema) -> Result<()> {
        let tid = self.gen_table_id()?;
        self.store_table(TableInfo::Sequence(SequenceInfo {
            name: schema.name,
            tid,
        }))
    }
    fn store_table(&mut self, info: TableInfo) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
}

fn check_name_clash<'a, I: IntoIterator<Item=II>, II: IntoIterator<Item=&'a ColSchema>>(kvs: I) -> Result<()> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for it in kvs.into_iter() {
        for el in it.into_iter() {
            if !seen.insert(&el.name as &str) {
                return Err(DdlReifyError::NameClash(el.name.clone()));
            }
        }
    }
    Ok(())
}

fn eval_defaults(cols: Vec<ColSchema>) -> Result<Vec<ColSchema>> {
    cols.into_iter().map(|ColSchema { name, typing, default }|
        match default.partial_eval(&()) {
            Ok(default) => Ok(ColSchema {
                name,
                typing,
                default,
            }),
            Err(e) => Err(e.into())
        }).collect::<Result<Vec<_>>>()
}

pub(crate) struct NodeDefEvalCtx<'a> {
    keys: &'a [ColSchema],
    vals: &'a [ColSchema],
    assoc_vals: &'a [&'a [ColSchema]],
}

impl<'a> NodeDefEvalCtx<'a> {
    fn resolve_name(&self, name: &str) -> Option<TupleSetIdx> {
        for (i, col) in self.keys.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: true,
                    t_set: 0,
                    col_idx: i,
                });
            }
        }
        for (i, col) in self.vals.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: false,
                    t_set: 0,
                    col_idx: i,
                });
            }
        }
        for (j, set) in self.assoc_vals.iter().enumerate() {
            for (i, col) in set.iter().enumerate() {
                if name == col.name {
                    return Some(TupleSetIdx {
                        is_key: false,
                        t_set: j + 1,
                        col_idx: i,
                    });
                }
            }
        }
        None
    }
}

impl<'a> PartialEvalContext for NodeDefEvalCtx<'a> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        self.resolve_name(key).map(Expr::TupleSetIdx)
    }

    // fn resolve_table_col(&self, _binding: &str, _col: &str) -> Option<(TableId, ColId)> {
    //     None
    // }
}


pub(crate) struct EdgeDefEvalCtx<'a> {
    keys: &'a [ColSchema],
    vals: &'a [ColSchema],
    src_keys: &'a [ColSchema],
    dst_keys: &'a [ColSchema],
    assoc_vals: &'a [&'a [ColSchema]],
}

impl<'a> EdgeDefEvalCtx<'a> {
    fn resolve_name(&self, name: &str) -> Option<TupleSetIdx> {
        for (i, col) in self.src_keys.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: true,
                    t_set: 0,
                    col_idx: i + 1,
                });
            }
        }
        if name.starts_with("_src_") {
            for (i, col) in self.keys.iter().enumerate() {
                if name.strip_prefix("_src_").unwrap() == col.name {
                    return Some(TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: i + 1 + self.src_keys.len(),
                    });
                }
            }
        }
        if name.starts_with("_dst_") {
            for (i, col) in self.dst_keys.iter().enumerate() {
                if name.strip_prefix("_dst_").unwrap() == col.name {
                    return Some(TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: i + 2 + self.src_keys.len() + self.dst_keys.len(),
                    });
                }
            }
        }
        for (i, col) in self.vals.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: false,
                    t_set: 0,
                    col_idx: i,
                });
            }
        }
        for (j, set) in self.assoc_vals.iter().enumerate() {
            for (i, col) in set.iter().enumerate() {
                if name == col.name {
                    return Some(TupleSetIdx {
                        is_key: false,
                        t_set: j + 1,
                        col_idx: i,
                    });
                }
            }
        }
        None
    }
}

impl<'a> PartialEvalContext for EdgeDefEvalCtx<'a> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        self.resolve_name(key).map(Expr::TupleSetIdx)
    }
}


struct MainDbContext<'a> {
    sess: &'a Session,
    txn: TransactionPtr,
}

impl<'a> DdlContext for MainDbContext<'a> {
    fn gen_table_id(&mut self) -> Result<TableId> {
        let id = self.sess.get_next_main_table_id()?;
        Ok(TableId { in_root: true, id })
    }

    fn store_table(&mut self, info: TableInfo) -> Result<()> {
        let tid = info.table_id().id;
        let tname = info.table_name();

        let mut name_key = OwnTuple::with_prefix(0);
        name_key.push_str(tname);

        if !matches!(self.txn.get_for_update_owned(&default_read_options(), &name_key)?, None) {
            return Err(NameConflict(tname.to_string()).into());
        }

        let mut idx_key = OwnTuple::with_prefix(0);
        idx_key.push_int(tid as i64);

        let read_opts = default_read_options();

        match &info {
            TableInfo::Edge(info) => {
                let mut key = OwnTuple::with_prefix(0);
                key.push_int(info.src_id.id as i64);
                key.push_int(DataKind::Edge as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0)
                };
                current.push_int(tid as i64);
                self.txn.put(&key, &current)?;

                key.truncate_all();
                key.push_int(info.dst_id.id as i64);
                key.push_int(DataKind::EdgeBwd as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0)
                };
                current.push_int(tid as i64);
                self.txn.put(&key, &current)?;
            }
            TableInfo::Assoc(info) => {
                let mut key = OwnTuple::with_prefix(0);
                key.push_int(info.src_id.id as i64);
                key.push_int(DataKind::Assoc as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0)
                };
                current.push_int(tid as i64);
                self.txn.put(&key, &current)?;
            }
            TableInfo::Index(info) => {
                let mut key = OwnTuple::with_prefix(0);
                key.push_int(info.src_id.id as i64);
                key.push_int(DataKind::Index as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0)
                };
                current.push_int(tid as i64);
                self.txn.put(&key, &current)?;
            }
            TableInfo::Node(_) => {}
            TableInfo::Sequence(_) => {}
        }

        // store name to idx
        self.txn.put(&name_key, &idx_key)?;
        // store info
        let info_tuple = Tuple::from(&info);
        self.txn.put(&idx_key, info_tuple)?;
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        Ok(self.txn.commit()?)
    }
}
//
impl<'a> DdlContext for TempDbContext<'a> {
    fn gen_table_id(&mut self) -> Result<TableId> {
        let id = self.sess.get_next_temp_table_id();
        Ok(TableId { in_root: false, id })
    }

    fn store_table(&mut self, info: TableInfo) -> Result<()> {
        let table_id = info.table_id();
        let tid = table_id.id;
        let tname = info.table_name();
        let stack_frame = self.sess.stack.last_mut().unwrap();
        if stack_frame.contains_key(tname) {
            return Err(NameConflict(tname.to_string()).into())
        } else {
            match &info {
                TableInfo::Edge(info) => {
                    let edge_assocs = self.sess.table_assocs.entry(DataKind::Edge).or_insert(Default::default());
                    let src_assocs = edge_assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);

                    let back_edge_assocs = self.sess.table_assocs.entry(DataKind::EdgeBwd).or_insert(Default::default());
                    let dst_assocs = back_edge_assocs.entry(info.dst_id).or_insert(Default::default());
                    dst_assocs.insert(tid);

                }
                TableInfo::Assoc(info) => {
                    let assocs = self.sess.table_assocs.entry(DataKind::Assoc).or_insert(Default::default());
                    let src_assocs = assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);

                }
                TableInfo::Index(info) => {
                    let idx_assocs = self.sess.table_assocs.entry(DataKind::Index).or_insert(Default::default());
                    let src_assocs = idx_assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);
                }
                TableInfo::Node(_) => {}
                TableInfo::Sequence(_) => {}
            }

            stack_frame.insert(tname.to_string(), SessionDefinable::Table(tid));
            self.sess.tables.insert(tid, info);
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

struct TempDbContext<'a> {
    sess: &'a mut Session,
}

impl Session {
    fn main_ctx(&self) -> MainDbContext {
        MainDbContext {
            sess: self,
            txn: self.txn(None)
        }
    }
    fn temp_ctx(&mut self) -> TempDbContext {
        TempDbContext {
            sess: self
        }
    }
}