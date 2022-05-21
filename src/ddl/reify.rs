use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::{Expr, ExprError, StaticExpr};
use crate::data::tuple::{
    DataKind, OwnTuple, Tuple, TupleError, DATAKIND_ASSOC, DATAKIND_EDGE, DATAKIND_INDEX,
    DATAKIND_NODE, DATAKIND_SEQUENCE,
};
use crate::data::tuple_set::{TableId, TupleSetError, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use crate::ddl::parser::{
    AssocSchema, ColSchema, DdlParseError, DdlSchema, EdgeSchema, IndexSchema, NodeSchema,
    SequenceSchema,
};
use crate::runtime::options::default_read_options;
use crate::runtime::session::{Session, SessionDefinable, TableAssocMap};
use cozorocks::TransactionPtr;
use std::collections::BTreeSet;
use std::result;

#[derive(thiserror::Error, Debug)]
pub enum DdlReifyError {
    #[error("Name clash: {0}")]
    NameClash(String),

    #[error(transparent)]
    Eval(#[from] EvalError),

    #[error(transparent)]
    Bridge(#[from] cozorocks::BridgeError),

    #[error(transparent)]
    Ddl(#[from] DdlParseError),

    #[error("Cannot find table {0:?}")]
    TableNotFound(TableId),

    #[error("Cannot find table {0}")]
    TableNameNotFound(String),

    #[error("Data corruption {0:?}")]
    Corruption(OwnTuple),

    #[error("Wrong table kind for {0:?}")]
    WrongDataKind(TableId),

    #[error(transparent)]
    Tuple(#[from] TupleError),

    #[error(transparent)]
    TupleSet(#[from] TupleSetError),

    #[error(transparent)]
    Expr(#[from] ExprError),

    #[error("Name conflict {0}")]
    NameConflict(String),
}

type Result<T> = result::Result<T, DdlReifyError>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TableInfo {
    Node(NodeInfo),
    Edge(EdgeInfo),
    Assoc(AssocInfo),
    Index(IndexInfo),
    Sequence(SequenceInfo),
}

impl TableInfo {
    pub(crate) fn data_kind(&self) -> DataKind {
        match self {
            TableInfo::Node(_) => DataKind::Node,
            TableInfo::Edge(_) => DataKind::Edge,
            TableInfo::Assoc(_) => DataKind::Assoc,
            TableInfo::Index(_) => DataKind::Index,
            TableInfo::Sequence(_) => DataKind::Index,
        }
    }
    pub(crate) fn table_id(&self) -> TableId {
        match self {
            TableInfo::Node(n) => n.tid,
            TableInfo::Edge(e) => e.tid,
            TableInfo::Assoc(a) => a.tid,
            TableInfo::Index(i) => i.tid,
            TableInfo::Sequence(s) => s.tid,
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
    pub(crate) fn set_table_id(&mut self, id: TableId) {
        match self {
            TableInfo::Node(t) => t.tid = id,
            TableInfo::Edge(t) => t.tid = id,
            TableInfo::Assoc(t) => t.tid = id,
            TableInfo::Index(t) => t.tid = id,
            TableInfo::Sequence(t) => t.tid = id,
        }
    }
}

impl<T: AsRef<[u8]>> TryFrom<Tuple<T>> for TableInfo {
    type Error = DdlReifyError;
    fn try_from(tuple: Tuple<T>) -> result::Result<Self, Self::Error> {
        let gen_err = || DdlReifyError::Corruption(tuple.to_owned());
        match tuple.get_prefix() {
            DATAKIND_NODE => {
                let mut it = tuple.iter();
                let name = it
                    .next()
                    .ok_or_else(gen_err)??
                    .get_str()
                    .ok_or_else(gen_err)?
                    .to_string();
                let tid = it.next().ok_or_else(gen_err)??;
                let tid = TableId::try_from(&tid)?;
                let keys = it.next().ok_or_else(gen_err)??;
                let keys = keys.get_slice().ok_or_else(gen_err)?;
                let keys = keys
                    .iter()
                    .map(|v| ColSchema::try_from(v.clone()).map_err(DdlReifyError::from))
                    .collect::<Result<Vec<_>>>()?;
                let vals = it.next().ok_or_else(gen_err)??;
                let vals = vals.get_slice().ok_or_else(gen_err)?;
                let vals = vals
                    .iter()
                    .map(|v| ColSchema::try_from(v.clone()).map_err(DdlReifyError::from))
                    .collect::<Result<Vec<_>>>()?;
                Ok(TableInfo::Node(NodeInfo {
                    name,
                    tid,
                    keys,
                    vals,
                }))
            }
            DATAKIND_EDGE => {
                let mut it = tuple.iter();
                let name = it
                    .next()
                    .ok_or_else(gen_err)??
                    .get_str()
                    .ok_or_else(gen_err)?
                    .to_string();
                let tid = it.next().ok_or_else(gen_err)??;
                let tid = TableId::try_from(&tid)?;
                let keys = it.next().ok_or_else(gen_err)??;
                let keys = keys.get_slice().ok_or_else(gen_err)?;
                let keys = keys
                    .iter()
                    .map(|v| ColSchema::try_from(v.clone()).map_err(DdlReifyError::from))
                    .collect::<Result<Vec<_>>>()?;
                let vals = it.next().ok_or_else(gen_err)??;
                let vals = vals.get_slice().ok_or_else(gen_err)?;
                let vals = vals
                    .iter()
                    .map(|v| ColSchema::try_from(v.clone()).map_err(DdlReifyError::from))
                    .collect::<Result<Vec<_>>>()?;
                let src_id = it.next().ok_or_else(gen_err)??;
                let src_id = TableId::try_from(&src_id)?;
                let dst_id = it.next().ok_or_else(gen_err)??;
                let dst_id = TableId::try_from(&dst_id)?;

                Ok(TableInfo::Edge(EdgeInfo {
                    name,
                    tid,
                    src_id,
                    dst_id,
                    keys,
                    vals,
                }))
            }
            DATAKIND_INDEX => {
                let mut it = tuple.iter();
                let name = it
                    .next()
                    .ok_or_else(gen_err)??
                    .get_str()
                    .ok_or_else(gen_err)?
                    .to_string();
                let tid = it.next().ok_or_else(gen_err)??;
                let tid = TableId::try_from(&tid)?;
                let indices = it.next().ok_or_else(gen_err)??;
                let indices = indices.get_slice().ok_or_else(gen_err)?;
                let indices = indices
                    .iter()
                    .map(|v| IndexCol::try_from(v.clone()))
                    .collect::<Result<Vec<_>>>()?;
                let src_id = it.next().ok_or_else(gen_err)??;
                let src_id = TableId::try_from(&src_id)?;
                let assoc_ids = it.next().ok_or_else(gen_err)??;
                let assoc_ids = assoc_ids.get_slice().ok_or_else(gen_err)?;
                let assoc_ids = assoc_ids
                    .iter()
                    .map(TableId::try_from)
                    .collect::<result::Result<Vec<_>, _>>()?;
                Ok(TableInfo::Index(IndexInfo {
                    name,
                    tid,
                    src_id,
                    assoc_ids,
                    index: indices,
                }))
            }
            DATAKIND_ASSOC => {
                let mut it = tuple.iter();
                let name = it
                    .next()
                    .ok_or_else(gen_err)??
                    .get_str()
                    .ok_or_else(gen_err)?
                    .to_string();
                let tid = it.next().ok_or_else(gen_err)??;
                let tid = TableId::try_from(&tid)?;
                let vals = it.next().ok_or_else(gen_err)??;
                let vals = vals.get_slice().ok_or_else(gen_err)?;
                let vals = vals
                    .iter()
                    .map(|v| ColSchema::try_from(v.clone()).map_err(DdlReifyError::from))
                    .collect::<Result<Vec<_>>>()?;
                let src_id = it.next().ok_or_else(gen_err)??;
                let src_id = TableId::try_from(&src_id)?;

                Ok(TableInfo::Assoc(AssocInfo {
                    name,
                    tid,
                    src_id,
                    vals,
                }))
            }
            DATAKIND_SEQUENCE => {
                let mut it = tuple.iter();
                let name = it
                    .next()
                    .ok_or_else(gen_err)??
                    .get_str()
                    .ok_or_else(gen_err)?
                    .to_string();
                let tid = it.next().ok_or_else(gen_err)??;
                let tid = TableId::try_from(&tid)?;
                Ok(TableInfo::Sequence(SequenceInfo { name, tid }))
            }
            _ => Err(gen_err()),
        }
    }
}

impl From<&TableInfo> for OwnTuple {
    fn from(ti: &TableInfo) -> Self {
        match ti {
            TableInfo::Node(NodeInfo {
                name,
                tid,
                keys,
                vals,
            }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Node);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let keys = keys.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(keys);
                let vals = vals.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(vals);
                target
            }
            TableInfo::Edge(EdgeInfo {
                name,
                tid,
                src_id,
                dst_id,
                keys,
                vals,
            }) => {
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
            TableInfo::Assoc(AssocInfo {
                name,
                tid,
                src_id,
                vals,
            }) => {
                let mut target = OwnTuple::with_data_prefix(DataKind::Assoc);
                target.push_str(name);
                target.push_value(&Value::from(*tid));
                let vals = vals.iter().map(|k| Value::from(k.clone()));
                target.push_values_as_list(vals);
                target.push_value(&Value::from(*src_id));
                target
            }
            TableInfo::Index(IndexInfo {
                name,
                tid,
                src_id,
                assoc_ids,
                index,
            }) => {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NodeInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EdgeInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) dst_id: TableId,
    pub(crate) keys: Vec<ColSchema>,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AssocInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) vals: Vec<ColSchema>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IndexCol {
    Col(TupleSetIdx),
    Expr(StaticExpr),
}

impl From<IndexCol> for StaticValue {
    fn from(ic: IndexCol) -> Self {
        match ic {
            IndexCol::Expr(expr) => StaticValue::from(expr),
            IndexCol::Col(c) => StaticValue::from(Expr::TupleSetIdx(c)),
        }
    }
}

impl<'a> TryFrom<Value<'a>> for IndexCol {
    type Error = DdlReifyError;

    fn try_from(value: Value<'a>) -> result::Result<Self, Self::Error> {
        Ok(match Expr::try_from(value)? {
            Expr::TupleSetIdx(tidx) => IndexCol::Col(tidx),
            expr => IndexCol::Expr(expr.to_static()),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
    pub(crate) src_id: TableId,
    pub(crate) assoc_ids: Vec<TableId>,
    pub(crate) index: Vec<IndexCol>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SequenceInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
}

pub(crate) trait DdlContext {
    fn gen_temp_table_id(&self) -> TableId;
    fn gen_table_id(&mut self) -> Result<TableId>;
    fn table_id_by_name(&self, name: &str) -> Result<TableId>;
    fn table_by_name<I: IntoIterator<Item = DataKind>>(
        &self,
        name: &str,
        kind: I,
    ) -> Result<TableInfo> {
        let id = self.table_id_by_name(name)?;
        let table = self.table_by_id(id)?;
        let set = kind.into_iter().collect::<BTreeSet<_>>();
        if set.contains(&table.data_kind()) {
            Ok(table)
        } else {
            Err(DdlReifyError::WrongDataKind(id))
        }
    }
    fn table_by_id(&self, tid: TableId) -> Result<TableInfo>;
    fn assoc_ids_by_main_id(&self, id: TableId) -> Result<Vec<TableId>>;
    fn assocs_by_main_id(&self, id: TableId) -> Result<Vec<AssocInfo>> {
        self.assoc_ids_by_main_id(id)?
            .into_iter()
            .map(|id| match self.table_by_id(id) {
                Err(e) => Err(e),
                Ok(TableInfo::Assoc(a)) => Ok(a),
                Ok(_t) => Err(DdlReifyError::WrongDataKind(id)),
            })
            .collect::<Result<_>>()
    }
    fn edge_ids_by_main_id(&self, id: TableId) -> Result<Vec<TableId>>;
    fn edges_by_main_id(&self, id: TableId) -> Result<Vec<EdgeInfo>> {
        self.edge_ids_by_main_id(id)?
            .into_iter()
            .map(|id| match self.table_by_id(id) {
                Err(e) => Err(e),
                Ok(TableInfo::Edge(a)) => Ok(a),
                Ok(_t) => Err(DdlReifyError::WrongDataKind(id)),
            })
            .collect::<Result<_>>()
    }
    fn bwd_edge_ids_by_main_id(&self, id: TableId) -> Result<Vec<TableId>>;
    fn bwd_edges_by_main_id(&self, id: TableId) -> Result<Vec<EdgeInfo>> {
        self.bwd_edge_ids_by_main_id(id)?
            .into_iter()
            .map(|id| match self.table_by_id(id) {
                Err(e) => Err(e),
                Ok(TableInfo::Edge(a)) => Ok(a),
                Ok(_t) => Err(DdlReifyError::WrongDataKind(id)),
            })
            .collect::<Result<_>>()
    }
    fn index_ids_by_main_id(&self, id: TableId) -> Result<Vec<TableId>>;
    fn indices_by_main_id(&self, id: TableId) -> Result<Vec<IndexInfo>> {
        self.index_ids_by_main_id(id)?
            .into_iter()
            .map(|id| match self.table_by_id(id) {
                Err(e) => Err(e),
                Ok(TableInfo::Index(a)) => Ok(a),
                Ok(_t) => Err(DdlReifyError::WrongDataKind(id)),
            })
            .collect::<Result<_>>()
    }
    fn build_table(&mut self, schema: DdlSchema) -> Result<()> {
        match schema {
            DdlSchema::Node(n) => self.build_node(n)?,
            DdlSchema::Edge(e) => self.build_edge(e)?,
            DdlSchema::Assoc(a) => self.build_assoc(a)?,
            DdlSchema::Index(i) => self.build_index(i)?,
            DdlSchema::Sequence(s) => self.build_sequence(s)?,
        };
        Ok(())
    }
    fn build_node(&mut self, schema: NodeSchema) -> Result<()> {
        check_name_clash([&schema.keys, &schema.vals])?;
        let info = NodeInfo {
            name: schema.name,
            tid: self.gen_temp_table_id(),
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Node(info))
    }
    fn build_edge(&mut self, schema: EdgeSchema) -> Result<()> {
        check_name_clash([&schema.keys, &schema.vals])?;
        let info = EdgeInfo {
            name: schema.name,
            tid: self.gen_temp_table_id(),
            src_id: self
                .table_by_name(&schema.src_name, [DataKind::Node])?
                .table_id(),
            dst_id: self
                .table_by_name(&schema.dst_name, [DataKind::Node])?
                .table_id(),
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Edge(info))
    }
    fn build_assoc(&mut self, schema: AssocSchema) -> Result<()> {
        let src_info = self.table_by_name(&schema.src_name, [DataKind::Node, DataKind::Edge])?;
        let src_id = src_info.table_id();
        let associates = self.assocs_by_main_id(src_id)?;
        let mut names_to_check: Vec<_> = associates.iter().map(|ai| &ai.vals).collect();
        names_to_check.push(&schema.vals);
        check_name_clash(names_to_check)?;
        let info = AssocInfo {
            name: schema.name,
            tid: self.gen_temp_table_id(),
            src_id,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_table(TableInfo::Assoc(info))
    }
    fn build_index(&mut self, schema: IndexSchema) -> Result<()> {
        let src_schema = self.table_by_name(&schema.src_name, [DataKind::Node, DataKind::Edge])?;
        let associates = self.assocs_by_main_id(src_schema.table_id())?;
        let assoc_vals = associates
            .iter()
            .map(|v| v.vals.as_slice())
            .collect::<Vec<_>>();
        let index_exprs = match &src_schema {
            TableInfo::Node(node_info) => {
                let ctx = NodeDefEvalCtx {
                    keys: &node_info.keys,
                    vals: &node_info.vals,
                    assoc_vals: &assoc_vals,
                };
                schema
                    .index
                    .into_iter()
                    .map(|ex| {
                        ex.partial_eval(&ctx).map(|ex| match ex {
                            Expr::TupleSetIdx(tidx) => IndexCol::Col(tidx),
                            ex => IndexCol::Expr(ex.to_static()),
                        })
                    })
                    .collect::<result::Result<Vec<_>, _>>()?
            }
            TableInfo::Edge(edge_info) => {
                let src_info = self.table_by_id(edge_info.src_id)?;
                let src_keys = match &src_info {
                    TableInfo::Node(n) => &n.keys,
                    _ => unreachable!(),
                };
                let dst_info = self.table_by_id(edge_info.dst_id)?;
                let dst_keys = match &dst_info {
                    TableInfo::Node(n) => &n.keys,
                    _ => unreachable!(),
                };
                let ctx = EdgeDefEvalCtx {
                    keys: &edge_info.keys,
                    vals: &edge_info.vals,
                    src_keys,
                    dst_keys,
                    assoc_vals: &assoc_vals,
                };
                schema
                    .index
                    .into_iter()
                    .map(|ex| {
                        ex.partial_eval(&ctx).map(|ex| match ex {
                            Expr::TupleSetIdx(tidx) => IndexCol::Col(tidx),
                            ex => IndexCol::Expr(ex.to_static()),
                        })
                    })
                    .collect::<result::Result<Vec<_>, _>>()?
            }
            _ => unreachable!(),
        };

        let info = IndexInfo {
            name: schema.name,
            tid: self.gen_temp_table_id(),
            src_id: src_schema.table_id(),
            assoc_ids: schema
                .assoc_names
                .iter()
                .map(|n| {
                    self.table_by_name(n, [DataKind::Assoc])
                        .map(|t| t.table_id())
                })
                .collect::<Result<Vec<_>>>()?,
            index: index_exprs,
        };
        self.store_table(TableInfo::Index(info))
    }
    fn build_sequence(&mut self, schema: SequenceSchema) -> Result<()> {
        let tid = self.gen_temp_table_id();
        self.store_table(TableInfo::Sequence(SequenceInfo {
            name: schema.name,
            tid,
        }))
    }
    fn store_table(&mut self, info: TableInfo) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
}

fn check_name_clash<'a, I: IntoIterator<Item = II>, II: IntoIterator<Item = &'a ColSchema>>(
    kvs: I,
) -> Result<()> {
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
    cols.into_iter()
        .map(
            |ColSchema {
                 name,
                 typing,
                 default,
             }| match default.partial_eval(&()) {
                Ok(default) => Ok(ColSchema {
                    name,
                    typing,
                    default,
                }),
                Err(e) => Err(e.into()),
            },
        )
        .collect::<Result<Vec<_>>>()
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

pub(crate) struct MainDbContext<'a> {
    sess: &'a Session,
    txn: TransactionPtr,
}

fn get_related_table_ids_in_stack(
    stack_map: &TableAssocMap,
    tid: TableId,
    tag: DataKind,
) -> Result<Vec<TableId>> {
    match stack_map.get(&tag) {
        None => Ok(vec![]),
        Some(t) => match t.get(&tid) {
            None => Ok(vec![]),
            Some(found) => Ok(found
                .iter()
                .map(|v| TableId {
                    in_root: false,
                    id: *v,
                })
                .collect()),
        },
    }
}

fn get_related_table_ids_in_main(
    txn: &TransactionPtr,
    id: u32,
    tag: DataKind,
) -> Result<Vec<TableId>> {
    let mut key = OwnTuple::with_prefix(0);
    key.push_int(id as i64);
    key.push_int(tag as i64);
    let assocs = txn.get_for_update_owned(&default_read_options(), &key)?;
    if let Some(slice) = assocs {
        let mut ret = vec![];
        let tuple = Tuple::new(slice);
        for val in tuple.iter() {
            let val = val?;
            let tid = val
                .get_int()
                .ok_or_else(|| DdlReifyError::Corruption(tuple.to_owned()))?;
            ret.push(TableId {
                in_root: true,
                id: tid as u32,
            })
        }
        Ok(ret)
    } else {
        Ok(vec![])
    }
}

fn find_table_in_main(txn: &TransactionPtr, name: &str) -> Result<TableId> {
    let mut name_key = OwnTuple::with_prefix(0);
    name_key.push_str(name);

    match txn.get_for_update_owned(&default_read_options(), &name_key)? {
        None => Err(DdlReifyError::TableNameNotFound(name.to_string())),
        Some(slice) => {
            let tuple = Tuple::new(slice);
            let id = tuple.get_int(0)?;
            Ok(TableId {
                in_root: true,
                id: id as u32,
            })
        }
    }
}

fn find_table_by_id_in_main(txn: &TransactionPtr, id: u32) -> Result<TableInfo> {
    let mut idx_key = OwnTuple::with_prefix(0);
    idx_key.push_int(id as i64);
    let res = txn
        .get_owned(&default_read_options(), &idx_key)?
        .ok_or(DdlReifyError::TableNotFound(TableId { id, in_root: true }))?;
    let info = TableInfo::try_from(Tuple::new(res))?;
    Ok(info)
}

impl<'a> DdlContext for MainDbContext<'a> {
    fn gen_temp_table_id(&self) -> TableId {
        TableId {
            in_root: true,
            id: 0,
        }
    }

    fn gen_table_id(&mut self) -> Result<TableId> {
        let id = self.sess.get_next_main_table_id()?;
        Ok(TableId { in_root: true, id })
    }

    fn table_id_by_name(&self, name: &str) -> Result<TableId> {
        find_table_in_main(&self.txn, name)
    }

    fn table_by_id(&self, TableId { id, in_root }: TableId) -> Result<TableInfo> {
        if !in_root {
            Err(DdlReifyError::TableNotFound(TableId { id, in_root }))
        } else {
            find_table_by_id_in_main(&self.txn, id)
        }
    }

    fn assoc_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        if tid.in_root {
            get_related_table_ids_in_main(&self.txn, tid.id, DataKind::Assoc)
        } else {
            Ok(vec![])
        }
    }

    fn edge_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        if tid.in_root {
            get_related_table_ids_in_main(&self.txn, tid.id, DataKind::Edge)
        } else {
            Ok(vec![])
        }
    }

    fn bwd_edge_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        if tid.in_root {
            get_related_table_ids_in_main(&self.txn, tid.id, DataKind::EdgeBwd)
        } else {
            Ok(vec![])
        }
    }

    fn index_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        if tid.in_root {
            get_related_table_ids_in_main(&self.txn, tid.id, DataKind::Index)
        } else {
            Ok(vec![])
        }
    }

    fn store_table(&mut self, mut info: TableInfo) -> Result<()> {
        let tname = info.table_name();

        let mut name_key = OwnTuple::with_prefix(0);
        name_key.push_str(tname);

        if let Some(existing_key) = self
            .txn
            .get_for_update_owned(&default_read_options(), &name_key)?
        {
            if let Some(existing) = self
                .txn
                .get_for_update_owned(&default_read_options(), existing_key)?
            {
                if let Ok(mut existing_info) = TableInfo::try_from(Tuple::new(existing)) {
                    existing_info.set_table_id(info.table_id());
                    if existing_info == info {
                        // exactly the same thing already exists, nothing to do!
                        return Ok(());
                    }
                }
            }
            return Err(DdlReifyError::NameConflict(tname.to_string()));
        }

        let mut idx_key = OwnTuple::with_prefix(0);
        let table_id = self.gen_table_id()?;
        let tid = table_id.id;
        info.set_table_id(table_id);
        idx_key.push_int(tid as i64);

        let read_opts = default_read_options();

        match &info {
            TableInfo::Edge(info) => {
                let mut key = OwnTuple::with_prefix(0);
                key.push_int(info.src_id.id as i64);
                key.push_int(DataKind::Edge as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0),
                };
                current.push_int(tid as i64);
                self.txn.put(&key, &current)?;

                key.truncate_all();
                key.push_int(info.dst_id.id as i64);
                key.push_int(DataKind::EdgeBwd as i64);
                let mut current = match self.txn.get_for_update_owned(&read_opts, &key)? {
                    Some(v) => OwnTuple::new(v.as_ref().to_vec()),
                    None => OwnTuple::with_prefix(0),
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
                    None => OwnTuple::with_prefix(0),
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
                    None => OwnTuple::with_prefix(0),
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

impl<'a> DdlContext for TempDbContext<'a> {
    fn gen_temp_table_id(&self) -> TableId {
        TableId {
            in_root: false,
            id: 0,
        }
    }

    fn gen_table_id(&mut self) -> Result<TableId> {
        let id = self.sess.get_next_temp_table_id();
        Ok(TableId { in_root: false, id })
    }

    fn table_id_by_name(&self, name: &str) -> Result<TableId> {
        for frame in self.sess.stack.iter().rev() {
            if let Some(found) = frame.get(name) {
                return if let SessionDefinable::Table(id) = found {
                    Ok(TableId {
                        in_root: false,
                        id: *id,
                    })
                } else {
                    Err(DdlReifyError::NameClash(name.to_string()))
                };
            }
        }
        find_table_in_main(&self.txn, name)
    }

    fn table_by_id(&self, TableId { id, in_root }: TableId) -> Result<TableInfo> {
        if !in_root {
            self.sess
                .tables
                .get(&id)
                .cloned()
                .ok_or(DdlReifyError::TableNotFound(TableId { id, in_root }))
        } else {
            find_table_by_id_in_main(&self.txn, id)
        }
    }

    fn assoc_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        let tag = DataKind::Assoc;
        let mut found = get_related_table_ids_in_stack(&self.sess.table_assocs, tid, tag)?;
        found.extend(get_related_table_ids_in_main(&self.txn, tid.id, tag)?);
        Ok(found)
    }

    fn edge_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        let tag = DataKind::Edge;
        let mut found = get_related_table_ids_in_stack(&self.sess.table_assocs, tid, tag)?;
        found.extend(get_related_table_ids_in_main(&self.txn, tid.id, tag)?);
        Ok(found)
    }

    fn bwd_edge_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        let tag = DataKind::EdgeBwd;
        let mut found = get_related_table_ids_in_stack(&self.sess.table_assocs, tid, tag)?;
        found.extend(get_related_table_ids_in_main(&self.txn, tid.id, tag)?);
        Ok(found)
    }

    fn index_ids_by_main_id(&self, tid: TableId) -> Result<Vec<TableId>> {
        let tag = DataKind::Index;
        let mut found = get_related_table_ids_in_stack(&self.sess.table_assocs, tid, tag)?;
        found.extend(get_related_table_ids_in_main(&self.txn, tid.id, tag)?);
        Ok(found)
    }

    fn store_table(&mut self, mut info: TableInfo) -> Result<()> {
        let table_id = self.gen_table_id()?;
        let tid = table_id.id;
        let stack_frame = self.sess.stack.last_mut().unwrap();
        if stack_frame.contains_key(info.table_name()) {
            return Err(DdlReifyError::NameConflict(info.table_name().to_string()));
        } else {
            info.set_table_id(table_id);
            match &info {
                TableInfo::Edge(info) => {
                    let edge_assocs = self
                        .sess
                        .table_assocs
                        .entry(DataKind::Edge)
                        .or_insert(Default::default());
                    let src_assocs = edge_assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);

                    let back_edge_assocs = self
                        .sess
                        .table_assocs
                        .entry(DataKind::EdgeBwd)
                        .or_insert(Default::default());
                    let dst_assocs = back_edge_assocs
                        .entry(info.dst_id)
                        .or_insert(Default::default());
                    dst_assocs.insert(tid);
                }
                TableInfo::Assoc(info) => {
                    let assocs = self
                        .sess
                        .table_assocs
                        .entry(DataKind::Assoc)
                        .or_insert(Default::default());
                    let src_assocs = assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);
                }
                TableInfo::Index(info) => {
                    let idx_assocs = self
                        .sess
                        .table_assocs
                        .entry(DataKind::Index)
                        .or_insert(Default::default());
                    let src_assocs = idx_assocs.entry(info.src_id).or_insert(Default::default());
                    src_assocs.insert(tid);
                }
                TableInfo::Node(_) => {}
                TableInfo::Sequence(_) => {}
            }

            stack_frame.insert(info.table_name().to_string(), SessionDefinable::Table(tid));
            self.sess.tables.insert(tid, info);
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct TempDbContext<'a> {
    sess: &'a mut Session,
    txn: TransactionPtr,
}

impl Session {
    pub(crate) fn main_ctx(&self) -> MainDbContext {
        let txn = self.txn(None);
        txn.set_snapshot();
        MainDbContext { sess: self, txn }
    }
    pub(crate) fn temp_ctx(&mut self) -> TempDbContext {
        let txn = self.txn(None);
        TempDbContext { sess: self, txn }
    }
}
