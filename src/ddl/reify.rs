use std::collections::BTreeSet;
use std::result;
use chrono::format::Item;
use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::{Expr, StaticExpr};
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::Value;
use crate::ddl::parser::{AssocSchema, ColSchema, DdlSchema, EdgeSchema, IndexSchema, NodeSchema, SequenceSchema};

#[derive(thiserror::Error, Debug)]
pub(crate) enum DdlReifyError {
    #[error("Name clash: {0}")]
    NameClash(String),

    #[error(transparent)]
    Eval(#[from] EvalError),
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
}

impl TableInfo {
    pub(crate) fn table_id(&self) -> TableId {
        match self {
            TableInfo::Node(n) => n.tid,
            TableInfo::Edge(e) => e.tid
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

pub(crate) struct SequenceInfo {
    pub(crate) name: String,
    pub(crate) tid: TableId,
}

pub(crate) trait DdlContext {
    fn gen_table_id(&mut self) -> TableId;
    fn resolve_table_id_for_derivation<I: IntoIterator<Item=TableKind>>(&self, name: &str, kind: I) -> Result<TableId>;
    fn resolve_table<I: IntoIterator<Item=TableKind>>(&self, name: &str, kind: I, for_derivation: bool) -> Result<TableInfo>;
    fn resolve_table_by_id(&self, tid: TableId) -> Result<TableInfo>;
    fn resolve_associates_for(&self, id: TableId) -> Vec<AssocInfo>;
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
            tid: self.gen_table_id(),
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_node(info)
    }
    fn store_node(&mut self, info: NodeInfo) -> Result<()>;
    fn build_edge(&mut self, schema: EdgeSchema) -> Result<()> {
        check_name_clash([&schema.keys, &schema.vals])?;
        let info = EdgeInfo {
            name: schema.name,
            tid: self.gen_table_id(),
            src_id: self.resolve_table_id_for_derivation(&schema.src_name, [TableKind::Node])?,
            dst_id: self.resolve_table_id_for_derivation(&schema.dst_name, [TableKind::Node])?,
            keys: eval_defaults(schema.keys)?,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_edge(info)
    }
    fn store_edge(&mut self, info: EdgeInfo) -> Result<()>;
    fn build_assoc(&mut self, schema: AssocSchema) -> Result<()> {
        let src_info = self.resolve_table(&schema.src_name, [TableKind::Node, TableKind::Edge], true)?;
        let src_id = src_info.table_id();
        let associates = self.resolve_associates_for(src_id);
        let mut names_to_check: Vec<_> = associates.iter().map(|ai| &ai.vals).collect();
        names_to_check.push(&schema.vals);
        check_name_clash(names_to_check)?;
        let info = AssocInfo {
            name: schema.name,
            tid: self.gen_table_id(),
            src_id,
            vals: eval_defaults(schema.vals)?,
        };
        self.store_assoc(info)
    }
    fn store_assoc(&mut self, info: AssocInfo) -> Result<()>;
    fn build_index(&mut self, schema: IndexSchema) -> Result<()> {
        let src_schema = self.resolve_table(&schema.src_name, [TableKind::Node, TableKind::Edge], true)?;
        let associates = self.resolve_associates_for(src_schema.table_id());
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
                let src_info = self.resolve_table_by_id(edge_info.src_id)?;
                let src_keys = match &src_info {
                    TableInfo::Node(n) => &n.keys,
                    _ => unreachable!()
                };
                let dst_info = self.resolve_table_by_id(edge_info.dst_id)?;
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
        };

        let info = IndexInfo {
            name: schema.name,
            tid: self.gen_table_id(),
            src_id: src_schema.table_id(),
            assoc_ids: schema.assoc_names.iter().map(|n|
                self.resolve_table_id_for_derivation(n, [TableKind::Assoc]))
                .collect::<Result<Vec<_>>>()?,
            index: index_exprs,
        };
        self.store_index(info)
    }
    fn store_index(&mut self, info: IndexInfo) -> Result<()>;
    fn build_sequence(&mut self, schema: SequenceSchema) -> Result<()> {
        let tid = self.gen_table_id();
        self.store_sequence(SequenceInfo {
            name: schema.name,
            tid,
        })
    }
    fn store_sequence(&mut self, info: SequenceInfo) -> Result<()>;
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

    fn resolve_table_col(&self, _binding: &str, _col: &str) -> Option<(TableId, ColId)> {
        None
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
        for (i, col) in self.keys.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: true,
                    t_set: 0,
                    col_idx: i + 1 + self.src_keys.len(),
                });
            }
        }
        for (i, col) in self.dst_keys.iter().enumerate() {
            if name == col.name {
                return Some(TupleSetIdx {
                    is_key: true,
                    t_set: 0,
                    col_idx: i + 2 + self.src_keys.len() + self.dst_keys.len(),
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

impl<'a> PartialEvalContext for EdgeDefEvalCtx<'a> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        self.resolve_name(key).map(Expr::TupleSetIdx)
    }

    fn resolve_table_col(&self, _binding: &str, _col: &str) -> Option<(TableId, ColId)> {
        None
    }
}
