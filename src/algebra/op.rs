use crate::algebra::parser::AlgebraParseError;
use crate::context::TempDbContext;
use crate::data::eval::PartialEvalContext;
use crate::data::tuple_set::{BindingMap, TableId, TupleSet};
use crate::ddl::parser::ColExtractor;
use crate::ddl::reify::{AssocInfo, DdlContext, DdlReifyError, EdgeInfo, IndexInfo, TableInfo};
use anyhow::Result;
use cozorocks::PinnableSlicePtr;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

mod assoc;
mod cartesian;
mod concat;
mod filter;
mod from;
mod group;
mod insert;
mod limit;
mod merge;
mod nested_loop;
mod scan;
mod select;
mod sort;
mod tagged;
mod union;
mod values;

use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::value::StaticValue;
use crate::runtime::options::default_read_options;
pub(crate) use assoc::*;
pub(crate) use cartesian::*;
pub(crate) use concat::*;
pub(crate) use filter::*;
pub(crate) use from::*;
pub(crate) use group::*;
pub(crate) use insert::*;
pub(crate) use limit::*;
pub(crate) use merge::*;
pub(crate) use nested_loop::*;
pub(crate) use scan::*;
pub(crate) use select::*;
pub(crate) use sort::*;
pub(crate) use tagged::*;
pub(crate) use union::*;
pub(crate) use values::*;

#[derive(thiserror::Error, Debug)]
pub(crate) enum QueryError {
    #[error("Data corruption")]
    Corruption,
}

pub(crate) trait InterpretContext: PartialEvalContext {
    fn resolve_table(&self, name: &str) -> Option<TableId>;
    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo>;
    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>>;
    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)>;
    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>>;
}

impl<'a> InterpretContext for TempDbContext<'a> {
    fn resolve_table(&self, name: &str) -> Option<TableId> {
        self.table_id_by_name(name).ok()
    }

    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo> {
        self.table_by_id(table_id)
            .map_err(|_| AlgebraParseError::TableIdNotFound(table_id).into())
    }

    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>> {
        let res = self.assocs_by_main_id(table_id)?;
        Ok(res)
    }

    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)> {
        let mut fwd_edges = vec![];
        let mut bwd_edges = vec![];

        if let Some(assoc_tables) = self.sess.table_assocs.get(&DataKind::Edge) {
            if let Some(edge_ids) = assoc_tables.get(&table_id) {
                for id in edge_ids {
                    let tid = TableId {
                        in_root: false,
                        id: *id,
                    };
                    let edge_info = self.get_table_info(tid)?.into_edge()?;
                    fwd_edges.push(edge_info)
                }
            }
        }

        if let Some(assoc_tables) = self.sess.table_assocs.get(&DataKind::EdgeBwd) {
            if let Some(edge_ids) = assoc_tables.get(&table_id) {
                for id in edge_ids {
                    let tid = TableId {
                        in_root: false,
                        id: *id,
                    };
                    let edge_info = self.get_table_info(tid)?.into_edge()?;
                    bwd_edges.push(edge_info)
                }
            }
        }

        if table_id.in_root {
            let mut slice = PinnableSlicePtr::default();
            let r_opts = default_read_options();
            let mut key = OwnTuple::with_prefix(0);
            key.push_int(table_id.id as i64);
            key.push_int(DataKind::Edge as i64);
            if self.txn.get_for_update(&r_opts, &key, &mut slice)? {
                let res_tuple = Tuple::new(&slice);
                for item in res_tuple.iter() {
                    let item = item?;
                    let id = item
                        .get_int()
                        .ok_or_else(|| DdlReifyError::Corruption(res_tuple.to_owned()))?;
                    let tid = TableId {
                        in_root: true,
                        id: id as u32,
                    };
                    let edge_info = self.get_table_info(tid)?.into_edge()?;
                    fwd_edges.push(edge_info)
                }
            }
            key.truncate_all();
            key.push_int(table_id.id as i64);
            key.push_int(DataKind::EdgeBwd as i64);
            if self.txn.get_for_update(&r_opts, &key, &mut slice)? {
                let res_tuple = Tuple::new(&slice);
                for item in res_tuple.iter() {
                    let item = item?;
                    let id = item
                        .get_int()
                        .ok_or_else(|| DdlReifyError::Corruption(res_tuple.to_owned()))?;
                    let tid = TableId {
                        in_root: true,
                        id: id as u32,
                    };
                    let edge_info = self.get_table_info(tid)?.into_edge()?;
                    bwd_edges.push(edge_info)
                }
            }
        }
        Ok((fwd_edges, bwd_edges))
    }

    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>> {
        let mut collected = vec![];

        if let Some(assoc_tables) = self.sess.table_assocs.get(&DataKind::Index) {
            if let Some(index_ids) = assoc_tables.get(&table_id) {
                for id in index_ids {
                    let tid = TableId {
                        in_root: false,
                        id: *id,
                    };
                    let info = self.get_table_info(tid)?.into_index()?;
                    collected.push(info)
                }
            }
        }

        if table_id.in_root {
            let mut slice = PinnableSlicePtr::default();
            let r_opts = default_read_options();
            let mut key = OwnTuple::with_prefix(0);
            key.push_int(table_id.id as i64);
            key.push_int(DataKind::Index as i64);
            if self.txn.get_for_update(&r_opts, &key, &mut slice)? {
                let res_tuple = Tuple::new(&slice);
                for item in res_tuple.iter() {
                    let item = item?;
                    let id = item
                        .get_int()
                        .ok_or_else(|| DdlReifyError::Corruption(res_tuple.to_owned()))?;
                    let tid = TableId {
                        in_root: true,
                        id: id as u32,
                    };
                    let edge_info = self.get_table_info(tid)?.into_index()?;
                    collected.push(edge_info)
                }
            }
        }

        Ok(collected)
    }
}

pub(crate) trait RelationalAlgebra {
    fn name(&self) -> &str;
    fn bindings(&self) -> Result<BTreeSet<String>>;
    fn binding_map(&self) -> Result<BindingMap>;
    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>>;
    fn identity(&self) -> Option<TableInfo>;
    fn get_values(&self) -> Result<Vec<StaticValue>> {
        let bmap = self.binding_map()?;
        let mut ret_map = BTreeMap::new();
        for (k, vs) in &bmap.inner_map {
            if !k.starts_with('@') {
                for (sk, v) in vs {
                    if ret_map.contains_key(sk) {
                        ret_map.insert("__".to_string() + k + "_" + sk, Expr::TupleSetIdx(*v));
                    } else {
                        ret_map.insert(sk.to_string(), Expr::TupleSetIdx(*v));
                    }
                }
            }
        }
        let bmap = Expr::Dict(ret_map);
        let mut collected = vec![];
        for tuple in self.iter()? {
            let tuple = tuple?;
            let val = bmap.row_eval(&tuple)?.into_static();
            collected.push(val);
        }
        Ok(collected)
    }
}

type KeyBuilderSet = (
    Vec<ColExtractor>,
    Vec<ColExtractor>,
    Option<Vec<ColExtractor>>,
);

struct TableInfoByNameCache<'a> {
    ctx: &'a TempDbContext<'a>,
    cache: BTreeMap<String, Rc<TableInfo>>,
}

impl<'a> TableInfoByNameCache<'a> {
    fn get_info(&mut self, name: &str) -> Result<Rc<TableInfo>> {
        if !self.cache.contains_key(name) {
            let tid = self.ctx.table_id_by_name(name)?;
            let info = self.ctx.get_table_info(tid)?;
            self.cache.insert(name.to_string(), info.into());
        }
        Ok(self.cache.get(name).unwrap().clone())
    }
}

struct TableInfoByIdCache<'a> {
    ctx: &'a TempDbContext<'a>,
    cache: BTreeMap<TableId, Rc<TableInfo>>,
}

impl<'a> TableInfoByIdCache<'a> {
    fn get_info(&mut self, tid: TableId) -> Result<Rc<TableInfo>> {
        if let Entry::Vacant(e) = self.cache.entry(tid) {
            let info = self.ctx.get_table_info(tid)?;
            e.insert(info.into());
        }
        Ok(self.cache.get(&tid).unwrap().clone())
    }
}
