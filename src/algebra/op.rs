use crate::algebra::parser::AlgebraParseError;
use crate::context::TempDbContext;
use crate::data::eval::PartialEvalContext;
use crate::data::tuple_set::{BindingMap, TableId, TupleSet};
use crate::ddl::parser::ColExtractor;
use crate::ddl::reify::{AssocInfo, DdlContext, EdgeInfo, IndexInfo, NodeInfo, TableInfo};
use crate::runtime::session::Definable;
use anyhow::Result;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::rc::Rc;

mod from_values;
mod insert;
mod insert_tagged;

pub(crate) use from_values::*;
pub(crate) use insert::*;
pub(crate) use insert_tagged::*;

pub(crate) trait InterpretContext: PartialEvalContext {
    fn resolve_definable(&self, name: &str) -> Option<Definable>;
    fn resolve_table(&self, name: &str) -> Option<TableId>;
    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo>;
    fn get_node_info(&self, table_id: TableId) -> Result<NodeInfo>;
    fn get_edge_info(&self, table_id: TableId) -> Result<EdgeInfo>;
    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>>;
    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)>;
    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>>;
}

impl<'a> InterpretContext for TempDbContext<'a> {
    fn resolve_definable(&self, _name: &str) -> Option<Definable> {
        todo!()
    }

    fn resolve_table(&self, name: &str) -> Option<TableId> {
        self.table_id_by_name(name).ok()
    }

    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo> {
        self.table_by_id(table_id)
            .map_err(|_| AlgebraParseError::TableIdNotFound(table_id).into())
    }

    fn get_node_info(&self, table_id: TableId) -> Result<NodeInfo> {
        match self.get_table_info(table_id)? {
            TableInfo::Node(n) => Ok(n),
            _ => Err(AlgebraParseError::WrongTableKind(table_id).into()),
        }
    }

    fn get_edge_info(&self, table_id: TableId) -> Result<EdgeInfo> {
        match self.get_table_info(table_id)? {
            TableInfo::Edge(n) => Ok(n),
            _ => Err(AlgebraParseError::WrongTableKind(table_id).into()),
        }
    }

    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>> {
        let res = self.assocs_by_main_id(table_id)?;
        Ok(res)
    }

    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)> {
        todo!()
    }

    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>> {
        todo!()
    }
}

pub(crate) trait RelationalAlgebra {
    fn name(&self) -> &str;
    fn binding_map(&self) -> Result<BindingMap>;
    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>>;
    fn identity(&self) -> Option<TableInfo>;
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
