use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::RaBox;
use crate::context::TempDbContext;
use crate::data::tuple_set::{BindingMap, TupleSet};
use crate::ddl::reify::{AssocInfo, TableInfo};
use anyhow::Result;
use std::collections::BTreeSet;
use crate::data::expr::StaticExpr;

pub(crate) const NAME_ASSOC: &str = "Assoc";

pub(crate) struct AssocOp<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) source: RaBox<'a>,
    pub(crate) assoc_infos: Vec<AssocInfo>,
    pub(crate) key_extractors: Vec<StaticExpr>,
}

impl<'b> RelationalAlgebra for AssocOp<'b> {
    fn name(&self) -> &str {
        NAME_ASSOC
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        todo!()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
