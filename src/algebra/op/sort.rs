use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::RaBox;
use crate::data::expr::StaticExpr;
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::ddl::reify::TableInfo;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_SORT: &str = "Sort";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SortDirection {
    Asc,
    Dsc,
}

pub(crate) struct SortOp<'a> {
    source: RaBox<'a>,
    sort_exprs: Vec<(StaticExpr, SortDirection)>,
}

impl<'a> Drop for SortOp<'a> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<'b> RelationalAlgebra for SortOp<'b> {
    fn name(&self) -> &str {
        NAME_SORT
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        self.source.binding_map()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
