use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::RaBox;
use crate::data::tuple_set::{BindingMap, TupleSet};
use crate::ddl::reify::TableInfo;
use anyhow::Result;
use std::collections::BTreeSet;

pub(crate) const NAME_NESTED_LOOP_LEFT: &str = "NestedLoop";

pub(crate) struct NestedLoopLeft<'a> {
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
    pub(crate) left_outer_join: bool,
}

impl<'b> RelationalAlgebra for NestedLoopLeft<'b> {
    fn name(&self) -> &str {
        todo!()
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        todo!()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        todo!()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        todo!()
    }
}

pub(crate) const NAME_NESTED_LOOP_RIGHT: &str = "NestedLoopRight";

pub(crate) struct NestedLoopRight<'a> {
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
}

impl<'b> RelationalAlgebra for NestedLoopRight<'b> {
    fn name(&self) -> &str {
        todo!()
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        todo!()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        todo!()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        todo!()
    }
}

pub(crate) const NAME_NESTED_LOOP_OUTER: &str = "NestedLoopOuter";

pub(crate) struct NestedLoopOuter<'a> {
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
}

impl<'b> RelationalAlgebra for NestedLoopOuter<'b> {
    fn name(&self) -> &str {
        todo!()
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        todo!()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        todo!()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        todo!()
    }
}
