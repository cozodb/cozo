use crate::algebra::op::{build_binding_map_from_info, RelationalAlgebra};
use crate::algebra::parser::RaBox;
use crate::context::TempDbContext;
use crate::data::tuple_set::{shift_merge_binding_map, BindingMap, TupleSet};
use crate::ddl::reify::TableInfo;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_NESTED_LOOP_LEFT: &str = "NestedLoop";

pub(crate) struct NestedLoopLeft<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
    pub(crate) right_binding: String,
    pub(crate) left_outer_join: bool,
}

fn nested_binding(left: &RaBox, binding: &str) -> Result<BTreeSet<String>> {
    let mut bindings = left.bindings()?;
    bindings.insert(binding.to_string());
    Ok(bindings)
}

fn nested_binding_map(
    ctx: &TempDbContext,
    left: &RaBox,
    right_info: &TableInfo,
    right_binding: &str,
) -> Result<BindingMap> {
    let mut binding_map = left.binding_map()?;
    let right = build_binding_map_from_info(ctx, right_info, &[])?;
    let right_map = BTreeMap::from([(right_binding.to_string(), right)]);
    shift_merge_binding_map(&mut binding_map, right_map);
    Ok(binding_map)
}

impl<'b> RelationalAlgebra for NestedLoopLeft<'b> {
    fn name(&self) -> &str {
        NAME_NESTED_LOOP_LEFT
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        nested_binding(&self.left, &self.right_binding)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        nested_binding_map(self.ctx, &self.left, &self.right, &self.right_binding)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

pub(crate) const NAME_NESTED_LOOP_RIGHT: &str = "NestedLoopRight";

pub(crate) struct NestedLoopRight<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
    pub(crate) right_binding: String,
}

impl<'b> RelationalAlgebra for NestedLoopRight<'b> {
    fn name(&self) -> &str {
        NAME_NESTED_LOOP_RIGHT
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        nested_binding(&self.left, &self.right_binding)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        nested_binding_map(self.ctx, &self.left, &self.right, &self.right_binding)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

pub(crate) const NAME_NESTED_LOOP_OUTER: &str = "NestedLoopOuter";

pub(crate) struct NestedLoopOuter<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
    pub(crate) right_binding: String,
}

impl<'b> RelationalAlgebra for NestedLoopOuter<'b> {
    fn name(&self) -> &str {
        NAME_NESTED_LOOP_OUTER
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        nested_binding(&self.left, &self.right_binding)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        nested_binding_map(self.ctx, &self.left, &self.right, &self.right_binding)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        todo!()
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
