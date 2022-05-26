use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple_set::{BindingMap, TupleSet};
use crate::ddl::reify::TableInfo;
use crate::parser::{Pairs, Rule};
use anyhow::Result;
use std::collections::BTreeSet;

pub(crate) const NAME_TAKE: &str = "Take";
pub(crate) const NAME_SKIP: &str = "Skip";
pub(crate) const NAME_LIMIT: &str = "Limit";

pub(crate) struct LimitOp<'a> {
    pub(crate) source: RaBox<'a>,
    pub(crate) take_n: Option<usize>,
    pub(crate) skip_n: Option<usize>,
}

impl<'a> LimitOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
        name: &str,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_LIMIT.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let arg = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();
        assert_rule(&arg, Rule::expr, NAME_LIMIT, 1)?;
        let expr = Expr::try_from(arg)?;
        let val = expr.interpret_eval(ctx)?;
        let n = val
            .get_int()
            .ok_or_else(|| AlgebraParseError::ValueError(val.to_static()))?;
        let n = n.abs() as usize;
        let (skip_n, take_n) = match name {
            NAME_SKIP => (Some(n), None),
            NAME_TAKE => (None, Some(n)),
            _ => unreachable!(),
        };
        Ok(Self {
            source,
            take_n,
            skip_n,
        })
    }
}

impl<'b> RelationalAlgebra for LimitOp<'b> {
    fn name(&self) -> &str {
        "Limit"
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        self.source.binding_map()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let iter = self.source.iter()?;
        match (self.take_n, self.skip_n) {
            (None, None) => Ok(iter),
            (Some(n), None) => Ok(Box::new(iter.take(n))),
            (None, Some(n)) => Ok(Box::new(iter.skip(n))),
            (Some(t), Some(s)) => Ok(Box::new(iter.skip(s).take(t))),
        }
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
