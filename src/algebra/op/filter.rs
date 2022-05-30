use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple_set::{BindingMap, BindingMapEvalContext, TupleSet, TupleSetEvalContext};
use crate::data::value::{StaticValue, Value};
use crate::ddl::reify::TableInfo;
use crate::parser::{Pairs, Rule};
use crate::runtime::options::default_write_options;
use anyhow::Result;
use std::collections::BTreeSet;

pub(crate) const NAME_WHERE: &str = "Where";

pub(crate) struct WhereFilter<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) source: RaBox<'a>,
    pub(crate) condition: Expr,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum FilterError {
    #[error("Expected boolean, evaluated to {0}")]
    ExpectBoolean(StaticValue),
}

impl<'a> WhereFilter<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_WHERE.to_string());
        let source = match prev {
            Some(source) => source,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let mut conds = vec![];
        for arg in args {
            let arg = arg.into_inner().next().unwrap();
            assert_rule(&arg, Rule::expr, NAME_WHERE, 1)?;
            let cond = Expr::try_from(arg)?;
            conds.push(cond);
        }
        let condition = Expr::OpAnd(conds);
        if !condition.is_not_aggr() {
            Err(AlgebraParseError::AggregateFnNotAllowed.into())
        } else {
            Ok(Self {
                ctx,
                source,
                condition,
            })
        }
    }
}

impl<'b> RelationalAlgebra for WhereFilter<'b> {
    fn name(&self) -> &str {
        NAME_WHERE
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        self.source.binding_map()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let condition = self.condition.clone().partial_eval(&binding_ctx)?;
        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let w_opts = default_write_options();

        let iter = self
            .source
            .iter()?
            .filter_map(move |tset| -> Option<Result<TupleSet>> {
                match tset {
                    Ok(tset) => {
                        let eval_ctx = TupleSetEvalContext {
                            tuple_set: &tset,
                            txn: &txn,
                            temp_db: &temp_db,
                            write_options: &w_opts,
                        };
                        match condition.row_eval(&eval_ctx) {
                            Ok(val) => match val {
                                Value::Null | Value::Bool(false) => None,
                                Value::Bool(true) => Some(Ok(tset)),
                                v => Some(Err(FilterError::ExpectBoolean(v.into_static()).into())),
                            },
                            Err(e) => Some(Err(e)),
                        }
                    }
                    e @ Err(_) => Some(e),
                }
            });
        Ok(Box::new(iter))
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
