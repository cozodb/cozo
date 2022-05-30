use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, BindingMapEvalContext, TupleSet, MIN_TABLE_ID_BOUND};
use crate::data::value::Value;
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::{Pairs, Rule};
use crate::runtime::options::default_read_options;
use anyhow::Result;
use log::error;
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) const NAME_SORT: &str = "Sort";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SortDirection {
    Asc,
    Dsc,
}

pub(crate) struct SortOp<'a> {
    pub(crate) source: RaBox<'a>,
    ctx: &'a TempDbContext<'a>,
    sort_exprs: Vec<(Expr, SortDirection)>,
    temp_table_id: AtomicU32,
}

impl<'a> SortOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_SORT.to_string());
        let source = match prev {
            Some(source) => source,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };

        let sort_exprs = args
            .map(|arg| -> Result<(Expr, SortDirection)> {
                let mut arg = arg.into_inner().next().unwrap();
                let mut dir = SortDirection::Asc;
                if arg.as_rule() == Rule::sort_arg {
                    let mut pairs = arg.into_inner();
                    arg = pairs.next().unwrap();
                    if pairs.next().unwrap().as_rule() == Rule::desc_dir {
                        dir = SortDirection::Dsc
                    }
                }
                let expr = Expr::try_from(arg)?;
                Ok((expr, dir))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            source,
            ctx,
            sort_exprs,
            temp_table_id: AtomicU32::new(0),
        })
    }
    fn sort_data(&self) -> Result<()> {
        let temp_table_id = self.temp_table_id.load(Ordering::SeqCst);
        assert!(temp_table_id > MIN_TABLE_ID_BOUND);
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let sort_exprs = self
            .sort_exprs
            .iter()
            .map(|(ex, dir)| -> Result<(Expr, SortDirection)> {
                let ex = ex.clone().partial_eval(&binding_ctx)?;
                if !ex.is_not_aggr() {
                    Err(AlgebraParseError::AggregateFnNotAllowed.into())
                } else {
                    Ok((ex, *dir))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut insertion_key = OwnTuple::with_prefix(temp_table_id);
        let mut insertion_val = OwnTuple::with_data_prefix(DataKind::Data);
        for (i, tset) in self.source.iter()?.enumerate() {
            insertion_key.truncate_all();
            insertion_val.truncate_all();
            let tset = tset?;
            for (expr, dir) in &sort_exprs {
                let mut val = expr.row_eval(&tset)?;
                if *dir == SortDirection::Dsc {
                    val = Value::DescVal(Reverse(val.into()))
                }
                insertion_key.push_value(&val);
            }
            insertion_key.push_int(i as i64);
            tset.encode_as_tuple(&mut insertion_val);
            self.ctx
                .sess
                .temp
                .put(&self.ctx.sess.w_opts_temp, &insertion_key, &insertion_val)?;
        }
        Ok(())
    }
}

pub(crate) fn drop_temp_table(ctx: &TempDbContext, id: u32) {
    if id > MIN_TABLE_ID_BOUND {
        let start_key = OwnTuple::with_prefix(id);
        let mut end_key = OwnTuple::with_prefix(id);
        end_key.seal_with_sentinel();
        if let Err(e) = ctx
            .sess
            .temp
            .del_range(&ctx.sess.w_opts_temp, start_key, end_key)
        {
            error!("Undefine temp table failed: {:?}", e)
        }
    }
}

impl<'a> Drop for SortOp<'a> {
    fn drop(&mut self) {
        drop_temp_table(self.ctx, self.temp_table_id.load(Ordering::SeqCst));
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
        if self.temp_table_id.load(Ordering::SeqCst) == 0 {
            let temp_id = self.ctx.gen_table_id()?.id;
            self.temp_table_id.store(temp_id, Ordering::SeqCst);
            self.sort_data()?;
        }
        let r_opts = default_read_options();
        let iter = self.ctx.sess.temp.iterator(&r_opts);
        let key = OwnTuple::with_prefix(self.temp_table_id.load(Ordering::SeqCst));
        Ok(Box::new(iter.iter_rows(key).map(
            |(_k, v)| -> Result<TupleSet> {
                let v = Tuple::new(v);
                let tset = TupleSet::decode_from_tuple(&v)?;
                Ok(tset)
            },
        )))
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
