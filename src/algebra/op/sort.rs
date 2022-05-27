use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::RaBox;
use crate::context::TempDbContext;
use crate::data::expr::StaticExpr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet, MIN_TABLE_ID_BOUND, BindingMapEvalContext};
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::runtime::options::{default_read_options};
use anyhow::Result;
use log::error;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BTreeSet};
use crate::data::value::Value;

pub(crate) const NAME_SORT: &str = "Sort";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SortDirection {
    Asc,
    Dsc,
}

pub(crate) struct SortOp<'a> {
    source: RaBox<'a>,
    ctx: &'a TempDbContext<'a>,
    sort_exprs: Vec<(StaticExpr, SortDirection)>,
    temp_table_id: RefCell<u32>,
}

impl<'a> SortOp<'a> {
    fn sort_data(&self) -> Result<()> {
        let temp_table_id = *self.temp_table_id.borrow();
        assert!(temp_table_id > MIN_TABLE_ID_BOUND);
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let sort_exprs = self.sort_exprs.iter().map(|(ex, dir)| -> Result<(StaticExpr, SortDirection)>{
            let ex = ex.clone().partial_eval(&binding_ctx)?.into_static();
            Ok((ex, *dir))
        }).collect::<Result<Vec<_>>>()?;
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
            self.ctx.sess.temp.put(&self.ctx.sess.w_opts_temp, &insertion_key, &insertion_val)?;
        }
        Ok(())
    }
}

impl<'a> Drop for SortOp<'a> {
    fn drop(&mut self) {
        let id = *self.temp_table_id.borrow();
        if id > MIN_TABLE_ID_BOUND {
            let start_key = OwnTuple::with_prefix(id);
            let mut end_key = OwnTuple::with_prefix(id);
            end_key.seal_with_sentinel();
            if let Err(e) =
                self.ctx
                    .sess
                    .temp
                    .del_range(&self.ctx.sess.w_opts_temp, start_key, end_key)
            {
                error!("Undefine temp table failed: {:?}", e)
            }
        }
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
        if *self.temp_table_id.borrow() == 0 {
            *self.temp_table_id.borrow_mut() = self.ctx.gen_temp_table_id().id;
            self.sort_data()?;
        }
        let r_opts = default_read_options();
        let iter = self.ctx.sess.temp.iterator(&r_opts);
        let key = OwnTuple::with_prefix(*self.temp_table_id.borrow());
        Ok(Box::new(iter.iter_rows(key).map(|(_k, v)| -> Result<TupleSet> {
            let v = Tuple::new(v);
            let tset = TupleSet::decode_from_tuple(&v)?;
            Ok(tset)
        })))
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
