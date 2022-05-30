use crate::algebra::op::{
    concat_binding_map, drop_temp_table, make_concat_iter, RelationalAlgebra,
};
use crate::algebra::parser::{build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::tuple::{OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet};
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::Pairs;
use crate::runtime::options::default_read_options;
use anyhow::Result;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) const NAME_UNION: &str = "Union";

pub(crate) struct UnionOp<'a> {
    pub(crate) sources: Vec<RaBox<'a>>,
    ctx: &'a TempDbContext<'a>,
    temp_table_id: AtomicU32,
}

impl<'a> Drop for UnionOp<'a> {
    fn drop(&mut self) {
        drop_temp_table(self.ctx, self.temp_table_id.load(Ordering::SeqCst));
    }
}

impl<'a> UnionOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_UNION.to_string());
        let mut sources = vec![];
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        sources.push(source);
        for arg in args {
            let source = build_relational_expr(ctx, arg)?;
            sources.push(source)
        }
        Ok(Self {
            sources,
            ctx,
            temp_table_id: Default::default(),
        })
    }

    fn dedup_data(&self) -> Result<()> {
        let iter = make_concat_iter(&self.sources, self.binding_map()?)?;

        let mut cache_tuple = OwnTuple::with_prefix(self.temp_table_id.load(SeqCst));
        let dummy = OwnTuple::empty_tuple();
        let db = &self.ctx.sess.temp;
        let w_opts = &self.ctx.sess.w_opts_temp;
        for tset in iter {
            let tset = tset?;
            tset.encode_as_tuple(&mut cache_tuple);
            db.put(w_opts, &cache_tuple, &dummy)?;
        }
        Ok(())
    }
}

impl<'b> RelationalAlgebra for UnionOp<'b> {
    fn name(&self) -> &str {
        NAME_UNION
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        let mut ret = BTreeSet::new();
        for el in &self.sources {
            ret.extend(el.bindings()?)
        }
        Ok(ret)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let maps = self
            .sources
            .iter()
            .map(|el| el.binding_map())
            .collect::<Result<Vec<_>>>()?;

        Ok(concat_binding_map(maps.into_iter()))
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        if self.temp_table_id.load(SeqCst) == 0 {
            let temp_id = self.ctx.gen_table_id()?.id;
            self.temp_table_id.store(temp_id, SeqCst);
            self.dedup_data()?;
        }
        let r_opts = default_read_options();
        let iter = self.ctx.sess.temp.iterator(&r_opts);
        let key = OwnTuple::with_prefix(self.temp_table_id.load(Ordering::SeqCst));
        Ok(Box::new(iter.iter_rows(key).map(
            |(k, _v)| -> Result<TupleSet> {
                let v = Tuple::new(k);
                let tset = TupleSet::decode_from_tuple(&v)?;
                Ok(tset)
            },
        )))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
