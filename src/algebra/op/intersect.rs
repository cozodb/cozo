use crate::algebra::op::{
    concat_binding_map, drop_temp_table, make_concat_iter, RelationalAlgebra,
};
use crate::algebra::parser::{build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet};
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::Pairs;
use crate::runtime::options::default_read_options;
use anyhow::Result;
use cozorocks::PinnableSlicePtr;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) const NAME_INTERSECT: &str = "Intersect";

pub(crate) struct IntersectOp<'a> {
    pub(crate) sources: Vec<RaBox<'a>>,
    ctx: &'a TempDbContext<'a>,
    temp_table_id: AtomicU32,
}

impl<'a> Drop for IntersectOp<'a> {
    fn drop(&mut self) {
        drop_temp_table(self.ctx, self.temp_table_id.load(Ordering::SeqCst));
    }
}

impl<'a> IntersectOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_INTERSECT.to_string());
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
        let mut counter = OwnTuple::with_data_prefix(DataKind::Data);
        let mut slice_cache = PinnableSlicePtr::default();
        let r_opts = default_read_options();
        let db = &self.ctx.sess.temp;
        let w_opts = &self.ctx.sess.w_opts_temp;
        for tset in iter {
            let tset = tset?;
            tset.encode_as_tuple(&mut cache_tuple);
            let existing = db.get(&r_opts, &cache_tuple, &mut slice_cache)?;
            if existing {
                let found = Tuple::new(slice_cache.as_ref());
                let i = found.get_int(0)?;
                counter.truncate_all();
                counter.push_int(i + 1);
            } else {
                counter.truncate_all();
                counter.push_int(1);
            }
            db.put(w_opts, &cache_tuple, &counter)?;
        }
        Ok(())
    }
}

impl<'b> RelationalAlgebra for IntersectOp<'b> {
    fn name(&self) -> &str {
        NAME_INTERSECT
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
        let n_required = self.sources.len() as i64;
        Ok(Box::new(iter.iter_rows(key).filter_map(
            move |(k, counter)| -> Option<Result<TupleSet>> {
                let v = Tuple::new(k);
                match TupleSet::decode_from_tuple(&v) {
                    Ok(tset) => {
                        let counter = Tuple::new(counter);
                        match counter.get_int(0) {
                            Ok(i) => {
                                if i == n_required {
                                    Some(Ok(tset))
                                } else {
                                    None
                                }
                            }
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            },
        )))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
