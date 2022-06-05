use crate::algebra::op::{build_binding_map_from_info, QueryError, RelationalAlgebra};
use crate::algebra::parser::{AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, ReifiedTuple, Tuple};
use crate::data::tuple_set::{
    shift_merge_binding_map, BindingMap, BindingMapEvalContext, TableId, TupleSet,
    TupleSetEvalContext,
};
use crate::ddl::reify::TableInfo;
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::Result;
use cozorocks::{DbPtr, PrefixIterator, ReadOptionsPtr, TransactionPtr, WriteOptionsPtr};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_NESTED_LOOP_LEFT: &str = "NestedLoop";

pub(crate) struct NestedLoopLeft<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) left: RaBox<'a>,
    pub(crate) right: TableInfo,
    pub(crate) right_binding: String,
    pub(crate) left_outer_join: bool,
    pub(crate) join_key_extractor: Vec<Expr>,
    pub(crate) key_is_prefix: bool,
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
    let right = build_binding_map_from_info(ctx, right_info, &[], true)?;
    let right_map = BindingMap {
        inner_map: BTreeMap::from([(right_binding.to_string(), right)]),
        key_size: 1,
        val_size: 1,
    };
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
        let source_map = self.left.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let key_extractors = self
            .join_key_extractor
            .iter()
            .map(|ex| -> Result<Expr> {
                let ex = ex.clone().partial_eval(&binding_ctx)?;
                if !ex.is_not_aggr() {
                    Err(AlgebraParseError::AggregateFnNotAllowed.into())
                } else {
                    Ok(ex)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let table_id = self.right.table_id();
        let key_tuple = OwnTuple::with_prefix(table_id.id);
        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let w_opts = default_write_options();
        let r_opts = default_read_options();
        let left_join = self.left_outer_join;

        if self.key_is_prefix {
            let left_iter = self.left.iter()?;
            let right_iter = if table_id.in_root {
                txn.iterator(&r_opts)
            } else {
                temp_db.iterator(&r_opts)
            };
            let right_iter = right_iter.iter_prefix(OwnTuple::empty_tuple());
            Ok(Box::new(NestLoopLeftPrefixIter {
                left_join,
                left_iter,
                right_iter,
                right_table_id: table_id,
                key_extractors,
                left_cache: None,
                left_cache_used: false,
                txn,
                temp_db,
                w_opts,
                r_opts,
                always_output_padded: false,
            }))
        } else {
            let iter = unique_prefix_nested_loop(
                self.left.iter()?,
                txn,
                temp_db,
                w_opts,
                r_opts,
                left_join,
                key_tuple,
                key_extractors,
                table_id,
            );
            Ok(Box::new(iter))
        }
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

pub(crate) fn unique_prefix_nested_loop<'a>(
    iter: Box<dyn Iterator<Item = Result<TupleSet>> + 'a>,
    txn: TransactionPtr,
    temp_db: DbPtr,
    w_opts: WriteOptionsPtr,
    r_opts: ReadOptionsPtr,
    left_join: bool,
    mut key_tuple: OwnTuple,
    key_extractors: Vec<Expr>,
    table_id: TableId,
) -> impl Iterator<Item = Result<TupleSet>> + 'a {
    iter.map(move |tset| -> Result<Option<TupleSet>> {
        let mut tset = tset?;
        let eval_ctx = TupleSetEvalContext {
            tuple_set: &tset,
            txn: &txn,
            temp_db: &temp_db,
            write_options: &w_opts,
        };
        key_tuple.truncate_all();
        for extractor in &key_extractors {
            let value = extractor.row_eval(&eval_ctx)?;
            key_tuple.push_value(&value)
        }
        let result = if table_id.in_root {
            txn.get_owned(&r_opts, &key_tuple)?
        } else {
            temp_db.get_owned(&r_opts, &key_tuple)?
        };
        match result {
            None => {
                if left_join {
                    tset.push_key(Tuple::empty_tuple().into());
                    tset.push_val(Tuple::empty_tuple().into());
                    Ok(Some(tset))
                } else {
                    Ok(None)
                }
            }
            Some(tuple) => {
                tset.push_key(key_tuple.clone().into());
                tset.push_val(Tuple::new(tuple).into());
                Ok(Some(tset))
            }
        }
    })
    .filter_map(|rs| match rs {
        Ok(None) => None,
        Ok(Some(t)) => Some(Ok(t)),
        Err(e) => Some(Err(e)),
    })
}

pub(crate) struct NestLoopLeftPrefixIter<'a> {
    pub(crate) left_join: bool,
    pub(crate) always_output_padded: bool,
    pub(crate) left_iter: Box<dyn Iterator<Item = Result<TupleSet>> + 'a>,
    pub(crate) right_iter: PrefixIterator<OwnTuple>,
    pub(crate) right_table_id: TableId,
    pub(crate) key_extractors: Vec<Expr>,
    pub(crate) left_cache: Option<TupleSet>,
    pub(crate) left_cache_used: bool,
    pub(crate) txn: TransactionPtr,
    pub(crate) temp_db: DbPtr,
    pub(crate) w_opts: WriteOptionsPtr,
    pub(crate) r_opts: ReadOptionsPtr,
}

impl<'a> NestLoopLeftPrefixIter<'a> {
    fn make_key_tuple(&self, tset: &TupleSet) -> Result<OwnTuple> {
        let mut key_tuple = OwnTuple::with_prefix(self.right_table_id.id);
        let eval_ctx = TupleSetEvalContext {
            tuple_set: tset,
            txn: &self.txn,
            temp_db: &self.temp_db,
            write_options: &self.w_opts,
        };

        for extractor in &self.key_extractors {
            let value = extractor.row_eval(&eval_ctx)?;
            key_tuple.push_value(&value)
        }
        Ok(key_tuple)
    }
    fn next_inner(&mut self) -> Result<Option<TupleSet>> {
        loop {
            match &self.left_cache {
                None => {
                    match self.left_iter.next() {
                        None => return Ok(None),
                        Some(tset) => {
                            let tset = tset?;
                            let key_tuple = self.make_key_tuple(&tset)?;
                            self.right_iter.reset_prefix(key_tuple);
                            self.left_cache = Some(tset);
                            self.left_cache_used = false;
                        }
                    };
                }

                Some(left_tset) => match self.right_iter.next() {
                    None => {
                        if self.left_join && !self.left_cache_used {
                            let mut left_tset = self.left_cache.take().unwrap();
                            self.left_cache_used = true;
                            left_tset.push_key(OwnTuple::empty_tuple().into());
                            left_tset.push_val(OwnTuple::empty_tuple().into());
                            return Ok(Some(left_tset));
                        } else {
                            // advance left on next hoop
                            self.left_cache.take();
                        }
                    }
                    Some((rk, rv)) => {
                        let mut left_tset = left_tset.clone();
                        let mut key: ReifiedTuple = Tuple::new(rk).into();
                        let mut val: ReifiedTuple = Tuple::new(rv).into();
                        if !matches!(val.data_kind(), Ok(DataKind::Data)) {
                            key = val;
                            val = if self.right_table_id.in_root {
                                Tuple::new(
                                    self.txn
                                        .get_owned(&self.r_opts, &key)?
                                        .ok_or(QueryError::Corruption)?,
                                )
                                .into()
                            } else {
                                Tuple::new(
                                    self.temp_db
                                        .get_owned(&self.r_opts, &key)?
                                        .ok_or(QueryError::Corruption)?,
                                )
                                .into()
                            }
                        }
                        left_tset.push_key(key);
                        left_tset.push_val(val);
                        if !self.always_output_padded {
                            self.left_cache_used = true;
                        }
                        return Ok(Some(left_tset));
                    }
                },
            }
        }
    }
}

impl Iterator for NestLoopLeftPrefixIter<'_> {
    type Item = Result<TupleSet>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_inner() {
            Ok(None) => None,
            Ok(Some(t)) => Some(Ok(t)),
            Err(e) => Some(Err(e)),
        }
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
