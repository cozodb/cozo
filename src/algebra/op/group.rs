use crate::algebra::op::{drop_temp_table, RelationalAlgebra, SelectOpError, SortDirection};
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::op_agg::OpAgg;
use crate::data::parser::{parse_keyed_dict, parse_scoped_dict};
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{
    BindingMap, BindingMapEvalContext, TupleSet, TupleSetEvalContext, TupleSetIdx,
    MIN_TABLE_ID_BOUND,
};
use crate::data::value::Value;
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::{Pairs, Rule};
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::Result;
use cozorocks::{DbPtr, IteratorPtr, TransactionPtr, WriteOptionsPtr};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;

pub(crate) const NAME_GROUP: &str = "Group";

pub(crate) struct GroupOp<'a> {
    pub(crate) source: RaBox<'a>,
    ctx: &'a TempDbContext<'a>,
    binding: String,
    group_keys: Vec<(String, Expr)>,
    sort_keys: Vec<(Expr, SortDirection)>,
    vals: Expr,
    temp_table_id: AtomicU32,
}

impl<'a> GroupOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_GROUP.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };

        let pair = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();
        let (binding, mut keys, extract_map) = match pair.as_rule() {
            Rule::keyed_dict => {
                let (keys, vals) = parse_keyed_dict(pair)?;
                ("_".to_string(), keys, vals)
            }
            _ => {
                assert_rule(&pair, Rule::scoped_dict, NAME_GROUP, 2)?;
                parse_scoped_dict(pair)?
            }
        };
        if keys.is_empty() {
            keys.insert("_sort_key".to_string(), Expr::Const(Value::Null));
        }

        let sort_keys = args
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
            binding,
            group_keys: keys.into_iter().collect::<Vec<_>>(),
            sort_keys,
            vals: extract_map,
            temp_table_id: Default::default(),
        })
    }

    fn group_data(&self) -> Result<()> {
        let temp_table_id = self.temp_table_id.load(SeqCst);
        assert!(temp_table_id > MIN_TABLE_ID_BOUND);
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let group_exprs = self
            .group_keys
            .iter()
            .map(|(_, ex)| -> Result<Expr> {
                let ex = ex.clone().partial_eval(&binding_ctx)?;
                if !ex.is_not_aggr() {
                    Err(AlgebraParseError::AggregateFnNotAllowed.into())
                } else {
                    Ok(ex)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_exprs = self
            .sort_keys
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
            for expr in &group_exprs {
                let val = expr.row_eval(&tset)?;
                insertion_key.push_value(&val);
            }
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

impl<'a> Drop for GroupOp<'a> {
    fn drop(&mut self) {
        drop_temp_table(self.ctx, self.temp_table_id.load(SeqCst));
    }
}

impl<'b> RelationalAlgebra for GroupOp<'b> {
    fn name(&self) -> &str {
        NAME_GROUP
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let mut inner = BTreeMap::new();

        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let extract_map = match self.vals.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d
                .keys()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.to_string(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            Expr::Const(Value::Dict(d)) => d
                .keys()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.to_string(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            _ex => return Err(SelectOpError::NeedsDict.into()),
        };

        for (i, (k, _)) in extract_map.iter().enumerate() {
            inner.insert(
                k.to_string(),
                TupleSetIdx {
                    is_key: false,
                    t_set: 0,
                    col_idx: i,
                },
            );
        }
        for (i, (k, _)) in self.group_keys.iter().enumerate() {
            inner.insert(
                k.to_string(),
                TupleSetIdx {
                    is_key: true,
                    t_set: 0,
                    col_idx: i,
                },
            );
        }
        Ok(BindingMap {
            inner_map: BTreeMap::from([(self.binding.clone(), inner)]),
            key_size: 1,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        if self.temp_table_id.load(SeqCst) == 0 {
            let temp_id = self.ctx.gen_table_id()?.id;
            self.temp_table_id.store(temp_id, SeqCst);
            self.group_data()?;
        }

        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };

        let mut val_extractors = vec![];
        let mut val_collectors = vec![];

        let extraction_vec = match self.vals.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d.values().cloned().collect::<Vec<_>>(),
            Expr::Const(Value::Dict(d)) => d
                .values()
                .map(|v| Expr::Const(v.clone()))
                .collect::<Vec<_>>(),
            _ex => return Err(SelectOpError::NeedsDict.into()),
        };

        for ex in extraction_vec {
            let ex = ex.clone().partial_eval(&binding_ctx)?;
            if !ex.is_aggr_compatible() {
                return Err(AlgebraParseError::ScalarFnNotAllowed.into());
            }
            val_collectors.extend(ex.clone().extract_aggr_heads()?);
            ex.aggr_reset();
            val_extractors.push(ex);
        }

        let r_opts = default_read_options();
        let iter = self.ctx.sess.temp.iterator(&r_opts);
        let key = OwnTuple::with_prefix(self.temp_table_id.load(SeqCst));
        iter.seek(&key);
        match iter.pair() {
            Some((k, v)) => {
                let last_key = Tuple::new(k).to_owned();
                let last_val = Tuple::new(v).to_owned();
                Ok(Box::new(GroupIterator {
                    source: iter,
                    last_key,
                    last_val,
                    group_key_len: self.group_keys.len(),
                    val_extractors,
                    val_collectors,
                    txn: self.ctx.txn.clone(),
                    temp_db: self.ctx.sess.temp.clone(),
                    write_options: default_write_options(),
                }))
            }
            None => Ok(Box::new([].into_iter())),
        }
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

pub(crate) struct GroupIterator {
    pub(crate) source: IteratorPtr,
    pub(crate) last_key: OwnTuple,
    pub(crate) last_val: OwnTuple,
    pub(crate) group_key_len: usize,
    pub(crate) val_extractors: Vec<Expr>,
    pub(crate) val_collectors: Vec<(OpAgg, Vec<Expr>)>,
    pub(crate) txn: TransactionPtr,
    pub(crate) temp_db: DbPtr,
    pub(crate) write_options: WriteOptionsPtr,
}

impl GroupIterator {
    fn reset_aggrs(&self) {
        for expr in &self.val_extractors {
            expr.aggr_reset();
        }
    }
    fn update_last_kv<T1: AsRef<[u8]>, T2: AsRef<[u8]>>(&mut self, cur_k: T1, cur_v: T2) {
        self.last_key.clear_cache();
        self.last_key.data.clear();
        self.last_key.data.extend_from_slice(cur_k.as_ref());
        self.last_val.clear_cache();
        self.last_val.data.clear();
        self.last_val.data.extend_from_slice(cur_v.as_ref());
    }
    fn get_tset(&mut self) -> Result<TupleSet> {
        let tset = TupleSet::decode_from_tuple(&self.last_val)?;
        let eval_ctx = TupleSetEvalContext {
            tuple_set: &tset,
            txn: &self.txn,
            temp_db: &self.temp_db,
            write_options: &self.write_options,
        };
        let mut val_tuple = OwnTuple::with_data_prefix(DataKind::Data);
        for extractor in &self.val_extractors {
            val_tuple.push_value(&extractor.row_eval(&eval_ctx)?);
        }
        let mut key_tuple = OwnTuple::with_prefix(self.last_key.get_prefix());
        for val in self.last_key.iter().take(self.group_key_len) {
            let val = val?;
            key_tuple.push_value(&val);
        }
        Ok(TupleSet {
            keys: vec![key_tuple.into()],
            vals: vec![val_tuple.into()],
        })
    }
    fn collect(&mut self) -> Result<()> {
        let tset = TupleSet::decode_from_tuple(&self.last_val)?;
        let eval_ctx = TupleSetEvalContext {
            tuple_set: &tset,
            txn: &self.txn,
            temp_db: &self.temp_db,
            write_options: &self.write_options,
        };
        for (op, args) in &self.val_collectors {
            match args.len() {
                0 => op.put(&[])?,
                1 => {
                    let arg = args.iter().next().unwrap();
                    let arg = arg.row_eval(&eval_ctx)?;
                    op.put(&[arg])?;
                }
                _ => {
                    let mut args_vals = Vec::with_capacity(args.len());
                    for arg in args {
                        args_vals.push(arg.row_eval(&eval_ctx)?);
                    }
                    op.put(&args_vals)?;
                }
            }
        }
        Ok(())
    }
    fn next_inner(&mut self) -> Result<Option<TupleSet>> {
        loop {
            if matches!(self.last_key.data_kind(), Ok(DataKind::Empty)) {
                return Ok(None);
            }
            self.source.next();
            match self.source.pair() {
                None => {
                    let ret = self.get_tset()?;
                    self.last_key.truncate_all();
                    self.last_key.overwrite_prefix(DataKind::Empty as u32);
                    return Ok(Some(ret));
                }
                Some((cur_k, cur_v)) => {
                    let cur_k = Tuple::new(cur_k);
                    match cur_k.key_part_truncate_cmp(&self.last_key, self.group_key_len) {
                        Ordering::Equal => {
                            self.collect()?;
                            self.update_last_kv(cur_k, cur_v);
                        }
                        Ordering::Greater => {
                            let ret = self.get_tset()?;
                            self.update_last_kv(cur_k, cur_v);
                            self.reset_aggrs();
                            return Ok(Some(ret));
                        }
                        Ordering::Less => unreachable!(),
                    }
                }
            }
        }
    }
}

impl Iterator for GroupIterator {
    type Item = Result<TupleSet>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_inner() {
            Ok(None) => None,
            Ok(Some(v)) => Some(Ok(v)),
            Err(e) => Some(Err(e)),
        }
    }
}
