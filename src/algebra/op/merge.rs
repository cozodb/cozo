use crate::algebra::op::{drop_temp_table, RelationalAlgebra};
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::op::OP_EQ;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{merge_binding_maps, BindingMap, BindingMapEvalContext, TupleSet};
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::{Pairs, Rule};
use anyhow::Result;
use cozorocks::IteratorPtr;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;

pub(crate) const NAME_INNER_JOIN: &str = "Join";
pub(crate) const NAME_LEFT_JOIN: &str = "LeftJoin";
pub(crate) const NAME_RIGHT_JOIN: &str = "RightJoin";
pub(crate) const NAME_OUTER_JOIN: &str = "OuterJoin";

pub(crate) struct MergeJoin<'a> {
    ctx: &'a TempDbContext<'a>,
    pub(crate) left: RaBox<'a>,
    pub(crate) right: RaBox<'a>,
    pub(crate) join_keys: Vec<(Expr, Expr)>,
    pub(crate) left_outer: bool,
    pub(crate) right_outer: bool,
    left_temp_id: AtomicU32,
    right_temp_id: AtomicU32,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum JoinError {
    #[error("Invalid join condition {0:?}")]
    JoinCondition(Expr),
    #[error("Join condition {0:?} must contain variables {1:?}")]
    NoBoundVariable(Expr, BTreeSet<String>),
    #[error("Join condition {0:?} must not contain variables {1:?}")]
    WrongBoundVariable(Expr, BTreeSet<String>),
}

impl<'a> MergeJoin<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
        kind: &str,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(kind.to_string());
        let left = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let right = build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?;

        let left_bindings = left.bindings()?;
        let right_bindings = right.bindings()?;
        if !left_bindings.is_disjoint(&right_bindings) {
            return Err(AlgebraParseError::DuplicateBinding(
                left_bindings
                    .intersection(&right_bindings)
                    .next()
                    .unwrap()
                    .to_string(),
            )
            .into());
        }
        let mut join_keys: Vec<(Expr, Expr)> = vec![];
        for (i, arg) in args.enumerate() {
            let pair = arg.into_inner().next().unwrap();
            assert_rule(&pair, Rule::expr, kind, i + 2)?;
            let expr = Expr::try_from(pair)?;
            match expr {
                Expr::BuiltinFn(op, args) if op == OP_EQ => {
                    let mut args = args.into_iter();
                    let left_condition = args.next().unwrap();
                    let right_condition = args.next().unwrap();
                    let left_variables = left_condition.all_variables();
                    let right_variables = right_condition.all_variables();
                    if left_variables.is_disjoint(&left_bindings) {
                        return Err(
                            JoinError::NoBoundVariable(left_condition, left_bindings).into()
                        );
                    }
                    if right_variables.is_disjoint(&right_bindings) {
                        return Err(
                            JoinError::NoBoundVariable(right_condition, right_bindings).into()
                        );
                    }
                    if !left_variables.is_disjoint(&right_bindings) {
                        return Err(
                            JoinError::WrongBoundVariable(left_condition, right_bindings).into(),
                        );
                    }
                    if !right_variables.is_disjoint(&left_bindings) {
                        return Err(
                            JoinError::WrongBoundVariable(right_condition, left_bindings).into(),
                        );
                    }
                    join_keys.push((left_condition, right_condition))
                }
                ex => return Err(JoinError::JoinCondition(ex).into()),
            }
        }

        Ok(Self {
            ctx,
            left,
            right,
            join_keys,
            left_outer: matches!(kind, NAME_LEFT_JOIN | NAME_OUTER_JOIN),
            right_outer: matches!(kind, NAME_RIGHT_JOIN | NAME_OUTER_JOIN),
            left_temp_id: Default::default(),
            right_temp_id: Default::default(),
        })
    }
    fn materialize(&self, temp_table_id: u32, keys: Vec<Expr>, source: &RaBox<'a>) -> Result<()> {
        let source_map = source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let sort_exprs = keys
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
        let mut insertion_key = OwnTuple::with_prefix(temp_table_id);
        let mut insertion_val = OwnTuple::with_data_prefix(DataKind::Data);
        for (i, tset) in source.iter()?.enumerate() {
            insertion_key.truncate_all();
            insertion_val.truncate_all();
            let tset = tset?;
            for expr in &sort_exprs {
                let val = expr.row_eval(&tset)?;
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

impl<'a> Drop for MergeJoin<'a> {
    fn drop(&mut self) {
        drop_temp_table(self.ctx, self.left_temp_id.load(SeqCst));
        drop_temp_table(self.ctx, self.right_temp_id.load(SeqCst));
    }
}

impl<'b> RelationalAlgebra for MergeJoin<'b> {
    fn name(&self) -> &str {
        match (self.left_outer, self.right_outer) {
            (false, false) => NAME_INNER_JOIN,
            (true, false) => NAME_LEFT_JOIN,
            (false, true) => NAME_RIGHT_JOIN,
            (true, true) => NAME_OUTER_JOIN,
        }
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        let mut left = self.left.bindings()?;
        let right = self.right.bindings()?;
        left.extend(right);
        Ok(left)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let left = self.left.binding_map()?;
        let right = self.right.binding_map()?;
        Ok(merge_binding_maps([left, right].into_iter()))
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let mut left_temp_id = self.left_temp_id.load(SeqCst);
        if left_temp_id == 0 {
            left_temp_id = self.ctx.gen_table_id()?.id;
            self.left_temp_id.store(left_temp_id, SeqCst);
            self.materialize(
                left_temp_id,
                self.join_keys
                    .iter()
                    .map(|(l, _)| l.clone())
                    .collect::<Vec<_>>(),
                &self.left,
            )?;
        }

        let mut right_temp_id = self.right_temp_id.load(SeqCst);
        if right_temp_id == 0 {
            right_temp_id = self.ctx.gen_table_id()?.id;
            self.left_temp_id.store(right_temp_id, SeqCst);
            self.materialize(
                right_temp_id,
                self.join_keys
                    .iter()
                    .map(|(_, r)| r.clone())
                    .collect::<Vec<_>>(),
                &self.right,
            )?;
        }
        Ok(Box::new(MergeJoinIterator {
            left_tid: left_temp_id,
            right_tid: right_temp_id,
            left_tset_size: self.left.binding_map()?.kv_size(),
            right_tset_size: self.right.binding_map()?.kv_size(),
            left_outer: self.left_outer,
            right_outer: self.right_outer,
            key_len: self.join_keys.len(),
            left_it: self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp),
            right_it: self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp),
            last_op: MergeJoinIteratorLastOp::NotStarted,
            scratch: OwnTuple::with_null_prefix(),
        }))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum MergeJoinIteratorLastOp {
    NotStarted,
    LeftOutput,
    RightOutput,
    NestedLoopOutput,
    Done,
}

struct MergeJoinIterator {
    left_tid: u32,
    right_tid: u32,
    left_tset_size: (usize, usize),
    right_tset_size: (usize, usize),
    left_outer: bool,
    right_outer: bool,
    key_len: usize,
    left_it: IteratorPtr,
    right_it: IteratorPtr,
    last_op: MergeJoinIteratorLastOp,
    scratch: OwnTuple,
}

impl MergeJoinIterator {
    fn next_inner(&mut self) -> Result<Option<TupleSet>> {
        loop {
            match self.last_op {
                MergeJoinIteratorLastOp::NotStarted => {
                    self.scratch.overwrite_prefix(self.left_tid);
                    self.left_it.seek(&self.scratch);
                    self.scratch.overwrite_prefix(self.right_tid);
                    self.right_it.seek(&self.scratch);
                }
                MergeJoinIteratorLastOp::Done => return Ok(None),
                MergeJoinIteratorLastOp::LeftOutput => {
                    self.left_it.next();
                }
                MergeJoinIteratorLastOp::RightOutput => {
                    self.right_it.next();
                }
                MergeJoinIteratorLastOp::NestedLoopOutput => {
                    self.right_it.next();
                    if let Some(right_key) = self.right_it.key() {
                        let right_key = Tuple::new(right_key);
                        let left_key = self.left_it.key().unwrap();
                        let left_key = Tuple::new(left_key);
                        match left_key.key_part_truncate_cmp(&right_key, self.key_len) {
                            Ordering::Less => {
                                let old_key = left_key.to_owned();
                                self.left_it.next();
                                match self.left_it.key() {
                                    None => {
                                        self.last_op = MergeJoinIteratorLastOp::RightOutput;
                                        continue;
                                    }
                                    Some(left_key) => {
                                        let left_key = Tuple::new(left_key);
                                        match old_key.key_part_truncate_cmp(&left_key, self.key_len)
                                        {
                                            Ordering::Less => {
                                                self.scratch.truncate_all();
                                                self.scratch.overwrite_prefix(self.right_tid);
                                                for val in old_key.iter() {
                                                    self.scratch.push_value(&val?)
                                                }
                                                self.right_it.seek(&self.scratch);
                                            }
                                            Ordering::Equal => {}
                                            Ordering::Greater => unreachable!(),
                                        }
                                    }
                                }
                            }
                            Ordering::Equal => {}
                            Ordering::Greater => unreachable!(),
                        }
                    } else {
                        self.last_op = MergeJoinIteratorLastOp::LeftOutput;
                        continue;
                    }
                }
            }
            match (self.left_it.pair(), self.right_it.pair()) {
                (None, None) => {
                    self.last_op = MergeJoinIteratorLastOp::Done;
                    return Ok(None);
                }
                (None, Some((_rk, rv))) => {
                    return if self.right_outer {
                        self.last_op = MergeJoinIteratorLastOp::RightOutput;
                        let rv = Tuple::new(rv);
                        let mut l_tset = TupleSet::padded_tset(self.left_tset_size);
                        let r_tset = TupleSet::decode_from_tuple(&rv)?;
                        l_tset.merge(r_tset);
                        Ok(Some(l_tset))
                    } else {
                        self.last_op = MergeJoinIteratorLastOp::Done;
                        Ok(None)
                    }
                }
                (Some((_lk, lv)), None) => {
                    return if self.left_outer {
                        self.last_op = MergeJoinIteratorLastOp::LeftOutput;
                        let lv = Tuple::new(lv);
                        let mut l_tset = TupleSet::decode_from_tuple(&lv)?;
                        let r_tset = TupleSet::padded_tset(self.right_tset_size);
                        l_tset.merge(r_tset);
                        Ok(Some(l_tset))
                    } else {
                        self.last_op = MergeJoinIteratorLastOp::Done;
                        Ok(None)
                    }
                }
                (Some((lk, lv)), Some((rk, rv))) => {
                    let lk = Tuple::new(lk);
                    let rk = Tuple::new(rk);
                    match lk.key_part_truncate_cmp(&rk, self.key_len) {
                        Ordering::Less => {
                            self.last_op = MergeJoinIteratorLastOp::LeftOutput;
                            if self.left_outer {
                                let lv = Tuple::new(lv);
                                let mut l_tset = TupleSet::decode_from_tuple(&lv)?;
                                let r_tset = TupleSet::padded_tset(self.right_tset_size);
                                l_tset.merge(r_tset);
                                return Ok(Some(l_tset));
                            } else {
                                continue;
                            }
                        }
                        Ordering::Greater => {
                            self.last_op = MergeJoinIteratorLastOp::RightOutput;
                            if self.right_outer {
                                let rv = Tuple::new(rv);
                                let mut l_tset = TupleSet::padded_tset(self.left_tset_size);
                                let r_tset = TupleSet::decode_from_tuple(&rv)?;
                                l_tset.merge(r_tset);
                                return Ok(Some(l_tset));
                            } else {
                                continue;
                            }
                        }
                        Ordering::Equal => {
                            self.last_op = MergeJoinIteratorLastOp::NestedLoopOutput;
                            let lv = Tuple::new(lv);
                            let mut l_tset = TupleSet::decode_from_tuple(&lv)?;
                            let rv = Tuple::new(rv);
                            let r_tset = TupleSet::decode_from_tuple(&rv)?;
                            l_tset.merge(r_tset);
                            return Ok(Some(l_tset));
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for MergeJoinIterator {
    type Item = Result<TupleSet>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_inner() {
            Ok(Some(v)) => Some(Ok(v)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
