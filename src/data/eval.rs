use crate::data::expr::Expr;
use crate::data::op::*;
use crate::data::op_agg::OpAgg;
use crate::data::tuple_set::TupleSetIdx;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use cozorocks::{DbPtr, TransactionPtr, WriteOptionsPtr};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[derive(thiserror::Error, Debug)]
pub enum EvalError {
    #[error("Unresolved variable `{0}`")]
    UnresolvedVariable(String),

    #[error("Cannot access field {0} for {1:?}")]
    FieldAccess(String, Expr),

    #[error("Cannot access index {0} for {1:?}")]
    IndexAccess(usize, Expr),

    #[error("Cannot apply `{0}` to `{1:?}`")]
    OpTypeMismatch(String, Vec<StaticValue>),

    #[error("Expect aggregate expression")]
    NotAggregate,

    #[error("Arity mismatch for {0}, {1} arguments given ")]
    ArityMismatch(String, usize),

    #[error("Incomplete evaluation {0}")]
    IncompleteEvaluation(String),

    #[error("Called resolve on null context")]
    NullContext,
}

// type Result<T> = result::Result<T, EvalError>;

#[derive(thiserror::Error, Debug)]
pub enum EvalContextError {
    #[error("called resolve on null context")]
    NullContext,
}

pub(crate) trait RowEvalContext {
    fn resolve(&self, idx: &TupleSetIdx) -> Result<Value>;
    fn get_temp_db(&self) -> Result<&DbPtr>;
    fn get_txn(&self) -> Result<&TransactionPtr>;
    fn get_write_options(&self) -> Result<&WriteOptionsPtr>;
}

impl RowEvalContext for () {
    fn resolve(&self, _idx: &TupleSetIdx) -> Result<Value> {
        Err(EvalContextError::NullContext.into())
    }

    fn get_temp_db(&self) -> Result<&DbPtr> {
        Err(EvalContextError::NullContext.into())
    }

    fn get_txn(&self) -> Result<&TransactionPtr> {
        Err(EvalContextError::NullContext.into())
    }

    fn get_write_options(&self) -> Result<&WriteOptionsPtr> {
        Err(EvalContextError::NullContext.into())
    }
}

pub(crate) trait PartialEvalContext {
    fn resolve(&self, key: &str) -> Option<Expr>;
}

impl PartialEvalContext for () {
    fn resolve(&self, _key: &str) -> Option<Expr> {
        None
    }
}

// fn extract_optimized_bin_args(args: Vec<Expr>) -> (Expr, Expr) {
//     let mut args = args.into_iter();
//     (
//         args.next().unwrap().optimize_ops(),
//         args.next().unwrap().optimize_ops(),
//     )
// }

// fn extract_optimized_u_args(args: Vec<Expr>) -> Expr {
//     args.into_iter().next().unwrap().optimize_ops()
// }

impl Expr {
    pub(crate) fn interpret_eval<C: PartialEvalContext>(self, ctx: &C) -> Result<Value> {
        match self.partial_eval(ctx)? {
            Expr::Const(v) => Ok(v),
            v => Err(EvalError::IncompleteEvaluation(format!("{:?}", v)).into()),
        }
    }

    pub(crate) fn extract_agg_heads(&self) -> Result<Vec<(OpAgg, Vec<Expr>)>> {
        let mut coll = vec![];
        fn do_extract(ex: Expr, coll: &mut Vec<(OpAgg, Vec<Expr>)>) -> Result<()> {
            match ex {
                Expr::Const(_) => {}
                Expr::List(l) => {
                    for ex in l {
                        do_extract(ex, coll)?;
                    }
                }
                Expr::Dict(d) => {
                    for (_, ex) in d {
                        do_extract(ex, coll)?;
                    }
                }
                Expr::Variable(_) => return Err(EvalError::NotAggregate.into()),
                Expr::TupleSetIdx(_) => return Err(EvalError::NotAggregate.into()),
                Expr::ApplyAgg(op, _, args) => coll.push((op, args)),
                Expr::FieldAcc(_, arg) => do_extract(*arg, coll)?,
                Expr::IdxAcc(_, arg) => do_extract(*arg, coll)?,
                Expr::IfExpr(args) => {
                    let (a, b, c) = *args;
                    do_extract(a, coll)?;
                    do_extract(b, coll)?;
                    do_extract(c, coll)?;
                }
                Expr::SwitchExpr(args) => {
                    for (cond, expr) in args {
                        do_extract(cond, coll)?;
                        do_extract(expr, coll)?;
                    }
                }
                Expr::OpAnd(args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
                Expr::OpOr(args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
                Expr::OpCoalesce(args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
                Expr::OpMerge(args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
                Expr::OpConcat(args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
                Expr::BuiltinFn(_, args) => {
                    for ex in args {
                        do_extract(ex, coll)?
                    }
                }
            }
            Ok(())
        }
        do_extract(self.clone(), &mut coll)?;
        Ok(coll)
    }

    pub(crate) fn is_agg_compatible(&self) -> bool {
        match self {
            Expr::Const(_) => true,
            Expr::List(l) => l.iter().all(|el| el.is_agg_compatible()),
            Expr::Dict(d) => d.values().all(|el| el.is_agg_compatible()),
            Expr::Variable(_) => false,
            Expr::TupleSetIdx(_) => false,
            Expr::ApplyAgg(_, _, _) => true,
            Expr::FieldAcc(_, arg) => arg.is_agg_compatible(),
            Expr::IdxAcc(_, arg) => arg.is_agg_compatible(),
            Expr::IfExpr(args) => {
                let (a, b, c) = args.as_ref();
                a.is_agg_compatible() && b.is_agg_compatible() && c.is_agg_compatible()
            }
            Expr::SwitchExpr(args) => args
                .iter()
                .all(|(cond, expr)| cond.is_agg_compatible() && expr.is_agg_compatible()),
            Expr::OpAnd(args) => args.iter().all(|el| el.is_agg_compatible()),
            Expr::OpOr(args) => args.iter().all(|el| el.is_agg_compatible()),
            Expr::OpCoalesce(args) => args.iter().all(|el| el.is_agg_compatible()),
            Expr::OpMerge(args) => args.iter().all(|el| el.is_agg_compatible()),
            Expr::OpConcat(args) => args.iter().all(|el| el.is_agg_compatible()),
            Expr::BuiltinFn(_, args) => args.iter().all(|el| el.is_agg_compatible()),
        }
    }

    pub(crate) fn partial_eval<C: PartialEvalContext>(self, ctx: &C) -> Result<Self> {
        let res = match self {
            v @ (Expr::Const(_) | Expr::TupleSetIdx(_)) => v,
            Expr::List(l) => {
                let mut has_unevaluated = false;
                let l = l
                    .into_iter()
                    .map(|v| -> Result<Expr> {
                        let v = v.partial_eval(ctx)?;
                        if !v.is_const() {
                            has_unevaluated = true;
                        }
                        Ok(v)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if has_unevaluated {
                    Expr::List(l)
                } else {
                    Expr::Const(Value::List(
                        l.into_iter().map(|v| v.extract_const().unwrap()).collect(),
                    ))
                }
            }
            Expr::Dict(d) => {
                let mut has_unevaluated = false;
                let d = d
                    .into_iter()
                    .map(|(k, v)| -> Result<(String, Expr)> {
                        let v = v.partial_eval(ctx)?;
                        if !v.is_const() {
                            has_unevaluated = true;
                        }
                        Ok((k, v))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                if has_unevaluated {
                    Expr::Dict(d)
                } else {
                    let c_vals = d
                        .into_iter()
                        .map(|(k, ex)| (k.into(), ex.extract_const().unwrap()))
                        .collect();
                    Expr::Const(Value::Dict(c_vals))
                }
            }
            Expr::Variable(var) => ctx
                .resolve(&var)
                .ok_or(EvalError::UnresolvedVariable(var))?,
            Expr::FieldAcc(f, arg) => match *arg {
                Expr::Dict(mut d) => {
                    // This skips evaluation of other keys
                    d.remove(&f as &str)
                        .unwrap_or(Expr::Const(Value::Null))
                        .partial_eval(ctx)?
                }
                arg => match arg.partial_eval(ctx)? {
                    Expr::Const(Value::Null) => Expr::Const(Value::Null),
                    Expr::Const(Value::Dict(mut d)) => {
                        Expr::Const(d.remove(&f as &str).unwrap_or(Value::Null))
                    }
                    v @ (Expr::IdxAcc(_, _)
                    | Expr::FieldAcc(_, _)
                    | Expr::BuiltinFn(_, _)
                    | Expr::ApplyAgg(_, _, _)
                    | Expr::OpConcat(_)
                    | Expr::OpMerge(_)
                    | Expr::OpCoalesce(_)) => Expr::FieldAcc(f, v.into()),
                    Expr::Dict(mut d) => d.remove(&f as &str).unwrap_or(Expr::Const(Value::Null)),
                    v => return Err(EvalError::FieldAccess(f, v).into()),
                },
            },
            Expr::IdxAcc(i, arg) => {
                match *arg {
                    // This skips evaluation of other keys
                    Expr::List(mut l) => {
                        if i >= l.len() {
                            Expr::Const(Value::Null)
                        } else {
                            l.swap_remove(i).partial_eval(ctx)?
                        }
                    }
                    arg => match arg.partial_eval(ctx)? {
                        Expr::Const(Value::Null) => Expr::Const(Value::Null),
                        Expr::Const(Value::List(mut l)) => {
                            if i >= l.len() {
                                Expr::Const(Value::Null)
                            } else {
                                Expr::Const(l.swap_remove(i))
                            }
                        }
                        Expr::List(mut l) => {
                            if i >= l.len() {
                                Expr::Const(Value::Null)
                            } else {
                                l.swap_remove(i)
                            }
                        }
                        v @ (Expr::IdxAcc(_, _)
                        | Expr::FieldAcc(_, _)
                        | Expr::BuiltinFn(_, _)
                        | Expr::ApplyAgg(_, _, _)) => Expr::IdxAcc(i, v.into()),
                        v => return Err(EvalError::IndexAccess(i, v).into()),
                    },
                }
            }
            Expr::BuiltinFn(op, args) => {
                if let Some(n) = op.arity {
                    if n as usize != args.len() {
                        return Err(
                            EvalError::ArityMismatch(op.name.to_string(), args.len()).into()
                        );
                    }
                }
                let mut has_unevaluated = false;
                let mut eval_args = Vec::with_capacity(args.len());
                for v in args {
                    let v = v.partial_eval(ctx)?;
                    if !matches!(v, Expr::Const(_)) {
                        has_unevaluated = true;
                        eval_args.push(v);
                    } else if op.non_null_args && matches!(v, Expr::Const(Value::Null)) {
                        return Ok(Expr::Const(Value::Null));
                    } else {
                        eval_args.push(v);
                    }
                }
                if has_unevaluated {
                    Expr::BuiltinFn(op, eval_args)
                } else {
                    let args = eval_args
                        .into_iter()
                        .map(|v| match v {
                            Expr::Const(v) => v,
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>();
                    (op.func)(&args).map(Expr::Const)?
                }
            }
            Expr::OpMerge(args) => partial_eval_merge_expr(ctx, args)?,
            Expr::OpConcat(args) => partial_eval_concat_expr(ctx, args)?,
            Expr::OpOr(args) => partial_eval_or(ctx, args)?,
            Expr::OpAnd(args) => partial_eval_and(ctx, args)?,
            Expr::OpCoalesce(args) => partial_eval_coalesce(ctx, args)?,
            Expr::ApplyAgg(op, a_args, args) => {
                let a_args = a_args
                    .into_iter()
                    .map(|v| v.interpret_eval(ctx).map(|v| v.into_static()))
                    .collect::<Result<Vec<_>>>()?;
                let args = args
                    .into_iter()
                    .map(|v| v.partial_eval(ctx))
                    .collect::<Result<Vec<_>>>()?;
                op.initialize(a_args)?;
                Expr::ApplyAgg(op, vec![], args)
            }
            Expr::IfExpr(args) => {
                let (cond, if_part, else_part) = *args;
                partial_eval_if_expr(ctx, cond, if_part, else_part)?
            }
            Expr::SwitchExpr(args) => partial_eval_switch_expr(ctx, args)?,
        };
        Ok(res)
    }
    pub(crate) fn row_eval<'a, C: RowEvalContext + 'a>(&'a self, ctx: &'a C) -> Result<Value<'a>> {
        let res: Value = match self {
            Expr::Const(v) => v.clone(),
            Expr::List(l) => l
                .iter()
                .map(|v| v.row_eval(ctx))
                .collect::<Result<Vec<_>>>()?
                .into(),
            Expr::Dict(d) => d
                .iter()
                .map(|(k, v)| -> Result<(Cow<str>, Value)> {
                    let v = v.row_eval(ctx)?;
                    Ok((k.into(), v))
                })
                .collect::<Result<BTreeMap<_, _>>>()?
                .into(),
            Expr::Variable(v) => return Err(EvalError::UnresolvedVariable(v.clone()).into()),
            Expr::TupleSetIdx(idx) => ctx.resolve(idx)?.clone(),
            Expr::BuiltinFn(op, args) => match args.len() {
                0 => (op.func)(&[])?,
                1 => {
                    let v = args.iter().next().unwrap().row_eval(ctx)?;
                    if op.non_null_args && v == Value::Null {
                        return Ok(Value::Null);
                    } else {
                        (op.func)(&[v])?
                    }
                }
                2 => {
                    let mut args = args.iter();
                    let v1 = args.next().unwrap().row_eval(ctx)?;
                    if op.non_null_args && v1 == Value::Null {
                        return Ok(Value::Null);
                    }
                    let v2 = args.next().unwrap().row_eval(ctx)?;
                    if op.non_null_args && v2 == Value::Null {
                        return Ok(Value::Null);
                    }
                    (op.func)(&[v1, v2])?
                }
                _ => {
                    let mut eval_args = Vec::with_capacity(args.len());
                    for v in args {
                        let v = v.row_eval(ctx)?;
                        if op.non_null_args && v == Value::Null {
                            return Ok(Value::Null);
                        } else {
                            eval_args.push(v);
                        }
                    }
                    (op.func)(&eval_args)?
                }
            },
            Expr::ApplyAgg(op, _, args) => {
                let mut eval_args = Vec::with_capacity(args.len());
                for v in args {
                    eval_args.push(v.row_eval(ctx)?);
                }
                op.put_get(&eval_args)?
            }
            Expr::FieldAcc(f, arg) => match arg.row_eval(ctx)? {
                Value::Null => Value::Null,
                Value::Dict(mut d) => d.remove(f as &str).unwrap_or(Value::Null),
                v => {
                    return Err(
                        EvalError::FieldAccess(f.clone(), Expr::Const(v.into_static())).into(),
                    );
                }
            },
            Expr::IdxAcc(idx, arg) => match arg.row_eval(ctx)? {
                Value::Null => Value::Null,
                Value::List(mut d) => {
                    if *idx >= d.len() {
                        Value::Null
                    } else {
                        d.swap_remove(*idx)
                    }
                }
                v => return Err(EvalError::IndexAccess(*idx, Expr::Const(v.into_static())).into()),
            },
            Expr::IfExpr(args) => {
                let (cond, if_part, else_part) = args.as_ref();
                row_eval_if_expr(ctx, cond, if_part, else_part)?
            }
            Expr::SwitchExpr(args) => row_eval_switch_expr(ctx, args)?,
            Expr::OpAnd(args) => row_eval_and(ctx, args)?,
            Expr::OpOr(args) => row_eval_or(ctx, args)?,
            Expr::OpCoalesce(args) => row_eval_coalesce(ctx, args)?,
            Expr::OpMerge(args) => row_eval_merge(ctx, args)?,
            Expr::OpConcat(args) => row_eval_concat(ctx, args)?,
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::parser::tests::str2expr;

    #[test]
    fn evaluations() -> Result<()> {
        dbg!(str2expr("123")?.row_eval(&())?);
        dbg!(str2expr("123")?.partial_eval(&())?);
        dbg!(str2expr("123 + 457")?.row_eval(&())?);
        dbg!(str2expr("123 + 457")?.partial_eval(&())?);
        dbg!(str2expr("123 + 457.1")?.row_eval(&())?);
        dbg!(str2expr("123 + 457.1")?.partial_eval(&())?);
        dbg!(str2expr("'123' ++ '457.1'")?.row_eval(&())?);
        dbg!(str2expr("'123' ++ '457.1'")?.partial_eval(&())?);
        dbg!(str2expr("null ~ null ~ 123 ~ null")?.row_eval(&())?);
        dbg!(str2expr("null ~ null ~ 123 ~ null")?.partial_eval(&())?);
        dbg!(str2expr("2*3+1/10")?.row_eval(&())?);
        dbg!(str2expr("2*3+1/10")?.partial_eval(&())?);
        dbg!(str2expr("1>null")?.row_eval(&())?);
        dbg!(str2expr("1>null")?.partial_eval(&())?);
        dbg!(str2expr("'c'>'d'")?.row_eval(&())?);
        dbg!(str2expr("'c'>'d'")?.partial_eval(&())?);
        dbg!(str2expr("null && true && null")?.row_eval(&())?);
        dbg!(str2expr("null && true && null")?.partial_eval(&())?);
        dbg!(str2expr("null && false && null")?.row_eval(&())?);
        dbg!(str2expr("null && false && null")?.partial_eval(&())?);
        dbg!(str2expr("null || true || null")?.row_eval(&())?);
        dbg!(str2expr("null || true || null")?.partial_eval(&())?);
        dbg!(str2expr("null || false || null")?.row_eval(&())?);
        dbg!(str2expr("null || false || null")?.partial_eval(&())?);
        dbg!(str2expr("!true")?.row_eval(&())?);
        dbg!(str2expr("!true")?.partial_eval(&())?);
        dbg!(str2expr("!null")?.row_eval(&())?);
        dbg!(str2expr("!null")?.partial_eval(&())?);
        dbg!(str2expr("if null {1} else {2}")?.row_eval(&())?);
        dbg!(str2expr("if null {1} else {2}")?.partial_eval(&())?);
        dbg!(str2expr("if 1 == 2 {'a'}")?.row_eval(&())?);
        dbg!(str2expr("if 1 == 2 {'a'}")?.partial_eval(&())?);
        dbg!(str2expr("if 1 == 2 {'a'} else if 3 == 3 {'b'} else {'c'}")?.row_eval(&())?);
        dbg!(str2expr("if 1 == 2 {'a'} else if 3 == 3 {'b'} else {'c'}")?.partial_eval(&())?);
        dbg!(str2expr("switch 1 {2 => '2', 0 => '3', .. => 'x'}")?.row_eval(&())?);
        dbg!(str2expr("switch 1 {2 => '2', 0 => '3', .. => 'x'}")?.partial_eval(&())?);
        dbg!(str2expr("switch 3 {2 => '2', 1+2 => '3', .. => 'x'}")?.row_eval(&())?);
        dbg!(str2expr("switch 3 {2 => '2', 1+2 => '3', .. => 'x'}")?.partial_eval(&())?);
        dbg!(str2expr("null.is_null()")?.row_eval(&())?);
        dbg!(str2expr("null.is_null()")?.partial_eval(&())?);
        dbg!(str2expr("null.not_null()")?.row_eval(&())?);
        dbg!(str2expr("null.not_null()")?.partial_eval(&())?);
        dbg!(str2expr("is_null(null)")?.row_eval(&())?);
        dbg!(str2expr("is_null(null)")?.partial_eval(&())?);
        dbg!(str2expr("is_null((null ~ 3)+2).is_null()")?.row_eval(&())?);
        dbg!(str2expr("is_null((null ~ 3)+2).is_null()")?.partial_eval(&())?);

        Ok(())
    }
}
