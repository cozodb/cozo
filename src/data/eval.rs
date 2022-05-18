use crate::data::expr::{Expr, StaticExpr};
use crate::data::expr_parser::ExprParseError;
use crate::data::op::{Op, OpAdd, OpAnd, OpCoalesce, OpConcat, OpDiv, OpEq, OpGe, OpGt, OpIsNull, OpLe, OpLt, OpMerge, OpMinus, OpMod, OpMul, OpNe, OpNot, OpNotNull, OpOr, OpPow, OpStrCat, OpSub, partial_eval_and, partial_eval_coalesce, partial_eval_if_expr, partial_eval_or, row_eval_and, row_eval_coalesce, row_eval_if_expr, row_eval_or};
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::result;

#[derive(thiserror::Error, Debug)]
pub(crate) enum EvalError {
    #[error("Unresolved variable `{0}`")]
    UnresolvedVariable(String),

    #[error("Unresolved table col {0:?}{1:?}")]
    UnresolveTableCol(TableId, ColId),

    #[error("Unresolved tuple index {0:?}")]
    UnresolveTupleIdx(TupleSetIdx),

    #[error("Cannot access field {0} for {1}")]
    FieldAccess(String, StaticValue),

    #[error("Cannot access index {0} for {1}")]
    IndexAccess(usize, StaticValue),

    #[error(transparent)]
    Parse(#[from] ExprParseError),

    #[error("Cannot apply `{0}` to `{1:?}`")]
    OpTypeMismatch(String, Vec<StaticValue>),

    #[error("Optimized before partial eval")]
    OptimizedBeforePartialEval,

    #[error("Arity mismatch for {0}, {1} arguments given ")]
    ArityMismatch(String, usize),

    #[error("Incomplete evaluation {0}")]
    IncompleteEvaluation(String),
}

type Result<T> = result::Result<T, EvalError>;

pub(crate) trait RowEvalContext {
    fn resolve<'a>(&'a self, idx: &TupleSetIdx) -> Result<&'a Value>;
}

impl RowEvalContext for () {
    fn resolve<'a>(&'a self, idx: &TupleSetIdx) -> Result<&'a Value> {
        Err(EvalError::UnresolveTupleIdx(*idx))
    }
}

pub(crate) trait ExprEvalContext {
    fn resolve<'a>(&'a self, key: &str) -> Option<Expr<'a>>;
    fn resolve_table_col<'a>(&'a self, binding: &str, col: &str) -> Option<(TableId, ColId)>;
}

impl ExprEvalContext for () {
    fn resolve<'a>(&'a self, _key: &str) -> Option<Expr<'a>> {
        None
    }
    fn resolve_table_col<'a>(&'a self, _binding: &str, _col: &str) -> Option<(TableId, ColId)> {
        None
    }
}

fn extract_optimized_bin_args(args: Vec<Expr>) -> (Expr, Expr) {
    let mut args = args.into_iter();
    (
        args.next().unwrap().optimize_ops(),
        args.next().unwrap().optimize_ops(),
    )
}

fn extract_optimized_u_args(args: Vec<Expr>) -> Expr {
    args.into_iter().next().unwrap().optimize_ops()
}

impl<'a> Expr<'a> {
    pub(crate) fn interpret_eval<C: ExprEvalContext + 'a>(self, ctx: &'a C) -> Result<Value> {
        match self.partial_eval(ctx)? {
            Expr::Const(v) => Ok(v),
            v => Err(EvalError::IncompleteEvaluation(format!("{:?}", v)))
        }
    }

    pub(crate) fn partial_eval<C: ExprEvalContext + 'a>(self, ctx: &'a C) -> Result<Self> {
        let res = match self {
            v @ (Expr::Const(_) | Expr::TableCol(_, _) | Expr::TupleSetIdx(_)) => v,
            Expr::List(l) => Expr::List(
                l.into_iter()
                    .map(|v| v.partial_eval(ctx))
                    .collect::<Result<Vec<_>>>()?,
            ),
            Expr::Dict(d) => Expr::Dict(
                d.into_iter()
                    .map(|(k, v)| -> Result<(String, Expr)> { Ok((k, v.partial_eval(ctx)?)) })
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ),
            Expr::Variable(var) => ctx
                .resolve(&var)
                .ok_or(EvalError::UnresolvedVariable(var))?,
            Expr::FieldAcc(f, arg) => {
                let expr = match *arg {
                    Expr::Variable(var) => {
                        if let Some((tid, cid)) = ctx.resolve_table_col(&var, &f) {
                            return Ok(Expr::TableCol(tid, cid));
                        } else {
                            ctx.resolve(&var)
                                .ok_or(EvalError::UnresolvedVariable(var))?
                                .partial_eval(ctx)?
                        }
                    }
                    expr => expr.partial_eval(ctx)?,
                };
                match expr {
                    Expr::Const(Value::Null) => Expr::Const(Value::Null),
                    Expr::Const(Value::Dict(mut d)) => {
                        Expr::Const(d.remove(&f as &str).unwrap_or(Value::Null))
                    }
                    v @ (Expr::IdxAcc(_, _)
                    | Expr::FieldAcc(_, _)
                    | Expr::TableCol(_, _)
                    | Expr::Apply(_, _)
                    | Expr::ApplyAgg(_, _, _)) => Expr::FieldAcc(f, v.into()),
                    Expr::Dict(mut d) => d.remove(&f as &str).unwrap_or(Expr::Const(Value::Null)),
                    v => return Err(EvalError::FieldAccess(f, Value::from(v).to_static())),
                }
            }
            Expr::IdxAcc(i, arg) => {
                let arg = arg.partial_eval(ctx)?;
                match arg {
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
                    | Expr::TableCol(_, _)
                    | Expr::Apply(_, _)
                    | Expr::ApplyAgg(_, _, _)) => Expr::IdxAcc(i, v.into()),
                    v => return Err(EvalError::IndexAccess(i, Value::from(v).to_static())),
                }
            }
            Expr::Apply(op, args) => {
                // special cases
                match op.name() {
                    n if n == OpAnd.name() => partial_eval_and(ctx, args)?,
                    n if n == OpOr.name() => partial_eval_or(ctx, args)?,
                    n if n == OpCoalesce.name() => partial_eval_coalesce(ctx, args)?,
                    _ => {
                        let mut has_unevaluated = false;
                        let mut has_null = false;
                        let args = args
                            .into_iter()
                            .map(|v| {
                                let v = v.partial_eval(ctx);
                                if !matches!(v, Ok(Expr::Const(_))) {
                                    has_unevaluated = true;
                                } else if matches!(v, Ok(Expr::Const(Value::Null))) {
                                    has_null = true;
                                }
                                v
                            }).collect::<Result<Vec<_>>>()?;
                        if has_unevaluated {
                            Expr::Apply(op, args)
                        } else {
                            let args = args.into_iter().map(|v| {
                                match v {
                                    Expr::Const(v) => v,
                                    _ => unreachable!()
                                }
                            }).collect();
                            op.eval(has_null, args)
                                .map(Expr::Const)?
                        }
                    }
                }
            }
            Expr::ApplyAgg(op, a_args, args) => {
                let a_args = a_args
                    .into_iter()
                    .map(|v| v.partial_eval(ctx))
                    .collect::<Result<Vec<_>>>()?;
                let args = args
                    .into_iter()
                    .map(|v| v.partial_eval(ctx))
                    .collect::<Result<Vec<_>>>()?;
                Expr::ApplyAgg(op, a_args, args)
            }
            Expr::IfExpr(args) => {
                let (cond, if_part, else_part) = *args;
                partial_eval_if_expr(ctx, cond, if_part, else_part)?
            }
            Expr::ApplyZero(_)
            | Expr::ApplyOne(_, _)
            | Expr::ApplyTwo(_, _)
            | Expr::Add(_)
            | Expr::Sub(_)
            | Expr::Mul(_)
            | Expr::Div(_)
            | Expr::Pow(_)
            | Expr::Mod(_)
            | Expr::StrCat(_)
            | Expr::Eq(_)
            | Expr::Ne(_)
            | Expr::Gt(_)
            | Expr::Ge(_)
            | Expr::Lt(_)
            | Expr::Le(_)
            | Expr::Not(_)
            | Expr::Minus(_)
            | Expr::IsNull(_)
            | Expr::NotNull(_)
            | Expr::Coalesce(_)
            | Expr::Or(_)
            | Expr::And(_) => return Err(EvalError::OptimizedBeforePartialEval),
        };
        Ok(res)
    }
    pub(crate) fn optimize_ops(self) -> Self {
        // Note: `and`, `or` and `coalesce` do not short-circuit if not optimized
        match self {
            Expr::List(l) => Expr::List(l.into_iter().map(|v| v.optimize_ops()).collect()),
            Expr::Dict(d) => {
                Expr::Dict(d.into_iter().map(|(k, v)| (k, v.optimize_ops())).collect())
            }
            Expr::Apply(op, args) => match op.name() {
                name if name == OpAdd.name() => Expr::Add(extract_optimized_bin_args(args).into()),
                name if name == OpSub.name() => Expr::Sub(extract_optimized_bin_args(args).into()),
                name if name == OpMul.name() => Expr::Mul(extract_optimized_bin_args(args).into()),
                name if name == OpDiv.name() => Expr::Div(extract_optimized_bin_args(args).into()),
                name if name == OpPow.name() => Expr::Pow(extract_optimized_bin_args(args).into()),
                name if name == OpMod.name() => Expr::Mod(extract_optimized_bin_args(args).into()),
                name if name == OpStrCat.name() => {
                    Expr::StrCat(extract_optimized_bin_args(args).into())
                }
                name if name == OpEq.name() => Expr::Eq(extract_optimized_bin_args(args).into()),
                name if name == OpNe.name() => Expr::Ne(extract_optimized_bin_args(args).into()),
                name if name == OpGt.name() => Expr::Gt(extract_optimized_bin_args(args).into()),
                name if name == OpGe.name() => Expr::Ge(extract_optimized_bin_args(args).into()),
                name if name == OpLt.name() => Expr::Lt(extract_optimized_bin_args(args).into()),
                name if name == OpLe.name() => Expr::Le(extract_optimized_bin_args(args).into()),
                name if name == OpNot.name() => {
                    Expr::Not(extract_optimized_u_args(args).into())
                }
                name if name == OpMinus.name() => {
                    Expr::Minus(extract_optimized_u_args(args).into())
                }
                name if name == OpIsNull.name() => {
                    Expr::IsNull(extract_optimized_u_args(args).into())
                }
                name if name == OpNotNull.name() => {
                    Expr::NotNull(extract_optimized_u_args(args).into())
                }
                name if name == OpCoalesce.name() => {
                    let mut args = args.into_iter();
                    let mut arg = args.next().unwrap().optimize_ops();
                    for nxt in args {
                        arg = Expr::Coalesce((arg, nxt.optimize_ops()).into());
                    }
                    arg
                }
                name if name == OpOr.name() => {
                    let mut args = args.into_iter();
                    let mut arg = args.next().unwrap().optimize_ops();
                    for nxt in args {
                        arg = Expr::Or((arg, nxt.optimize_ops()).into());
                    }
                    arg
                }
                name if name == OpAnd.name() => {
                    let mut args = args.into_iter();
                    let mut arg = args.next().unwrap().optimize_ops();
                    for nxt in args {
                        arg = Expr::And((arg, nxt.optimize_ops()).into());
                    }
                    arg
                }
                _ => {
                    match op.arity() {
                        Some(0) => Expr::ApplyZero(op),
                        Some(1) => Expr::ApplyOne(op, args.into_iter().next().unwrap().into()),
                        Some(2) => {
                            let mut args = args.into_iter();
                            let left = args.next().unwrap();
                            let right = args.next().unwrap();
                            Expr::ApplyTwo(op, (left, right).into())
                        }
                        _ => Expr::Apply(op, args.into_iter().map(|v| v.optimize_ops()).collect())
                    }
                }
            },
            Expr::ApplyAgg(op, a_args, args) => Expr::ApplyAgg(
                op,
                a_args.into_iter().map(|v| v.optimize_ops()).collect(),
                args.into_iter().map(|v| v.optimize_ops()).collect(),
            ),
            Expr::FieldAcc(f, arg) => Expr::FieldAcc(f, arg.optimize_ops().into()),
            Expr::IdxAcc(i, arg) => Expr::IdxAcc(i, arg.optimize_ops().into()),
            Expr::IfExpr(args) => {
                let (cond, if_part, else_part) = *args;
                Expr::IfExpr((cond.optimize_ops(), if_part.optimize_ops(), else_part.optimize_ops()).into())
            },
            v @ (Expr::Const(_)
            | Expr::Variable(_)
            | Expr::TableCol(_, _)
            | Expr::TupleSetIdx(_)
            | Expr::ApplyZero(_)
            | Expr::ApplyOne(_, _)
            | Expr::ApplyTwo(_, _)
            | Expr::Add(_)
            | Expr::Sub(_)
            | Expr::Mul(_)
            | Expr::Div(_)
            | Expr::Pow(_)
            | Expr::Mod(_)
            | Expr::StrCat(_)
            | Expr::Eq(_)
            | Expr::Ne(_)
            | Expr::Gt(_)
            | Expr::Ge(_)
            | Expr::Lt(_)
            | Expr::Le(_)
            | Expr::Not(_)
            | Expr::Minus(_)
            | Expr::IsNull(_)
            | Expr::NotNull(_)
            | Expr::Coalesce(_)
            | Expr::Or(_)
            | Expr::And(_)) => v,
        }
    }
    pub(crate) fn row_eval<C: RowEvalContext + 'a>(&'a self, ctx: &'a C) -> Result<Value<'a>> {
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
            Expr::Variable(v) => return Err(EvalError::UnresolvedVariable(v.clone())),
            Expr::TableCol(tid, cid) => return Err(EvalError::UnresolveTableCol(*tid, *cid)),
            Expr::TupleSetIdx(idx) => ctx.resolve(idx)?.clone(),
            Expr::Apply(op, vals) => {
                let (has_null, args) = vals.iter().try_fold(
                    (false, Vec::with_capacity(vals.len())),
                    |(has_null, mut acc), v| {
                        v.row_eval(ctx).map(|v| match v {
                            Value::Null => {
                                acc.push(Value::Null);
                                (true, acc)
                            }
                            v => {
                                acc.push(v);
                                (has_null, acc)
                            }
                        })
                    },
                )?;
                op.eval(has_null, args)?
            }
            Expr::ApplyZero(op) => op.eval_zero()?,
            Expr::ApplyOne(op, arg) => {
                let arg = arg.row_eval(ctx)?;
                if op.non_null_args() {
                    if arg == Value::Null {
                        Value::Null
                    } else {
                        op.eval_one_non_null(arg)?
                    }
                } else {
                    op.eval_one(arg)?
                }
            }
            Expr::ApplyTwo(op, args) => {
                if op.non_null_args() {
                    let left = args.as_ref().0.row_eval(ctx)?;
                    if left == Value::Null {
                        return Ok(Value::Null);
                    }
                    let right = args.as_ref().0.row_eval(ctx)?;
                    if right == Value::Null {
                        return Ok(Value::Null);
                    }
                    op.eval_two_non_null(left, right)?
                } else {
                    let left = args.as_ref().0.row_eval(ctx)?;
                    let right = args.as_ref().0.row_eval(ctx)?;
                    op.eval_two(left, right)?
                }
            }
            Expr::ApplyAgg(_, _, _) => {
                todo!()
            }
            Expr::FieldAcc(f, arg) => match arg.row_eval(ctx)? {
                Value::Null => Value::Null,
                Value::Dict(mut d) => d.remove(f as &str).unwrap_or(Value::Null),
                v => return Err(EvalError::FieldAccess(f.clone(), v.to_static())),
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
                v => return Err(EvalError::IndexAccess(*idx, v.to_static())),
            },
            Expr::IfExpr(args) => {
                let (cond, if_part, else_part) = args.as_ref();
                row_eval_if_expr(ctx, cond, if_part, else_part)?
            }
            // optimized implementations, not really necessary
            Expr::Add(args) => OpAdd.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Sub(args) => OpSub.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Mul(args) => OpMul.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Div(args) => OpDiv.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Pow(args) => OpPow.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Mod(args) => OpMod.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::StrCat(args) => OpStrCat.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Eq(args) => OpEq.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Ne(args) => OpNe.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Gt(args) => OpGt.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Ge(args) => OpGe.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Lt(args) => OpLt.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Le(args) => OpLe.eval_two_non_null(
                match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
                match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                },
            )?,
            Expr::Not(arg) => {
                OpNot.eval_one_non_null(match arg.as_ref().row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v,
                })?
            }
            Expr::Minus(arg) => OpMinus.eval_one_non_null(match arg.as_ref().row_eval(ctx)? {
                v @ Value::Null => return Ok(v),
                v => v,
            })?,
            Expr::IsNull(arg) => OpIsNull.eval_one(arg.as_ref().row_eval(ctx)?)?,
            Expr::NotNull(arg) => OpNotNull.eval_one(arg.as_ref().row_eval(ctx)?)?,
            // These implementations are special in that they short-circuit
            Expr::Coalesce(args) => row_eval_coalesce(
                ctx,
                &args.as_ref().0,
                &args.as_ref().1,
            )?,
            Expr::Or(args) => row_eval_or(
                ctx,
                &args.as_ref().0,
                &args.as_ref().1,
            )?,
            Expr::And(args) => row_eval_and(
                ctx,
                &args.as_ref().0,
                &args.as_ref().1,
            )?,
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::expr_parser::tests::str2expr;

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

        Ok(())
    }
}
