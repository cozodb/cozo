use crate::data::expr::Expr;
use crate::data::expr_parser::ExprParseError;
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::result;
use crate::data::op::{Op, OpAdd, OpAnd, OpCoalesce, OpDiv, OpEq, OpGe, OpGt, OpIsNull, OpLe, OpLt, OpMinus, OpMod, OpMul, OpNe, OpNegate, OpNotNull, OpOr, OpPow, OpStrCat, OpSub};

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

pub(crate) trait ExprEvalContext {}

fn extract_optimized_bin_args(args: Vec<Expr>) -> (Expr, Expr) {
    let mut args = args.into_iter();
    (args.next().unwrap().optimize_ops(), args.next().unwrap().optimize_ops())
}

fn extract_optimized_u_args(args: Vec<Expr>) -> Expr {
    args.into_iter().next().unwrap().optimize_ops()
}

impl<'a> Expr<'a> {
    pub(crate) fn partial_eval<C: ExprEvalContext + 'a>(&'a self, ctx: &'a C) -> Result<Self> {
        todo!()
    }
    pub(crate) fn optimize_ops(self) -> Self {
        match self {
            Expr::List(l) => Expr::List(l.into_iter().map(|v| v.optimize_ops()).collect()),
            Expr::Dict(d) => Expr::Dict(d.into_iter().map(|(k, v)| (k, v.optimize_ops())).collect()),
            Expr::Apply(op, args) => {
                match op.name() {
                    name if name == OpAdd.name() => Expr::Add(extract_optimized_bin_args(args).into()),
                    name if name == OpSub.name() => Expr::Sub(extract_optimized_bin_args(args).into()),
                    name if name == OpMul.name() => Expr::Mul(extract_optimized_bin_args(args).into()),
                    name if name == OpDiv.name() => Expr::Div(extract_optimized_bin_args(args).into()),
                    name if name == OpPow.name() => Expr::Pow(extract_optimized_bin_args(args).into()),
                    name if name == OpMod.name() => Expr::Mod(extract_optimized_bin_args(args).into()),
                    name if name == OpStrCat.name() => Expr::StrCat(extract_optimized_bin_args(args).into()),
                    name if name == OpEq.name() => Expr::Eq(extract_optimized_bin_args(args).into()),
                    name if name == OpNe.name() => Expr::Ne(extract_optimized_bin_args(args).into()),
                    name if name == OpGt.name() => Expr::Gt(extract_optimized_bin_args(args).into()),
                    name if name == OpGe.name() => Expr::Ge(extract_optimized_bin_args(args).into()),
                    name if name == OpLt.name() => Expr::Lt(extract_optimized_bin_args(args).into()),
                    name if name == OpLe.name() => Expr::Le(extract_optimized_bin_args(args).into()),
                    name if name == OpNegate.name() => Expr::Negate(extract_optimized_u_args(args).into()),
                    name if name == OpMinus.name() => Expr::Minus(extract_optimized_u_args(args).into()),
                    name if name == OpIsNull.name() => Expr::IsNull(extract_optimized_u_args(args).into()),
                    name if name == OpNotNull.name() => Expr::NotNull(extract_optimized_u_args(args).into()),
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
                    _ => Expr::Apply(op, args.into_iter().map(|v| v.optimize_ops()).collect())
                }
            }
            Expr::ApplyAgg(op, a_args, args) => {
                Expr::ApplyAgg(op,
                               a_args.into_iter().map(|v| v.optimize_ops()).collect(),
                               args.into_iter().map(|v| v.optimize_ops()).collect(),
                )
            }
            Expr::FieldAcc(f, arg) => Expr::FieldAcc(f, arg.optimize_ops().into()),
            Expr::IdxAcc(i, arg) => Expr::IdxAcc(i, arg.optimize_ops().into()),

            v @ (Expr::Const(_) |
            Expr::Variable(_) |
            Expr::TableCol(_, _) |
            Expr::TupleSetIdx(_) |
            Expr::Add(_) |
            Expr::Sub(_) |
            Expr::Mul(_) |
            Expr::Div(_) |
            Expr::Pow(_) |
            Expr::Mod(_) |
            Expr::StrCat(_) |
            Expr::Eq(_) |
            Expr::Ne(_) |
            Expr::Gt(_) |
            Expr::Ge(_) |
            Expr::Lt(_) |
            Expr::Le(_) |
            Expr::Negate(_) |
            Expr::Minus(_) |
            Expr::IsNull(_) |
            Expr::NotNull(_) |
            Expr::Coalesce(_) |
            Expr::Or(_) |
            Expr::And(_)) => v
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
                // TODO for non-null operators, short-circuit
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
            // optimized implementations, not really necessary
            Expr::Add(args) => {
                OpAdd.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Sub(args) => {
                OpSub.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Mul(args) => {
                OpMul.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Div(args) => {
                OpDiv.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Pow(args) => {
                OpPow.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Mod(args) => {
                OpMod.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::StrCat(args) => {
                OpStrCat.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Eq(args) => {
                OpEq.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Ne(args) => {
                OpNe.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Gt(args) => {
                OpGt.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Ge(args) => {
                OpGe.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Lt(args) => {
                OpLt.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Le(args) => {
                OpLe.eval_two_non_null(match args.as_ref().0.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                }, match args.as_ref().1.row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Negate(arg) => {
                OpNegate.eval_one_non_null(match arg.as_ref().row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::Minus(arg) => {
                OpMinus.eval_one_non_null(match arg.as_ref().row_eval(ctx)? {
                    v @ Value::Null => return Ok(v),
                    v => v
                })?
            }
            Expr::IsNull(arg) => {
                OpIsNull.eval_one(arg.as_ref().row_eval(ctx)?)?
            }
            Expr::NotNull(arg) => {
                OpNotNull.eval_one(arg.as_ref().row_eval(ctx)?)?
            }
            Expr::Coalesce(args) => {
                OpCoalesce.eval_two(args.as_ref().0.row_eval(ctx)?, args.as_ref().1.row_eval(ctx)?)?
            }
            Expr::Or(args) => {
                OpOr.eval_two(args.as_ref().0.row_eval(ctx)?, args.as_ref().1.row_eval(ctx)?)?
            }
            Expr::And(args) => {
                OpAnd.eval_two(args.as_ref().0.row_eval(ctx)?, args.as_ref().1.row_eval(ctx)?)?
            }
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
        dbg!(str2expr("123 + 457")?.row_eval(&())?);
        dbg!(str2expr("123 + 457.1")?.row_eval(&())?);
        dbg!(str2expr("'123' ++ '457.1'")?.row_eval(&())?);
        dbg!(str2expr("null ~ null ~ 123 ~ null")?.row_eval(&())?);
        dbg!(str2expr("2*3+1/10")?.row_eval(&())?);
        dbg!(str2expr("1>null")?.row_eval(&())?);
        dbg!(str2expr("'c'>'d'")?.row_eval(&())?);
        dbg!(str2expr("null && true && null")?.row_eval(&())?);
        dbg!(str2expr("null && false && null")?.row_eval(&())?);
        dbg!(str2expr("null || true || null")?.row_eval(&())?);
        dbg!(str2expr("null || false || null")?.row_eval(&())?);
        dbg!(str2expr("!true")?.row_eval(&())?);
        dbg!(str2expr("!null")?.row_eval(&())?);

        Ok(())
    }
}
