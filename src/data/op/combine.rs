use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::Expr;
use crate::data::op::Op;
use crate::data::value::Value;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct OpConcat;

pub(crate) const NAME_OP_CONCAT: &str = "concat";

impl Op for OpConcat {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_CONCAT
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut coll = vec![];
        for v in args.into_iter() {
            match v {
                Value::Null => {}
                Value::List(l) => coll.extend(l),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(coll.into())
    }
}

pub(crate) fn partial_eval_concat_expr<'a, T: PartialEvalContext>(
    ctx: &'a T,
    args: Vec<Expr<'a>>,
) -> Result<Expr<'a>> {
    let mut can_concat = true;
    let mut all_const = true;
    let args = args
        .into_iter()
        .map(|a| -> Result<Expr> {
            let a = a.partial_eval(ctx)?;
            all_const = all_const && a.is_const();
            can_concat = all_const && (a.is_const() || matches!(a, Expr::List(_)));
            Ok(a)
        })
        .collect::<Result<Vec<_>>>()?;
    if all_const {
        let mut result = vec![];
        for arg in args.into_iter() {
            match arg.extract_const().unwrap() {
                Value::List(l) => result.extend(l),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        NAME_OP_CONCAT.to_string(),
                        vec![v.into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(Expr::Const(Value::List(result)))
    } else if can_concat {
        let mut result = vec![];
        for arg in args.into_iter() {
            match arg {
                Expr::Const(Value::Null) => {}
                Expr::Const(Value::List(l)) => {
                    for a in l {
                        result.push(Expr::Const(a))
                    }
                }
                Expr::List(l) => result.extend(l),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        NAME_OP_CONCAT.to_string(),
                        vec![Value::from(v).into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(Expr::List(result))
    } else {
        Ok(Expr::Apply(Arc::new(OpConcat), args))
    }
}

pub(crate) struct OpMerge;

pub(crate) const NAME_OP_MERGE: &str = "merge";

impl Op for OpMerge {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_MERGE
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut coll = BTreeMap::new();
        for v in args.into_iter() {
            match v {
                Value::Null => {}
                Value::Dict(d) => coll.extend(d),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(coll.into())
    }
}

pub(crate) fn partial_eval_merge_expr<'a, T: PartialEvalContext>(
    ctx: &'a T,
    args: Vec<Expr<'a>>,
) -> Result<Expr<'a>> {
    let mut can_merge = true;
    let mut all_const = true;
    let args = args
        .into_iter()
        .map(|ex| -> Result<Expr> {
            let ex = ex.partial_eval(ctx)?;
            all_const = all_const && ex.is_const();
            can_merge = can_merge && (ex.is_const() || matches!(ex, Expr::Dict(_)));
            Ok(ex)
        })
        .collect::<Result<Vec<_>>>()?;
    if all_const {
        let mut result = BTreeMap::new();
        for arg in args.into_iter() {
            match arg.extract_const().unwrap() {
                Value::Null => {}
                Value::Dict(d) => result.extend(d),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        NAME_OP_MERGE.to_string(),
                        vec![v.into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(Expr::Const(Value::Dict(result)))
    } else if can_merge {
        let mut result = BTreeMap::new();
        for arg in args.into_iter() {
            match arg {
                Expr::Const(Value::Null) => {}
                Expr::Const(Value::Dict(d)) => {
                    for (k, v) in d {
                        result.insert(k.to_string(), Expr::Const(v));
                    }
                }
                Expr::Dict(d) => result.extend(d),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        NAME_OP_MERGE.to_string(),
                        vec![Value::from(v).into_static()],
                    )
                    .into());
                }
            }
        }
        Ok(Expr::Dict(result))
    } else {
        Ok(Expr::Apply(Arc::new(OpMerge), args))
    }
}
