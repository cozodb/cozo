use crate::data::eval::{EvalError, PartialEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::value::Value;
use anyhow::Result;
use std::borrow::Cow;
use std::collections::BTreeMap;

pub(crate) const NAME_OP_CONCAT: &str = "concat";

pub(crate) fn row_eval_concat<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &'a [Expr],
) -> Result<Value<'a>> {
    let mut coll: Vec<Value> = vec![];
    for v in args.iter() {
        match v.row_eval(ctx)? {
            Value::Null => {}
            Value::List(l) => coll.extend(l.clone()),
            v => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_CONCAT.to_string(),
                    vec![v.clone().into_static()],
                )
                .into());
            }
        }
    }
    Ok(coll.into())
}

pub(crate) fn partial_eval_concat_expr<T: PartialEvalContext>(
    ctx: &T,
    args: Vec<Expr>,
) -> Result<Expr> {
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
        Ok(Expr::OpConcat(args))
    }
}

pub(crate) const NAME_OP_MERGE: &str = "merge";

pub(crate) fn row_eval_merge<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &'a [Expr],
) -> Result<Value<'a>> {
    let mut coll: BTreeMap<Cow<str>, Value> = BTreeMap::new();
    for v in args.iter() {
        match v.row_eval(ctx)? {
            Value::Null => {}
            Value::Dict(d) => coll.extend(d.clone()),
            v => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_MERGE.to_string(),
                    vec![v.clone().into_static()],
                )
                .into());
            }
        }
    }
    Ok(coll.into())
}

pub(crate) fn partial_eval_merge_expr<T: PartialEvalContext>(
    ctx: &T,
    args: Vec<Expr>,
) -> Result<Expr> {
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
        Ok(Expr::OpMerge(args))
    }
}
