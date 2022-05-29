use crate::data::eval::{EvalError, PartialEvalContext, RowEvalContext};
use crate::data::expr::{BuiltinFn, Expr};
use crate::data::value::Value;
use anyhow::Result;

pub(crate) const OP_IS_NULL: BuiltinFn = BuiltinFn {
    name: NAME_OP_IS_NULL,
    arity: Some(1),
    non_null_args: false,
    func: op_is_null,
};

pub(crate) const NAME_OP_IS_NULL: &str = "is_null";

pub(crate) fn op_is_null<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let arg = args.into_iter().next().unwrap();
    Ok((*arg == Value::Null).into())
}

pub(crate) const OP_NOT_NULL: BuiltinFn = BuiltinFn {
    name: NAME_OP_NOT_NULL,
    arity: Some(1),
    non_null_args: false,
    func: op_not_null,
};

pub(crate) const NAME_OP_NOT_NULL: &str = "not_null";

pub(crate) fn op_not_null<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let arg = args.into_iter().next().unwrap();
    Ok((*arg != Value::Null).into())
}

pub(crate) const NAME_OP_OR: &str = "||";

pub(crate) fn partial_eval_or<'a, T: PartialEvalContext>(
    ctx: &'a T,
    args: Vec<Expr>,
) -> Result<Expr> {
    let mut collected = vec![];
    let mut has_null = false;
    for arg in args {
        match arg.partial_eval(ctx)? {
            Expr::Const(Value::Null) => has_null = true,
            Expr::Const(Value::Bool(b)) => {
                if b {
                    return Ok(Expr::Const(Value::Bool(true)));
                }
            }
            Expr::Const(v) => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_OR.to_string(),
                    vec![v.into_static()],
                )
                .into());
            }
            Expr::OpOr(mut args) => {
                if args.last() == Some(&Expr::Const(Value::Null)) {
                    has_null = true;
                    args.pop();
                }
                collected.extend(args);
            }
            expr => collected.push(expr),
        }
    }
    if has_null {
        collected.push(Expr::Const(Value::Null));
    }
    Ok(match collected.len() {
        0 => Expr::Const(Value::Bool(false)),
        1 => collected.pop().unwrap(),
        _ => Expr::OpOr(collected),
    })
}

pub(crate) fn row_eval_or<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &[Expr]
) -> Result<Value<'a>> {
    let mut has_null = false;
    for arg in args {
        let arg = arg.row_eval(ctx)?;
        match arg {
            Value::Null => has_null = true,
            Value::Bool(true) => return Ok(Value::Bool(true)),
            Value::Bool(false) => {}
            v => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_OR.to_string(),
                    vec![v.clone().into_static()],
                )
                    .into());
            }
        }
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(false))
    }
}

pub(crate) const NAME_OP_AND: &str = "&&";

pub(crate) fn partial_eval_and<'a, T: PartialEvalContext>(
    ctx: &'a T,
    args: Vec<Expr>,
) -> Result<Expr> {
    let mut collected = vec![];
    let mut has_null = false;
    for arg in args {
        match arg.partial_eval(ctx)? {
            Expr::Const(Value::Null) => has_null = true,
            Expr::Const(Value::Bool(b)) => {
                if !b {
                    return Ok(Expr::Const(Value::Bool(false)));
                }
            }
            Expr::Const(v) => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_AND.to_string(),
                    vec![v.into_static()],
                )
                .into());
            }
            Expr::OpAnd(mut args) => {
                if args.last() == Some(&Expr::Const(Value::Null)) {
                    has_null = true;
                    args.pop();
                }
                collected.extend(args);
            }
            expr => collected.push(expr),
        }
    }
    if has_null {
        collected.push(Expr::Const(Value::Null));
    }
    Ok(match collected.len() {
        0 => Expr::Const(Value::Bool(true)),
        1 => collected.pop().unwrap(),
        _ => Expr::OpAnd(collected),
    })
}

pub(crate) fn row_eval_and<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    args: &[Expr]
) -> Result<Value<'a>> {
    let mut has_null = false;
    for arg in args {
        let arg = arg.row_eval(ctx)?;
        match arg {
            Value::Null => has_null = true,
            Value::Bool(false) => return Ok(Value::Bool(false)),
            Value::Bool(true) => {}
            v => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_AND.to_string(),
                    vec![v.clone().into_static()],
                )
                    .into());
            }
        }
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(true))
    }
}

pub(crate) const OP_NOT: BuiltinFn = BuiltinFn {
    name: NAME_OP_NOT,
    arity: Some(1),
    non_null_args: true,
    func: op_not,
};

pub(crate) const NAME_OP_NOT: &str = "!";

pub(crate) fn op_not<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    match args.into_iter().next().unwrap() {
        Value::Bool(b) => Ok((!b).into()),
        v => Err(EvalError::OpTypeMismatch(NAME_OP_NOT.to_string(), vec![v.clone().into_static()]).into()),
    }
}
