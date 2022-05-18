use crate::data::eval::{EvalError, ExprEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::op::Op;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use std::result;
use std::sync::Arc;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpIsNull;

impl Op for OpIsNull {
    fn name(&self) -> &str {
        "is_null"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, has_null: bool, _args: Vec<Value<'a>>) -> Result<Value<'a>> {
        Ok(has_null.into())
    }
    fn eval_one<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        Ok((arg == Value::Null).into())
    }
}

pub(crate) struct OpNotNull;

impl Op for OpNotNull {
    fn name(&self) -> &str {
        "not_null"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, has_null: bool, _args: Vec<Value<'a>>) -> Result<Value<'a>> {
        Ok((!has_null).into())
    }
    fn eval_one<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        Ok((arg != Value::Null).into())
    }
}

pub(crate) struct OpOr;

impl Op for OpOr {
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        "||"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        for arg in args {
            match arg {
                Value::Null => {}
                Value::Bool(true) => return Ok(Value::Bool(true)),
                Value::Bool(false) => {}
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
                    ));
                }
            }
        }
        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(false))
        }
    }
}

pub(crate) fn partial_eval_or<'a, T: ExprEvalContext + 'a>(
    ctx: &'a T,
    args: Vec<Expr<'a>>,
) -> Result<Expr<'a>> {
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
                    OpOr.name().to_string(),
                    vec![v.to_static()],
                ));
            }
            Expr::Apply(op, mut args) if op.name() == OpOr.name() => {
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
        _ => Expr::Apply(Arc::new(OpOr), collected),
    })
}

pub(crate) fn row_eval_or<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    left: &'a Expr<'a>,
    right: &'a Expr<'a>,
) -> Result<Value<'a>> {
    let left = left.row_eval(ctx)?;
    if left == Value::Bool(true) {
        return Ok(Value::Bool(true));
    }
    let right = right.row_eval(ctx)?;
    match (left, right) {
        (Value::Null, Value::Bool(true)) => Ok(true.into()),
        (Value::Null, Value::Bool(false)) => Ok(Value::Null),
        (Value::Bool(false), Value::Null) => Ok(Value::Null),
        (Value::Bool(false), Value::Bool(r)) => Ok(r.into()),
        (l, r) => Err(EvalError::OpTypeMismatch(
            OpOr.name().to_string(),
            vec![l.to_static(), r.to_static()],
        )),
    }
}

pub(crate) struct OpAnd;

impl Op for OpAnd {
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        "&&"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        for arg in args {
            match arg {
                Value::Null => {}
                Value::Bool(false) => return Ok(Value::Bool(false)),
                Value::Bool(true) => {}
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
                    ));
                }
            }
        }
        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(true))
        }
    }
}

pub(crate) fn partial_eval_and<'a, T: ExprEvalContext + 'a>(
    ctx: &'a T,
    args: Vec<Expr<'a>>,
) -> Result<Expr<'a>> {
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
                    OpAnd.name().to_string(),
                    vec![v.to_static()],
                ));
            }
            Expr::Apply(op, mut args) if op.name() == OpAnd.name() => {
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
        _ => Expr::Apply(Arc::new(OpAnd), collected),
    })
}

pub(crate) fn row_eval_and<'a, T: RowEvalContext + 'a>(
    ctx: &'a T,
    left: &'a Expr<'a>,
    right: &'a Expr<'a>,
) -> Result<Value<'a>> {
    let left = left.row_eval(ctx)?;
    if left == Value::Bool(false) {
        return Ok(Value::Bool(false));
    }
    let right = right.row_eval(ctx)?;
    match (left, right) {
        (Value::Null, Value::Bool(false)) => Ok(false.into()),
        (Value::Null, Value::Bool(true)) => Ok(Value::Null),
        (Value::Bool(true), Value::Null) => Ok(Value::Null),
        (Value::Bool(true), Value::Bool(r)) => Ok(r.into()),
        (l, r) => Err(EvalError::OpTypeMismatch(
            OpAnd.name().to_string(),
            vec![l.to_static(), r.to_static()],
        )),
    }
}

pub(crate) struct OpNot;

impl Op for OpNot {
    fn name(&self) -> &str {
        "!"
    }
    fn eval_one_non_null<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        match arg {
            Value::Bool(b) => Ok((!b).into()),
            v => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![v.to_static()],
            )),
        }
    }
}
