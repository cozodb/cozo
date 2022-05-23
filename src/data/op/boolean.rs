use crate::data::eval::{EvalError, PartialEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::op::Op;
use crate::data::value::Value;
use anyhow::Result;
use std::sync::Arc;

pub(crate) struct OpIsNull;

impl OpIsNull {
    pub(crate) fn eval_one<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        Ok((arg == Value::Null).into())
    }
}

pub(crate) const NAME_OP_IS_NULL: &str = "is_null";

impl Op for OpIsNull {
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn has_side_effect(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        NAME_OP_IS_NULL
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        self.eval_one(args.into_iter().next().unwrap())
    }
}

pub(crate) struct OpNotNull;

impl OpNotNull {
    pub(crate) fn eval_one<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        Ok((arg != Value::Null).into())
    }
}

pub(crate) const NAME_OP_NOT_NULL: &str = "not_null";

impl Op for OpNotNull {
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn has_side_effect(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        NAME_OP_NOT_NULL
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        self.eval_one(args.into_iter().next().unwrap())
    }
}

pub(crate) struct OpOr;

pub(crate) const NAME_OP_OR: &str = "||";

impl Op for OpOr {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_OR
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut has_null = false;
        for arg in args {
            match arg {
                Value::Null => has_null = true,
                Value::Bool(true) => return Ok(Value::Bool(true)),
                Value::Bool(false) => {}
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
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
}

pub(crate) fn partial_eval_or<'a, T: PartialEvalContext>(
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
                )
                .into());
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
        )
        .into()),
    }
}

pub(crate) struct OpAnd;

pub(crate) const NAME_OP_AND: &str = "&&";

impl Op for OpAnd {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_AND
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut has_null = false;
        for arg in args {
            match arg {
                Value::Null => has_null = true,
                Value::Bool(false) => return Ok(Value::Bool(false)),
                Value::Bool(true) => {}
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
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
}

pub(crate) fn partial_eval_and<'a, T: PartialEvalContext>(
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
                )
                .into());
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
        )
        .into()),
    }
}

pub(crate) struct OpNot;

impl OpNot {
    pub(crate) fn eval_one_non_null<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        match arg {
            Value::Bool(b) => Ok((!b).into()),
            v => {
                Err(EvalError::OpTypeMismatch(self.name().to_string(), vec![v.to_static()]).into())
            }
        }
    }
}

pub(crate) const NAME_OP_NOT: &str = "!";

impl Op for OpNot {
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn has_side_effect(&self) -> bool {
        false
    }
    fn non_null_args(&self) -> bool {
        true
    }
    fn name(&self) -> &str {
        NAME_OP_NOT
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        self.eval_one_non_null(args.into_iter().next().unwrap())
    }
}
