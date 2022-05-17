use std::result;
use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::op::Op;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};

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

pub(crate) struct OpCoalesce;

impl Op for OpCoalesce {
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        "~~"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, _has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        for arg in args {
            if arg != Value::Null {
                return Ok(arg);
            }
        }
        Ok(Value::Null)
    }
    fn eval_two<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        match (left, right) {
            (Value::Null, v) => Ok(v),
            (l, _r) => Ok(l)
        }
    }
    fn partial_eval<'a>(&self, args: Vec<Expr<'a>>) -> crate::data::op::Result<Option<Expr<'a>>> {
        todo!()
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
                v => return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.to_static()],
                )),
            }
        }
        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(false))
        }
    }
    fn eval_two<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        match (left, right) {
            (Value::Null, Value::Bool(true)) => Ok(true.into()),
            (Value::Null, Value::Bool(false)) => Ok(Value::Null),
            (Value::Bool(true), Value::Null) => Ok(true.into()),
            (Value::Bool(false), Value::Null) => Ok(Value::Null),
            (Value::Bool(l), Value::Bool(r)) => Ok((l || r).into()),
            (l, r) => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![l.to_static(), r.to_static()],
            ))
        }
    }
    fn partial_eval<'a>(&self, args: Vec<Expr<'a>>) -> crate::data::op::Result<Option<Expr<'a>>> {
        todo!()
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
                v => return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.to_static()],
                )),
            }
        }
        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(true))
        }
    }
    fn eval_two<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        match (left, right) {
            (Value::Null, Value::Bool(false)) => Ok(false.into()),
            (Value::Null, Value::Bool(true)) => Ok(Value::Null),
            (Value::Bool(false), Value::Null) => Ok(false.into()),
            (Value::Bool(true), Value::Null) => Ok(Value::Null),
            (Value::Bool(l), Value::Bool(r)) => Ok((l && r).into()),
            (l, r) => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![l.to_static(), r.to_static()],
            ))
        }
    }
    fn partial_eval<'a>(&self, args: Vec<Expr<'a>>) -> crate::data::op::Result<Option<Expr<'a>>> {
        todo!()
    }
}
