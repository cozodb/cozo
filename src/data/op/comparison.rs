use std::result;
use crate::data::eval::EvalError;
use crate::data::op::Op;
use crate::data::value::Value;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpEq;

impl Op for OpEq {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "=="
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        Ok((left == right).into())
    }
}

pub(crate) struct OpNe;

impl Op for OpNe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "!="
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        Ok((left != right).into())
    }
}

pub(crate) struct OpGt;

impl Op for OpGt {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        ">"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l > r).into(),
            (Value::Float(l), Value::Int(r)) => (l > (r as f64).into()).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) > r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l > r).into(),
            (Value::Text(l), Value::Text(r)) => (l > r).into(),
            (l, r) => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![l.to_static(), r.to_static()],
                ));
            }
        };
        Ok(res)
    }
}

pub(crate) struct OpGe;

impl Op for OpGe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        ">="
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l >= r).into(),
            (Value::Float(l), Value::Int(r)) => (l >= (r as f64).into()).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) >= r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l >= r).into(),
            (Value::Text(l), Value::Text(r)) => (l >= r).into(),
            (l, r) => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![l.to_static(), r.to_static()],
                ));
            }
        };
        Ok(res)
    }
}

pub(crate) struct OpLt;

impl Op for OpLt {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "<"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l < r).into(),
            (Value::Float(l), Value::Int(r)) => (l < (r as f64).into()).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) < r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l < r).into(),
            (Value::Text(l), Value::Text(r)) => (l < r).into(),
            (l, r) => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![l.to_static(), r.to_static()],
                ));
            }
        };
        Ok(res)
    }
}

pub(crate) struct OpLe;

impl Op for OpLe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "<="
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l <= r).into(),
            (Value::Float(l), Value::Int(r)) => (l <= (r as f64).into()).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) <= r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l <= r).into(),
            (Value::Text(l), Value::Text(r)) => (l <= r).into(),
            (l, r) => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![l.to_static(), r.to_static()],
                ));
            }
        };
        Ok(res)
    }
}