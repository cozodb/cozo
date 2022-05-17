use std::result;
use crate::data::eval::EvalError;
use crate::data::op::Op;
use crate::data::value::Value;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpAdd;

impl Op for OpAdd {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "+"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l + r).into(),
            (Value::Float(l), Value::Int(r)) => (l + (r as f64)).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) + r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l.into_inner() + r.into_inner()).into(),
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

pub(crate) struct OpSub;

impl Op for OpSub {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "-"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l - r).into(),
            (Value::Float(l), Value::Int(r)) => (l - (r as f64)).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) - r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l.into_inner() - r.into_inner()).into(),
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

pub(crate) struct OpMul;

impl Op for OpMul {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "*"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l * r).into(),
            (Value::Float(l), Value::Int(r)) => (l * (r as f64)).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) * r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l.into_inner() * r.into_inner()).into(),
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

pub(crate) struct OpDiv;

impl Op for OpDiv {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "/"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l as f64 / r as f64).into(),
            (Value::Float(l), Value::Int(r)) => (l / (r as f64)).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64) / r.into_inner()).into(),
            (Value::Float(l), Value::Float(r)) => (l.into_inner() / r.into_inner()).into(),
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

pub(crate) struct OpMod;

impl Op for OpMod {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "%"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => (l % r).into(),
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

pub(crate) struct OpPow;

impl Op for OpPow {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "**"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        let res: Value = match (left, right) {
            (Value::Int(l), Value::Int(r)) => ((l as f64).powf(r as f64)).into(),
            (Value::Float(l), Value::Int(r)) => ((l.into_inner()).powf(r as f64)).into(),
            (Value::Int(l), Value::Float(r)) => ((l as f64).powf(r.into_inner())).into(),
            (Value::Float(l), Value::Float(r)) => ((l.into_inner()).powf(r.into_inner())).into(),
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


pub(crate) struct OpMinus;

impl Op for OpMinus {
    fn name(&self) -> &str {
        "--"
    }
    fn eval_one_non_null<'a>(&self, arg: Value<'a>) -> Result<Value<'a>> {
        match arg {
            Value::Int(i) => Ok((-i).into()),
            Value::Float(i) => Ok((-i).into()),
            v => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![v.to_static()],
            )),
        }
    }
}
