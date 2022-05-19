use crate::data::eval::EvalError;
use crate::data::op::{extract_two_args, Op};
use crate::data::value::Value;
use std::result;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpEq;

impl OpEq {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        Ok((left == right).into())
    }
}

pub(crate) const NAME_OP_EQ: &str = "==";

impl Op for OpEq {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_EQ
    }

    fn non_null_args(&self) -> bool {
        true
    }

    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}

pub(crate) struct OpNe;

impl OpNe {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        Ok((left != right).into())
    }
}

pub(crate) const NAME_OP_NE: &str = "!=";

impl Op for OpNe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_NE
    }

    fn non_null_args(&self) -> bool {
        true
    }

    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}

pub(crate) struct OpGt;

impl OpGt {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
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

pub(crate) const NAME_OP_GT: &str = ">";

impl Op for OpGt {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_GT
    }

    fn non_null_args(&self) -> bool {
        true
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}

pub(crate) struct OpGe;

impl OpGe {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
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

pub(crate) const NAME_OP_GE: &str = ">=";

impl Op for OpGe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_GE
    }

    fn non_null_args(&self) -> bool {
        true
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}

pub(crate) struct OpLt;

impl OpLt {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
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

pub(crate) const NAME_OP_LT: &str = "<";

impl Op for OpLt {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_LT
    }

    fn non_null_args(&self) -> bool {
        true
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}

pub(crate) struct OpLe;

impl OpLe {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
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

pub(crate) const NAME_OP_LE: &str = "<=";

impl Op for OpLe {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_LE
    }

    fn non_null_args(&self) -> bool {
        true
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}
