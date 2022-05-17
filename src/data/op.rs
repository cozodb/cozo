use std::cmp::max;
use std::collections::BTreeMap;
use crate::data::eval::{EvalError, ExprEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use std::fmt::{Debug, Formatter};
use std::result;

type Result<T> = result::Result<T, EvalError>;

pub(crate) trait Op: Send + Sync {
    fn is_resolved(&self) -> bool {
        true
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn name(&self) -> &str;
    fn non_null_args(&self) -> bool {
        true
    }
    fn typing_eval(&self, args: &[Typing]) -> Typing {
        let representatives = args.iter().map(|v| v.representative_value()).collect();
        match self.eval_non_null(representatives) {
            Ok(t) => t.deduce_typing(),
            Err(_) => Typing::Any,
        }
    }
    fn eval<'a>(&self, has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        if self.non_null_args() {
            if has_null {
                Ok(Value::Null)
            } else {
                match self.arity() {
                    Some(0) => self.eval_zero(),
                    Some(1) => self.eval_one_non_null(args.into_iter().next().unwrap()),
                    Some(2) => {
                        let mut args = args.into_iter();
                        self.eval_two_non_null(args.next().unwrap(), args.next().unwrap())
                    }
                    _ => self.eval_non_null(args),
                }
            }
        } else {
            panic!(
                "Required method `eval` not implemented for `{}`",
                self.name()
            )
        }
    }
    fn eval_non_null<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        panic!(
            "Required method `eval_non_null` not implemented for `{}`",
            self.name()
        )
    }
    fn eval_zero(&self) -> Result<StaticValue> {
        panic!(
            "Required method `eval_zero` not implemented for `{}`",
            self.name()
        )
    }
    fn eval_one_non_null<'a>(&self, _arg: Value<'a>) -> Result<Value<'a>> {
        panic!(
            "Required method `eval_one` not implemented for `{}`",
            self.name()
        )
    }
    fn eval_two_non_null<'a>(&self, _left: Value<'a>, _right: Value<'a>) -> Result<Value<'a>> {
        panic!(
            "Required method `eval_two` not implemented for `{}`",
            self.name()
        )
    }
    fn expr_eval(&self, ctx: &dyn ExprEvalContext, args: ()) -> () {}
}

pub(crate) trait AggOp: Send + Sync {
    fn is_resolved(&self) -> bool {
        true
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn name(&self) -> &str;
    fn row_eval(&self, ctx: (), args: ()) -> () {
        unimplemented!()
    }
    fn expr_eval(&self, ctx: (), args: ()) -> () {
        self.row_eval(ctx, args)
    }
}

pub(crate) struct UnresolvedOp(pub String);

impl Op for UnresolvedOp {
    fn is_resolved(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        &self.0
    }
}

impl AggOp for UnresolvedOp {
    fn name(&self) -> &str {
        &self.0
    }
}

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

pub(crate) struct OpStrCat;

impl Op for OpStrCat {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }
    fn name(&self) -> &str {
        "++"
    }
    fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
        match (left, right) {
            (Value::Text(l), Value::Text(r)) => {
                let mut l = l.into_owned();
                l += r.as_ref();
                Ok(l.into())
            }
            (l, r) => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![l.to_static(), r.to_static()],
            )),
        }
    }
}

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

pub(crate) struct OpNegate;

impl Op for OpNegate {
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
}

pub(crate) struct OpConcat;

impl Op for OpConcat {
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        "concat"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, _has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut coll = vec![];
        for v in args.into_iter() {
            match v {
                Value::Null => {}
                Value::List(l) => coll.extend(l),
                v => return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.to_static()],
                )),
            }
        }
        Ok(coll.into())
    }
}

pub(crate) struct OpMerge;

impl Op for OpMerge {
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        "merge"
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut coll = BTreeMap::new();
        for v in args.into_iter() {
            match v {
                Value::Null => {}
                Value::Dict(d) => coll.extend(d),
                v => return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.to_static()],
                )),
            }
        }
        Ok(coll.into())
    }
}
