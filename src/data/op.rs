mod arithmetic;
mod text;
mod comparison;
mod boolean;
mod combine;

use crate::data::eval::{EvalError};
use crate::data::expr::Expr;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use std::result;

pub(crate) use arithmetic::*;
pub(crate) use text::*;
pub(crate) use comparison::*;
pub(crate) use boolean::*;
pub(crate) use combine::*;

type Result<T> = result::Result<T, EvalError>;

pub(crate) trait Op: Send + Sync {
    fn is_resolved(&self) -> bool {
        true
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn has_side_effect(&self) -> bool {
        false
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
    fn eval_one<'a>(&self, _arg: Value<'a>) -> Result<Value<'a>> {
        panic!(
            "Required method `eval_one` not implemented for `{}`",
            self.name()
        )
    }
    fn eval_two<'a>(&self, _left: Value<'a>, _right: Value<'a>) -> Result<Value<'a>> {
        panic!(
            "Required method `eval_two` not implemented for `{}`",
            self.name()
        )
    }
    fn partial_eval<'a>(&self, args: Vec<Expr<'a>>) -> Result<Option<Expr<'a>>> {
        // usually those functions that needs specialized implementations are those with arity None
        if let Some(arity) = self.arity() {
            if arity != args.len() {
                return Err(EvalError::ArityMismatch(self.name().to_string(), arity))
            }
        }
        let mut has_null = false;
        match args.iter().map(|v| {
            match v {
                Expr::Const(v) => {
                    if *v == Value::Null {
                        has_null = true;
                    }
                    Some(v.clone())
                }
                _ => None
            }
        }).collect::<Vec<_>>().into_iter().collect::<Option<Vec<Value>>>() {
            Some(args) => {
                Ok(Some(Expr::Const(self.eval(has_null, args)?)))
            }
            None => {
                if self.non_null_args() && has_null {
                    Ok(Some(Expr::Const(Value::Null)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

pub(crate) trait AggOp: Send + Sync {
    fn is_resolved(&self) -> bool {
        true
    }
    fn arity(&self) -> Option<usize> {
        Some(1)
    }
    fn has_side_effect(&self) -> bool {
        false
    }
    fn name(&self) -> &str;
    fn partial_eval<'a>(&self, a_args: Vec<Expr<'a>>, args: Vec<Expr<'a>>) -> Result<Option<Expr<'a>>> {
        todo!()
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
