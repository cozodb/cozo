mod arithmetic;
mod boolean;
mod combine;
mod comparison;
mod control;
mod sequence;
mod text;
mod uuid;

use crate::data::eval::EvalError;
use crate::data::value::Value;
use std::result;

use crate::data::expr::Expr;
pub(crate) use arithmetic::*;
pub(crate) use boolean::*;
pub(crate) use combine::*;
pub(crate) use comparison::*;
pub(crate) use control::*;
pub(crate) use text::*;

type Result<T> = result::Result<T, EvalError>;

pub(crate) trait Op: Send + Sync {
    fn arity(&self) -> Option<usize>;
    fn has_side_effect(&self) -> bool;
    fn name(&self) -> &str;
    fn non_null_args(&self) -> bool;
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>>;
}

pub(crate) trait AggOp: Send + Sync {
    fn arity(&self) -> Option<usize>;
    fn has_side_effect(&self) -> bool;
    fn name(&self) -> &str;
    fn prep(&self, args: &[Expr]) -> Result<()>;
    fn get(&self, args: &[Expr]) -> Result<Value>;
}

pub(crate) struct UnresolvedOp(pub String);

impl Op for UnresolvedOp {
    fn non_null_args(&self) -> bool {
        false
    }
    fn has_side_effect(&self) -> bool {
        true
    }
    fn arity(&self) -> Option<usize> {
        None
    }
    fn name(&self) -> &str {
        &self.0
    }
    fn eval<'a>(&self, _args: Vec<Value<'a>>) -> Result<Value<'a>> {
        unimplemented!()
    }
}

impl AggOp for UnresolvedOp {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        &self.0
    }

    fn prep(&self, _args: &[Expr]) -> Result<()> {
        todo!()
    }

    fn get(&self, _args: &[Expr]) -> Result<Value> {
        todo!()
    }
}

pub(crate) fn extract_two_args<'a>(args: Vec<Value<'a>>) -> (Value<'a>, Value<'a>) {
    let mut args = args.into_iter();
    (args.next().unwrap(), args.next().unwrap())
}
