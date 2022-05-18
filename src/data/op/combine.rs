use crate::data::eval::{EvalError, ExprEvalContext};
use crate::data::expr::Expr;
use crate::data::op::Op;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use std::collections::BTreeMap;
use std::result;

type Result<T> = result::Result<T, EvalError>;

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
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
                    ))
                }
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
    fn eval<'a>(&self, _has_null: bool, args: Vec<Value<'a>>) -> Result<Value<'a>> {
        let mut coll = BTreeMap::new();
        for v in args.into_iter() {
            match v {
                Value::Null => {}
                Value::Dict(d) => coll.extend(d),
                v => {
                    return Err(EvalError::OpTypeMismatch(
                        self.name().to_string(),
                        vec![v.to_static()],
                    ))
                }
            }
        }
        Ok(coll.into())
    }
}