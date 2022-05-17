use std::result;
use crate::data::eval::EvalError;
use crate::data::op::Op;
use crate::data::value::Value;

type Result<T> = result::Result<T, EvalError>;


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