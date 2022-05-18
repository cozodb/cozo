use crate::data::eval::EvalError;
use crate::data::op::{extract_two_args, Op};
use crate::data::value::Value;
use std::result;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpStrCat;

impl OpStrCat {
    pub(crate) fn eval_two_non_null<'a>(&self, left: Value<'a>, right: Value<'a>) -> Result<Value<'a>> {
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

impl Op for OpStrCat {
    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "++"
    }

    fn non_null_args(&self) -> bool {
        true
    }

    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        let (left, right) = extract_two_args(args);
        self.eval_two_non_null(left, right)
    }
}
