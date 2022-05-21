use crate::data::eval::EvalError;
use crate::data::op::Op;
use crate::data::value::Value;
use std::collections::BTreeMap;
use std::result;

type Result<T> = result::Result<T, EvalError>;

pub(crate) struct OpConcat;

pub(crate) const NAME_OP_CONCAT: &str = "concat";

impl Op for OpConcat {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_CONCAT
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
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

pub(crate) const NAME_OP_MERGE: &str = "merge";

impl Op for OpMerge {
    fn arity(&self) -> Option<usize> {
        None
    }

    fn has_side_effect(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        NAME_OP_MERGE
    }
    fn non_null_args(&self) -> bool {
        false
    }
    fn eval<'a>(&self, args: Vec<Value<'a>>) -> Result<Value<'a>> {
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
