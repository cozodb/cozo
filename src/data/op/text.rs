use crate::data::eval::EvalError;
use crate::data::value::Value;
use anyhow::Result;
use crate::data::expr::BuiltinFn;

pub(crate) const OP_STR_CAT: BuiltinFn = BuiltinFn {
    name: NAME_OP_STR_CAT,
    arity: None,
    non_null_args: true,
    func: op_str_cat
};

pub(crate) const NAME_OP_STR_CAT: &str = "++";
pub(crate) fn op_str_cat<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut ret = String::new();
    for arg in args {
        match arg {
            Value::Text(t) => {
                ret += t.as_ref();
            }
            _ => {
                return Err(EvalError::OpTypeMismatch(
                    NAME_OP_STR_CAT.to_string(),
                    args.iter().cloned().map(|v| v.into_static()).collect(),
                )
                .into());
            }
        }
    }
    Ok(ret.into())
}
