use crate::data::eval::EvalError;
use crate::data::expr::BuiltinFn;
use crate::data::value::Value;
use anyhow::Result;

pub(crate) const OP_EQ: BuiltinFn = BuiltinFn {
    name: NAME_OP_EQ,
    arity: Some(2),
    non_null_args: true,
    func: op_eq,
};

pub(crate) const NAME_OP_EQ: &str = "==";

pub(crate) fn op_eq<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    Ok((left == right).into())
}

pub(crate) const OP_NE: BuiltinFn = BuiltinFn {
    name: NAME_OP_NE,
    arity: Some(2),
    non_null_args: true,
    func: op_ne,
};

pub(crate) const NAME_OP_NE: &str = "!=";

pub(crate) fn op_ne<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    Ok((left != right).into())
}

pub(crate) const OP_GT: BuiltinFn = BuiltinFn {
    name: NAME_OP_GT,
    arity: Some(2),
    non_null_args: true,
    func: op_gt,
};

pub(crate) const NAME_OP_GT: &str = ">";

pub(crate) fn op_gt<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l > r).into(),
        (Value::Float(l), Value::Int(r)) => (*l > (*r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) > r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l > r).into(),
        (Value::Text(l), Value::Text(r)) => (l > r).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_GT.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_GE: BuiltinFn = BuiltinFn {
    name: NAME_OP_GE,
    arity: Some(2),
    non_null_args: true,
    func: op_ge,
};

pub(crate) const NAME_OP_GE: &str = ">=";

pub(crate) fn op_ge<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l >= r).into(),
        (Value::Float(l), Value::Int(r)) => (*l >= (*r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) >= r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l >= r).into(),
        (Value::Text(l), Value::Text(r)) => (l >= r).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_GE.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_LT: BuiltinFn = BuiltinFn {
    name: NAME_OP_LT,
    arity: Some(2),
    non_null_args: true,
    func: op_lt,
};

pub(crate) const NAME_OP_LT: &str = "<";

pub(crate) fn op_lt<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l < r).into(),
        (Value::Float(l), Value::Int(r)) => (*l < (*r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) < r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l < r).into(),
        (Value::Text(l), Value::Text(r)) => (l < r).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_LT.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_LE: BuiltinFn = BuiltinFn {
    name: NAME_OP_LE,
    arity: Some(2),
    non_null_args: true,
    func: op_le,
};

pub(crate) const NAME_OP_LE: &str = "<=";

pub(crate) fn op_le<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l <= r).into(),
        (Value::Float(l), Value::Int(r)) => (*l <= (*r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) <= r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l <= r).into(),
        (Value::Text(l), Value::Text(r)) => (l <= r).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_LE.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}
