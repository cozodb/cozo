use crate::data::eval::EvalError;
use crate::data::expr::BuiltinFn;
use crate::data::value::Value;
use anyhow::Result;

pub(crate) const OP_ADD: BuiltinFn = BuiltinFn {
    name: NAME_OP_ADD,
    arity: Some(2),
    non_null_args: true,
    func: op_add,
};

pub(crate) const NAME_OP_ADD: &str = "+";

pub(crate) fn op_add<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();
    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l + r).into(),
        (Value::Float(l), Value::Int(r)) => (l + (*r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) + r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() + r.into_inner()).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_ADD.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_SUB: BuiltinFn = BuiltinFn {
    name: NAME_OP_SUB,
    arity: Some(2),
    non_null_args: true,
    func: op_sub,
};

pub(crate) const NAME_OP_SUB: &str = "-";

pub(crate) fn op_sub<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();
    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l - r).into(),
        (Value::Float(l), Value::Int(r)) => (l - (*r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) - r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() - r.into_inner()).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_SUB.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_MUL: BuiltinFn = BuiltinFn {
    name: NAME_OP_MUL,
    arity: Some(2),
    non_null_args: true,
    func: op_mul,
};

pub(crate) const NAME_OP_MUL: &str = "*";

pub(crate) fn op_mul<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l * r).into(),
        (Value::Float(l), Value::Int(r)) => (l * (*r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) * r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() * r.into_inner()).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_MUL.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_DIV: BuiltinFn = BuiltinFn {
    name: NAME_OP_DIV,
    arity: Some(2),
    non_null_args: true,
    func: op_div,
};

pub(crate) const NAME_OP_DIV: &str = "/";

pub(crate) fn op_div<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (*l as f64 / *r as f64).into(),
        (Value::Float(l), Value::Int(r)) => (l / (*r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64) / r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() / r.into_inner()).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_DIV.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_MOD: BuiltinFn = BuiltinFn {
    name: NAME_OP_MOD,
    arity: Some(2),
    non_null_args: true,
    func: op_mod,
};

pub(crate) const NAME_OP_MOD: &str = "%";

pub(crate) fn op_mod<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l % r).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_MOD.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_POW: BuiltinFn = BuiltinFn {
    name: NAME_OP_POW,
    arity: Some(2),
    non_null_args: true,
    func: op_pow,
};

pub(crate) const NAME_OP_POW: &str = "**";

pub(crate) fn op_pow<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let mut args = args.into_iter();
    let left = args.next().unwrap();
    let right = args.next().unwrap();

    let res: Value = match (left, right) {
        (Value::Int(l), Value::Int(r)) => ((*l as f64).powf(*r as f64)).into(),
        (Value::Float(l), Value::Int(r)) => ((l.into_inner()).powf(*r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((*l as f64).powf(r.into_inner())).into(),
        (Value::Float(l), Value::Float(r)) => ((l.into_inner()).powf(r.into_inner())).into(),
        (l, r) => {
            return Err(EvalError::OpTypeMismatch(
                NAME_OP_POW.to_string(),
                vec![l.clone().into_static(), r.clone().into_static()],
            )
            .into());
        }
    };
    Ok(res)
}

pub(crate) const OP_MINUS: BuiltinFn = BuiltinFn {
    name: NAME_OP_MINUS,
    arity: Some(1),
    non_null_args: true,
    func: op_mul,
};

pub(crate) const NAME_OP_MINUS: &str = "--";

pub(crate) fn op_minus<'a>(args: &[Value<'a>]) -> Result<Value<'a>> {
    let arg = args.into_iter().next().unwrap();
    match arg {
        Value::Int(i) => Ok((-i).into()),
        Value::Float(i) => Ok((-i).into()),
        v => Err(EvalError::OpTypeMismatch(
            NAME_OP_MINUS.to_string(),
            vec![v.clone().into_static()],
        )
        .into()),
    }
}
