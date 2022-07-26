use std::fmt::{Debug, Formatter};

use anyhow::Result;
use itertools::Itertools;

use crate::data::expr::ExprError::UnexpectedArgs;
use crate::data::value::DataValue;
use crate::query::compile::Term;

#[derive(Debug, thiserror::Error)]
pub enum ExprError {
    #[error("unexpected args for {0}: {1:?}")]
    UnexpectedArgs(&'static str, Vec<DataValue>),
}

#[derive(Debug, Clone)]
pub(crate) enum Expr {
    Const(Term<DataValue>),
    Apply(&'static Op, Box<[Expr]>),
}

#[derive(Clone)]
pub(crate) struct Op {
    name: &'static str,
    min_arity: usize,
    vararg: bool,
    is_predicate: bool,
    inner: fn(&[DataValue]) -> Result<DataValue>,
}

impl Debug for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(self.name).field(&self.min_arity).finish()
    }
}

macro_rules! define_op {
    ($name:ident, $min_arity:expr, $vararg:expr, $is_pred:expr) => {
        pub(crate) const $name: Op = Op {
            name: stringify!($name),
            min_arity: $min_arity,
            vararg: $vararg,
            is_predicate: $is_pred,
            inner: ::casey::lower!($name),
        };
    };
}

define_op!(OP_EQ, 0, true, true);
pub(crate) fn op_eq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args.iter().all_equal()))
}

define_op!(OP_NEQ, 0, true, true);
fn op_neq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(!args.iter().all_equal()))
}

define_op!(OP_ADD, 0, true, false);
fn op_add(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 0i64;
    let mut f_accum = 0.0f64;
    for arg in args {
        match arg {
            DataValue::Int(i) => i_accum += i,
            DataValue::Float(f) => f_accum += f.0,
            _ => return Err(UnexpectedArgs("add", args.to_vec()).into()),
        }
    }
    if f_accum == 0.0f64 {
        Ok(DataValue::Int(i_accum))
    } else {
        Ok(DataValue::Float((i_accum as f64 + f_accum).into()))
    }
}

define_op!(OP_SUB, 2, false, false);
fn op_sub(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Int(b)) => DataValue::Int(*a - *b),
        (DataValue::Float(a), DataValue::Float(b)) => DataValue::Float(*a - *b),
        (DataValue::Int(a), DataValue::Float(b)) => DataValue::Float(((*a as f64) - b.0).into()),
        (DataValue::Float(a), DataValue::Int(b)) => DataValue::Float((a.0 - (*b as f64)).into()),
        _ => return Err(UnexpectedArgs("sub", args.to_vec()).into()),
    })
}

define_op!(OP_MUL, 0, true, false);
fn op_mul(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 1i64;
    let mut f_accum = 1.0f64;
    for arg in args {
        match arg {
            DataValue::Int(i) => i_accum *= i,
            DataValue::Float(f) => f_accum *= f.0,
            _ => return Err(UnexpectedArgs("mul", args.to_vec()).into()),
        }
    }
    if f_accum == 1.0f64 {
        Ok(DataValue::Int(i_accum))
    } else {
        Ok(DataValue::Float((i_accum as f64 + f_accum).into()))
    }
}

define_op!(OP_DIV, 2, false, false);
fn op_div(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Int(b)) => {
            DataValue::Float(((*a as f64) / (*b as f64)).into())
        }
        (DataValue::Float(a), DataValue::Float(b)) => DataValue::Float(*a / *b),
        (DataValue::Int(a), DataValue::Float(b)) => DataValue::Float(((*a as f64) / b.0).into()),
        (DataValue::Float(a), DataValue::Int(b)) => DataValue::Float((a.0 / (*b as f64)).into()),
        _ => return Err(UnexpectedArgs("div", args.to_vec()).into()),
    })
}

define_op!(OP_AND, 0, true, true);
fn op_and(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if let DataValue::Bool(b) = arg {
            if !b {
                return Ok(DataValue::Bool(false));
            }
        } else {
            return Err(UnexpectedArgs("and", args.to_vec()).into());
        }
    }
    Ok(DataValue::Bool(true))
}

define_op!(OP_OR, 0, true, true);
fn op_or(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if let DataValue::Bool(b) = arg {
            if *b {
                return Ok(DataValue::Bool(true));
            }
        } else {
            return Err(UnexpectedArgs("or", args.to_vec()).into());
        }
    }
    Ok(DataValue::Bool(false))
}

define_op!(OP_NOT, 1, false, true);
fn op_not(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bool(b) = &args[0] {
        Ok(DataValue::Bool(!*b))
    } else {
        Err(UnexpectedArgs("not", args.to_vec()).into())
    }
}