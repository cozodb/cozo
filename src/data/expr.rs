use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use anyhow::Result;
use itertools::Itertools;

use crate::data::expr::ExprError::UnexpectedArgs;
use crate::data::keyword::Keyword;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

#[derive(Debug, thiserror::Error)]
pub enum ExprError {
    #[error("unexpected args for {0}: {1:?}")]
    UnexpectedArgs(&'static str, Vec<DataValue>),
    #[error("unexpected return type: expected {0}, got {1:?}")]
    UnexpectedReturnType(String, DataValue),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Binding(Keyword, Option<usize>),
    Const(DataValue),
    Apply(&'static Op, Box<[Expr]>),
}

impl Expr {
    pub(crate) fn fill_binding_indices(&mut self, binding_map: &BTreeMap<Keyword, usize>) {
        match self {
            Expr::Binding(k, idx) => {
                let found_idx = *binding_map.get(k).unwrap();
                *idx = Some(found_idx)
            }
            Expr::Const(_) => {}
            Expr::Apply(_, args) => {
                for arg in args.iter_mut() {
                    arg.fill_binding_indices(binding_map);
                }
            }
        }
    }
    pub(crate) fn bindings(&self) -> BTreeSet<Keyword> {
        let mut ret = BTreeSet::new();
        self.collect_bindings(&mut ret);
        ret
    }
    pub(crate) fn collect_bindings(&self, coll: &mut BTreeSet<Keyword>) {
        match self {
            Expr::Binding(b, _) => {
                coll.insert(b.clone());
            }
            Expr::Const(_) => {}
            Expr::Apply(_, args) => {
                for arg in args.iter() {
                    arg.collect_bindings(coll)
                }
            }
        }
    }
    pub(crate) fn eval(&self, bindings: &Tuple) -> Result<DataValue> {
        match self {
            Expr::Binding(_, i) => Ok(bindings.0[i.unwrap()].clone()),
            Expr::Const(d) => Ok(d.clone()),
            Expr::Apply(op, args) => {
                let args: Box<[DataValue]> = args.iter().map(|v| v.eval(bindings)).try_collect()?;
                (op.inner)(&args)
            }
        }
    }
    pub(crate) fn eval_pred(&self, bindings: &Tuple) -> Result<bool> {
        match self.eval(bindings)? {
            DataValue::Bool(b) => Ok(b),
            v => Err(ExprError::UnexpectedReturnType("bool".to_string(), v).into()),
        }
    }
}

#[derive(Clone)]
pub struct Op {
    pub(crate) name: &'static str,
    pub(crate) min_arity: usize,
    pub(crate) vararg: bool,
    pub(crate) is_predicate: bool,
    pub(crate) inner: fn(&[DataValue]) -> Result<DataValue>,
}

impl Debug for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
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

define_op!(OP_GT, 2, false, true);
fn op_gt(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] > args[1]))
}

define_op!(OP_GE, 2, false, true);
fn op_ge(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] >= args[1]))
}

define_op!(OP_LT, 2, false, true);
fn op_lt(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] < args[1]))
}

define_op!(OP_LE, 2, false, true);
fn op_le(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] <= args[1]))
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
