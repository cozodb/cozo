use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::mem;

use anyhow::Result;
use itertools::Itertools;
use ordered_float::Float;
use smartstring::{LazyCompact, SmartString};

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
    pub(crate) fn partial_eval(&mut self) -> Result<()> {
        if let Expr::Apply(_, args) = self {
            let mut all_evaluated = true;
            for arg in args.iter_mut() {
                arg.partial_eval()?;
                all_evaluated = all_evaluated && matches!(arg, Expr::Const(_));
            }
            if all_evaluated {
                let result = self.eval(&Tuple(vec![]))?;
                mem::swap(self, &mut Expr::Const(result));
            }
        }
        Ok(())
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

define_op!(OP_MAX, 0, true, false);
fn op_max(args: &[DataValue]) -> Result<DataValue> {
    let res = args.iter().try_fold(None, |accum, nxt| {
        match (accum, nxt) {
            (None, d@DataValue::Int(_)) => Ok(Some(d.clone())),
            (None, d@DataValue::Float(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Int(a)), DataValue::Int(b)) => Ok(Some(DataValue::Int(a.max(*b)))),
            (Some(DataValue::Int(a)), DataValue::Float(b)) => Ok(Some(DataValue::Float(b.0.max(a as f64).into()))),
            (Some(DataValue::Float(a)), DataValue::Int(b)) => Ok(Some(DataValue::Float(a.0.max(*b as f64).into()))),
            (Some(DataValue::Float(a)), DataValue::Float(b)) => Ok(Some(DataValue::Float(a.0.max(b.0).into()))),
            _ => Err(UnexpectedArgs("max", args.to_vec())),
        }
    })?;
    match res {
        None => Ok(DataValue::Float(f64::neg_infinity().into())),
        Some(v) => Ok(v)
    }
}


define_op!(OP_MIN, 0, true, false);
fn op_min(args: &[DataValue]) -> Result<DataValue> {
    let res = args.iter().try_fold(None, |accum, nxt| {
        match (accum, nxt) {
            (None, d@DataValue::Int(_)) => Ok(Some(d.clone())),
            (None, d@DataValue::Float(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Int(a)), DataValue::Int(b)) => Ok(Some(DataValue::Int(a.min(*b)))),
            (Some(DataValue::Int(a)), DataValue::Float(b)) => Ok(Some(DataValue::Float(b.0.min(a as f64).into()))),
            (Some(DataValue::Float(a)), DataValue::Int(b)) => Ok(Some(DataValue::Float(a.0.min(*b as f64).into()))),
            (Some(DataValue::Float(a)), DataValue::Float(b)) => Ok(Some(DataValue::Float(a.0.min(b.0).into()))),
            _ => Err(UnexpectedArgs("min", args.to_vec())),
        }
    })?;
    match res {
        None => Ok(DataValue::Float(f64::infinity().into())),
        Some(v) => Ok(v)
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

define_op!(OP_MINUS, 1, false, false);
fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(-(*i)),
        DataValue::Float(f) => DataValue::Float(-(*f)),
        _ => return Err(UnexpectedArgs("minus", args.to_vec()).into()),
    })
}

define_op!(OP_ABS, 1, false, false);
fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(i.abs()),
        DataValue::Float(f) => DataValue::Float(f.abs()),
        _ => return Err(UnexpectedArgs("abs", args.to_vec()).into()),
    })
}

define_op!(OP_SIGNUM, 1, false, false);
fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(i.signum()),
        DataValue::Float(f) => DataValue::Float(f.signum()),
        _ => return Err(UnexpectedArgs("signum", args.to_vec()).into()),
    })
}

define_op!(OP_EXP, 1, false, false);
fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("exp", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.exp().into()))
}

define_op!(OP_EXP2, 1, false, false);
fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("exp2", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.exp2().into()))
}

define_op!(OP_LN, 1, false, false);
fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("ln", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.ln().into()))
}

define_op!(OP_LOG2, 1, false, false);
fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("log2", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.log2().into()))
}

define_op!(OP_LOG10, 1, false, false);
fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("log10", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.log10().into()))
}

define_op!(OP_SIN, 1, false, false);
fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("sin", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.sin().into()))
}

define_op!(OP_COS, 1, false, false);
fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("cos", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.cos().into()))
}

define_op!(OP_TAN, 1, false, false);
fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("tan", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.tan().into()))
}

define_op!(OP_ASIN, 1, false, false);
fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("asin", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.asin().into()))
}

define_op!(OP_ACOS, 1, false, false);
fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("acos", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.acos().into()))
}

define_op!(OP_ATAN, 1, false, false);
fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("atan", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.atan().into()))
}

define_op!(OP_ATAN2, 2, false, false);
fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("atan2", args.to_vec()).into()),
    };
    let b = match &args[1] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("atan2", args.to_vec()).into()),
    };

    Ok(DataValue::Float(a.atan2(b).into()))
}

define_op!(OP_SINH, 1, false, false);
fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("sinh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.sinh().into()))
}

define_op!(OP_COSH, 1, false, false);
fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("cosh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.cosh().into()))
}

define_op!(OP_TANH, 1, false, false);
fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("tanh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.tanh().into()))
}

define_op!(OP_ASINH, 1, false, false);
fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("asinh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.asinh().into()))
}

define_op!(OP_ACOSH, 1, false, false);
fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("acosh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.acosh().into()))
}

define_op!(OP_ATANH, 1, false, false);
fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("atanh", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.atanh().into()))
}

define_op!(OP_POW, 2, false, false);
fn op_pow(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("pow", args.to_vec()).into()),
    };
    let b = match &args[1] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        _ => return Err(UnexpectedArgs("pow", args.to_vec()).into()),
    };
    Ok(DataValue::Float(a.powf(b).into()))
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

define_op!(OP_STR_CAT, 0, true, false);
fn op_str_cat(args: &[DataValue]) -> Result<DataValue> {
    let mut ret: String = Default::default();
    for arg in args {
        if let DataValue::String(s) = arg {
            ret += s;
        } else {
            return Err(UnexpectedArgs("strcat", args.to_vec()).into());
        }
    }
    Ok(DataValue::String(ret.into()))
}