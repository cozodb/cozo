use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Rem;

use anyhow::{anyhow, bail, Result};
use itertools::Itertools;

use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::{DataValue, Number};

#[derive(Debug, Clone)]
pub(crate) enum Expr {
    Binding(Symbol, Option<usize>),
    Param(Symbol),
    Const(DataValue),
    Apply(&'static Op, Box<[Expr]>),
}

impl Expr {
    pub(crate) fn build_equate(exprs: Vec<Expr>) -> Self {
        Expr::Apply(&OP_EQ, exprs.into())
    }
    pub(crate) fn negate(self) -> Self {
        Expr::Apply(&OP_NOT, Box::new([self]))
    }
    pub(crate) fn fill_binding_indices(&mut self, binding_map: &BTreeMap<Symbol, usize>) {
        match self {
            Expr::Binding(k, idx) => {
                let found_idx = *binding_map.get(k).unwrap();
                *idx = Some(found_idx)
            }
            Expr::Const(_) | Expr::Param(_) => {}
            Expr::Apply(_, args) => {
                for arg in args.iter_mut() {
                    arg.fill_binding_indices(binding_map);
                }
            }
        }
    }
    pub(crate) fn partial_eval(&mut self, param_pool: &BTreeMap<Symbol, DataValue>) -> Result<()> {
        let found_val = if let Expr::Param(s) = self {
            Some(
                param_pool
                    .get(s)
                    .ok_or_else(|| anyhow!("input parameter {} not found", s))?,
            )
        } else {
            None
        };
        if let Some(found_val) = found_val {
            *self = Expr::Const(found_val.clone());
            return Ok(());
        }
        if let Expr::Apply(_, args) = self {
            let mut all_evaluated = true;
            for arg in args.iter_mut() {
                arg.partial_eval(param_pool)?;
                all_evaluated = all_evaluated && matches!(arg, Expr::Const(_));
            }
            if all_evaluated {
                let result = self.eval(&Tuple(vec![]))?;
                mem::swap(self, &mut Expr::Const(result));
            }
            // nested not's can accumulate during conversion to normal form
            if let Expr::Apply(op1, arg1) = self {
                if op1.name == OP_NOT.name {
                    if let Some(Expr::Apply(op2, arg2)) = arg1.first() {
                        if op2.name == OP_NOT.name {
                            let mut new_self = arg2[0].clone();
                            mem::swap(self, &mut new_self);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn bindings(&self) -> BTreeSet<Symbol> {
        let mut ret = BTreeSet::new();
        self.collect_bindings(&mut ret);
        ret
    }
    pub(crate) fn collect_bindings(&self, coll: &mut BTreeSet<Symbol>) {
        match self {
            Expr::Binding(b, _) => {
                coll.insert(b.clone());
            }
            Expr::Const(_) | Expr::Param(_) => {}
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
            Expr::Param(s) => bail!("input var {} not bound", s),
        }
    }
    pub(crate) fn eval_pred(&self, bindings: &Tuple) -> Result<bool> {
        match self.eval(bindings)? {
            DataValue::Bool(b) => Ok(b),
            v => bail!("predicate must have boolean return type, got {:?}", v),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Op {
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
        const $name: Op = Op {
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
            DataValue::Number(Number::Int(i)) => i_accum += i,
            DataValue::Number(Number::Float(f)) => f_accum += f,
            v => bail!("unexpected arg {:?} for OP_ADD", v),
        }
    }
    if f_accum == 0.0f64 {
        Ok(DataValue::Number(Number::Int(i_accum)))
    } else {
        Ok(DataValue::Number(Number::Float(i_accum as f64 + f_accum)))
    }
}

define_op!(OP_MAX, 0, true, false);
fn op_max(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Number(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Number(a)), DataValue::Number(b)) => {
                Ok(Some(DataValue::Number(a.max(*b))))
            }
            v => bail!("unexpected arg {:?} for OP_MAX", v),
        })?;
    match res {
        None => Ok(DataValue::Number(Number::Float(f64::NEG_INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_MIN, 0, true, false);
fn op_min(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Number(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Number(a)), DataValue::Number(b)) => {
                Ok(Some(DataValue::Number(a.min(*b))))
            }
            v => bail!("unexpected arg {:?} for OP_MIN", v),
        })?;
    match res {
        None => Ok(DataValue::Number(Number::Float(f64::INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_SUB, 2, false, false);
fn op_sub(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Int(*a - *b))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float(*a - *b))
        }
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float((*a as f64) - b))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Float(a - (*b as f64)))
        }
        v => bail!("unexpected arg {:?} for OP_SUB", v),
    })
}

define_op!(OP_MUL, 0, true, false);
fn op_mul(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 1i64;
    let mut f_accum = 1.0f64;
    for arg in args {
        match arg {
            DataValue::Number(Number::Int(i)) => i_accum *= i,
            DataValue::Number(Number::Float(f)) => f_accum *= f,
            v => bail!("unexpected arg {:?} for OP_MUL", v),
        }
    }
    if f_accum == 1.0f64 {
        Ok(DataValue::Number(Number::Int(i_accum)))
    } else {
        Ok(DataValue::Number(Number::Float(i_accum as f64 + f_accum)))
    }
}

define_op!(OP_DIV, 2, false, false);
fn op_div(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Float((*a as f64) / (*b as f64)))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float(*a / *b))
        }
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float((*a as f64) / b))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Float(a / (*b as f64)))
        }
        v => bail!("unexpected arg {:?} for OP_DIV", v),
    })
}

define_op!(OP_MINUS, 1, false, false);
fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(-(*i))),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(-(*f))),
        v => bail!("unexpected arg {:?} for OP_MINUS", v),
    })
}

define_op!(OP_ABS, 1, false, false);
fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(i.abs())),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.abs())),
        v => bail!("unexpected arg {:?} for OP_ABS", v),
    })
}

define_op!(OP_SIGNUM, 1, false, false);
fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(i.signum())),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.signum())),
        v => bail!("unexpected arg {:?} for OP_SIGNUM", v),
    })
}

define_op!(OP_FLOOR, 1, false, false);
fn op_floor(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.floor())),
        v => bail!("unexpected arg {:?} for OP_FLOOR", v),
    })
}

define_op!(OP_CEIL, 1, false, false);
fn op_ceil(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.ceil())),
        v => bail!("unexpected arg {:?} for OP_CEIL", v),
    })
}

define_op!(OP_ROUND, 1, false, false);
fn op_round(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.round())),
        v => bail!("unexpected arg {:?} for OP_ROUND", v),
    })
}

define_op!(OP_EXP, 1, false, false);
fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_EXP", v),
    };
    Ok(DataValue::Number(Number::Float(a.exp())))
}

define_op!(OP_EXP2, 1, false, false);
fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_EXP2", v),
    };
    Ok(DataValue::Number(Number::Float(a.exp2())))
}

define_op!(OP_LN, 1, false, false);
fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LN", v),
    };
    Ok(DataValue::Number(Number::Float(a.ln())))
}

define_op!(OP_LOG2, 1, false, false);
fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LOG2", v),
    };
    Ok(DataValue::Number(Number::Float(a.log2())))
}

define_op!(OP_LOG10, 1, false, false);
fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LOG10", v),
    };
    Ok(DataValue::Number(Number::Float(a.log10())))
}

define_op!(OP_SIN, 1, false, false);
fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_SIN", v),
    };
    Ok(DataValue::Number(Number::Float(a.sin())))
}

define_op!(OP_COS, 1, false, false);
fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_COS", v),
    };
    Ok(DataValue::Number(Number::Float(a.cos())))
}

define_op!(OP_TAN, 1, false, false);
fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_TAN", v),
    };
    Ok(DataValue::Number(Number::Float(a.tan())))
}

define_op!(OP_ASIN, 1, false, false);
fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ASIN", v),
    };
    Ok(DataValue::Number(Number::Float(a.asin())))
}

define_op!(OP_ACOS, 1, false, false);
fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ACOS", v),
    };
    Ok(DataValue::Number(Number::Float(a.acos())))
}

define_op!(OP_ATAN, 1, false, false);
fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATAN", v),
    };
    Ok(DataValue::Number(Number::Float(a.atan())))
}

define_op!(OP_ATAN2, 2, false, false);
fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATAN2", v),
    };
    let b = match &args[1] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATAN2", v),
    };

    Ok(DataValue::Number(Number::Float(a.atan2(b))))
}

define_op!(OP_SINH, 1, false, false);
fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_SINH", v),
    };
    Ok(DataValue::Number(Number::Float(a.sinh())))
}

define_op!(OP_COSH, 1, false, false);
fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_COSH", v),
    };
    Ok(DataValue::Number(Number::Float(a.cosh())))
}

define_op!(OP_TANH, 1, false, false);
fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_TANH", v),
    };
    Ok(DataValue::Number(Number::Float(a.tanh())))
}

define_op!(OP_ASINH, 1, false, false);
fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ASINH", v),
    };
    Ok(DataValue::Number(Number::Float(a.asinh())))
}

define_op!(OP_ACOSH, 1, false, false);
fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ACOSH", v),
    };
    Ok(DataValue::Number(Number::Float(a.acosh())))
}

define_op!(OP_ATANH, 1, false, false);
fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATANH", v),
    };
    Ok(DataValue::Number(Number::Float(a.atanh())))
}

define_op!(OP_POW, 2, false, false);
fn op_pow(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_POW", v),
    };
    let b = match &args[1] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_POW", v),
    };
    Ok(DataValue::Number(Number::Float(a.powf(b))))
}

define_op!(OP_MOD, 2, false, false);
fn op_mod(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Int(a.rem(b)))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float(a.rem(*b)))
        }
        (DataValue::Number(Number::Int(a)), DataValue::Number(Number::Float(b))) => {
            DataValue::Number(Number::Float((*a as f64).rem(b)))
        }
        (DataValue::Number(Number::Float(a)), DataValue::Number(Number::Int(b))) => {
            DataValue::Number(Number::Float(a.rem(*b as f64)))
        }
        v => bail!("unexpected arg {:?} for OP_MOD", v),
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
            bail!("unexpected arg {:?} for OP_AND", arg);
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
            bail!("unexpected arg {:?} for OP_OR", arg);
        }
    }
    Ok(DataValue::Bool(false))
}

define_op!(OP_NOT, 1, false, true);
fn op_not(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bool(b) = &args[0] {
        Ok(DataValue::Bool(!*b))
    } else {
        bail!("unexpected arg {:?} for OP_NOT", args);
    }
}

define_op!(OP_STR_CAT, 0, true, false);
fn op_str_cat(args: &[DataValue]) -> Result<DataValue> {
    let mut ret: String = Default::default();
    for arg in args {
        if let DataValue::String(s) = arg {
            ret += s;
        } else {
            bail!("unexpected arg {:?} for OP_ADD", arg);
        }
    }
    Ok(DataValue::String(ret.into()))
}

define_op!(OP_STARTS_WITH, 2, false, true);
fn op_starts_with(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::String(s) => s,
        v => bail!("unexpected arg {:?} for OP_STARTS_WITH", v),
    };
    let b = match &args[0] {
        DataValue::String(s) => s,
        v => bail!("unexpected arg {:?} for OP_STARTS_WITH", v),
    };
    Ok(DataValue::Bool(a.starts_with(b as &str)))
}

define_op!(OP_ENDS_WITH, 2, false, true);
fn op_ends_with(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::String(s) => s,
        v => bail!("unexpected arg {:?} for OP_ENDS_WITH", v),
    };
    let b = match &args[0] {
        DataValue::String(s) => s,
        v => bail!("unexpected arg {:?} for OP_ENDS_WITH", v),
    };
    Ok(DataValue::Bool(a.ends_with(b as &str)))
}

define_op!(OP_IS_NULL, 1, false, true);
fn op_is_null(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Null)))
}

define_op!(OP_IS_INT, 1, false, true);
fn op_is_int(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Int(_))
    )))
}

define_op!(OP_IS_FLOAT, 1, false, true);
fn op_is_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Float(_))
    )))
}

define_op!(OP_IS_NUM, 1, false, true);
fn op_is_num(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Int(_)) | DataValue::Number(Number::Float(_))
    )))
}

define_op!(OP_IS_STRING, 1, false, true);
fn op_is_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::String(_))))
}

define_op!(OP_IS_LIST, 1, false, true);
fn op_is_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::List(_))))
}

define_op!(OP_IS_BYTES, 1, false, true);
fn op_is_bytes(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Bytes(_))))
}

define_op!(OP_IS_UUID, 1, false, true);
fn op_is_uuid(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Uuid(_))))
}

define_op!(OP_IS_TIMESTAMP, 1, false, true);
fn op_is_timestamp(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Timestamp(_))))
}

pub(crate) fn get_op(name: &str) -> Option<&'static Op> {
    Some(match name {
        "Add" => &OP_ADD,
        "Sub" => &OP_SUB,
        "Mul" => &OP_MUL,
        "Div" => &OP_DIV,
        "Minus" => &OP_MINUS,
        "Abs" => &OP_ABS,
        "Signum" => &OP_SIGNUM,
        "Floor" => &OP_FLOOR,
        "Ceil" => &OP_CEIL,
        "Round" => &OP_ROUND,
        "Mod" => &OP_MOD,
        "Max" => &OP_MAX,
        "Min" => &OP_MIN,
        "Pow" => &OP_POW,
        "Exp" => &OP_EXP,
        "Exp2" => &OP_EXP2,
        "Ln" => &OP_LN,
        "Log2" => &OP_LOG2,
        "Log10" => &OP_LOG10,
        "Sin" => &OP_SIN,
        "Cos" => &OP_COS,
        "Tan" => &OP_TAN,
        "Asin" => &OP_ASIN,
        "Acos" => &OP_ACOS,
        "Atan" => &OP_ATAN,
        "Atan2" => &OP_ATAN2,
        "Sinh" => &OP_SINH,
        "Cosh" => &OP_COSH,
        "Tanh" => &OP_TANH,
        "Asinh" => &OP_ASINH,
        "Acosh" => &OP_ACOSH,
        "Atanh" => &OP_ATANH,
        "Eq" => &OP_EQ,
        "Neq" => &OP_NEQ,
        "Gt" => &OP_GT,
        "Ge" => &OP_GE,
        "Lt" => &OP_LT,
        "Le" => &OP_LE,
        "Or" => &OP_OR,
        "And" => &OP_AND,
        "Not" => &OP_NOT,
        "StrCat" => &OP_STR_CAT,
        "StartsWith" => &OP_STARTS_WITH,
        "EndsWith" => &OP_ENDS_WITH,
        "IsNull" => &OP_IS_NULL,
        "IsInt" => &OP_IS_INT,
        "IsFloat" => &OP_IS_FLOAT,
        "IsNum" => &OP_IS_NUM,
        "IsString" => &OP_IS_STRING,
        "IsList" => &OP_IS_LIST,
        "IsBytes" => &OP_IS_BYTES,
        "IsUuid" => &OP_IS_UUID,
        "IsTimestamp" => &OP_IS_TIMESTAMP,
        _ => return None,
    })
}
