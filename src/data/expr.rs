use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Rem;

use anyhow::{bail, Result};
use itertools::Itertools;
use ordered_float::{Float, OrderedFloat};

use crate::data::keyword::Keyword;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

#[derive(Debug, Clone)]
pub enum Expr {
    Binding(Keyword, Option<usize>),
    Const(DataValue),
    Apply(&'static Op, Box<[Expr]>),
}

impl Expr {
    pub(crate) fn negate(self) -> Self {
        Expr::Apply(&OP_NOT, Box::new([self]))
    }
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
            v => bail!("predicate must have boolean return type, got {:?}", v),
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
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Float(b)) => OrderedFloat(*a as f64) > *b,
        (DataValue::Float(a), DataValue::Int(b)) => *a > OrderedFloat(*b as f64),
        (_, _) => args[0] > args[1],
    }))
}

define_op!(OP_GE, 2, false, true);
fn op_ge(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Float(b)) => OrderedFloat(*a as f64) >= *b,
        (DataValue::Float(a), DataValue::Int(b)) => *a >= OrderedFloat(*b as f64),
        (_, _) => args[0] >= args[1],
    }))
}

define_op!(OP_LT, 2, false, true);
fn op_lt(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Float(b)) => OrderedFloat(*a as f64) < *b,
        (DataValue::Float(a), DataValue::Int(b)) => *a < OrderedFloat(*b as f64),
        (_, _) => args[0] < args[1],
    }))
}

define_op!(OP_LE, 2, false, true);
fn op_le(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Float(b)) => OrderedFloat(*a as f64) <= *b,
        (DataValue::Float(a), DataValue::Int(b)) => *a <= OrderedFloat(*b as f64),
        (_, _) => args[0] <= args[1],
    }))
}

define_op!(OP_ADD, 0, true, false);
fn op_add(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 0i64;
    let mut f_accum = 0.0f64;
    for arg in args {
        match arg {
            DataValue::Int(i) => i_accum += i,
            DataValue::Float(f) => f_accum += f.0,
            v => bail!("unexpected arg {:?} for OP_ADD", v),
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
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Int(_)) => Ok(Some(d.clone())),
            (None, d @ DataValue::Float(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Int(a)), DataValue::Int(b)) => Ok(Some(DataValue::Int(a.max(*b)))),
            (Some(DataValue::Int(a)), DataValue::Float(b)) => {
                Ok(Some(DataValue::Float(b.0.max(a as f64).into())))
            }
            (Some(DataValue::Float(a)), DataValue::Int(b)) => {
                Ok(Some(DataValue::Float(a.0.max(*b as f64).into())))
            }
            (Some(DataValue::Float(a)), DataValue::Float(b)) => {
                Ok(Some(DataValue::Float(a.0.max(b.0).into())))
            }
            v => bail!("unexpected arg {:?} for OP_MAX", v),
        })?;
    match res {
        None => Ok(DataValue::Float(f64::neg_infinity().into())),
        Some(v) => Ok(v),
    }
}

define_op!(OP_MIN, 0, true, false);
fn op_min(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Int(_)) => Ok(Some(d.clone())),
            (None, d @ DataValue::Float(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Int(a)), DataValue::Int(b)) => Ok(Some(DataValue::Int(a.min(*b)))),
            (Some(DataValue::Int(a)), DataValue::Float(b)) => {
                Ok(Some(DataValue::Float(b.0.min(a as f64).into())))
            }
            (Some(DataValue::Float(a)), DataValue::Int(b)) => {
                Ok(Some(DataValue::Float(a.0.min(*b as f64).into())))
            }
            (Some(DataValue::Float(a)), DataValue::Float(b)) => {
                Ok(Some(DataValue::Float(a.0.min(b.0).into())))
            }
            v => bail!("unexpected arg {:?} for OP_MIN", v),
        })?;
    match res {
        None => Ok(DataValue::Float(f64::infinity().into())),
        Some(v) => Ok(v),
    }
}

define_op!(OP_SUB, 2, false, false);
fn op_sub(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Int(b)) => DataValue::Int(*a - *b),
        (DataValue::Float(a), DataValue::Float(b)) => DataValue::Float(*a - *b),
        (DataValue::Int(a), DataValue::Float(b)) => DataValue::Float(((*a as f64) - b.0).into()),
        (DataValue::Float(a), DataValue::Int(b)) => DataValue::Float((a.0 - (*b as f64)).into()),
        v => bail!("unexpected arg {:?} for OP_SUB", v),
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
            v => bail!("unexpected arg {:?} for OP_MUL", v),
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
        v => bail!("unexpected arg {:?} for OP_DIV", v),
    })
}

define_op!(OP_MINUS, 1, false, false);
fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(-(*i)),
        DataValue::Float(f) => DataValue::Float(-(*f)),
        v => bail!("unexpected arg {:?} for OP_MINUS", v),
    })
}

define_op!(OP_ABS, 1, false, false);
fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(i.abs()),
        DataValue::Float(f) => DataValue::Float(f.abs()),
        v => bail!("unexpected arg {:?} for OP_ABS", v),
    })
}

define_op!(OP_SIGNUM, 1, false, false);
fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(i.signum()),
        DataValue::Float(f) => DataValue::Float(f.signum()),
        v => bail!("unexpected arg {:?} for OP_SIGNUM", v),
    })
}

define_op!(OP_FLOOR, 1, false, false);
fn op_floor(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(*i),
        DataValue::Float(f) => DataValue::Float(f.floor()),
        v => bail!("unexpected arg {:?} for OP_FLOOR", v),
    })
}

define_op!(OP_CEIL, 1, false, false);
fn op_ceil(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(*i),
        DataValue::Float(f) => DataValue::Float(f.ceil()),
        v => bail!("unexpected arg {:?} for OP_CEIL", v),
    })
}

define_op!(OP_ROUND, 1, false, false);
fn op_round(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Int(i) => DataValue::Int(*i),
        DataValue::Float(f) => DataValue::Float(f.round()),
        v => bail!("unexpected arg {:?} for OP_ROUND", v),
    })
}

define_op!(OP_EXP, 1, false, false);
fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_EXP", v),
    };
    Ok(DataValue::Float(a.exp().into()))
}

define_op!(OP_EXP2, 1, false, false);
fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_EXP2", v),
    };
    Ok(DataValue::Float(a.exp2().into()))
}

define_op!(OP_LN, 1, false, false);
fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_LN", v),
    };
    Ok(DataValue::Float(a.ln().into()))
}

define_op!(OP_LOG2, 1, false, false);
fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_LOG2", v),
    };
    Ok(DataValue::Float(a.log2().into()))
}

define_op!(OP_LOG10, 1, false, false);
fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_LOG10", v),
    };
    Ok(DataValue::Float(a.log10().into()))
}

define_op!(OP_SIN, 1, false, false);
fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_SIN", v),
    };
    Ok(DataValue::Float(a.sin().into()))
}

define_op!(OP_COS, 1, false, false);
fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_COS", v),
    };
    Ok(DataValue::Float(a.cos().into()))
}

define_op!(OP_TAN, 1, false, false);
fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_TAN", v),
    };
    Ok(DataValue::Float(a.tan().into()))
}

define_op!(OP_ASIN, 1, false, false);
fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ASIN", v),
    };
    Ok(DataValue::Float(a.asin().into()))
}

define_op!(OP_ACOS, 1, false, false);
fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ACOS", v),
    };
    Ok(DataValue::Float(a.acos().into()))
}

define_op!(OP_ATAN, 1, false, false);
fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ATAN", v),
    };
    Ok(DataValue::Float(a.atan().into()))
}

define_op!(OP_ATAN2, 2, false, false);
fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ATAN2", v),
    };
    let b = match &args[1] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ATAN2", v),
    };

    Ok(DataValue::Float(a.atan2(b).into()))
}

define_op!(OP_SINH, 1, false, false);
fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_SINH", v),
    };
    Ok(DataValue::Float(a.sinh().into()))
}

define_op!(OP_COSH, 1, false, false);
fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_COSH", v),
    };
    Ok(DataValue::Float(a.cosh().into()))
}

define_op!(OP_TANH, 1, false, false);
fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_TANH", v),
    };
    Ok(DataValue::Float(a.tanh().into()))
}

define_op!(OP_ASINH, 1, false, false);
fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ASINH", v),
    };
    Ok(DataValue::Float(a.asinh().into()))
}

define_op!(OP_ACOSH, 1, false, false);
fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ACOSH", v),
    };
    Ok(DataValue::Float(a.acosh().into()))
}

define_op!(OP_ATANH, 1, false, false);
fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_ATANH", v),
    };
    Ok(DataValue::Float(a.atanh().into()))
}

define_op!(OP_POW, 2, false, false);
fn op_pow(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_POW", v),
    };
    let b = match &args[1] {
        DataValue::Int(i) => *i as f64,
        DataValue::Float(f) => f.0,
        v => bail!("unexpected arg {:?} for OP_POW", v),
    };
    Ok(DataValue::Float(a.powf(b).into()))
}

define_op!(OP_MOD, 2, false, false);
fn op_mod(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Int(a), DataValue::Int(b)) => DataValue::Int(a.rem(b)),
        (DataValue::Float(a), DataValue::Float(b)) => DataValue::Float(a.rem(*b)),
        (DataValue::Int(a), DataValue::Float(b)) => DataValue::Float(((*a as f64).rem(b.0)).into()),
        (DataValue::Float(a), DataValue::Int(b)) => DataValue::Float((a.0.rem(*b as f64)).into()),
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
    Ok(DataValue::Bool(matches!(args[0], DataValue::Int(_))))
}

define_op!(OP_IS_FLOAT, 1, false, true);
fn op_is_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Float(_))))
}

define_op!(OP_IS_NUM, 1, false, true);
fn op_is_num(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Int(_) | DataValue::Float(_)
    )))
}

define_op!(OP_IS_ID, 1, false, true);
fn op_is_id(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::EnId(_))))
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
        "IsId" => &OP_IS_ID,
        "IsString" => &OP_IS_STRING,
        "IsList" => &OP_IS_LIST,
        "IsBytes" => &OP_IS_BYTES,
        "IsUuid" => &OP_IS_UUID,
        "IsTimestamp" => &OP_IS_TIMESTAMP,
        _ => return None,
    })
}
