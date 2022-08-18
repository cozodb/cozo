use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Rem;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use num_traits::FloatConst;
use smartstring::SmartString;

use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::{DataValue, Number, LARGEST_UTF_CHAR};

#[derive(Debug, Clone)]
pub(crate) enum Expr {
    Binding(Symbol, Option<usize>),
    Param(Symbol),
    Const(DataValue),
    Apply(&'static Op, Box<[Expr]>),
}

impl Expr {
    pub(crate) fn get_binding(&self) -> Option<&Symbol> {
        if let Expr::Binding(symb, _) = self {
            Some(symb)
        } else {
            None
        }
    }
    pub(crate) fn get_const(&self) -> Option<&DataValue> {
        if let Expr::Const(val) = self {
            Some(val)
        } else {
            None
        }
    }
    pub(crate) fn build_equate(exprs: Vec<Expr>) -> Self {
        Expr::Apply(&OP_EQ, exprs.into())
    }
    pub(crate) fn build_is_in(exprs: Vec<Expr>) -> Self {
        Expr::Apply(&OP_IS_IN, exprs.into())
    }
    pub(crate) fn negate(self) -> Self {
        Expr::Apply(&OP_NOT, Box::new([self]))
    }
    pub(crate) fn to_conjunction(&self) -> Vec<Self> {
        match self {
            Expr::Apply(op, exprs) if **op == OP_AND => exprs.to_vec(),
            v => vec![v.clone()],
        }
    }
    pub(crate) fn fill_binding_indices(
        &mut self,
        binding_map: &BTreeMap<Symbol, usize>,
    ) -> Result<()> {
        match self {
            Expr::Binding(k, idx) => {
                let found_idx = *binding_map.get(k).ok_or_else(|| {
                    anyhow!("cannot find binding {}, this indicates a system error", k)
                })?;
                *idx = Some(found_idx)
            }
            Expr::Const(_) | Expr::Param(_) => {}
            Expr::Apply(_, args) => {
                for arg in args.iter_mut() {
                    arg.fill_binding_indices(binding_map)?;
                }
            }
        }
        Ok(())
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
    pub(crate) fn eval_bound(&self, bindings: &Tuple) -> Result<Self> {
        Ok(match self {
            Expr::Binding(b, i) => match bindings.0.get(i.unwrap()) {
                None => Expr::Binding(b.clone(), i.clone()),
                Some(v) => Expr::Const(v.clone()),
            },
            e @ Expr::Const(_) => e.clone(),
            Expr::Apply(op, args) => {
                let args: Box<[Expr]> =
                    args.iter().map(|v| v.eval_bound(bindings)).try_collect()?;
                let const_args = args
                    .iter()
                    .map(|v| v.get_const().cloned())
                    .collect::<Option<Box<[DataValue]>>>();
                match const_args {
                    None => Expr::Apply(*op, args),
                    Some(args) => {
                        let res = (op.inner)(&args)?;
                        Expr::Const(res)
                    }
                }
            }
            Expr::Param(s) => bail!("input var {} not bound", s),
        })
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
    pub(crate) fn extract_bound(&self, target: &Symbol) -> Result<ValueRange> {
        Ok(match self {
            Expr::Binding(_, _) | Expr::Param(_) | Expr::Const(_) => ValueRange::default(),
            Expr::Apply(op, args) => match op.name {
                n if n == OP_GE.name || n == OP_GT.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                return Ok(ValueRange::lower_bound(val.clone()));
                            }
                        }
                    }
                    if let Some(symb) = args[1].get_binding() {
                        if let Some(val) = args[0].get_const() {
                            if target == symb {
                                return Ok(ValueRange::upper_bound(val.clone()));
                            }
                        }
                    }
                    ValueRange::default()
                }
                n if n == OP_LE.name || n == OP_LT.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                return Ok(ValueRange::upper_bound(val.clone()));
                            }
                        }
                    }
                    if let Some(symb) = args[1].get_binding() {
                        if let Some(val) = args[0].get_const() {
                            if target == symb {
                                return Ok(ValueRange::lower_bound(val.clone()));
                            }
                        }
                    }
                    ValueRange::default()
                }
                n if n == OP_STARTS_WITH.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                let s = val.get_string().ok_or_else(|| {
                                    anyhow!("unexpected arg {:?} for OP_STARTS_WITH", val)
                                })?;
                                let lower = DataValue::String(SmartString::from(s));
                                let mut upper = SmartString::from(s);
                                upper.push(LARGEST_UTF_CHAR);
                                let upper = DataValue::String(upper);
                                return Ok(ValueRange::new(lower, upper));
                            }
                        }
                    }
                    ValueRange::default()
                }
                _ => ValueRange::default(),
            },
        })
    }
}

pub(crate) fn compute_bounds(
    filters: &[Expr],
    symbols: &[Symbol],
) -> Result<(Vec<DataValue>, Vec<DataValue>)> {
    let mut lowers = vec![];
    let mut uppers = vec![];
    for current in symbols {
        let mut cur_bound = ValueRange::default();
        for filter in filters {
            let nxt = filter.extract_bound(current)?;
            cur_bound = cur_bound.merge(nxt);
        }
        lowers.push(cur_bound.lower);
        uppers.push(cur_bound.upper);
    }

    Ok((lowers, uppers))
}

pub(crate) fn compute_single_bound(
    filters: &[Expr],
    symbol: &Symbol,
) -> Result<Option<(DataValue, DataValue)>> {
    let mut cur_bound = ValueRange::default();
    for filter in filters {
        let nxt = filter.extract_bound(symbol)?;
        cur_bound = cur_bound.merge(nxt);
    }
    Ok(if cur_bound == ValueRange::default() {
        None
    } else {
        Some((cur_bound.lower, cur_bound.upper))
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueRange {
    pub(crate) lower: DataValue,
    pub(crate) upper: DataValue,
}

impl ValueRange {
    fn merge(self, other: Self) -> Self {
        let lower = max(self.lower, other.lower);
        let upper = min(self.upper, other.upper);
        if lower > upper {
            Self::null()
        } else {
            Self { lower, upper }
        }
    }
    fn null() -> Self {
        Self {
            lower: DataValue::Bottom,
            upper: DataValue::Bottom,
        }
    }
    fn new(lower: DataValue, upper: DataValue) -> Self {
        Self { lower, upper }
    }
    fn lower_bound(val: DataValue) -> Self {
        Self {
            lower: val,
            upper: DataValue::Bottom,
        }
    }
    fn upper_bound(val: DataValue) -> Self {
        Self {
            lower: DataValue::Null,
            upper: val,
        }
    }
}

impl Default for ValueRange {
    fn default() -> Self {
        Self {
            lower: DataValue::Null,
            upper: DataValue::Bottom,
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

impl PartialEq for Op {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Op {}

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

define_op!(OP_LIST, 0, true, false);
pub(crate) fn op_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(args.to_vec()))
}

define_op!(OP_EQ, 0, true, true);
pub(crate) fn op_eq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args.iter().all_equal()))
}

define_op!(OP_IS_IN, 2, false, true);
pub(crate) fn op_is_in(args: &[DataValue]) -> Result<DataValue> {
    let left = &args[0];
    let right = args[1]
        .get_list()
        .ok_or_else(|| anyhow!("right hand side of 'is_in' is not a list"))?;
    Ok(DataValue::Bool(right.contains(left)))
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
    let b = match &args[1] {
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

define_op!(OP_APPEND, 2, false, false);
fn op_append(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => {
            let mut l = l.clone();
            l.push(args[1].clone());
            Ok(DataValue::List(l))
        }
        v => bail!("cannot append to {:?}", v),
    }
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

define_op!(OP_LENGTH, 1, false, false);
fn op_length(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Set(s) => s.len() as i64,
        DataValue::List(l) => l.len() as i64,
        DataValue::String(s) => s.chars().count() as i64,
        DataValue::Bytes(b) => b.len() as i64,
        v => bail!("cannot apply 'length' to {:?}", v),
    }))
}

define_op!(OP_SORT, 1, false, false);
fn op_sort(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot apply 'sort' to {:?}", args))?
        .to_vec();
    arg.sort();
    Ok(DataValue::List(arg))
}

define_op!(OP_PI, 0, false, false);
fn op_pi(_args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(f64::PI()))
}

define_op!(OP_E, 0, false, false);
fn op_e(_args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(f64::E()))
}

define_op!(OP_HAVERSINE, 4, false, false);
fn op_haversine(args: &[DataValue]) -> Result<DataValue> {
    let gen_err = || anyhow!("cannot computer haversine distance for {:?}", args);
    let x1 = args[0].get_float().ok_or_else(gen_err)?;
    let y1 = args[1].get_float().ok_or_else(gen_err)?;
    let x2 = args[2].get_float().ok_or_else(gen_err)?;
    let y2 = args[3].get_float().ok_or_else(gen_err)?;
    let ret = 2.
        * f64::acos(f64::sqrt(
            f64::sin((x1 - y1) / 2.).powi(2)
                + f64::cos(x1) * f64::cos(y1) * f64::sin((x2 - y2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_HAVERSINE_DEG, 4, false, false);
fn op_haversine_deg(args: &[DataValue]) -> Result<DataValue> {
    let gen_err = || anyhow!("cannot computer haversine distance for {:?}", args);
    let x1 = args[0].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let y1 = args[1].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let x2 = args[2].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let y2 = args[3].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let ret = 2.
        * f64::acos(f64::sqrt(
            f64::sin((x1 - y1) / 2.).powi(2)
                + f64::cos(x1) * f64::cos(y1) * f64::sin((x2 - y2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret * 180. / f64::PI()))
}

define_op!(OP_DEG_TO_RAD, 1, false, false);
fn op_deg_to_rad(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| anyhow!("cannot convert to radian: {:?}", args))?;
    Ok(DataValue::from(x * f64::PI() / 180.))
}

define_op!(OP_RAD_TO_DEG, 1, false, false);
fn op_rad_to_deg(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| anyhow!("cannot convert to degrees: {:?}", args))?;
    Ok(DataValue::from(x * 180. / f64::PI()))
}

define_op!(OP_FIRST, 1, false, false);
fn op_first(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot compute 'first' of {:?}", args))?
        .first()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_LAST, 1, false, false);
fn op_last(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot compute 'last' of {:?}", args))?
        .last()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_CHUNKS, 2, false, false);
fn op_chunks(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0].get_list().ok_or_else(|| {
        anyhow!(
            "first argument of 'chunks' must be a list, got {:?}",
            args[0]
        )
    })?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument of 'chunks' must be an integer, got {:?}",
            args[1]
        )
    })?;
    ensure!(
        n > 0,
        "second argument to 'chunks' must be positive, got {}",
        n
    );
    let res = arg
        .chunks(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_CHUNKS_EXACT, 2, false, false);
fn op_chunks_exact(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0].get_list().ok_or_else(|| {
        anyhow!(
            "first argument of 'chunks_exact' must be a list, got {:?}",
            args[0]
        )
    })?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument of 'chunks_exact' must be an integer, got {:?}",
            args[1]
        )
    })?;
    ensure!(
        n > 0,
        "second argument to 'chunks_exact' must be positive, got {}",
        n
    );
    let res = arg
        .chunks_exact(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_WINDOWS, 2, false, false);
fn op_windows(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0].get_list().ok_or_else(|| {
        anyhow!(
            "first argument of 'windows' must be a list, got {:?}",
            args[0]
        )
    })?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument of 'windows' must be an integer, got {:?}",
            args[1]
        )
    })?;
    ensure!(
        n > 0,
        "second argument to 'windows' must be positive, got {}",
        n
    );
    let res = arg
        .windows(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_NTH, 2, false, false);
fn op_nth(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| anyhow!("first argument to 'nth' mut be a list, got args {:?}", args))?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'nth' mut be an integer, got args {:?}",
            args
        )
    })?;
    Ok(if n >= 0 {
        let n = n as usize;
        if n >= l.len() {
            DataValue::Null
        } else {
            l[n].clone()
        }
    } else {
        let len = l.len() as i64;
        let idx = len + n;
        if idx < 0 {
            DataValue::Null
        } else {
            l[idx as usize].clone()
        }
    })
}

pub(crate) fn get_op(name: &str) -> Option<&'static Op> {
    Some(match name {
        "list" => &OP_LIST,
        "add" => &OP_ADD,
        "sub" => &OP_SUB,
        "mul" => &OP_MUL,
        "div" => &OP_DIV,
        "minus" => &OP_MINUS,
        "abs" => &OP_ABS,
        "signum" => &OP_SIGNUM,
        "floor" => &OP_FLOOR,
        "ceil" => &OP_CEIL,
        "round" => &OP_ROUND,
        "mod" => &OP_MOD,
        "max" => &OP_MAX,
        "min" => &OP_MIN,
        "pow" => &OP_POW,
        "exp" => &OP_EXP,
        "exp2" => &OP_EXP2,
        "ln" => &OP_LN,
        "log2" => &OP_LOG2,
        "log10" => &OP_LOG10,
        "sin" => &OP_SIN,
        "cos" => &OP_COS,
        "tan" => &OP_TAN,
        "asin" => &OP_ASIN,
        "acos" => &OP_ACOS,
        "atan" => &OP_ATAN,
        "atan2" => &OP_ATAN2,
        "sinh" => &OP_SINH,
        "cosh" => &OP_COSH,
        "tanh" => &OP_TANH,
        "asinh" => &OP_ASINH,
        "acosh" => &OP_ACOSH,
        "atanh" => &OP_ATANH,
        "eq" => &OP_EQ,
        "neq" => &OP_NEQ,
        "gt" => &OP_GT,
        "ge" => &OP_GE,
        "lt" => &OP_LT,
        "le" => &OP_LE,
        "or" => &OP_OR,
        "and" => &OP_AND,
        "not" => &OP_NOT,
        "str_cat" => &OP_STR_CAT,
        "starts_with" => &OP_STARTS_WITH,
        "ends_with" => &OP_ENDS_WITH,
        "is_null" => &OP_IS_NULL,
        "is_int" => &OP_IS_INT,
        "is_float" => &OP_IS_FLOAT,
        "is_num" => &OP_IS_NUM,
        "is_string" => &OP_IS_STRING,
        "is_list" => &OP_IS_LIST,
        "is_bytes" => &OP_IS_BYTES,
        "is_uuid" => &OP_IS_UUID,
        "is_timestamp" => &OP_IS_TIMESTAMP,
        "is_in" => &OP_IS_IN,
        "length" => &OP_LENGTH,
        "sort" => &OP_SORT,
        "append" => &OP_APPEND,
        "pi" => &OP_PI,
        "e" => &OP_E,
        "haversine" => &OP_HAVERSINE,
        "haversine_deg" => &OP_HAVERSINE_DEG,
        "deg_to_rad" => &OP_DEG_TO_RAD,
        "rad_to_deg" => &OP_RAD_TO_DEG,
        "nth" => &OP_NTH,
        "first" => &OP_FIRST,
        "last" => &OP_LAST,
        "chunks" => &OP_CHUNKS,
        "chunks_exact" => &OP_CHUNKS_EXACT,
        "windows" => &OP_WINDOWS,
        _ => return None,
    })
}
