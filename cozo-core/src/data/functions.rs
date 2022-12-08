/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeSet;
use std::ops::{Div, Rem};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
#[cfg(feature = "wasm")]
use js_sys::Date;
use miette::{bail, ensure, miette, Result};
use num_traits::FloatConst;
use rand::prelude::*;
use smartstring::SmartString;
use unicode_normalization::UnicodeNormalization;
use uuid::v1::Timestamp;

use crate::data::expr::Op;
use crate::data::json::JsonValue;
use crate::data::value::{DataValue, Num, RegexWrapper, UuidWrapper};

macro_rules! define_op {
    ($name:ident, $min_arity:expr, $vararg:expr) => {
        pub(crate) const $name: Op = Op {
            name: stringify!($name),
            min_arity: $min_arity,
            vararg: $vararg,
            inner: ::casey::lower!($name),
        };
    };
}

fn ensure_same_value_type(a: &DataValue, b: &DataValue) -> Result<()> {
    use DataValue::*;
    if !matches!(
        (a, b),
        (Null, Null)
            | (Bool(_), Bool(_))
            | (Num(_), Num(_))
            | (Str(_), Str(_))
            | (Bytes(_), Bytes(_))
            | (Regex(_), Regex(_))
            | (List(_), List(_))
            | (Set(_), Set(_))
            | (Bot, Bot)
    ) {
        bail!(
            "comparison can only be done between the same datatypes, got {:?} and {:?}",
            a,
            b
        )
    }
    Ok(())
}

define_op!(OP_LIST, 0, true);
pub(crate) fn op_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(args.to_vec()))
}

define_op!(OP_COALESCE, 0, true);
pub(crate) fn op_coalesce(args: &[DataValue]) -> Result<DataValue> {
    for val in args {
        if *val != DataValue::Null {
            return Ok(val.clone());
        }
    }
    Ok(DataValue::Null)
}

define_op!(OP_EQ, 2, false);
pub(crate) fn op_eq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(f)), DataValue::Num(Num::Int(i)))
        | (DataValue::Num(Num::Int(i)), DataValue::Num(Num::Float(f))) => *i as f64 == *f,
        (a, b) => a == b,
    }))
}

define_op!(OP_IS_UUID, 1, false);
pub(crate) fn op_is_uuid(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Uuid(_))))
}

define_op!(OP_IS_IN, 2, false);
pub(crate) fn op_is_in(args: &[DataValue]) -> Result<DataValue> {
    let left = &args[0];
    let right = args[1]
        .get_list()
        .ok_or_else(|| miette!("right hand side of 'is_in' must be a list"))?;
    Ok(DataValue::Bool(right.contains(left)))
}

define_op!(OP_NEQ, 2, false);
pub(crate) fn op_neq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(f)), DataValue::Num(Num::Int(i)))
        | (DataValue::Num(Num::Int(i)), DataValue::Num(Num::Float(f))) => *i as f64 != *f,
        (a, b) => a != b,
    }))
}

define_op!(OP_GT, 2, false);
pub(crate) fn op_gt(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l as f64 > *r as f64,
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => *l as f64 > *r as f64,
        (a, b) => a > b,
    }))
}

define_op!(OP_GE, 2, false);
pub(crate) fn op_ge(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l as f64 >= *r as f64,
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => *l as f64 >= *r as f64,
        (a, b) => a >= b,
    }))
}

define_op!(OP_LT, 2, false);
pub(crate) fn op_lt(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => (*l as f64) < (*r as f64),
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => (*l as f64) < (*r as f64),
        (a, b) => a < b,
    }))
}

define_op!(OP_LE, 2, false);
pub(crate) fn op_le(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => (*l as f64) <= (*r as f64),
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => (*l as f64) <= (*r as f64),
        (a, b) => a <= b,
    }))
}

define_op!(OP_ADD, 0, true);
pub(crate) fn op_add(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 0i64;
    let mut f_accum = 0.0f64;
    for arg in args {
        match arg {
            DataValue::Num(Num::Int(i)) => i_accum += i,
            DataValue::Num(Num::Float(f)) => f_accum += f,
            _ => bail!("addition requires numbers"),
        }
    }
    if f_accum == 0.0f64 {
        Ok(DataValue::Num(Num::Int(i_accum)))
    } else {
        Ok(DataValue::Num(Num::Float(i_accum as f64 + f_accum)))
    }
}

define_op!(OP_MAX, 1, true);
pub(crate) fn op_max(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Num(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Num(a)), DataValue::Num(b)) => Ok(Some(DataValue::Num(a.max(*b)))),
            _ => bail!("'max can only be applied to numbers'"),
        })?;
    match res {
        None => Ok(DataValue::Num(Num::Float(f64::NEG_INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_MIN, 1, true);
pub(crate) fn op_min(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Num(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Num(a)), DataValue::Num(b)) => Ok(Some(DataValue::Num(a.min(*b)))),
            _ => bail!("'min' can only be applied to numbers"),
        })?;
    match res {
        None => Ok(DataValue::Num(Num::Float(f64::INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_SUB, 2, false);
pub(crate) fn op_sub(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Int(*a - *b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(*a - *b))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64) - b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a - (*b as f64)))
        }
        _ => bail!("subtraction requires numbers"),
    })
}

define_op!(OP_MUL, 0, true);
pub(crate) fn op_mul(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 1i64;
    let mut f_accum = 1.0f64;
    for arg in args {
        match arg {
            DataValue::Num(Num::Int(i)) => i_accum *= i,
            DataValue::Num(Num::Float(f)) => f_accum *= f,
            _ => bail!("multiplication requires numbers"),
        }
    }
    if f_accum == 1.0f64 {
        Ok(DataValue::Num(Num::Int(i_accum)))
    } else {
        Ok(DataValue::Num(Num::Float(i_accum as f64 * f_accum)))
    }
}

define_op!(OP_DIV, 2, false);
pub(crate) fn op_div(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float((*a as f64) / (*b as f64)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(*a / *b))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64) / b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a / (*b as f64)))
        }
        _ => bail!("division requires numbers"),
    })
}

define_op!(OP_MINUS, 1, false);
pub(crate) fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(-(*i))),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(-(*f))),
        _ => bail!("minus can only be applied to numbers"),
    })
}

define_op!(OP_ABS, 1, false);
pub(crate) fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(i.abs())),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.abs())),
        _ => bail!("'abs' requires numbers"),
    })
}

define_op!(OP_SIGNUM, 1, false);
pub(crate) fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(i.signum())),
        DataValue::Num(Num::Float(f)) => {
            if f.signum() < 0. {
                DataValue::from(-1)
            } else if *f == 0. {
                DataValue::from(0)
            } else if *f > 0. {
                DataValue::from(1)
            } else {
                DataValue::from(f64::NAN)
            }
        }
        _ => bail!("'signum' requires numbers"),
    })
}

define_op!(OP_FLOOR, 1, false);
pub(crate) fn op_floor(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.floor())),
        _ => bail!("'floor' requires numbers"),
    })
}

define_op!(OP_CEIL, 1, false);
pub(crate) fn op_ceil(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.ceil())),
        _ => bail!("'ceil' requires numbers"),
    })
}

define_op!(OP_ROUND, 1, false);
pub(crate) fn op_round(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.round())),
        _ => bail!("'round' requires numbers"),
    })
}

define_op!(OP_EXP, 1, false);
pub(crate) fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'exp' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.exp())))
}

define_op!(OP_EXP2, 1, false);
pub(crate) fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'exp2' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.exp2())))
}

define_op!(OP_LN, 1, false);
pub(crate) fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'ln' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.ln())))
}

define_op!(OP_LOG2, 1, false);
pub(crate) fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'log2' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.log2())))
}

define_op!(OP_LOG10, 1, false);
pub(crate) fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'log10' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.log10())))
}

define_op!(OP_SIN, 1, false);
pub(crate) fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'sin' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.sin())))
}

define_op!(OP_COS, 1, false);
pub(crate) fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'cos' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.cos())))
}

define_op!(OP_TAN, 1, false);
pub(crate) fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'tan' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.tan())))
}

define_op!(OP_ASIN, 1, false);
pub(crate) fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'asin' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.asin())))
}

define_op!(OP_ACOS, 1, false);
pub(crate) fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'acos' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.acos())))
}

define_op!(OP_ATAN, 1, false);
pub(crate) fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atan' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.atan())))
}

define_op!(OP_ATAN2, 2, false);
pub(crate) fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atan2' requires numbers"),
    };
    let b = match &args[1] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atan2' requires numbers"),
    };

    Ok(DataValue::Num(Num::Float(a.atan2(b))))
}

define_op!(OP_SINH, 1, false);
pub(crate) fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'sinh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.sinh())))
}

define_op!(OP_COSH, 1, false);
pub(crate) fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'cosh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.cosh())))
}

define_op!(OP_TANH, 1, false);
pub(crate) fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'tanh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.tanh())))
}

define_op!(OP_ASINH, 1, false);
pub(crate) fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'asinh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.asinh())))
}

define_op!(OP_ACOSH, 1, false);
pub(crate) fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'acosh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.acosh())))
}

define_op!(OP_ATANH, 1, false);
pub(crate) fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atanh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.atanh())))
}

define_op!(OP_POW, 2, false);
pub(crate) fn op_pow(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'pow' requires numbers"),
    };
    let b = match &args[1] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'pow' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.powf(b))))
}

define_op!(OP_MOD, 2, false);
pub(crate) fn op_mod(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Int(a.rem(b)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(a.rem(*b)))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64).rem(b)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a.rem(*b as f64)))
        }
        _ => bail!("'mod' requires numbers"),
    })
}

define_op!(OP_AND, 0, true);
pub(crate) fn op_and(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if !arg
            .get_bool()
            .ok_or_else(|| miette!("'and' requires booleans"))?
        {
            return Ok(DataValue::Bool(false));
        }
    }
    Ok(DataValue::Bool(true))
}

define_op!(OP_OR, 0, true);
pub(crate) fn op_or(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if arg
            .get_bool()
            .ok_or_else(|| miette!("'or' requires booleans"))?
        {
            return Ok(DataValue::Bool(true));
        }
    }
    Ok(DataValue::Bool(false))
}

define_op!(OP_NEGATE, 1, false);
pub(crate) fn op_negate(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bool(b) = &args[0] {
        Ok(DataValue::Bool(!*b))
    } else {
        bail!("'negate' requires booleans");
    }
}

define_op!(OP_BIT_AND, 2, false);
pub(crate) fn op_bit_and(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_and' must have the same lengths"
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l &= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_and' requires bytes"),
    }
}

define_op!(OP_BIT_OR, 2, false);
pub(crate) fn op_bit_or(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_or' must have the same lengths",
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l |= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_or' requires bytes"),
    }
}

define_op!(OP_BIT_NOT, 1, false);
pub(crate) fn op_bit_not(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(arg) => {
            let mut ret = arg.clone();
            for l in ret.iter_mut() {
                *l = !*l;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_not' requires bytes"),
    }
}

define_op!(OP_BIT_XOR, 2, false);
pub(crate) fn op_bit_xor(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_xor' must have the same lengths"
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l ^= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_xor' requires bytes"),
    }
}

define_op!(OP_UNPACK_BITS, 1, false);
pub(crate) fn op_unpack_bits(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bytes(bs) = &args[0] {
        let mut ret = vec![false; bs.len() * 8];
        for (chunk, byte) in bs.iter().enumerate() {
            ret[chunk * 8] = (*byte & 0b10000000) != 0;
            ret[chunk * 8 + 1] = (*byte & 0b01000000) != 0;
            ret[chunk * 8 + 2] = (*byte & 0b00100000) != 0;
            ret[chunk * 8 + 3] = (*byte & 0b00010000) != 0;
            ret[chunk * 8 + 4] = (*byte & 0b00001000) != 0;
            ret[chunk * 8 + 5] = (*byte & 0b00000100) != 0;
            ret[chunk * 8 + 6] = (*byte & 0b00000010) != 0;
            ret[chunk * 8 + 7] = (*byte & 0b00000001) != 0;
        }
        Ok(DataValue::List(
            ret.into_iter().map(DataValue::Bool).collect_vec(),
        ))
    } else {
        bail!("'unpack_bits' requires bytes")
    }
}

define_op!(OP_PACK_BITS, 1, false);
pub(crate) fn op_pack_bits(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::List(v) = &args[0] {
        let l = (v.len() as f64 / 8.).ceil() as usize;
        let mut res = vec![0u8; l];
        for (i, b) in v.iter().enumerate() {
            match b {
                DataValue::Bool(b) => {
                    if *b {
                        let chunk = i.div(&8);
                        let idx = i % 8;
                        let target = res.get_mut(chunk).unwrap();
                        match idx {
                            0 => *target |= 0b10000000,
                            1 => *target |= 0b01000000,
                            2 => *target |= 0b00100000,
                            3 => *target |= 0b00010000,
                            4 => *target |= 0b00001000,
                            5 => *target |= 0b00000100,
                            6 => *target |= 0b00000010,
                            7 => *target |= 0b00000001,
                            _ => unreachable!(),
                        }
                    }
                }
                _ => bail!("'pack_bits' requires list of booleans"),
            }
        }
        Ok(DataValue::Bytes(res))
    } else if let DataValue::Set(v) = &args[0] {
        let l = v.iter().cloned().collect_vec();
        op_pack_bits(&[DataValue::List(l)])
    } else {
        bail!("'pack_bits' requires list of booleans")
    }
}

define_op!(OP_CONCAT, 1, true);
pub(crate) fn op_concat(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(_) => {
            let mut ret: String = Default::default();
            for arg in args {
                if let DataValue::Str(s) = arg {
                    ret += s;
                } else {
                    bail!("'concat' requires strings, or lists");
                }
            }
            Ok(DataValue::Str(SmartString::from(ret)))
        }
        DataValue::List(_) | DataValue::Set(_) => {
            let mut ret = vec![];
            for arg in args {
                if let DataValue::List(l) = arg {
                    ret.extend_from_slice(l);
                } else if let DataValue::Set(s) = arg {
                    ret.extend(s.iter().cloned());
                } else {
                    bail!("'concat' requires strings, or lists");
                }
            }
            Ok(DataValue::List(ret))
        }
        _ => bail!("'concat' requires strings, or lists"),
    }
}

define_op!(OP_STR_INCLUDES, 2, false);
pub(crate) fn op_str_includes(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(l), DataValue::Str(r)) => Ok(DataValue::Bool(l.find(r as &str).is_some())),
        _ => bail!("'str_includes' requires strings"),
    }
}

define_op!(OP_LOWERCASE, 1, false);
pub(crate) fn op_lowercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::Str(SmartString::from(s.to_lowercase()))),
        _ => bail!("'lowercase' requires strings"),
    }
}

define_op!(OP_UPPERCASE, 1, false);
pub(crate) fn op_uppercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::Str(SmartString::from(s.to_uppercase()))),
        _ => bail!("'uppercase' requires strings"),
    }
}

define_op!(OP_TRIM, 1, false);
pub(crate) fn op_trim(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::Str(SmartString::from(s.trim()))),
        _ => bail!("'trim' requires strings"),
    }
}

define_op!(OP_TRIM_START, 1, false);
pub(crate) fn op_trim_start(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::Str(SmartString::from(s.trim_start()))),
        _ => bail!("'trim_start' requires strings"),
    }
}

define_op!(OP_TRIM_END, 1, false);
pub(crate) fn op_trim_end(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::Str(SmartString::from(s.trim_end()))),
        _ => bail!("'trim_end' requires strings"),
    }
}

define_op!(OP_STARTS_WITH, 2, false);
pub(crate) fn op_starts_with(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Str(s) => s,
        _ => bail!("'starts_with' requires strings"),
    };
    let b = match &args[1] {
        DataValue::Str(s) => s,
        _ => bail!("'starts_with' requires strings"),
    };
    Ok(DataValue::Bool(a.starts_with(b as &str)))
}

define_op!(OP_ENDS_WITH, 2, false);
pub(crate) fn op_ends_with(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Str(s) => s,
        _ => bail!("'ends_with' requires strings"),
    };
    let b = match &args[1] {
        DataValue::Str(s) => s,
        _ => bail!("'ends_with' requires strings"),
    };
    Ok(DataValue::Bool(a.ends_with(b as &str)))
}

define_op!(OP_REGEX, 1, false);
pub(crate) fn op_regex(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        r @ DataValue::Regex(_) => r.clone(),
        DataValue::Str(s) => {
            DataValue::Regex(RegexWrapper(regex::Regex::new(s).map_err(|err| {
                miette!("The string cannot be interpreted as regex: {}", err)
            })?))
        }
        _ => bail!("'regex' requires strings"),
    })
}

define_op!(OP_REGEX_MATCHES, 2, false);
pub(crate) fn op_regex_matches(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => Ok(DataValue::Bool(r.0.is_match(s))),
        _ => bail!("'regex_matches' requires strings"),
    }
}

define_op!(OP_REGEX_REPLACE, 3, false);
pub(crate) fn op_regex_replace(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1], &args[2]) {
        (DataValue::Str(s), DataValue::Regex(r), DataValue::Str(rp)) => {
            Ok(DataValue::Str(r.0.replace(s, rp as &str).into()))
        }
        _ => bail!("'regex_replace' requires strings"),
    }
}

define_op!(OP_REGEX_REPLACE_ALL, 3, false);
pub(crate) fn op_regex_replace_all(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1], &args[2]) {
        (DataValue::Str(s), DataValue::Regex(r), DataValue::Str(rp)) => {
            Ok(DataValue::Str(r.0.replace_all(s, rp as &str).into()))
        }
        _ => bail!("'regex_replace' requires strings"),
    }
}

define_op!(OP_REGEX_EXTRACT, 2, false);
pub(crate) fn op_regex_extract(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => {
            let found =
                r.0.find_iter(s)
                    .map(|v| DataValue::Str(SmartString::from(v.as_str())))
                    .collect_vec();
            Ok(DataValue::List(found))
        }
        _ => bail!("'regex_extract' requires strings"),
    }
}

define_op!(OP_REGEX_EXTRACT_FIRST, 2, false);
pub(crate) fn op_regex_extract_first(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => {
            let found =
                r.0.find(s)
                    .map(|v| DataValue::Str(SmartString::from(v.as_str())));
            Ok(found.unwrap_or(DataValue::Null))
        }
        _ => bail!("'regex_extract_first' requires strings"),
    }
}

define_op!(OP_IS_NULL, 1, false);
pub(crate) fn op_is_null(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Null)))
}

define_op!(OP_IS_INT, 1, false);
pub(crate) fn op_is_int(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Num(Num::Int(_))
    )))
}

define_op!(OP_IS_FLOAT, 1, false);
pub(crate) fn op_is_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Num(Num::Float(_))
    )))
}

define_op!(OP_IS_NUM, 1, false);
pub(crate) fn op_is_num(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Num(Num::Int(_)) | DataValue::Num(Num::Float(_))
    )))
}

define_op!(OP_IS_FINITE, 1, false);
pub(crate) fn op_is_finite(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match &args[0] {
        DataValue::Num(Num::Int(_)) => true,
        DataValue::Num(Num::Float(f)) => f.is_finite(),
        _ => false,
    }))
}

define_op!(OP_IS_INFINITE, 1, false);
pub(crate) fn op_is_infinite(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match &args[0] {
        DataValue::Num(Num::Float(f)) => f.is_infinite(),
        _ => false,
    }))
}

define_op!(OP_IS_NAN, 1, false);
pub(crate) fn op_is_nan(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match &args[0] {
        DataValue::Num(Num::Float(f)) => f.is_nan(),
        _ => false,
    }))
}

define_op!(OP_IS_STRING, 1, false);
pub(crate) fn op_is_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Str(_))))
}

define_op!(OP_IS_LIST, 1, false);
pub(crate) fn op_is_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::List(_) | DataValue::Set(_)
    )))
}

define_op!(OP_APPEND, 2, false);
pub(crate) fn op_append(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => {
            let mut l = l.clone();
            l.push(args[1].clone());
            Ok(DataValue::List(l))
        }
        DataValue::Set(l) => {
            let mut l = l.iter().cloned().collect_vec();
            l.push(args[1].clone());
            Ok(DataValue::List(l))
        }
        _ => bail!("'append' requires first argument to be a list"),
    }
}

define_op!(OP_PREPEND, 2, false);
pub(crate) fn op_prepend(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(pl) => {
            let mut l = vec![args[1].clone()];
            l.extend_from_slice(pl);
            Ok(DataValue::List(l))
        }
        DataValue::Set(pl) => {
            let mut l = vec![args[1].clone()];
            l.extend(pl.iter().cloned());
            Ok(DataValue::List(l))
        }
        _ => bail!("'prepend' requires first argument to be a list"),
    }
}

define_op!(OP_IS_BYTES, 1, false);
pub(crate) fn op_is_bytes(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Bytes(_))))
}

define_op!(OP_LENGTH, 1, false);
pub(crate) fn op_length(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Set(s) => s.len() as i64,
        DataValue::List(l) => l.len() as i64,
        DataValue::Str(s) => s.chars().count() as i64,
        DataValue::Bytes(b) => b.len() as i64,
        _ => bail!("'length' requires lists"),
    }))
}

define_op!(OP_UNICODE_NORMALIZE, 2, false);
pub(crate) fn op_unicode_normalize(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Str(n)) => Ok(DataValue::Str(match n as &str {
            "nfc" => s.nfc().collect(),
            "nfd" => s.nfd().collect(),
            "nfkc" => s.nfkc().collect(),
            "nfkd" => s.nfkd().collect(),
            u => bail!("unknown normalization {} for 'unicode_normalize'", u),
        })),
        _ => bail!("'unicode_normalize' requires strings"),
    }
}

define_op!(OP_SORTED, 1, false);
pub(crate) fn op_sorted(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_list()
        .ok_or_else(|| miette!("'sort' requires lists"))?
        .to_vec();
    arg.sort();
    Ok(DataValue::List(arg))
}

define_op!(OP_REVERSE, 1, false);
pub(crate) fn op_reverse(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_list()
        .ok_or_else(|| miette!("'reverse' requires lists"))?
        .to_vec();
    arg.reverse();
    Ok(DataValue::List(arg))
}

define_op!(OP_HAVERSINE, 4, false);
pub(crate) fn op_haversine(args: &[DataValue]) -> Result<DataValue> {
    let miette = || miette!("'haversine' requires numbers");
    let lat1 = args[0].get_float().ok_or_else(miette)?;
    let lon1 = args[1].get_float().ok_or_else(miette)?;
    let lat2 = args[2].get_float().ok_or_else(miette)?;
    let lon2 = args[3].get_float().ok_or_else(miette)?;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_HAVERSINE_DEG_INPUT, 4, false);
pub(crate) fn op_haversine_deg_input(args: &[DataValue]) -> Result<DataValue> {
    let miette = || miette!("'haversine_deg_input' requires numbers");
    let lat1 = args[0].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lon1 = args[1].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lat2 = args[2].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lon2 = args[3].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_DEG_TO_RAD, 1, false);
pub(crate) fn op_deg_to_rad(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| miette!("'deg_to_rad' requires numbers"))?;
    Ok(DataValue::from(x * f64::PI() / 180.))
}

define_op!(OP_RAD_TO_DEG, 1, false);
pub(crate) fn op_rad_to_deg(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| miette!("'rad_to_deg' requires numbers"))?;
    Ok(DataValue::from(x * 180. / f64::PI()))
}

define_op!(OP_FIRST, 1, false);
pub(crate) fn op_first(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| miette!("'first' requires lists"))?
        .first()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_LAST, 1, false);
pub(crate) fn op_last(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| miette!("'last' requires lists"))?
        .last()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_CHUNKS, 2, false);
pub(crate) fn op_chunks(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument of 'chunks' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'chunks' must be an integer"))?;
    ensure!(n > 0, "second argument to 'chunks' must be positive");
    let res = arg
        .chunks(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_CHUNKS_EXACT, 2, false);
pub(crate) fn op_chunks_exact(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument of 'chunks_exact' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'chunks_exact' must be an integer"))?;
    ensure!(n > 0, "second argument to 'chunks_exact' must be positive");
    let res = arg
        .chunks_exact(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_WINDOWS, 2, false);
pub(crate) fn op_windows(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument of 'windows' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'windows' must be an integer"))?;
    ensure!(n > 0, "second argument to 'windows' must be positive");
    let res = arg
        .windows(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

fn get_index(mut i: i64, total: usize) -> Result<usize> {
    if i < 0 {
        i += total as i64;
    }
    Ok(if i >= 0 {
        let i = i as usize;
        if i >= total {
            bail!("index {} out of bound", i)
        } else {
            i
        }
    } else {
        bail!("index {} out of bound", i)
    })
}

define_op!(OP_GET, 2, false);
pub(crate) fn op_get(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument to 'get' mut be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument to 'get' mut be an integer"))?;
    let idx = get_index(n, l.len())?;
    Ok(l[idx].clone())
}

define_op!(OP_MAYBE_GET, 2, false);
pub(crate) fn op_maybe_get(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument to 'maybe_get' mut be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument to 'maybe_get' mut be an integer"))?;
    if let Ok(idx) = get_index(n, l.len()) {
        Ok(l[idx].clone())
    } else {
        Ok(DataValue::Null)
    }
}

define_op!(OP_SLICE, 3, false);
pub(crate) fn op_slice(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| miette!("first argument to 'slice' mut be a list"))?;
    let m = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument to 'slice' mut be an integer"))?;
    let n = args[2]
        .get_int()
        .ok_or_else(|| miette!("third argument to 'slice' mut be an integer"))?;
    let m = get_index(m, l.len())?;
    let n = get_index(n, l.len())?;
    Ok(DataValue::List(l[m..n].to_vec()))
}

define_op!(OP_CHARS, 1, false);
pub(crate) fn op_chars(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(
        args[0]
            .get_string()
            .ok_or_else(|| miette!("'chars' requires strings"))?
            .chars()
            .map(|c| {
                let mut s = SmartString::new();
                s.push(c);
                DataValue::Str(s)
            })
            .collect_vec(),
    ))
}

define_op!(OP_FROM_SUBSTRINGS, 1, false);
pub(crate) fn op_from_substrings(args: &[DataValue]) -> Result<DataValue> {
    let mut ret = String::new();
    match &args[0] {
        DataValue::List(ss) => {
            for arg in ss {
                if let DataValue::Str(s) = arg {
                    ret.push_str(s);
                } else {
                    bail!("'from_substring' requires a list of strings")
                }
            }
        }
        DataValue::Set(ss) => {
            for arg in ss {
                if let DataValue::Str(s) = arg {
                    ret.push_str(s);
                } else {
                    bail!("'from_substring' requires a list of strings")
                }
            }
        }
        _ => bail!("'from_substring' requires a list of strings"),
    }
    Ok(DataValue::Str(SmartString::from(ret)))
}

define_op!(OP_ENCODE_BASE64, 1, false);
pub(crate) fn op_encode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(b) => {
            let s = base64::encode(b);
            Ok(DataValue::Str(SmartString::from(s)))
        }
        _ => bail!("'encode_base64' requires bytes"),
    }
}

define_op!(OP_DECODE_BASE64, 1, false);
pub(crate) fn op_decode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => {
            let b = base64::decode(s).map_err(|_| miette!("Data is not properly encoded"))?;
            Ok(DataValue::Bytes(b))
        }
        _ => bail!("'decode_base64' requires strings"),
    }
}

define_op!(OP_TO_BOOL, 1, false);
pub(crate) fn op_to_bool(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match &args[0] {
        DataValue::Null => false,
        DataValue::Bool(b) => *b,
        DataValue::Num(n) => n.get_int() != Some(0),
        DataValue::Str(s) => !s.is_empty(),
        DataValue::Bytes(b) => !b.is_empty(),
        DataValue::Uuid(u) => !u.0.is_nil(),
        DataValue::Regex(r) => !r.0.as_str().is_empty(),
        DataValue::List(l) => !l.is_empty(),
        DataValue::Set(s) => !s.is_empty(),
        DataValue::Bot => false,
    }))
}


define_op!(OP_TO_UNITY, 1, false);
pub(crate) fn op_to_unity(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Null => 0,
        DataValue::Bool(b) => *b as i64,
        DataValue::Num(n) => (n.get_float() != 0.) as i64,
        DataValue::Str(s) => if s.is_empty() {0} else { 1},
        DataValue::Bytes(b) => if b.is_empty() {0} else { 1},
        DataValue::Uuid(u) => if u.0.is_nil() {0} else { 1 },
        DataValue::Regex(r) => if r.0.as_str().is_empty() {0 } else { 1},
        DataValue::List(l) => if l.is_empty() {0} else {1},
        DataValue::Set(s) => if s.is_empty() {0} else {1},
        DataValue::Bot => 0,
    }))
}


define_op!(OP_TO_FLOAT, 1, false);
pub(crate) fn op_to_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(n) => n.get_float().into(),
        DataValue::Null => DataValue::from(0.0),
        DataValue::Bool(b) => DataValue::from(if *b { 1.0 } else { 0.0 }),
        DataValue::Str(t) => match t as &str {
            "PI" => f64::PI().into(),
            "E" => f64::E().into(),
            "NAN" => f64::NAN.into(),
            "INF" => f64::INFINITY.into(),
            "NEG_INF" => f64::NEG_INFINITY.into(),
            s => f64::from_str(s)
                .map_err(|_| miette!("The string cannot be interpreted as float"))?
                .into(),
        },
        v => bail!("'to_float' does not recognize {:?}", v),
    })
}

define_op!(OP_TO_STRING, 1, false);
pub(crate) fn op_to_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Str(s) => DataValue::Str(s.clone()),
        v => {
            let jv = JsonValue::from(v.clone());
            let s = jv.to_string();
            DataValue::Str(SmartString::from(s))
        }
    })
}

define_op!(OP_RAND_FLOAT, 0, false);
pub(crate) fn op_rand_float(_args: &[DataValue]) -> Result<DataValue> {
    Ok(thread_rng().gen::<f64>().into())
}

define_op!(OP_RAND_BERNOULLI, 1, false);
pub(crate) fn op_rand_bernoulli(args: &[DataValue]) -> Result<DataValue> {
    let prob = match &args[0] {
        DataValue::Num(n) => {
            let f = n.get_float();
            ensure!(
                (0. ..=1.).contains(&f),
                "'rand_bernoulli' requires number between 0. and 1."
            );
            f
        }
        _ => bail!("'rand_bernoulli' requires number between 0. and 1."),
    };
    Ok(DataValue::Bool(thread_rng().gen_bool(prob)))
}

define_op!(OP_RAND_INT, 2, false);
pub(crate) fn op_rand_int(args: &[DataValue]) -> Result<DataValue> {
    let lower = &args[0]
        .get_int()
        .ok_or_else(|| miette!("'rand_int' requires integers"))?;
    let upper = &args[1]
        .get_int()
        .ok_or_else(|| miette!("'rand_int' requires integers"))?;
    Ok(thread_rng().gen_range(*lower..=*upper).into())
}

define_op!(OP_RAND_CHOOSE, 1, false);
pub(crate) fn op_rand_choose(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => Ok(l
            .choose(&mut thread_rng())
            .cloned()
            .unwrap_or(DataValue::Null)),
        DataValue::Set(l) => Ok(l
            .iter()
            .collect_vec()
            .choose(&mut thread_rng())
            .cloned()
            .cloned()
            .unwrap_or(DataValue::Null)),
        _ => bail!("'rand_choice' requires lists"),
    }
}

define_op!(OP_ASSERT, 1, true);
pub(crate) fn op_assert(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bool(true) => Ok(DataValue::Bool(true)),
        _ => bail!("assertion failed: {:?}", args),
    }
}

define_op!(OP_UNION, 1, true);
pub(crate) fn op_union(args: &[DataValue]) -> Result<DataValue> {
    let mut ret = BTreeSet::new();
    for arg in args {
        match arg {
            DataValue::List(l) => {
                for el in l {
                    ret.insert(el.clone());
                }
            }
            DataValue::Set(s) => {
                for el in s {
                    ret.insert(el.clone());
                }
            }
            _ => bail!("'union' requires lists"),
        }
    }
    Ok(DataValue::List(ret.into_iter().collect()))
}

define_op!(OP_DIFFERENCE, 2, true);
pub(crate) fn op_difference(args: &[DataValue]) -> Result<DataValue> {
    let mut start: BTreeSet<_> = match &args[0] {
        DataValue::List(l) => l.iter().cloned().collect(),
        DataValue::Set(s) => s.iter().cloned().collect(),
        _ => bail!("'difference' requires lists"),
    };
    for arg in &args[1..] {
        match arg {
            DataValue::List(l) => {
                for el in l {
                    start.remove(el);
                }
            }
            DataValue::Set(s) => {
                for el in s {
                    start.remove(el);
                }
            }
            _ => bail!("'difference' requires lists"),
        }
    }
    Ok(DataValue::List(start.into_iter().collect()))
}

define_op!(OP_INTERSECTION, 1, true);
pub(crate) fn op_intersection(args: &[DataValue]) -> Result<DataValue> {
    let mut start: BTreeSet<_> = match &args[0] {
        DataValue::List(l) => l.iter().cloned().collect(),
        DataValue::Set(s) => s.iter().cloned().collect(),
        _ => bail!("'intersection' requires lists"),
    };
    for arg in &args[1..] {
        match arg {
            DataValue::List(l) => {
                let other: BTreeSet<_> = l.iter().cloned().collect();
                start = start.intersection(&other).cloned().collect();
            }
            DataValue::Set(s) => start = start.intersection(s).cloned().collect(),
            _ => bail!("'intersection' requires lists"),
        }
    }
    Ok(DataValue::List(start.into_iter().collect()))
}

define_op!(OP_TO_UUID, 1, false);
pub(crate) fn op_to_uuid(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        d @ DataValue::Uuid(_u) => Ok(d.clone()),
        DataValue::Str(s) => {
            let id = uuid::Uuid::try_parse(s).map_err(|_| miette!("invalid UUID"))?;
            Ok(DataValue::uuid(id))
        }
        _ => bail!("'to_uuid' requires a string"),
    }
}

define_op!(OP_NOW, 0, false);
#[cfg(feature = "wasm")]
pub(crate) fn op_now(_args: &[DataValue]) -> Result<DataValue> {
    let d: f64 = Date::now() / 1000.;
    Ok(DataValue::from(d))
}
#[cfg(not(feature = "wasm"))]
pub(crate) fn op_now(_args: &[DataValue]) -> Result<DataValue> {
    let now = SystemTime::now();
    Ok(DataValue::from(
        now.duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
    ))
}

define_op!(OP_FORMAT_TIMESTAMP, 1, true);
pub(crate) fn op_format_timestamp(args: &[DataValue]) -> Result<DataValue> {
    #[cfg(feature = "wasm")]
    let dt = Utc
        .timestamp_millis_opt(Date::now() as i64)
        .latest()
        .ok_or_else(|| miette!("bad time input"))?;
    #[cfg(not(feature = "wasm"))]
    let dt = {
        let f = args[0]
            .get_float()
            .ok_or_else(|| miette!("'format_timestamp' expects a number"))?;
        let millis = (f * 1000.) as i64;
        Utc.timestamp_millis_opt(millis)
            .latest()
            .ok_or_else(|| miette!("bad time: {}", f))?
    };
    match args.get(1) {
        Some(tz_v) => {
            let tz_s = tz_v.get_string().ok_or_else(|| {
                miette!("'format_timestamp' timezone specification requires a string")
            })?;
            let tz = chrono_tz::Tz::from_str(tz_s)
                .map_err(|_| miette!("bad timezone specification: {}", tz_s))?;
            let dt_tz = dt.with_timezone(&tz);
            let s = SmartString::from(dt_tz.to_rfc3339());
            Ok(DataValue::Str(s))
        }
        None => {
            let s = SmartString::from(dt.to_rfc3339());
            Ok(DataValue::Str(s))
        }
    }
}

define_op!(OP_PARSE_TIMESTAMP, 1, false);
pub(crate) fn op_parse_timestamp(args: &[DataValue]) -> Result<DataValue> {
    let s = args[0]
        .get_string()
        .ok_or_else(|| miette!("'parse_timestamp' expects a string"))?;
    let dt = DateTime::parse_from_rfc3339(s).map_err(|_| miette!("bad datetime: {}", s))?;
    let st: SystemTime = dt.into();
    Ok(DataValue::from(
        st.duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
    ))
}

define_op!(OP_RAND_UUID_V1, 0, false);
pub(crate) fn op_rand_uuid_v1(_args: &[DataValue]) -> Result<DataValue> {
    let mut rng = rand::thread_rng();
    let uuid_ctx = uuid::v1::Context::new(rng.gen());
    #[cfg(feature = "wasm")]
    let ts = {
        let since_epoch: f64 = Date::now();
        let seconds = since_epoch.floor();
        let fractional = (since_epoch - seconds) * 1.0e9;
        Timestamp::from_unix(uuid_ctx, seconds as u64, fractional as u32)
    };
    #[cfg(not(feature = "wasm"))]
    let ts = {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        Timestamp::from_unix(uuid_ctx, since_epoch.as_secs(), since_epoch.subsec_nanos())
    };
    let mut rand_vals = [0u8; 6];
    rng.fill(&mut rand_vals);
    let id = uuid::Uuid::new_v1(ts, &rand_vals);
    Ok(DataValue::uuid(id))
}

define_op!(OP_RAND_UUID_V4, 0, false);
pub(crate) fn op_rand_uuid_v4(_args: &[DataValue]) -> Result<DataValue> {
    let id = uuid::Uuid::new_v4();
    Ok(DataValue::uuid(id))
}

define_op!(OP_UUID_TIMESTAMP, 1, false);
pub(crate) fn op_uuid_timestamp(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Uuid(UuidWrapper(id)) => match id.get_timestamp() {
            None => DataValue::Null,
            Some(t) => {
                let (s, subs) = t.to_unix();
                let s = (s as f64) + (subs as f64 / 10_000_000.);
                s.into()
            }
        },
        _ => bail!("not an UUID"),
    })
}
