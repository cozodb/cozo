use std::ops::{Div, Rem};
use std::str::FromStr;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use num_traits::FloatConst;
use rand::prelude::*;
use smartstring::SmartString;

use crate::data::expr::Op;
use crate::data::value::{DataValue, Number, RegexWrapper};

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

define_op!(OP_EQ, 2, false, true);
pub(crate) fn op_eq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Number(Number::Float(f)), DataValue::Number(Number::Int(i)))
        | (DataValue::Number(Number::Int(i)), DataValue::Number(Number::Float(f))) => {
            *i as f64 == *f
        }
        (a, b) => a == b,
    }))
}

define_op!(OP_IS_IN, 2, false, true);
pub(crate) fn op_is_in(args: &[DataValue]) -> Result<DataValue> {
    let left = &args[0];
    let right = args[1]
        .get_list()
        .ok_or_else(|| anyhow!("right hand side of 'is_in' is not a list"))?;
    Ok(DataValue::Bool(right.contains(left)))
}

define_op!(OP_NEQ, 2, false, true);
pub(crate) fn op_neq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(match (&args[0], &args[1]) {
        (DataValue::Number(Number::Float(f)), DataValue::Number(Number::Int(i)))
        | (DataValue::Number(Number::Int(i)), DataValue::Number(Number::Float(f))) => {
            *i as f64 != *f
        }
        (a, b) => a != b,
    }))
}

define_op!(OP_GT, 2, false, true);
pub(crate) fn op_gt(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] > args[1]))
}

define_op!(OP_GE, 2, false, true);
pub(crate) fn op_ge(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] >= args[1]))
}

define_op!(OP_LT, 2, false, true);
pub(crate) fn op_lt(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] < args[1]))
}

define_op!(OP_LE, 2, false, true);
pub(crate) fn op_le(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(args[0] <= args[1]))
}

define_op!(OP_ADD, 0, true, false);
pub(crate) fn op_add(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_max(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_min(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_sub(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_mul(args: &[DataValue]) -> Result<DataValue> {
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
        Ok(DataValue::Number(Number::Float(i_accum as f64 * f_accum)))
    }
}

define_op!(OP_DIV, 2, false, false);
pub(crate) fn op_div(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(-(*i))),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(-(*f))),
        v => bail!("unexpected arg {:?} for OP_MINUS", v),
    })
}

define_op!(OP_ABS, 1, false, false);
pub(crate) fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(i.abs())),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.abs())),
        v => bail!("unexpected arg {:?} for OP_ABS", v),
    })
}

define_op!(OP_SIGNUM, 1, false, false);
pub(crate) fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(i.signum())),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.signum())),
        v => bail!("unexpected arg {:?} for OP_SIGNUM", v),
    })
}

define_op!(OP_FLOOR, 1, false, false);
pub(crate) fn op_floor(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.floor())),
        v => bail!("unexpected arg {:?} for OP_FLOOR", v),
    })
}

define_op!(OP_CEIL, 1, false, false);
pub(crate) fn op_ceil(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.ceil())),
        v => bail!("unexpected arg {:?} for OP_CEIL", v),
    })
}

define_op!(OP_ROUND, 1, false, false);
pub(crate) fn op_round(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(Number::Int(i)) => DataValue::Number(Number::Int(*i)),
        DataValue::Number(Number::Float(f)) => DataValue::Number(Number::Float(f.round())),
        v => bail!("unexpected arg {:?} for OP_ROUND", v),
    })
}

define_op!(OP_EXP, 1, false, false);
pub(crate) fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_EXP", v),
    };
    Ok(DataValue::Number(Number::Float(a.exp())))
}

define_op!(OP_EXP2, 1, false, false);
pub(crate) fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_EXP2", v),
    };
    Ok(DataValue::Number(Number::Float(a.exp2())))
}

define_op!(OP_LN, 1, false, false);
pub(crate) fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LN", v),
    };
    Ok(DataValue::Number(Number::Float(a.ln())))
}

define_op!(OP_LOG2, 1, false, false);
pub(crate) fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LOG2", v),
    };
    Ok(DataValue::Number(Number::Float(a.log2())))
}

define_op!(OP_LOG10, 1, false, false);
pub(crate) fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_LOG10", v),
    };
    Ok(DataValue::Number(Number::Float(a.log10())))
}

define_op!(OP_SIN, 1, false, false);
pub(crate) fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_SIN", v),
    };
    Ok(DataValue::Number(Number::Float(a.sin())))
}

define_op!(OP_COS, 1, false, false);
pub(crate) fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_COS", v),
    };
    Ok(DataValue::Number(Number::Float(a.cos())))
}

define_op!(OP_TAN, 1, false, false);
pub(crate) fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_TAN", v),
    };
    Ok(DataValue::Number(Number::Float(a.tan())))
}

define_op!(OP_ASIN, 1, false, false);
pub(crate) fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ASIN", v),
    };
    Ok(DataValue::Number(Number::Float(a.asin())))
}

define_op!(OP_ACOS, 1, false, false);
pub(crate) fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ACOS", v),
    };
    Ok(DataValue::Number(Number::Float(a.acos())))
}

define_op!(OP_ATAN, 1, false, false);
pub(crate) fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATAN", v),
    };
    Ok(DataValue::Number(Number::Float(a.atan())))
}

define_op!(OP_ATAN2, 2, false, false);
pub(crate) fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_SINH", v),
    };
    Ok(DataValue::Number(Number::Float(a.sinh())))
}

define_op!(OP_COSH, 1, false, false);
pub(crate) fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_COSH", v),
    };
    Ok(DataValue::Number(Number::Float(a.cosh())))
}

define_op!(OP_TANH, 1, false, false);
pub(crate) fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_TANH", v),
    };
    Ok(DataValue::Number(Number::Float(a.tanh())))
}

define_op!(OP_ASINH, 1, false, false);
pub(crate) fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ASINH", v),
    };
    Ok(DataValue::Number(Number::Float(a.asinh())))
}

define_op!(OP_ACOSH, 1, false, false);
pub(crate) fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ACOSH", v),
    };
    Ok(DataValue::Number(Number::Float(a.acosh())))
}

define_op!(OP_ATANH, 1, false, false);
pub(crate) fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Number(Number::Int(i)) => *i as f64,
        DataValue::Number(Number::Float(f)) => *f,
        v => bail!("unexpected arg {:?} for OP_ATANH", v),
    };
    Ok(DataValue::Number(Number::Float(a.atanh())))
}

define_op!(OP_POW, 2, false, false);
pub(crate) fn op_pow(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_mod(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_and(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_or(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_not(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bool(b) = &args[0] {
        Ok(DataValue::Bool(!*b))
    } else {
        bail!("unexpected arg {:?} for OP_NOT", args);
    }
}

define_op!(OP_BIT_AND, 2, false, false);
pub(crate) fn op_bit_and(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_and' must have the same lengths, got {:x?} and {:x?}",
                left,
                right
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l &= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        v => bail!("cannot apply 'bit_and' to {:?}", v),
    }
}

define_op!(OP_BIT_OR, 2, false, false);
pub(crate) fn op_bit_or(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_or' must have the same lengths, got {:x?} and {:x?}",
                left,
                right
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l |= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        v => bail!("cannot apply 'bit_or' to {:?}", v),
    }
}

define_op!(OP_BIT_NOT, 1, false, false);
pub(crate) fn op_bit_not(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(arg) => {
            let mut ret = arg.clone();
            for l in ret.iter_mut() {
                *l = !*l;
            }
            Ok(DataValue::Bytes(ret))
        }
        v => bail!("cannot apply 'bit_not' to {:?}", v),
    }
}

define_op!(OP_BIT_XOR, 2, false, false);
pub(crate) fn op_bit_xor(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_xor' must have the same lengths, got {:x?} and {:x?}",
                left,
                right
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l ^= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        v => bail!("cannot apply 'bit_xor' to {:?}", v),
    }
}

define_op!(OP_UNPACK_BITS, 1, false, false);
pub(crate) fn op_unpack_bits(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bytes(bs) = &args[0] {
        let mut ret = vec![false; bs.len() * 8];
        for (chunk, byte) in bs.iter().enumerate() {
            ret[chunk * 8 + 0] = (*byte & 0b10000000) != 0;
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
        bail!("cannot apply 'unpack_bits' to {:?}", args)
    }
}

define_op!(OP_PACK_BITS, 1, false, false);
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
                v => bail!("cannot apply 'pack_bits' to {:?}", v),
            }
        }
        Ok(DataValue::Bytes(res.into()))
    } else {
        bail!("cannot apply 'pack_bits' to {:?}", args)
    }
}

define_op!(OP_STR_CAT, 0, true, false);
pub(crate) fn op_str_cat(args: &[DataValue]) -> Result<DataValue> {
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

define_op!(OP_STR_INCLUDES, 2, false, true);
pub(crate) fn op_str_includes(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::String(l), DataValue::String(r)) => {
            Ok(DataValue::Bool(l.find(r as &str).is_some()))
        }
        v => bail!("cannot apply 'str_includes' to {:?}", v),
    }
}

define_op!(OP_LOWERCASE, 1, false, false);
pub(crate) fn op_lowercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => Ok(DataValue::String(s.to_lowercase().into())),
        v => bail!("cannot apply 'lowercase' to {:?}", v),
    }
}

define_op!(OP_UPPERCASE, 1, false, false);
pub(crate) fn op_uppercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => Ok(DataValue::String(s.to_uppercase().into())),
        v => bail!("cannot apply 'uppercase' to {:?}", v),
    }
}

define_op!(OP_TRIM, 1, false, false);
pub(crate) fn op_trim(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => Ok(DataValue::String(s.trim().into())),
        v => bail!("cannot apply 'trim' to {:?}", v),
    }
}

define_op!(OP_TRIM_START, 1, false, false);
pub(crate) fn op_trim_start(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => Ok(DataValue::String(s.trim_start().into())),
        v => bail!("cannot apply 'trim_start' to {:?}", v),
    }
}

define_op!(OP_TRIM_END, 1, false, false);
pub(crate) fn op_trim_end(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => Ok(DataValue::String(s.trim_end().into())),
        v => bail!("cannot apply 'trim_end' to {:?}", v),
    }
}

define_op!(OP_STARTS_WITH, 2, false, true);
pub(crate) fn op_starts_with(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_ends_with(args: &[DataValue]) -> Result<DataValue> {
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

define_op!(OP_REGEX, 1, false, false);
pub(crate) fn op_regex(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        r @ DataValue::Regex(_) => r.clone(),
        DataValue::String(s) => DataValue::Regex(RegexWrapper(regex::Regex::new(s)?)),
        v => bail!("cannot apply 'regex' to {:?}", v),
    })
}

define_op!(OP_REGEX_MATCHES, 2, false, true);
pub(crate) fn op_regex_matches(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::String(s), DataValue::Regex(r)) => Ok(DataValue::Bool(r.0.is_match(s))),
        v => bail!("cannot apply 'regex_matches' to {:?}", v),
    }
}

define_op!(OP_REGEX_REPLACE, 3, false, false);
pub(crate) fn op_regex_replace(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1], &args[2]) {
        (DataValue::String(s), DataValue::Regex(r), DataValue::String(rp)) => {
            Ok(DataValue::String(r.0.replace(s, rp as &str).into()))
        }
        v => bail!("cannot apply 'regex_replace' to {:?}", v),
    }
}

define_op!(OP_REGEX_EXTRACT, 2, false, false);
pub(crate) fn op_regex_extract(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::String(s), DataValue::Regex(r)) => {
            let found =
                r.0.find_iter(s)
                    .map(|v| DataValue::String(v.as_str().into()))
                    .collect_vec();
            Ok(DataValue::List(found))
        }
        v => bail!("cannot apply 'regex_extract' to {:?}", v),
    }
}

define_op!(OP_REGEX_EXTRACT_FIRST, 2, false, false);
pub(crate) fn op_regex_extract_first(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::String(s), DataValue::Regex(r)) => {
            let found = r.0.find(s).map(|v| DataValue::String(v.as_str().into()));
            Ok(found.unwrap_or(DataValue::Null))
        }
        v => bail!("cannot apply 'regex_extract_first' to {:?}", v),
    }
}

define_op!(OP_IS_NULL, 1, false, true);
pub(crate) fn op_is_null(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Null)))
}

define_op!(OP_IS_INT, 1, false, true);
pub(crate) fn op_is_int(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Int(_))
    )))
}

define_op!(OP_IS_FLOAT, 1, false, true);
pub(crate) fn op_is_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Float(_))
    )))
}

define_op!(OP_IS_NUM, 1, false, true);
pub(crate) fn op_is_num(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(
        args[0],
        DataValue::Number(Number::Int(_)) | DataValue::Number(Number::Float(_))
    )))
}

define_op!(OP_IS_STRING, 1, false, true);
pub(crate) fn op_is_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::String(_))))
}

define_op!(OP_IS_LIST, 1, false, true);
pub(crate) fn op_is_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::List(_))))
}

define_op!(OP_APPEND, 2, false, false);
pub(crate) fn op_append(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_is_bytes(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Bool(matches!(args[0], DataValue::Bytes(_))))
}

define_op!(OP_LENGTH, 1, false, false);
pub(crate) fn op_length(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Set(s) => s.len() as i64,
        DataValue::List(l) => l.len() as i64,
        DataValue::String(s) => s.chars().count() as i64,
        DataValue::Bytes(b) => b.len() as i64,
        v => bail!("cannot apply 'length' to {:?}", v),
    }))
}

define_op!(OP_SORT, 1, false, false);
pub(crate) fn op_sort(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot apply 'sort' to {:?}", args))?
        .to_vec();
    arg.sort();
    Ok(DataValue::List(arg))
}

define_op!(OP_PI, 0, false, false);
pub(crate) fn op_pi(_args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(f64::PI()))
}

define_op!(OP_E, 0, false, false);
pub(crate) fn op_e(_args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(f64::E()))
}

define_op!(OP_HAVERSINE, 4, false, false);
pub(crate) fn op_haversine(args: &[DataValue]) -> Result<DataValue> {
    let gen_err = || anyhow!("cannot computer haversine distance for {:?}", args);
    let lat1 = args[0].get_float().ok_or_else(gen_err)?;
    let lon1 = args[1].get_float().ok_or_else(gen_err)?;
    let lat2 = args[2].get_float().ok_or_else(gen_err)?;
    let lon2 = args[3].get_float().ok_or_else(gen_err)?;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_HAVERSINE_DEG_INPUT, 4, false, false);
pub(crate) fn op_haversine_deg_input(args: &[DataValue]) -> Result<DataValue> {
    let gen_err = || anyhow!("cannot computer haversine distance for {:?}", args);
    let lat1 = args[0].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let lon1 = args[1].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let lat2 = args[2].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let lon2 = args[3].get_float().ok_or_else(gen_err)? * f64::PI() / 180.;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_DEG_TO_RAD, 1, false, false);
pub(crate) fn op_deg_to_rad(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| anyhow!("cannot convert to radian: {:?}", args))?;
    Ok(DataValue::from(x * f64::PI() / 180.))
}

define_op!(OP_RAD_TO_DEG, 1, false, false);
pub(crate) fn op_rad_to_deg(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| anyhow!("cannot convert to degrees: {:?}", args))?;
    Ok(DataValue::from(x * 180. / f64::PI()))
}

define_op!(OP_FIRST, 1, false, false);
pub(crate) fn op_first(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot compute 'first' of {:?}", args))?
        .first()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_LAST, 1, false, false);
pub(crate) fn op_last(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_list()
        .ok_or_else(|| anyhow!("cannot compute 'last' of {:?}", args))?
        .last()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_CHUNKS, 2, false, false);
pub(crate) fn op_chunks(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_chunks_exact(args: &[DataValue]) -> Result<DataValue> {
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
pub(crate) fn op_windows(args: &[DataValue]) -> Result<DataValue> {
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

define_op!(OP_NTH, 2, false, false);
pub(crate) fn op_nth(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| anyhow!("first argument to 'nth' mut be a list, got args {:?}", args))?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'nth' mut be an integer, got args {:?}",
            args
        )
    })?;
    let idx = get_index(n, l.len())?;
    Ok(l[idx].clone())
}

define_op!(OP_MAYBE_NTH, 2, false, false);
pub(crate) fn op_maybe_nth(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_list()
        .ok_or_else(|| anyhow!("first argument to 'nth' mut be a list, got args {:?}", args))?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'nth' mut be an integer, got args {:?}",
            args
        )
    })?;
    if let Ok(idx) = get_index(n, l.len()) {
        Ok(l[idx].clone())
    } else {
        Ok(DataValue::Null)
    }
}

define_op!(OP_SLICE, 3, false, false);
pub(crate) fn op_slice(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0].get_list().ok_or_else(|| {
        anyhow!(
            "first argument to 'slice' mut be a list, got args {:?}",
            args
        )
    })?;
    let m = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'slice' mut be an integer, got args {:?}",
            args
        )
    })?;
    let n = args[2].get_int().ok_or_else(|| {
        anyhow!(
            "third argument to 'slice' mut be an integer, got args {:?}",
            args
        )
    })?;
    let m = get_index(m, l.len())?;
    let n = get_index(n, l.len())?;
    Ok(DataValue::List(l[m..n].to_vec()))
}

define_op!(OP_CHARS, 1, false, false);
pub(crate) fn op_chars(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(
        args[0]
            .get_string()
            .ok_or_else(|| anyhow!("'chars' can only be applied to string, got {:?}", args))?
            .chars()
            .map(|c| {
                let mut s = SmartString::new();
                s.push(c);
                DataValue::String(s)
            })
            .collect_vec(),
    ))
}

define_op!(OP_NTH_CHAR, 2, false, false);
pub(crate) fn op_nth_char(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0].get_string().ok_or_else(|| {
        anyhow!(
            "first argument to 'nth_char' mut be a string, got args {:?}",
            args
        )
    })?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'nth_char' mut be an integer, got args {:?}",
            args
        )
    })?;
    let chars = l.chars().collect_vec();
    let idx = get_index(n, chars.len())?;
    let mut c = SmartString::new();
    c.push(chars[idx]);
    Ok(DataValue::String(c))
}

define_op!(OP_MAYBE_NTH_CHAR, 2, false, false);
pub(crate) fn op_maybe_nth_char(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0].get_string().ok_or_else(|| {
        anyhow!(
            "first argument to 'nth_char' mut be a string, got args {:?}",
            args
        )
    })?;
    let n = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'nth_char' mut be an integer, got args {:?}",
            args
        )
    })?;
    let chars = l.chars().collect_vec();
    if let Ok(idx) = get_index(n, chars.len()) {
        let mut c = SmartString::new();
        c.push(chars[idx]);
        Ok(DataValue::String(c))
    } else {
        Ok(DataValue::Null)
    }
}

define_op!(OP_STR_SLICE, 3, false, false);
pub(crate) fn op_str_slice(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0].get_string().ok_or_else(|| {
        anyhow!(
            "first argument to 'str_slice' mut be a string, got args {:?}",
            args
        )
    })?;
    let m = args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'str_slice' mut be an integer, got args {:?}",
            args
        )
    })?;
    let n = args[2].get_int().ok_or_else(|| {
        anyhow!(
            "third argument to 'str_slice' mut be an integer, got args {:?}",
            args
        )
    })?;
    let l = l.chars().collect_vec();
    let m = get_index(m, l.len())?;
    let n = get_index(n, l.len())?;
    let ret: String = l[m..n].iter().collect();
    Ok(DataValue::String(ret.into()))
}

define_op!(OP_ENCODE_BASE64, 1, false, false);
pub(crate) fn op_encode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(b) => {
            let s = base64::encode(b);
            Ok(DataValue::String(s.into()))
        }
        v => bail!("'encode_base64' can only be applied to bytes, got {:?}", v),
    }
}

define_op!(OP_DECODE_BASE64, 1, false, false);
pub(crate) fn op_decode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::String(s) => {
            let b = base64::decode(s)?;
            Ok(DataValue::Bytes(b.into()))
        }
        v => bail!("'decode_base64' can only be applied to string, got {:?}", v),
    }
}

define_op!(OP_TO_FLOAT, 1, false, false);
pub(crate) fn op_to_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Number(n) => n.get_float().into(),
        DataValue::String(t) => match t as &str {
            "NAN" => f64::NAN.into(),
            "INFINITY" => f64::INFINITY.into(),
            "NEGATIVE_INFINITY" => f64::NEG_INFINITY.into(),
            s => f64::from_str(s)?.into(),
        },
        v => bail!("'to_float' cannot be applied to {:?}", v),
    })
}

define_op!(OP_RAND_FLOAT, 0, false, false);
pub(crate) fn op_rand_float(_args: &[DataValue]) -> Result<DataValue> {
    Ok(thread_rng().gen::<f64>().into())
}

define_op!(OP_RAND_BERNOULLI, 0, true, false);
pub(crate) fn op_rand_bernoulli(args: &[DataValue]) -> Result<DataValue> {
    let prob = match args.get(0) {
        None => 0.5,
        Some(DataValue::Number(n)) => {
            let f = n.get_float();
            ensure!(
                f >= 0. && f <= 1.,
                "'rand_bernoulli' requires number between 0. and 1., got {}",
                f
            );
            f
        }
        Some(v) => bail!(
            "'rand_bernoulli' requires number between 0. and 1., got {:?}",
            v
        ),
    };
    Ok(DataValue::Bool(thread_rng().gen_bool(prob)))
}

define_op!(OP_RAND_INT, 2, false, false);
pub(crate) fn op_rand_int(args: &[DataValue]) -> Result<DataValue> {
    let lower = &args[0].get_int().ok_or_else(|| {
        anyhow!(
            "first argument to 'rand_int' must be an integer, got args {:?}",
            args
        )
    })?;
    let upper = &args[1].get_int().ok_or_else(|| {
        anyhow!(
            "second argument to 'rand_int' must be an integer, got args {:?}",
            args
        )
    })?;
    Ok(thread_rng().gen_range(*lower..=*upper).into())
}

define_op!(OP_RAND_CHOOSE, 1, false, true);
pub(crate) fn op_rand_choose(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => Ok(l
            .choose(&mut thread_rng())
            .cloned()
            .unwrap_or(DataValue::Null)),
        v => bail!("'rand_choice' can only be applied to list, got {:?}", v),
    }
}
