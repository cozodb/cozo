use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::op::{OP_ADD, OP_DIV, OP_IS_NULL, OP_MUL, OP_POW, OP_SUB};
use crate::data::op_agg::{build_op_count_non_null, OpAgg, OpAggT, OpCountWith};
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) const NAME_OP_SUM: &str = "sum";
pub(crate) const NAME_OP_AVG: &str = "avg";
pub(crate) const NAME_OP_VAR: &str = "var";

pub(crate) fn build_op_sum(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Arc::new(OpSum::default())), a_args, args)
}

pub(crate) fn build_op_avg(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Arc::new(OpAvg::default())), a_args, args)
}

pub(crate) fn build_op_var(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Arc::new(OpVar::default())), a_args, args)
}

#[derive(Default)]
pub struct OpSum {
    total: Mutex<f64>,
}

impl OpAggT for OpSum {
    fn name(&self) -> &str {
        NAME_OP_SUM
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        let mut total = self.total.lock().unwrap();
        *total = 0.;
    }

    fn initialize(&self, _a_args: Vec<StaticValue>) -> Result<()> {
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let arg = args.iter().next().unwrap();
        let to_add = match arg {
            Value::Int(i) => (*i) as f64,
            Value::Float(f) => f.into_inner(),
            Value::Null => return Ok(()),
            v => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.clone().into_static()],
                )
                .into())
            }
        };
        *self.total.lock().unwrap() += to_add;
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let f = *self.total.lock().unwrap();
        Ok(f.into())
    }
}

#[derive(Default)]
pub struct OpAvg {
    total: Mutex<f64>,
    ct: AtomicUsize,
}

impl OpAggT for OpAvg {
    fn name(&self) -> &str {
        NAME_OP_AVG
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        let mut total = self.total.lock().unwrap();
        *total = 0.;
        self.ct.store(0, Ordering::Relaxed);
    }

    fn initialize(&self, _a_args: Vec<StaticValue>) -> Result<()> {
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let arg = args.iter().next().unwrap();
        let to_add = match arg {
            Value::Int(i) => (*i) as f64,
            Value::Float(f) => f.into_inner(),
            Value::Null => return Ok(()),
            v => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.clone().into_static()],
                )
                .into())
            }
        };
        *self.total.lock().unwrap() += to_add;
        self.ct.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let f = *self.total.lock().unwrap();
        let ct = self.ct.load(Ordering::Relaxed);
        Ok((f / ct as f64).into())
    }
}

#[derive(Default)]
pub struct OpVar {
    sum: Mutex<f64>,
    sum_sq: Mutex<f64>,
    ct: AtomicUsize,
}

impl OpAggT for OpVar {
    fn name(&self) -> &str {
        NAME_OP_AVG
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        *self.sum.lock().unwrap() = 0.;
        *self.sum_sq.lock().unwrap() = 0.;
        self.ct.store(0, Ordering::Relaxed);
    }

    fn initialize(&self, _a_args: Vec<StaticValue>) -> Result<()> {
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let arg = args.iter().next().unwrap();
        let to_add = match arg {
            Value::Int(i) => (*i) as f64,
            Value::Float(f) => f.into_inner(),
            Value::Null => return Ok(()),
            v => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.clone().into_static()],
                )
                .into())
            }
        };
        *self.sum.lock().unwrap() += to_add;
        *self.sum_sq.lock().unwrap() += f64::powf(to_add, 2.);
        self.ct.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let sum = *self.sum.lock().unwrap();
        let sum_sq = *self.sum_sq.lock().unwrap();
        let ct = self.ct.load(Ordering::Relaxed) as f64;
        let res = (sum_sq / ct - (sum / ct).powf(2.)) * ct / (ct - 1.);
        Ok(res.into())
    }
}
