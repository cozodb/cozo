use std::cell::RefCell;
use std::rc::Rc;
use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::op_agg::{OpAgg, OpAggT};
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const NAME_OP_SUM: &str = "sum";
pub(crate) const NAME_OP_AVG: &str = "avg";
pub(crate) const NAME_OP_VAR: &str = "var";

pub(crate) fn build_op_sum(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Rc::new(OpSum::default())), a_args, args)
}

pub(crate) fn build_op_avg(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Rc::new(OpAvg::default())), a_args, args)
}

pub(crate) fn build_op_var(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Rc::new(OpVar::default())), a_args, args)
}

#[derive(Default)]
pub struct OpSum {
    total: RefCell<f64>,
}

impl OpAggT for OpSum {
    fn name(&self) -> &str {
        NAME_OP_SUM
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        let mut total = self.total.borrow_mut();
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
        *self.total.borrow_mut() += to_add;
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let f = *self.total.borrow();
        Ok(f.into())
    }
}

#[derive(Default)]
pub struct OpAvg {
    total: RefCell<f64>,
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
        let mut total = self.total.borrow_mut();
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
        *self.total.borrow_mut() += to_add;
        self.ct.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let f = *self.total.borrow();
        let ct = self.ct.load(Ordering::Relaxed);
        Ok((f / ct as f64).into())
    }
}

#[derive(Default)]
pub struct OpVar {
    sum: RefCell<f64>,
    sum_sq: RefCell<f64>,
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
        *self.sum.borrow_mut() = 0.;
        *self.sum_sq.borrow_mut() = 0.;
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
        *self.sum.borrow_mut() += to_add;
        *self.sum_sq.borrow_mut() += f64::powf(to_add, 2.);
        self.ct.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let sum = *self.sum.borrow();
        let sum_sq = *self.sum_sq.borrow();
        let ct = self.ct.load(Ordering::Relaxed) as f64;
        let res = (sum_sq / ct - (sum / ct).powf(2.)) * ct / (ct - 1.);
        Ok(res.into())
    }
}
