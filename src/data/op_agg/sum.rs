use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::op::{OP_ADD, OP_DIV, OP_IS_NULL, OP_MUL, OP_POW, OP_SUB};
use crate::data::op_agg::{build_op_count_non_null, OpAgg, OpAggT, OpCountWith};
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct OpSum {
    total: Mutex<f64>,
}

pub(crate) const NAME_OP_SUM: &str = "sum";
pub(crate) const NAME_OP_AVG: &str = "avg";
pub(crate) const NAME_OP_VAR: &str = "var";

pub(crate) fn build_op_sum(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Arc::new(OpSum::default())), a_args, args)
}

pub(crate) fn build_op_avg(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    let op_sum = build_op_sum(a_args.clone(), args.clone());
    let op_count = build_op_count_non_null(a_args, args);
    Expr::BuiltinFn(OP_DIV, vec![op_sum, op_count])
}

pub(crate) fn build_op_var(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    let op_avg = build_op_avg(a_args.clone(), args.clone());
    let op_count = build_op_count_non_null(a_args.clone(), args.clone());
    let sq_args = args
        .clone()
        .into_iter()
        .map(|v| Expr::BuiltinFn(OP_POW, vec![v, Expr::Const((2.).into())]))
        .collect::<Vec<_>>();
    let op_sum_sq = build_op_sum(a_args.clone(), sq_args);
    let avg_sum_sq = Expr::BuiltinFn(OP_DIV, vec![op_sum_sq, op_count]);
    let expr = Expr::BuiltinFn(
        OP_SUB,
        vec![
            avg_sum_sq,
            Expr::BuiltinFn(OP_POW, vec![op_avg, Expr::Const((2.).into())]),
        ],
    );
    let factor = Expr::BuiltinFn(
        OP_DIV,
        vec![
            build_op_count_non_null(a_args.clone(), args.clone()),
            Expr::BuiltinFn(
                OP_SUB,
                vec![
                    build_op_count_non_null(a_args.clone(), args.clone()),
                    Expr::Const(1.into()),
                ],
            ),
        ],
    );
    Expr::BuiltinFn(OP_MUL, vec![factor, expr])
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
