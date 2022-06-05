use std::rc::Rc;
use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::op::OP_IS_NULL;
use crate::data::op_agg::{OpAgg, OpAggT};
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct OpCountWith {
    total: AtomicUsize,
}

pub(crate) const NAME_OP_COUNT: &str = "count";
pub(crate) const NAME_OP_COUNT_WITH: &str = "count_with";
pub(crate) const NAME_OP_COUNT_NON_NULL: &str = "count_non_null";

pub(crate) fn build_op_count_with(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Rc::new(OpCountWith::default())), a_args, args)
}

pub(crate) fn build_op_count(a_args: Vec<Expr>, _args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(
        OpAgg(Rc::new(OpCountWith::default())),
        a_args,
        vec![Expr::Const(Value::Int(1))],
    )
}

pub(crate) fn build_op_count_non_null(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(
        OpAgg(Rc::new(OpCountWith::default())),
        a_args,
        vec![Expr::IfExpr(
            (
                Expr::BuiltinFn(OP_IS_NULL, args),
                Expr::Const(Value::from(0i64)),
                Expr::Const(Value::from(1i64)),
            )
                .into(),
        )],
    )
}

impl OpAggT for OpCountWith {
    fn name(&self) -> &str {
        NAME_OP_COUNT
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        self.total.swap(0, Ordering::Relaxed);
    }

    fn initialize(&self, _a_args: Vec<StaticValue>) -> Result<()> {
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let arg = args.iter().next().unwrap();
        match arg {
            Value::Int(i) => {
                self.total.fetch_add(*i as usize, Ordering::Relaxed);
                Ok(())
            }
            Value::Null => Ok(()),
            v => Err(EvalError::OpTypeMismatch(
                self.name().to_string(),
                vec![v.clone().into_static()],
            )
            .into()),
        }
    }

    fn get(&self) -> Result<StaticValue> {
        Ok((self.total.load(Ordering::Relaxed) as i64).into())
    }
}
