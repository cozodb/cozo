use crate::data::eval::EvalError;
use crate::data::op_agg::{OpAgg, OpAggT};
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::{Arc, Mutex};
use crate::data::expr::Expr;

#[derive(Default)]
pub(crate) struct OpCollectIf {
    buffer: Mutex<Vec<StaticValue>>,
}

pub(crate) const NAME_OP_COLLECT_IF: &str = "collect_if";
pub(crate) const NAME_OP_COLLECT: &str = "collect";

pub(crate) fn build_op_collect_if(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    Expr::ApplyAgg(OpAgg(Arc::new(OpCollectIf::default())), a_args, args)
}

pub(crate) fn build_op_collect(a_args: Vec<Expr>, args: Vec<Expr>) -> Expr {
    let args = vec![args.into_iter().next().unwrap(), Expr::Const(Value::Bool(true))];
    Expr::ApplyAgg(OpAgg(Arc::new(OpCollectIf::default())), a_args, args)
}

impl OpAggT for OpCollectIf {
    fn name(&self) -> &str {
        NAME_OP_COLLECT_IF
    }

    fn arity(&self) -> Option<usize> {
        Some(2)
    }

    fn reset(&self) {
        self.buffer.lock().unwrap().clear();
    }

    fn initialize(&self, a_args: Vec<StaticValue>) -> Result<()> {
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let mut args = args.iter();
        let val = args.next().ok_or_else(||EvalError::ArityMismatch(self.name().to_string(), 1))?;
        let cond = args.next().ok_or_else(||EvalError::ArityMismatch(self.name().to_string(), 2))?;

        match cond {
            Value::Bool(false) | Value::Null => {
                return Ok(())
            }
            Value::Bool(true) => {},
            v => {
                return Err(EvalError::OpTypeMismatch(
                    self.name().to_string(),
                    vec![v.clone().into_static()],
                )
                    .into())
            }
        }

        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(val.clone().into_static());

        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        Ok(Value::List(self.buffer.lock().unwrap().clone()))
    }
}
