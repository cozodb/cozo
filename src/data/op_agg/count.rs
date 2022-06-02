use crate::data::eval::EvalError;
use crate::data::op_agg::OpAggT;
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
