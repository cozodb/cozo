use crate::data::eval::EvalError;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct OpAgg(pub(crate) Arc<dyn OpAggT + Send + Sync>);

impl Deref for OpAgg {
    type Target = Arc<dyn OpAggT + Send + Sync>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for OpAgg {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

pub trait OpAggT {
    fn name(&self) -> &str;
    fn arity(&self) -> Option<usize>;
    fn reset(&self);
    fn initialize(&self, a_args: Vec<StaticValue>) -> Result<()>;
    fn put(&self, args: &[Value]) -> Result<()>;
    fn get(&self) -> Result<StaticValue>;
    fn put_get(&self, args: &[Value]) -> Result<StaticValue> {
        self.put(args)?;
        self.get()
    }
}

#[derive(Default)]
pub struct OpSum {
    total: AtomicUsize,
}

pub(crate) const NAME_OP_COUNT: &str = "count";
pub(crate) const NAME_OP_SUM: &str = "sum";

impl OpAggT for OpSum {
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
