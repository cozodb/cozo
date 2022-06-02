use crate::data::eval::EvalError;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

#[derive(Default)]
pub(crate) struct OpLag {
    buffer: Mutex<Vec<StaticValue>>,
    ptr: AtomicUsize,
}

pub(crate) const NAME_OP_LAG: &str = "lag";

impl OpAggT for OpLag {
    fn name(&self) -> &str {
        NAME_OP_LAG
    }

    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn reset(&self) {
        self.buffer.lock().unwrap().fill(Value::Null);
        self.ptr.store(0, Ordering::Relaxed);
    }

    fn initialize(&self, a_args: Vec<StaticValue>) -> Result<()> {
        let n = a_args
            .into_iter()
            .next()
            .ok_or_else(|| EvalError::ArityMismatch(self.name().to_string(), 0))?;
        let n = n
            .get_int()
            .ok_or_else(|| EvalError::OpTypeMismatch(self.name().to_string(), vec![]))?;
        let mut buffer = self.buffer.lock().unwrap();
        buffer.clear();
        buffer.resize((n + 1) as usize, Value::Null);
        Ok(())
    }

    fn put(&self, args: &[Value]) -> Result<()> {
        let mut buffer = self.buffer.lock().unwrap();
        let n = buffer.len();
        let mut i = self.ptr.load(Ordering::Relaxed);
        for arg in args {
            buffer[i] = arg.clone().into_static();
            i = (i + 1) % n;
        }
        self.ptr.store(i, Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Result<StaticValue> {
        let i = self.ptr.load(Ordering::Relaxed);
        let buffer = self.buffer.lock().unwrap();
        Ok(buffer.get(i).unwrap().clone())
    }
}
