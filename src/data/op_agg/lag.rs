use crate::data::eval::EvalError;
use crate::data::op_agg::OpAggT;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

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
