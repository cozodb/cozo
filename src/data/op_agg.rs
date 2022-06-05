mod collect;
mod count;
mod lag;
mod min_max;
mod sum;

use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::ops::Deref;
use std::rc::Rc;

pub(crate) use collect::*;
pub(crate) use count::*;
pub(crate) use lag::*;
pub(crate) use min_max::*;
pub(crate) use sum::*;

#[derive(Clone)]
pub struct OpAgg(pub(crate) Rc<dyn OpAggT>);

impl Deref for OpAgg {
    type Target = Rc<dyn OpAggT>;

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
