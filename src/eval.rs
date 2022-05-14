use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use crate::relation::value::Value;
use crate::error::Result;
use crate::relation::table::MegaTuple;
use crate::relation::typing::Typing;

pub struct InterpretContext;

pub struct ApplyContext;

pub struct ArgsIterator<'a> {
    value: Value<'a>
}

pub struct TypingIterator;

pub trait Op {
    fn arity(&self) -> Range<usize>;
    fn apply_raw<'a>(&self, arg: Value<'a>) -> Result<Value<'a>>;
    fn typing(&self, arg_types: TypingIterator) -> Result<Typing>;
    fn apply<'a>(&self, ctx: &ApplyContext, args: ArgsIterator<'a>) -> Result<Value<'a>>;
    fn interpret<'a>(&self, ctx: &InterpretContext, arg: ArgsIterator<'a>) -> Result<(Value<'a>, bool)>;
}

pub trait AggregationOp {

}

// NOTE: the interpreter can hold global states for itself

// Tiers of values:
//
// * Scalars
// * Aggregates
// * Typings
// * RelPlan
// * QueryTemplate

// Global DB should be a C++ sharedptr
// Every session gets its own temp DB
// Sessions are reused after use

// lower sectors layouts
// [env_stack_depth; string_name, flags*] -> resolvable data
// [env_stack_depth; tid, flags*] -> table definitions
// table 10000 for serial numbers (auto incrementing, no transaction)
// tables start at 10001

pub struct DBInstance;

impl DBInstance {
    pub fn get_meta_by_id() {}
    pub fn get_meta_by_name() {}
    pub fn put_meta() {}
}

pub struct InterpreterSession {
    global_db: DBInstance,
    local_db: DBInstance,
    env_depth: usize,
    session_params: BTreeMap<String, Arc<dyn Op>>,
}

impl InterpreterSession {
    pub fn push_env() {}
    pub fn pop_env() {}
    pub fn destroy() {}
}