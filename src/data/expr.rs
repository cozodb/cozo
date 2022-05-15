use crate::data::op::Op;
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) enum Expr<'a> {
    Const(Value<'a>),
    List(Vec<Expr<'a>>),
    Dict(BTreeMap<String, Expr<'a>>),
    Variable(String),
    TableCol(TableId, ColId),
    TupleSetIdx(TupleSetIdx),
    Apply(Arc<Op>, Vec<Expr<'a>>),
    FieldAcc(String, Box<Expr<'a>>),
    IdxAcc(usize, Box<Expr<'a>>),
}

pub(crate) type StaticExpr = Expr<'static>;

// TODO serde expr into value
