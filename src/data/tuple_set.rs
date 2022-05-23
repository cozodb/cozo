use crate::data::eval::{PartialEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::tuple::{OwnTuple, ReifiedTuple};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::result;

#[derive(thiserror::Error, Debug)]
pub enum TupleSetError {
    #[error("table id not allowed: {0}")]
    InvalidTableId(u32),
    #[error("Failed to deserialize {0}")]
    Deser(StaticValue),
}

pub(crate) const MIN_TABLE_ID_BOUND: u32 = 10000;

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Hash)]
pub struct TableId {
    pub(crate) in_root: bool,
    pub(crate) id: u32,
}

impl From<TableId> for StaticValue {
    fn from(tid: TableId) -> Self {
        Value::from(vec![Value::from(tid.in_root), (tid.id as i64).into()])
    }
}

impl<'a> TryFrom<&'a Value<'a>> for TableId {
    type Error = TupleSetError;

    fn try_from(value: &'a Value<'a>) -> result::Result<Self, Self::Error> {
        let make_err = || TupleSetError::Deser(value.clone().to_static());
        let fields = value.get_slice().ok_or_else(make_err)?;
        let in_root = fields
            .get(0)
            .ok_or_else(make_err)?
            .get_bool()
            .ok_or_else(make_err)?;
        let id = fields
            .get(1)
            .ok_or_else(make_err)?
            .get_int()
            .ok_or_else(make_err)? as u32;
        Ok(TableId { in_root, id })
    }
}

impl Debug for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}{}", if self.in_root { 'G' } else { 'L' }, self.id)
    }
}

impl TableId {
    pub(crate) fn new(in_root: bool, id: u32) -> Result<Self> {
        if id <= MIN_TABLE_ID_BOUND {
            Err(TupleSetError::InvalidTableId(id).into())
        } else {
            Ok(TableId { in_root, id })
        }
    }
    pub(crate) fn is_valid(&self) -> bool {
        self.id > MIN_TABLE_ID_BOUND
    }
}

impl From<(bool, u32)> for TableId {
    fn from((in_root, id): (bool, u32)) -> Self {
        Self { in_root, id }
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
pub struct ColId {
    pub(crate) is_key: bool,
    pub(crate) id: usize,
}

impl Debug for ColId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, ".{}{}", if self.is_key { 'K' } else { 'D' }, self.id)
    }
}

impl From<(bool, usize)> for ColId {
    fn from((is_key, id): (bool, usize)) -> Self {
        Self { is_key, id: id }
    }
}

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq)]
pub struct TupleSetIdx {
    pub(crate) is_key: bool,
    pub(crate) t_set: usize,
    pub(crate) col_idx: usize,
}

impl Debug for TupleSetIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "@{}{}{}",
            self.t_set,
            if self.is_key { 'K' } else { 'D' },
            self.col_idx
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct TupleSet {
    keys: Vec<ReifiedTuple>,
    vals: Vec<ReifiedTuple>,
}

impl TupleSet {
    pub(crate) fn push_key(&mut self, t: ReifiedTuple) {
        self.keys.push(t);
    }
    pub(crate) fn push_val(&mut self, v: ReifiedTuple) {
        self.vals.push(v);
    }
    pub(crate) fn merge(&mut self, o: TupleSet) {
        self.keys.extend(o.keys);
        self.vals.extend(o.vals);
    }
    pub(crate) fn extend_keys<I, T>(&mut self, keys: I)
    where
        I: IntoIterator<Item = T>,
        ReifiedTuple: From<T>,
    {
        self.keys.extend(keys.into_iter().map(ReifiedTuple::from));
    }
    pub(crate) fn extend_vals<I, T>(&mut self, keys: I)
    where
        I: IntoIterator<Item = T>,
        ReifiedTuple: From<T>,
    {
        self.vals.extend(keys.into_iter().map(ReifiedTuple::from));
    }

    pub(crate) fn all_keys_eq(&self, other: &Self) -> bool {
        if self.keys.len() != other.keys.len() {
            return false;
        }
        for (l, r) in self.keys.iter().zip(&other.keys) {
            if !l.key_part_eq(r) {
                return false;
            }
        }
        true
    }
    pub(crate) fn all_keys_cmp(&self, other: &Self) -> Ordering {
        for (l, r) in self.keys.iter().zip(&other.keys) {
            match l.key_part_cmp(r) {
                Ordering::Equal => {}
                v => return v,
            }
        }
        Ordering::Equal
    }

    pub(crate) fn get_value(
        &self,
        TupleSetIdx {
            is_key,
            t_set,
            col_idx,
        }: &TupleSetIdx,
    ) -> Result<Value> {
        let tuples = if *is_key { &self.keys } else { &self.vals };
        let tuple = tuples.get(*t_set);
        match tuple {
            None => Ok(Value::Null),
            Some(tuple) => {
                let res = tuple.get(*col_idx)?;
                Ok(res)
            }
        }
    }
}

impl<I1, T1, I2, T2> From<(I1, I2)> for TupleSet
where
    I1: IntoIterator<Item = T1>,
    ReifiedTuple: From<T1>,
    I2: IntoIterator<Item = T2>,
    ReifiedTuple: From<T2>,
{
    fn from((keys, vals): (I1, I2)) -> Self {
        TupleSet {
            keys: keys.into_iter().map(ReifiedTuple::from).collect(),
            vals: vals.into_iter().map(ReifiedTuple::from).collect(),
        }
    }
}

impl RowEvalContext for TupleSet {
    fn resolve(&self, idx: &TupleSetIdx) -> Result<Value> {
        let val = self.get_value(idx)?;
        Ok(val)
    }
}

pub(crate) type TupleBuilder<'a> = Vec<(Expr<'a>, Typing)>;

impl TupleSet {
    pub(crate) fn eval_to_tuple(&self, prefix: u32, builder: &TupleBuilder) -> Result<OwnTuple> {
        let mut target = OwnTuple::with_prefix(prefix);
        for (expr, typing) in builder {
            let value = expr.row_eval(self)?;
            let value = typing.coerce(value)?;
            target.push_value(&value);
        }
        Ok(target)
    }
}

pub(crate) type BindingMap = BTreeMap<String, BTreeMap<String, TupleSetIdx>>;

pub(crate) struct BindingMapEvalContext<'a, T: PartialEvalContext + 'a> {
    pub(crate) map: &'a BindingMap,
    pub(crate) parent: &'a T,
}

impl<'a, T: PartialEvalContext + 'a> PartialEvalContext for BindingMapEvalContext<'a, T> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        match self.map.get(key) {
            None => self.parent.resolve(key),
            Some(d) => {
                let d = d
                    .iter()
                    .map(|(k, v)| (k.clone(), Expr::TupleSetIdx(*v)))
                    .collect();
                Some(Expr::Dict(d))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::tuple::OwnTuple;
    use std::mem;

    #[test]
    fn sizes() {
        let t = OwnTuple::with_prefix(0);
        let t2 = OwnTuple::with_prefix(0);
        let ts = TupleSet::from(([t], [t2]));
        dbg!(ts);
        dbg!(mem::size_of::<ReifiedTuple>());
        dbg!(mem::size_of::<TupleSet>());
    }
}
