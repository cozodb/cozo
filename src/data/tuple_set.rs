use crate::data::eval::{PartialEvalContext, RowEvalContext};
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, ReifiedTuple};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use anyhow::Result;
use cozorocks::{DbPtr, TransactionPtr, WriteOptionsPtr};
use std::cmp::{Ordering};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::result;

#[derive(thiserror::Error, Debug)]
pub enum TupleSetError {
    #[error("table id not allowed: {0}")]
    InvalidTableId(u32),
    #[error("Failed to deserialize {0}")]
    Deser(StaticValue),
    #[error("resolve db on raw tuple set")]
    RawTupleSetDbResolve,
}

pub(crate) const MIN_TABLE_ID_BOUND: u32 = 10000;

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Hash)]
pub struct TableId {
    pub(crate) in_root: bool,
    pub(crate) id: u32,
}

impl TableId {
    pub(crate) fn int_for_storage(&self) -> i64 {
        if self.in_root {
            self.id as i64
        } else {
            -(self.id as i64)
        }
    }
}

impl From<TableId> for StaticValue {
    fn from(tid: TableId) -> Self {
        // Value::from(vec![Value::from(tid.in_root), (tid.id as i64).into()])
        Value::from(tid.int_for_storage())
    }
}

impl<'a> TryFrom<&'a Value<'a>> for TableId {
    type Error = TupleSetError;

    fn try_from(value: &'a Value<'a>) -> result::Result<Self, Self::Error> {
        let make_err = || TupleSetError::Deser(value.clone().into_static());
        let id = value.get_int().ok_or_else(make_err)?;
        Ok(TableId {
            in_root: id > 0,
            id: id.abs() as u32,
        })
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
        Self { is_key, id }
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
    pub(crate) keys: Vec<ReifiedTuple>,
    pub(crate) vals: Vec<ReifiedTuple>,
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
                if matches!(tuple.data_kind(), Ok(DataKind::Empty)) {
                    Ok(Value::Null)
                } else {
                    let res = tuple.get(*col_idx)?;
                    Ok(res)
                }
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

pub(crate) struct TupleSetEvalContext<'a> {
    pub(crate) tuple_set: &'a TupleSet,
    pub(crate) txn: &'a TransactionPtr,
    pub(crate) temp_db: &'a DbPtr,
    pub(crate) write_options: &'a WriteOptionsPtr,
}

impl<'a> RowEvalContext for TupleSetEvalContext<'a> {
    fn resolve(&self, idx: &TupleSetIdx) -> Result<Value> {
        let val = self.tuple_set.get_value(idx)?;
        Ok(val)
    }

    fn get_temp_db(&self) -> Result<&DbPtr> {
        Ok(self.temp_db)
    }

    fn get_txn(&self) -> Result<&TransactionPtr> {
        Ok(self.txn)
    }

    fn get_write_options(&self) -> Result<&WriteOptionsPtr> {
        Ok(self.write_options)
    }
}

impl RowEvalContext for TupleSet {
    fn resolve(&self, idx: &TupleSetIdx) -> Result<Value> {
        let val = self.get_value(idx)?;
        Ok(val)
    }

    fn get_temp_db(&self) -> Result<&DbPtr> {
        Err(TupleSetError::RawTupleSetDbResolve.into())
    }

    fn get_txn(&self) -> Result<&TransactionPtr> {
        Err(TupleSetError::RawTupleSetDbResolve.into())
    }

    fn get_write_options(&self) -> Result<&WriteOptionsPtr> {
        Err(TupleSetError::RawTupleSetDbResolve.into())
    }
}

pub(crate) type TupleBuilder<'a> = Vec<(Expr<'a>, Typing)>;

impl<'a> TupleSetEvalContext<'a> {
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

#[derive(Default, Clone, Debug)]
pub(crate) struct BindingMap {
    pub(crate) inner_map: BTreeMap<String, BTreeMap<String, TupleSetIdx>>,
    pub(crate) key_size: usize,
    pub(crate) val_size: usize,
}

pub(crate) fn merge_binding_maps(bmaps: impl Iterator<Item = BindingMap>) -> BindingMap {
    let mut ret: BindingMap = Default::default();

    for cur in bmaps {
        shift_merge_binding_map(&mut ret, cur);
    }

    ret
}

pub(crate) fn shift_binding_map(right: &mut BindingMap, left: &BindingMap) {
    for vs in right.inner_map.values_mut() {
        for v in vs.values_mut() {
            if v.is_key {
                v.t_set += left.key_size;
            } else {
                v.t_set += left.val_size;
            }
        }
    }
}

pub(crate) fn shift_merge_binding_map(left: &mut BindingMap, mut right: BindingMap) {
    shift_binding_map(&mut right, left);
    left.inner_map.extend(right.inner_map);
    left.key_size += right.key_size;
    left.val_size += right.val_size;
}

pub(crate) struct BindingMapEvalContext<'a, T: PartialEvalContext + 'a> {
    pub(crate) map: &'a BindingMap,
    pub(crate) parent: &'a T,
}

impl<'a, T: PartialEvalContext + 'a> PartialEvalContext for BindingMapEvalContext<'a, T> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        match self.map.inner_map.get(key) {
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
