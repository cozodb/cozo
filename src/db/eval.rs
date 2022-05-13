use crate::db::cnf_transform::{cnf_transform, extract_tables};
use crate::db::engine::Session;
use crate::db::plan::AccessorMap;
use crate::db::table::{ColId, TableId};
use crate::error::CozoError::{InvalidArgument, LogicError};
use crate::error::{CozoError, Result};
use crate::relation::data::DataKind;
use crate::relation::table::MegaTuple;
use crate::relation::value;
use crate::relation::value::Value;
use std::borrow::Cow;
use std::cmp::{max, min, Ordering};
use std::collections::{BTreeMap, BTreeSet};

pub fn extract_table_ref<'a>(
    tuples: &'a MegaTuple,
    tid: &TableId,
    cid: &ColId,
) -> Result<Value<'a>> {
    let targets = if cid.is_key {
        &tuples.keys
    } else {
        &tuples.vals
    };
    let target = targets.get(tid.id as usize).ok_or_else(|| {
        LogicError(format!(
            "Tuple ref out of bound: wanted {:?} for {}",
            tid,
            targets.len()
        ))
    })?;
    if matches!(target.data_kind(), Ok(DataKind::Empty)) {
        Ok(Value::Null)
    } else {
        target
            .get(cid.id as usize)
            .ok_or_else(|| LogicError("Tuple ref out of bound".to_string()))
    }
}

pub fn compare_tuple_by_keys<'a>(
    left: (&'a MegaTuple, &'a [(TableId, ColId)]),
    right: (&'a MegaTuple, &'a [(TableId, ColId)]),
) -> Result<Ordering> {
    for ((l_tid, l_cid), (r_tid, r_cid)) in left.1.iter().zip(right.1) {
        let left_val = extract_table_ref(left.0, l_tid, l_cid)?;
        let right_val = extract_table_ref(right.0, r_tid, r_cid)?;
        match left_val.cmp(&right_val) {
            Ordering::Equal => {}
            v => return Ok(v),
        }
    }
    Ok(Ordering::Equal)
}

pub fn tuple_eval<'a>(value: &'a Value<'a>, tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let res: Value = match value {
        v @ (Value::Null
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::Uuid(_)
        | Value::Text(_)) => v.clone(),
        Value::List(l) => {
            let l = l
                .iter()
                .map(|v| tuple_eval(v, tuples))
                .collect::<Result<Vec<_>>>()?;
            Value::List(l)
        }
        Value::Dict(d) => {
            let d = d
                .iter()
                .map(|(k, v)| tuple_eval(v, tuples).map(|v| (k.clone(), v)))
                .collect::<Result<BTreeMap<_, _>>>()?;
            Value::Dict(d)
        }
        Value::Variable(v) => {
            return Err(LogicError(format!("Cannot resolve variable {}", v)));
        }
        Value::TupleRef(tid, cid) => {
            extract_table_ref(tuples, tid, cid)?
            // let targets = if cid.is_key { &tuples.keys } else { &tuples.vals };
            // let target = targets.get(tid.id as usize).ok_or_else(|| {
            //     LogicError("Tuple ref out of bound".to_string())
            // })?;
            // if matches!(target.data_kind(), Ok(DataKind::Empty)) {
            //     Value::Null
            // } else {
            //     target.get(cid.id as usize)
            //         .ok_or_else(|| LogicError("Tuple ref out of bound".to_string()))?
            // }
        }
        Value::Apply(op, args) => match op.as_ref() {
            value::OP_STR_CAT => str_cat_values(args, tuples)?,
            value::OP_ADD => add_values(args, tuples)?,
            value::OP_SUB => sub_values(args, tuples)?,
            value::OP_MUL => mul_values(args, tuples)?,
            value::OP_DIV => div_values(args, tuples)?,
            value::OP_EQ => eq_values(args, tuples)?,
            value::OP_NE => ne_values(args, tuples)?,
            value::OP_OR => or_values(args, tuples)?,
            value::OP_AND => and_values(args, tuples)?,
            value::OP_MOD => mod_values(args, tuples)?,
            value::OP_GT => gt_values(args, tuples)?,
            value::OP_GE => ge_values(args, tuples)?,
            value::OP_LT => lt_values(args, tuples)?,
            value::OP_LE => le_values(args, tuples)?,
            value::OP_POW => pow_values(args, tuples)?,
            value::OP_COALESCE => coalesce_values(args, tuples)?,
            value::OP_NEGATE => negate_values(args, tuples)?,
            value::OP_MINUS => minus_values(args, tuples)?,
            value::METHOD_IS_NULL => is_null_values(args, tuples)?,
            value::METHOD_NOT_NULL => not_null_values(args, tuples)?,
            value::METHOD_CONCAT => concat_values(args, tuples)?,
            value::METHOD_MERGE => merge_values(args, tuples)?,
            _ => {
                todo!()
            }
        },
        Value::FieldAccess(field, arg) => {
            let arg = tuple_eval(arg, tuples)?;
            match arg {
                Value::Dict(mut d) => d.remove(field.as_ref()).unwrap_or(Value::Null),
                _ => return Err(LogicError("Field access failed".to_string())),
            }
        }
        Value::IdxAccess(idx, arg) => {
            let arg = tuple_eval(arg, tuples)?;
            match arg {
                Value::List(mut l) => {
                    if *idx >= l.len() {
                        Value::Null
                    } else {
                        l.swap_remove(*idx)
                    }
                }
                _ => return Err(LogicError("Idx access failed".to_string())),
            }
        }
        Value::EndSentinel => {
            return Err(LogicError("Encountered end sentinel".to_string()));
        }
        Value::DescSort(_) => {
            return Err(LogicError("Encountered desc sort value".to_string()));
        }
    };
    Ok(res)
}

fn coalesce_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    for v in args {
        match tuple_eval(v, tuples)? {
            Value::Null => {}
            v => return Ok(v),
        }
    }
    Ok(Value::Null)
}

fn str_cat_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut ret = String::new();
    for v in args {
        let v = tuple_eval(v, tuples)?;
        match v {
            Value::Null => return Ok(Value::Null),
            Value::Text(s) => ret += s.as_ref(),
            _ => return Err(InvalidArgument),
        }
    }
    Ok(ret.into())
}

fn add_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l + r).into(),
        (Value::Float(l), Value::Int(r)) => (l + (r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) + r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() + r.into_inner()).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn sub_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l - r).into(),
        (Value::Float(l), Value::Int(r)) => (l - (r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) - r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() - r.into_inner()).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn minus_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let left = tuple_eval(args.get(0).unwrap(), tuples)?;
    Ok(match left {
        Value::Int(l) => (-l).into(),
        Value::Float(l) => (-l).into(),
        _ => return Err(CozoError::InvalidArgument),
    })
}

fn negate_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let left = tuple_eval(args.get(0).unwrap(), tuples)?;
    Ok(match left {
        Value::Bool(l) => (!l).into(),
        _ => return Err(CozoError::InvalidArgument),
    })
}

fn is_null_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let left = tuple_eval(args.get(0).unwrap(), tuples)?;
    Ok((left == Value::Null).into())
}

fn not_null_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let left = tuple_eval(args.get(0).unwrap(), tuples)?;
    Ok((left != Value::Null).into())
}

fn pow_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => ((l as f64).powf(r as f64)).into(),
        (Value::Float(l), Value::Int(r)) => ((l.into_inner()).powf(r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64).powf(r.into_inner())).into(),
        (Value::Float(l), Value::Float(r)) => ((l.into_inner()).powf(r.into_inner())).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn gt_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l > r).into(),
        (Value::Float(l), Value::Int(r)) => (l > (r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) > r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l > r).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn lt_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l < r).into(),
        (Value::Float(l), Value::Int(r)) => (l < (r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) < r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l < r).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn ge_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l >= r).into(),
        (Value::Float(l), Value::Int(r)) => (l >= (r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) >= r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l >= r).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn le_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l <= r).into(),
        (Value::Float(l), Value::Int(r)) => (l <= (r as f64).into()).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) <= r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l <= r).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn mod_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l % r).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn mul_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l * r).into(),
        (Value::Float(l), Value::Int(r)) => (l * (r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) * r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() * r.into_inner()).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn div_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok(match (left, right) {
        (Value::Int(l), Value::Int(r)) => (l as f64 / r as f64).into(),
        (Value::Float(l), Value::Int(r)) => (l / (r as f64)).into(),
        (Value::Int(l), Value::Float(r)) => ((l as f64) / r.into_inner()).into(),
        (Value::Float(l), Value::Float(r)) => (l.into_inner() / r.into_inner()).into(),
        (_, _) => return Err(CozoError::InvalidArgument),
    })
}

fn eq_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok((left == right).into())
}

fn ne_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut args = args.iter();
    let left = tuple_eval(args.next().unwrap(), tuples)?;
    if left == Value::Null {
        return Ok(Value::Null);
    }
    let right = tuple_eval(args.next().unwrap(), tuples)?;
    if right == Value::Null {
        return Ok(Value::Null);
    }
    Ok((left != right).into())
}

fn or_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut accum = -1;
    for v in args.iter() {
        let v = tuple_eval(v, tuples)?;
        match v {
            Value::Null => accum = max(accum, 0),
            Value::Bool(false) => {}
            Value::Bool(true) => return Ok(true.into()),
            _ => return Err(CozoError::InvalidArgument),
        }
    }
    Ok(match accum {
        -1 => false.into(),
        0 => Value::Null,
        _ => unreachable!(),
    })
}

fn concat_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut coll = vec![];
    for v in args.iter() {
        let v = tuple_eval(v, tuples)?;
        match v {
            Value::Null => {}
            Value::List(l) => coll.extend(l),
            _ => return Err(CozoError::InvalidArgument),
        }
    }
    Ok(coll.into())
}

fn merge_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut coll = BTreeMap::new();
    for v in args.iter() {
        let v = tuple_eval(v, tuples)?;
        match v {
            Value::Null => {}
            Value::Dict(d) => coll.extend(d),
            _ => return Err(CozoError::InvalidArgument),
        }
    }
    Ok(coll.into())
}

fn and_values<'a>(args: &'a [Value<'a>], tuples: &'a MegaTuple) -> Result<Value<'a>> {
    let mut accum = 1;
    for v in args.iter() {
        let v = tuple_eval(v, tuples)?;
        match v {
            Value::Null => accum = min(accum, 0),
            Value::Bool(true) => {}
            Value::Bool(false) => return Ok(false.into()),
            _ => return Err(CozoError::InvalidArgument),
        }
    }
    Ok(match accum {
        1 => true.into(),
        0 => Value::Null,
        _ => unreachable!(),
    })
}

impl<'s> Session<'s> {
    pub fn partial_cnf_eval<'a>(
        &self,
        mut value: Value<'a>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        loop {
            let (ev, new_v) = self.partial_eval(value.clone(), params, table_bindings)?;
            let new_v = cnf_transform(new_v.clone());
            if new_v == value {
                return Ok((ev, new_v));
            } else {
                value = new_v
            }
        }
    }

    pub fn cnf_with_table_refs<'a>(
        &self,
        value: Value<'a>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<BTreeMap<BTreeSet<TableId>, Value<'a>>> {
        let (_, value) = self.partial_cnf_eval(value, params, table_bindings)?;
        let conjunctives;
        if let Value::Apply(op, args) = value {
            if op == value::OP_AND {
                conjunctives = args;
            } else {
                conjunctives = vec![Value::Apply(op, args)];
            }
        } else {
            conjunctives = vec![value]
        }
        let grouped = conjunctives
            .into_iter()
            .fold(BTreeMap::new(), |mut coll, v| {
                let tids = extract_tables(&v);
                let ent = coll.entry(tids).or_insert(vec![]);
                ent.push(v);
                coll
            })
            .into_iter()
            .map(|(k, mut v)| {
                let v = match v.len() {
                    0 => Value::Bool(true),
                    1 => v.pop().unwrap(),
                    _ => Value::Apply(value::OP_AND.into(), v),
                };
                (k, v)
            })
            .collect::<BTreeMap<_, _>>();
        Ok(grouped)
    }

    pub fn partial_eval<'a>(
        &self,
        value: Value<'a>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        match value {
            v @ (Value::Null
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Uuid(_)
            | Value::Text(_)
            | Value::EndSentinel) => Ok((true, v)),
            v @ Value::TupleRef(_, _) => Ok((false, v)),
            Value::List(l) => {
                let init_vec = Vec::with_capacity(l.len());
                let res: Result<(bool, Vec<Value>)> =
                    l.into_iter()
                        .try_fold((true, init_vec), |(is_evaluated, mut accum), val| {
                            let (ev, new_val) = self.partial_eval(val, params, table_bindings)?;
                            accum.push(new_val);
                            Ok((ev && is_evaluated, accum))
                        });
                let (is_ev, v) = res?;
                Ok((is_ev, v.into()))
            }
            Value::Dict(d) => {
                let res: Result<(bool, BTreeMap<Cow<str>, Value>)> = d.into_iter().try_fold(
                    (true, BTreeMap::new()),
                    |(is_evaluated, mut accum), (k, v)| {
                        let (ev, new_val) = self.partial_eval(v, params, table_bindings)?;
                        accum.insert(k, new_val);
                        Ok((ev && is_evaluated, accum))
                    },
                );
                let (is_ev, v) = res?;
                Ok((is_ev, v.into()))
            }
            Value::Variable(v) => {
                if v.starts_with('$') {
                    Ok(if let Some(d) = params.get(v.as_ref()) {
                        (true, d.clone())
                    } else {
                        (false, Value::Variable(v))
                    })
                } else {
                    Ok(match self.resolve_value(&v)? {
                        None => (false, Value::Variable(v)),
                        Some(rs) => (rs.is_evaluated(), rs.to_static()),
                    })
                }
            }
            Value::FieldAccess(field, arg) => {
                // convert to tuple refs
                if let Value::Variable(v) = &*arg {
                    if let Some(sub_dict) = table_bindings.get(v.as_ref()) {
                        return match sub_dict.get(field.as_ref()) {
                            None => Err(LogicError(
                                "Cannot resolve field in bound table".to_string(),
                            )),
                            Some(d) => Ok((false, Value::TupleRef(d.0, d.1))),
                        };
                    }
                }

                // normal evaluation flow
                let (_is_ev, arg) = self.partial_eval(*arg, params, table_bindings)?;
                match arg {
                    v @ (Value::Variable(_)
                    | Value::IdxAccess(_, _)
                    | Value::FieldAccess(_, _)
                    | Value::Apply(_, _)) => Ok((false, Value::FieldAccess(field, v.into()))),
                    Value::Dict(mut d) => Ok(d
                        .remove(field.as_ref())
                        .map(|v| (v.is_evaluated(), v))
                        .unwrap_or((true, Value::Null))),
                    _ => Err(LogicError("Field access failed".to_string())),
                }
            }
            Value::IdxAccess(idx, arg) => {
                let (_is_ev, arg) = self.partial_eval(*arg, params, table_bindings)?;
                match arg {
                    v @ (Value::Variable(_)
                    | Value::IdxAccess(_, _)
                    | Value::FieldAccess(_, _)
                    | Value::Apply(_, _)) => Ok((false, Value::IdxAccess(idx, v.into()))),
                    Value::List(mut l) => {
                        if idx >= l.len() {
                            Ok((true, Value::Null))
                        } else {
                            let v = l.swap_remove(idx);
                            Ok((v.is_evaluated(), v))
                        }
                    }
                    _ => Err(LogicError("Idx access failed".to_string())),
                }
            }
            Value::Apply(op, args) => Ok(match op.as_ref() {
                value::OP_STR_CAT => self.str_cat_values_partial(args, params, table_bindings)?,
                value::OP_ADD => self.add_values_partial(args, params, table_bindings)?,
                value::OP_SUB => self.sub_values_partial(args, params, table_bindings)?,
                value::OP_MUL => self.mul_values_partial(args, params, table_bindings)?,
                value::OP_DIV => self.div_values_partial(args, params, table_bindings)?,
                value::OP_EQ => self.eq_values_partial(args, params, table_bindings)?,
                value::OP_NE => self.ne_values_partial(args, params, table_bindings)?,
                value::OP_OR => self.or_values_partial(args, params, table_bindings)?,
                value::OP_AND => self.and_values_partial(args, params, table_bindings)?,
                value::OP_MOD => self.mod_values_partial(args, params, table_bindings)?,
                value::OP_GT => self.gt_values_partial(args, params, table_bindings)?,
                value::OP_GE => self.ge_values_partial(args, params, table_bindings)?,
                value::OP_LT => self.lt_values_partial(args, params, table_bindings)?,
                value::OP_LE => self.le_values_partial(args, params, table_bindings)?,
                value::OP_POW => self.pow_values_partial(args, params, table_bindings)?,
                value::OP_COALESCE => self.coalesce_values_partial(args, params, table_bindings)?,
                value::OP_NEGATE => self.negate_values_partial(args, params, table_bindings)?,
                value::OP_MINUS => self.minus_values_partial(args, params, table_bindings)?,
                value::METHOD_IS_NULL => {
                    self.is_null_values_partial(args, params, table_bindings)?
                }
                value::METHOD_NOT_NULL => {
                    self.not_null_values_partial(args, params, table_bindings)?
                }
                value::METHOD_CONCAT => self.concat_values_partial(args, params, table_bindings)?,
                value::METHOD_MERGE => self.merge_values_partial(args, params, table_bindings)?,
                _ => {
                    todo!()
                }
            }),
            Value::DescSort(_) => {
                return Err(LogicError("Cannot process desc value".to_string()));
            }
        }
    }
}

impl<'s> Session<'s> {
    fn coalesce_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let res = args.into_iter().try_fold(vec![], |mut accum, cur| {
            match self.partial_eval(cur, params, table_bindings) {
                Ok((ev, cur)) => {
                    if ev {
                        if cur == Value::Null {
                            Ok(accum)
                        } else {
                            accum.push(cur);
                            Err(Ok(accum))
                        }
                    } else {
                        accum.push(cur);
                        Ok(accum)
                    }
                }
                Err(e) => Err(Err(e)),
            }
        });
        match res {
            Ok(accum) => match accum.len() {
                0 => Ok((true, Value::Null)),
                1 => Ok((false, accum.into_iter().next().unwrap())),
                _ => Ok((false, Value::Apply(value::OP_COALESCE.into(), accum))),
            },
            Err(Ok(accum)) => match accum.len() {
                0 => Ok((true, Value::Null)),
                1 => Ok((true, accum.into_iter().next().unwrap())),
                _ => Ok((false, Value::Apply(value::OP_COALESCE.into(), accum))),
            },
            Err(Err(e)) => Err(e),
        }
    }

    fn str_cat_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((
                false,
                Value::Apply(value::OP_STR_CAT.into(), vec![left, right]),
            ));
        }
        Ok(match (left, right) {
            (Value::Text(l), Value::Text(r)) => (true, (l.to_string() + r.as_ref()).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn add_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_ADD.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l + r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l + (r as f64)).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) + r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l.into_inner() + r.into_inner()).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn sub_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_SUB.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l - r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l - (r as f64)).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) - r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l.into_inner() - r.into_inner()).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn minus_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le {
            return Ok((false, Value::Apply(value::OP_MINUS.into(), vec![left])));
        }
        Ok(match left {
            Value::Int(l) => (true, (-l).into()),
            Value::Float(l) => (true, (-l).into()),
            _ => return Err(CozoError::InvalidArgument),
        })
    }

    fn negate_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le {
            return Ok((false, Value::Apply(value::OP_NEGATE.into(), vec![left])));
        }
        Ok(match left {
            Value::Bool(l) => (true, (!l).into()),
            _ => return Err(CozoError::InvalidArgument),
        })
    }

    fn is_null_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, true.into()));
        }
        if !le {
            return Ok((
                false,
                Value::Apply(value::METHOD_IS_NULL.into(), vec![left]),
            ));
        }
        Ok((true, false.into()))
    }

    fn not_null_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, false.into()));
        }
        if !le {
            return Ok((
                false,
                Value::Apply(value::METHOD_NOT_NULL.into(), vec![left]),
            ));
        }
        Ok((true, true.into()))
    }

    fn pow_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_POW.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, ((l as f64).powf(r as f64)).into()),
            (Value::Float(l), Value::Int(r)) => (true, ((l.into_inner()).powf(r as f64)).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64).powf(r.into_inner())).into()),
            (Value::Float(l), Value::Float(r)) => {
                (true, ((l.into_inner()).powf(r.into_inner())).into())
            }
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn gt_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_GT.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l > r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l > (r as f64).into()).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) > r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l > r).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn lt_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_LT.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l < r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l < (r as f64).into()).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) < r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l < r).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn ge_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_GE.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l >= r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l >= (r as f64).into()).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) >= r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l >= r).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn le_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_LE.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l <= r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l <= (r as f64).into()).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) <= r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l <= r).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn mod_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_MOD.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l % r).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn mul_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_MUL.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l * r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l * (r as f64)).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) * r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l.into_inner() * r.into_inner()).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn div_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_DIV.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Int(l), Value::Int(r)) => (true, (l as f64 / r as f64).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l / (r as f64)).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) / r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l.into_inner() / r.into_inner()).into()),
            (_, _) => return Err(CozoError::InvalidArgument),
        })
    }

    fn eq_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_EQ.into(), vec![left, right])));
        }
        Ok((true, (left == right).into()))
    }

    fn ne_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_NE.into(), vec![left, right])));
        }
        Ok((true, (left != right).into()))
    }

    fn or_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let res = args
            .into_iter()
            .map(|v| self.partial_eval(v, params, table_bindings))
            .try_fold(
                (true, false, vec![]),
                |(is_evaluated, has_null, mut collected), x| {
                    match x {
                        Ok((cur_eval, cur_val)) => {
                            if cur_eval {
                                match cur_val {
                                    Value::Null => Ok((is_evaluated, true, collected)),
                                    Value::Bool(b) => {
                                        if b {
                                            Err(Ok((true, Value::Bool(true)))) // Early return on true
                                        } else {
                                            Ok((is_evaluated, has_null, collected))
                                        }
                                    }
                                    _ => Err(Err(CozoError::InvalidArgument)),
                                }
                            } else {
                                match cur_val {
                                    Value::Null
                                    | Value::Bool(_)
                                    | Value::Int(_)
                                    | Value::Float(_)
                                    | Value::Uuid(_)
                                    | Value::EndSentinel
                                    | Value::Text(_) => unreachable!(),
                                    Value::List(_) | Value::Dict(_) => {
                                        Err(Err(CozoError::InvalidArgument))
                                    }
                                    cur_val @ (Value::Variable(_)
                                    | Value::IdxAccess(_, _)
                                    | Value::FieldAccess(_, _)
                                    | Value::Apply(_, _)) => {
                                        collected.push(cur_val);
                                        Ok((false, has_null, collected))
                                    }
                                    Value::TupleRef(_, _) => {
                                        todo!()
                                    }
                                    Value::DescSort(_) => {
                                        return Err(Err(LogicError("Cannot process desc value".to_string())));
                                    }
                                }
                            }
                        }
                        Err(e) => Err(Err(e)),
                    }
                },
            );
        match res {
            Ok((is_evaluated, has_null, mut unevaluated)) => {
                if is_evaluated {
                    if has_null {
                        Ok((true, Value::Null))
                    } else {
                        Ok((true, Value::Bool(false)))
                    }
                } else {
                    if has_null {
                        unevaluated.push(Value::Null);
                    }
                    Ok((false, Value::Apply(value::OP_OR.into(), unevaluated)))
                }
            }
            Err(Ok(res)) => Ok(res),
            Err(Err(e)) => Err(e),
        }
    }

    fn concat_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut total_ret = vec![];
        let mut cur_ret = vec![];
        let mut evaluated = true;
        for val in args.into_iter() {
            let (ev, val) = self.partial_eval(val, params, table_bindings)?;
            evaluated = ev && evaluated;
            match val {
                Value::Null => {}
                Value::List(l) => {
                    if cur_ret.is_empty() {
                        cur_ret = l;
                    } else {
                        cur_ret.extend(l);
                    }
                }
                v @ (Value::Variable(_)
                | Value::Apply(_, _)
                | Value::FieldAccess(_, _)
                | Value::IdxAccess(_, _)) => {
                    if !cur_ret.is_empty() {
                        total_ret.push(Value::List(cur_ret));
                        cur_ret = vec![];
                    }
                    total_ret.push(v);
                }
                _ => {
                    return Err(LogicError("Cannot concat incompatible types".to_string()));
                }
            }
        }
        if total_ret.is_empty() {
            Ok((evaluated, cur_ret.into()))
        } else {
            if !cur_ret.is_empty() {
                total_ret.push(cur_ret.into());
            }
            Ok((false, Value::Apply(value::METHOD_CONCAT.into(), total_ret)))
        }
    }

    fn merge_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let mut total_ret = vec![];
        let mut cur_ret = BTreeMap::new();
        let mut evaluated = true;
        for val in args.into_iter() {
            let (ev, val) = self.partial_eval(val, params, table_bindings)?;
            evaluated = ev && evaluated;
            match val {
                Value::Null => {}
                Value::Dict(d) => {
                    if cur_ret.is_empty() {
                        cur_ret = d;
                    } else {
                        cur_ret.extend(d);
                    }
                }
                v @ (Value::Variable(_)
                | Value::Apply(_, _)
                | Value::FieldAccess(_, _)
                | Value::IdxAccess(_, _)) => {
                    if !cur_ret.is_empty() {
                        total_ret.push(Value::Dict(cur_ret));
                        cur_ret = BTreeMap::new();
                    }
                    total_ret.push(v);
                }
                _ => {
                    return Err(LogicError("Cannot concat incompatible types".to_string()));
                }
            }
        }
        if total_ret.is_empty() {
            Ok((evaluated, cur_ret.into()))
        } else {
            if !cur_ret.is_empty() {
                total_ret.push(cur_ret.into());
            }
            Ok((false, Value::Apply(value::METHOD_MERGE.into(), total_ret)))
        }
    }

    fn and_values_partial<'a>(
        &self,
        args: Vec<Value<'a>>,
        params: &BTreeMap<String, Value<'a>>,
        table_bindings: &AccessorMap,
    ) -> Result<(bool, Value<'a>)> {
        let res = args
            .into_iter()
            .map(|v| self.partial_eval(v, params, table_bindings))
            .try_fold(
                (true, false, vec![]),
                |(is_evaluated, has_null, mut collected), x| {
                    match x {
                        Ok((cur_eval, cur_val)) => {
                            if cur_eval {
                                match cur_val {
                                    Value::Null => Ok((is_evaluated, true, collected)),
                                    Value::Bool(b) => {
                                        if b {
                                            Ok((is_evaluated, has_null, collected))
                                        } else {
                                            Err(Ok((true, Value::Bool(false)))) // Early return on true
                                        }
                                    }
                                    _ => Err(Err(CozoError::InvalidArgument)),
                                }
                            } else {
                                match cur_val {
                                    Value::Null
                                    | Value::Bool(_)
                                    | Value::Int(_)
                                    | Value::Float(_)
                                    | Value::Uuid(_)
                                    | Value::EndSentinel
                                    | Value::Text(_) => unreachable!(),
                                    Value::List(_) | Value::Dict(_) => {
                                        Err(Err(CozoError::InvalidArgument))
                                    }
                                    cur_val @ (Value::Variable(_)
                                    | Value::IdxAccess(_, _)
                                    | Value::FieldAccess(_, _)
                                    | Value::Apply(_, _)) => {
                                        collected.push(cur_val);
                                        Ok((false, has_null, collected))
                                    }
                                    Value::TupleRef(_, _) => {
                                        todo!()
                                    }
                                    Value::DescSort(_) => {
                                        Err(Err(LogicError("Cannot process desc value".to_string())))
                                    }
                                }
                            }
                        }
                        Err(e) => Err(Err(e)),
                    }
                },
            );
        match res {
            Ok((is_evaluated, has_null, mut unevaluated)) => {
                if is_evaluated {
                    if has_null {
                        Ok((true, Value::Null))
                    } else {
                        Ok((true, Value::Bool(true)))
                    }
                } else {
                    if has_null {
                        unevaluated.push(Value::Null);
                    }
                    Ok((false, Value::Apply(value::OP_AND.into(), unevaluated)))
                }
            }
            Err(Ok(res)) => Ok(res),
            Err(Err(e)) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::engine::Engine;
    use crate::parser::{Parser, Rule};
    use crate::relation::tuple::Tuple;
    use pest::Parser as PestParser;
    use std::fs;

    #[test]
    fn node() {
        let s = r#"
            create node "Person" {
                *id: Int,
                name: Text,
                email: ?Text,
                habits: ?[?Text]
            }

            create edge (Person)-[Friend]->(Person) {
                relation: ?Text
            }

            create type XXY = {me: Int, f: ?[Text]}

            local assoc WorkInfo: Person {
                email: Text
            }
        "#;
        let db_path = "_test_node";
        {
            let engine = Engine::new(db_path.to_string(), true).unwrap();
            let mut env = engine.session().unwrap();

            let mut parsed = Parser::parse(Rule::file, s).unwrap();

            let t = parsed.next().unwrap();
            env.run_definition(t).unwrap();
            println!("{:?}", env.resolve("Person"));

            let t = parsed.next().unwrap();
            env.run_definition(t).unwrap();
            println!("{:?}", env.resolve("Friend"));

            let t = parsed.next().unwrap();
            env.run_definition(t).unwrap();
            println!("{:?}", env.resolve("XXY"));

            let t = parsed.next().unwrap();
            env.run_definition(t).unwrap();
            println!("{:?}", env.resolve("WorkInfo"));
            println!("{:?}", env.resolve("Person"));
            env.commit().unwrap();

            let it = env.txn.iterator(false, &env.perm_cf);
            it.to_first();
            while let Some((k, v)) = unsafe { it.pair() } {
                println!("{:?}, {:?}", Tuple::new(k), Tuple::new(v));
                it.next();
            }

            let it = env.txn.iterator(false, &env.temp_cf);
            it.to_first();
            while let Some((k, v)) = unsafe { it.pair() } {
                println!("{:?}, {:?}", Tuple::new(k), Tuple::new(v));
                it.next();
            }
        }
        fs::remove_dir_all(db_path).unwrap();
    }

    #[test]
    fn eval_expr() {
        let db_path = "_test_db_expr_eval";
        let engine = Engine::new(db_path.to_string(), true).unwrap();
        let sess = engine.session().unwrap();

        let parse_expr_from_str = |s: &str| -> (bool, Value) {
            let (b, v) = sess
                .partial_eval(
                    Value::from_pair(Parser::parse(Rule::expr, s).unwrap().next().unwrap())
                        .unwrap(),
                    &Default::default(),
                    &Default::default(),
                )
                .unwrap();
            (b, v.to_static())
        };

        assert_eq!(
            (true, Value::from(1024.1)),
            parse_expr_from_str("1/10+(-2+3)*4^5")
        );
        assert_eq!(
            (true, Value::from(false)),
            parse_expr_from_str("true && false")
        );
        assert_eq!(
            (true, Value::from(true)),
            parse_expr_from_str("true || false")
        );
        assert_eq!(
            (true, Value::from(true)),
            parse_expr_from_str("true || null")
        );
        assert_eq!(
            (true, Value::from(true)),
            parse_expr_from_str("null || true")
        );
        assert_eq!((true, Value::Null), parse_expr_from_str("true && null"));
        let ex = parse_expr_from_str("a + b - 1*2*3*100*c * d");
        println!("{:?} {}", ex.0, ex.1);
        drop(sess);
        drop(engine);
        fs::remove_dir_all(db_path).unwrap();
    }
}
