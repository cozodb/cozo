use std::borrow::Cow;
use std::collections::{BTreeMap};
use crate::db::engine::{Session};
use crate::db::env::TableEnv;
use crate::relation::value::{Value};
use crate::error::{CozoError, Result};
use crate::error::CozoError::LogicError;
use crate::relation::value;

impl<'s> Session<'s> {
    pub fn partial_eval<'a>(&self, value: Value<'a>, params: &BTreeMap<String, Value<'a>>,
                            table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        match value {
            v @ (Value::Null |
            Value::Bool(_) |
            Value::Int(_) |
            Value::Float(_) |
            Value::Uuid(_) |
            Value::Text(_) |
            Value::EndSentinel) => Ok((true, v)),
            Value::List(l) => {
                let init_vec = Vec::with_capacity(l.len());
                let res: Result<(bool, Vec<Value>)> = l.into_iter()
                    .try_fold((true, init_vec), |(is_evaluated, mut accum), val| {
                        let (ev, new_val) = self.partial_eval(val, params, table_bindings)?;
                        accum.push(new_val);
                        Ok((ev && is_evaluated, accum))
                    });
                let (is_ev, v) = res?;
                Ok((is_ev, v.into()))
            }
            Value::Dict(d) => {
                let res: Result<(bool, BTreeMap<Cow<str>, Value>)> = d.into_iter()
                    .try_fold((true, BTreeMap::new()), |(is_evaluated, mut accum), (k, v)| {
                        let (ev, new_val) = self.partial_eval(v, params, table_bindings)?;
                        accum.insert(k, new_val);
                        Ok((ev && is_evaluated, accum))
                    });
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
                        Some(rs) => {
                            (rs.is_evaluated(), rs.to_static())
                        }
                    })
                }
            }
            Value::FieldAccess(field, arg) => {
                match *arg {
                    v @ (Value::Variable(_) |
                    Value::IdxAccess(_, _) |
                    Value::FieldAccess(_, _) |
                    Value::Apply(_, _)) => Ok((false, Value::FieldAccess(field, v.into()))),
                    Value::Dict(mut d) => {
                        Ok(d.remove(field.as_ref())
                            .map(|v| (v.is_evaluated(), v))
                            .unwrap_or((true, Value::Null)))
                    }
                    _ => Err(LogicError("Field access failed".to_string()))
                }
            }
            Value::IdxAccess(idx, arg) => {
                match *arg {
                    v @ (Value::Variable(_) |
                    Value::IdxAccess(_, _) |
                    Value::FieldAccess(_, _) |
                    Value::Apply(_, _)) => Ok((false, Value::IdxAccess(idx, v.into()))),
                    Value::List(mut l) => {
                        if idx >= l.len() {
                            Ok((true, Value::Null))
                        } else {
                            let v = l.swap_remove(idx);
                            Ok((v.is_evaluated(), v))
                        }
                    }
                    _ => Err(LogicError("Idx access failed".to_string()))
                }
            }
            Value::Apply(op, args) => {
                Ok(match op.as_ref() {
                    value::OP_STR_CAT => self.str_cat_values(args, params, table_bindings)?,
                    value::OP_ADD => self.add_values(args, params, table_bindings)?,
                    value::OP_SUB => self.sub_values(args, params, table_bindings)?,
                    value::OP_MUL => self.mul_values(args, params, table_bindings)?,
                    value::OP_DIV => self.div_values(args, params, table_bindings)?,
                    value::OP_EQ => self.eq_values(args, params, table_bindings)?,
                    value::OP_NE => self.ne_values(args, params, table_bindings)?,
                    value::OP_OR => self.or_values(args, params, table_bindings)?,
                    value::OP_AND => self.and_values(args, params, table_bindings)?,
                    value::OP_MOD => self.mod_values(args, params, table_bindings)?,
                    value::OP_GT => self.gt_values(args, params, table_bindings)?,
                    value::OP_GE => self.ge_values(args, params, table_bindings)?,
                    value::OP_LT => self.lt_values(args, params, table_bindings)?,
                    value::OP_LE => self.le_values(args, params, table_bindings)?,
                    value::OP_POW => self.pow_values(args, params, table_bindings)?,
                    value::OP_COALESCE => self.coalesce_values(args, params, table_bindings)?,
                    value::OP_NEGATE => self.negate_values(args, params, table_bindings)?,
                    value::OP_MINUS => self.minus_values(args, params, table_bindings)?,
                    value::METHOD_IS_NULL => self.is_null_values(args, params, table_bindings)?,
                    value::METHOD_NOT_NULL => self.not_null_values(args, params, table_bindings)?,
                    value::METHOD_CONCAT => self.concat_values(args, params, table_bindings)?,
                    value::METHOD_MERGE => self.merge_values(args, params, table_bindings)?,
                    _ => { todo!() }
                })
            }
        }
    }

    fn coalesce_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                           table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let res = args.into_iter().try_fold(vec![], |mut accum, cur| {
            match self.partial_eval(cur, params, table_bindings) {
                Ok((ev, cur)) => {
                    if ev {
                        if cur == Value::Null {
                            Ok(accum)
                        } else {
                            Err(Ok(cur))
                        }
                    } else {
                        accum.push(cur);
                        Ok(accum)
                    }
                }
                Err(e) => Err(Err(e))
            }
        });
        match res {
            Ok(accum) => {
                match accum.len() {
                    0 => Ok((true, Value::Null)),
                    1 => Ok((false, accum.into_iter().next().unwrap())),
                    _ => Ok((false, Value::Apply(value::OP_COALESCE.into(), accum)))
                }
            }
            Err(Ok(v)) => Ok((true, v)),
            Err(Err(e)) => Err(e)
        }
    }
    fn str_cat_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                          table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        let (re, right) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null || right == Value::Null {
            return Ok((true, Value::Null));
        }
        if !le || !re {
            return Ok((false, Value::Apply(value::OP_STR_CAT.into(), vec![left, right])));
        }
        Ok(match (left, right) {
            (Value::Text(l), Value::Text(r)) => (true, (l.to_string() + r.as_ref()).into()),
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn add_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn sub_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn minus_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                        table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            _ => return Err(CozoError::InvalidArgument)
        })
    }
    fn negate_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                         table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            _ => return Err(CozoError::InvalidArgument)
        })
    }
    fn is_null_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                          table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, true.into()));
        }
        if !le {
            return Ok((false, Value::Apply(value::METHOD_IS_NULL.into(), vec![left])));
        }
        Ok((true, false.into()))
    }
    fn not_null_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                           table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let mut args = args.into_iter();
        let (le, left) = self.partial_eval(args.next().unwrap(), params, table_bindings)?;
        if left == Value::Null {
            return Ok((true, false.into()));
        }
        if !le {
            return Ok((false, Value::Apply(value::METHOD_NOT_NULL.into(), vec![left])));
        }
        Ok((true, true.into()))
    }
    fn pow_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (Value::Float(l), Value::Float(r)) => (true, ((l.into_inner()).powf(r.into_inner())).into()),
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn gt_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn lt_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn ge_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn le_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (Value::Int(l), Value::Int(r)) => (true, (l <= r).into()),
            (Value::Float(l), Value::Int(r)) => (true, (l <= (r as f64).into()).into()),
            (Value::Int(l), Value::Float(r)) => (true, ((l as f64) <= r.into_inner()).into()),
            (Value::Float(l), Value::Float(r)) => (true, (l <= r).into()),
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn mod_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn mul_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn div_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
            (_, _) => return Err(CozoError::InvalidArgument)
        })
    }
    fn eq_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
    fn ne_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
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
    fn or_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                     table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let res = args.into_iter().map(|v| self.partial_eval(v, params, table_bindings))
            .try_fold(
                (true, false, vec![]),
                |(is_evaluated, has_null, mut collected), x| {
                    match x {
                        Ok((cur_eval, cur_val)) => {
                            if cur_eval {
                                match cur_val {
                                    Value::Null => {
                                        Ok((is_evaluated, true, collected))
                                    }
                                    Value::Bool(b) => if b {
                                        Err(Ok((true, Value::Bool(true)))) // Early return on true
                                    } else {
                                        Ok((is_evaluated, has_null, collected))
                                    },
                                    _ => Err(Err(CozoError::InvalidArgument))
                                }
                            } else {
                                match cur_val {
                                    Value::Null |
                                    Value::Bool(_) |
                                    Value::Int(_) |
                                    Value::Float(_) |
                                    Value::Uuid(_) |
                                    Value::EndSentinel |
                                    Value::Text(_) => unreachable!(),
                                    Value::List(_) |
                                    Value::Dict(_) => Err(Err(CozoError::InvalidArgument)),
                                    cur_val @ (Value::Variable(_) |
                                    Value::IdxAccess(_, _) |
                                    Value::FieldAccess(_, _) |
                                    Value::Apply(_, _)) => {
                                        collected.push(cur_val);
                                        Ok((false, has_null, collected))
                                    }
                                }
                            }
                        }
                        Err(e) => Err(Err(e))
                    }
                });
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
            Err(Err(e)) => Err(e)
        }
    }
    fn concat_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                         table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let mut total_ret = vec![];
        let mut cur_ret = vec![];
        let mut evaluated = true;
        for val in args.into_iter() {
            let (ev, val) = self.partial_eval(val, params, table_bindings)?;
            evaluated = ev && evaluated;
            match val {
                Value::List(l) => {
                    if cur_ret.is_empty() {
                        cur_ret = l;
                    } else {
                        cur_ret.extend(l);
                    }
                }
                v @ (Value::Variable(_) |
                Value::Apply(_, _) |
                Value::FieldAccess(_, _) |
                Value::IdxAccess(_, _)) => {
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
    fn merge_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                        table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let mut total_ret = vec![];
        let mut cur_ret = BTreeMap::new();
        let mut evaluated = true;
        for val in args.into_iter() {
            let (ev, val) = self.partial_eval(val, params, table_bindings)?;
            evaluated = ev && evaluated;
            match val {
                Value::Dict(d) => {
                    if cur_ret.is_empty() {
                        cur_ret = d;
                    } else {
                        cur_ret.extend(d);
                    }
                }
                v @ (Value::Variable(_) |
                Value::Apply(_, _) |
                Value::FieldAccess(_, _) |
                Value::IdxAccess(_, _)) => {
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
    fn and_values<'a>(&self, args: Vec<Value<'a>>, params: &BTreeMap<String, Value<'a>>,
                      table_bindings: &TableEnv) -> Result<(bool, Value<'a>)> {
        let res = args.into_iter().map(|v| self.partial_eval(v, params, table_bindings))
            .try_fold(
                (true, false, vec![]),
                |(is_evaluated, has_null, mut collected), x| {
                    match x {
                        Ok((cur_eval, cur_val)) => {
                            if cur_eval {
                                match cur_val {
                                    Value::Null => {
                                        Ok((is_evaluated, true, collected))
                                    }
                                    Value::Bool(b) => if b {
                                        Ok((is_evaluated, has_null, collected))
                                    } else {
                                        Err(Ok((true, Value::Bool(false)))) // Early return on true
                                    },
                                    _ => Err(Err(CozoError::InvalidArgument))
                                }
                            } else {
                                match cur_val {
                                    Value::Null |
                                    Value::Bool(_) |
                                    Value::Int(_) |
                                    Value::Float(_) |
                                    Value::Uuid(_) |
                                    Value::EndSentinel |
                                    Value::Text(_) => unreachable!(),
                                    Value::List(_) |
                                    Value::Dict(_) => Err(Err(CozoError::InvalidArgument)),
                                    cur_val @ (Value::Variable(_) |
                                    Value::IdxAccess(_, _) |
                                    Value::FieldAccess(_, _) |
                                    Value::Apply(_, _)) => {
                                        collected.push(cur_val);
                                        Ok((false, has_null, collected))
                                    }
                                }
                            }
                        }
                        Err(e) => Err(Err(e))
                    }
                });
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
            Err(Err(e)) => Err(e)
        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;
    use crate::parser::{Parser, Rule};
    use pest::Parser as PestParser;
    use crate::db::engine::Engine;
    use crate::relation::tuple::Tuple;

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
            for (k, v) in it.iter() {
                println!("{:?}, {:?}", Tuple::new(k), Tuple::new(v));
            }

            let it = env.txn.iterator(false, &env.temp_cf);
            it.to_first();
            for (k, v) in it.iter() {
                println!("{:?}, {:?}", Tuple::new(k), Tuple::new(v));
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
            let (b, v) = sess.partial_eval(
                Value::from_pair(Parser::parse(Rule::expr, s)
                    .unwrap().next().unwrap()).unwrap(),
                &Default::default(), &Default::default()).unwrap();
            (b, v.to_static())
        };

        assert_eq!((true, Value::from(1024.1)), parse_expr_from_str("1/10+(-2+3)*4^5"));
        assert_eq!((true, Value::from(false)), parse_expr_from_str("true && false"));
        assert_eq!((true, Value::from(true)), parse_expr_from_str("true || false"));
        assert_eq!((true, Value::from(true)), parse_expr_from_str("true || null"));
        assert_eq!((true, Value::from(true)), parse_expr_from_str("null || true"));
        assert_eq!((true, Value::Null), parse_expr_from_str("true && null"));
        let ex = parse_expr_from_str("a + b - 1*2*3*100*c * d");
        println!("{:?} {}", ex.0, ex.1);
        drop(sess);
        drop(engine);
        fs::remove_dir_all(db_path).unwrap();
    }

    #[test]
    fn table_env() {
        let mut tenv = TableEnv::default();
        tenv.current.insert("c".into(), ());
        let child = tenv.derive();
        let mut another = child.derive();
        another.current.insert("a".into(), ());
        println!("{:?}", another.resolve("c"));
        println!("{:?}", another.resolve("a"));
        println!("{:?}", another.resolve("d"));
    }
}

//     fn test_null_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         Ok(match exprs {
//             [a] => {
//                 match self.visit_expr(a)? {
//                     Const(Null) => Const(Bool(true)),
//                     Const(_) => Const(Bool(false)),
//                     v => Value::Apply(Op::IsNull, vec![v])
//                 }
//             }
//             _ => unreachable!()
//         })
//     }
//
//     fn not_null_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         Ok(match exprs {
//             [a] => {
//                 match self.visit_expr(a)? {
//                     Const(Null) => Const(Bool(false)),
//                     Const(_) => Const(Bool(true)),
//                     v => Value::Apply(Op::IsNull, vec![v])
//                 }
//             }
//             _ => unreachable!()
//         })
//     }
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn operators() {
//         let ev = Evaluator::new(DummyStorage {}).unwrap();
//
//     }
// }