use crate::db::table::TableId;
use crate::relation::value;
use crate::relation::value::Value;
use std::collections::BTreeSet;

pub fn extract_tables(val: &Value) -> BTreeSet<TableId> {
    let mut coll = BTreeSet::new();
    do_extract_tables(val, &mut coll);
    coll
}

fn do_extract_tables(val: &Value, coll: &mut BTreeSet<TableId>) {
    match val {
        Value::Null
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::Uuid(_)
        | Value::Text(_) => {}
        Value::List(l) => {
            for v in l {
                do_extract_tables(v, coll);
            }
        }
        Value::Dict(d) => {
            for v in d.values() {
                do_extract_tables(v, coll);
            }
        }
        Value::Variable(_) => {}
        Value::TupleRef(tid, _cid) => {
            coll.insert(*tid);
        }
        Value::Apply(_, args) => {
            for v in args {
                do_extract_tables(v, coll);
            }
        }
        Value::FieldAccess(_, _) => {}
        Value::IdxAccess(_, _) => {}
        Value::EndSentinel => {}
    }
}

pub fn cnf_transform(mut val: Value) -> Value {
    loop {
        let (changed, new_val) = do_cnf_transform(val);
        if changed {
            val = new_val
        } else {
            break new_val;
        }
    }
}

fn do_cnf_transform(val: Value) -> (bool, Value) {
    if let Value::Apply(op, args) = val {
        match op.as_ref() {
            value::OP_OR => cnf_transform_or(args),
            value::OP_AND => cnf_transform_and(args),
            value::OP_NEGATE => cnf_transform_negate(args.into_iter().next().unwrap()),
            _ => (false, Value::Apply(op, args)),
        }
    } else {
        (false, val)
    }
}

fn cnf_transform_or(args: Vec<Value>) -> (bool, Value) {
    let mut changed = false;
    let mut collected = Vec::with_capacity(args.len());
    let mut to_and = None;
    for v in args {
        let (vc, v) = do_cnf_transform(v);
        changed |= vc;
        if let Value::Apply(op, args) = v {
            match op.as_ref() {
                value::OP_OR => {
                    changed = true;
                    collected.extend(args)
                }
                value::OP_AND => {
                    if to_and == None {
                        changed = true;
                        to_and = Some(args);
                    } else {
                        collected.push(Value::Apply(op, args))
                    }
                }
                _ => collected.push(Value::Apply(op, args)),
            }
        } else {
            collected.push(v);
        }
    }
    if let Some(to_and) = to_and {
        let args = to_and
            .into_iter()
            .map(|v| {
                let mut to_or = collected.clone();
                to_or.push(v);
                Value::Apply(value::OP_OR.into(), to_or)
            })
            .collect();
        (true, Value::Apply(value::OP_AND.into(), args))
    } else if collected.is_empty() {
        (true, true.into())
    } else if collected.len() == 1 {
        (true, collected.pop().unwrap())
    } else {
        (changed, Value::Apply(value::OP_OR.into(), collected))
    }
}

fn cnf_transform_and(args: Vec<Value>) -> (bool, Value) {
    let mut changed = false;
    let mut collected = Vec::with_capacity(args.len());

    for v in args {
        let (vc, v) = do_cnf_transform(v);
        changed |= vc;
        if let Value::Apply(op, args) = v {
            match op.as_ref() {
                value::OP_AND => {
                    changed = true;
                    for v in args {
                        collected.push(v);
                    }
                }
                _ => {
                    collected.push(Value::Apply(op, args));
                }
            }
        } else {
            collected.push(v);
        }
    }
    if collected.is_empty() {
        (true, true.into())
    } else if collected.len() == 1 {
        (true, collected.pop().unwrap())
    } else {
        (changed, Value::Apply(value::OP_AND.into(), collected))
    }
}

fn cnf_transform_negate(arg: Value) -> (bool, Value) {
    if let Value::Apply(op, args) = arg {
        let mut new_args = Vec::with_capacity(args.len());
        let mut changed = false;
        for v in args {
            let (vc, v) = do_cnf_transform(v);
            changed |= vc;
            new_args.push(v);
        }
        match op.as_ref() {
            value::OP_OR => (
                true,
                Value::Apply(
                    value::OP_AND.into(),
                    new_args
                        .into_iter()
                        .map(|v| {
                            let (_, v) = do_cnf_transform(v);
                            Value::Apply(value::OP_NEGATE.into(), vec![v])
                        })
                        .collect(),
                ),
            ),
            value::OP_AND => (
                true,
                Value::Apply(
                    value::OP_OR.into(),
                    new_args
                        .into_iter()
                        .map(|v| {
                            let (_, v) = do_cnf_transform(v);
                            Value::Apply(value::OP_NEGATE.into(), vec![v])
                        })
                        .collect(),
                ),
            ),
            value::OP_NEGATE => (true, new_args.into_iter().next().unwrap()),
            _ => (
                changed,
                Value::Apply(value::OP_NEGATE.into(), vec![Value::Apply(op, new_args)]),
            ),
        }
    } else {
        (false, Value::Apply(value::OP_NEGATE.into(), vec![arg]))
    }
}

#[cfg(test)]
mod tests {
    use crate::db::cnf_transform::cnf_transform;
    use crate::error::Result;
    use crate::relation::value::Value;

    #[test]
    fn test_cnf() -> Result<()> {
        for s in [
            "a",
            "!a",
            "!!a",
            "!!!a",
            "!(b || c)",
            "a && (b && c)",
            "a && b || c",
            "a || b && c && d",
            "a && (b || d && e)",
            "a && !b || c && !!d || !!!e && f",
            "(a || !b || !c) && (!d || e || f)",
            "(a || b) && c",
            "a || b",
        ] {
            let v = Value::parse_str(s)?;
            println!("{}", v);
            let v2 = cnf_transform(v);
            println!("=> {}", v2);
        }

        Ok(())
    }
}
