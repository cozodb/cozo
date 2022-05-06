use crate::relation::value;
use crate::relation::value::Value;

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
            _ => (false, Value::Apply(op, args))
        }
    } else {
        (false, val)
    }
}

fn cnf_transform_or(args: Vec<Value>) -> (bool, Value) {
    todo!()
}

fn cnf_transform_and(args: Vec<Value>) -> (bool, Value) {
    let mut changed = false;
    let mut collected = Vec::with_capacity(args.len());
    for v in args {
        if let Value::Apply(op, args) = v {
            match op.as_ref() {
                value::OP_AND => todo!(),
                _ => todo!()
            }
        } else {
            collected.push(v);
        }
    }
    if collected.is_empty() {
        (true, true.into())
    } else {
        (changed, Value::Apply(value::OP_AND.into(), collected))
    }
}

fn cnf_transform_negate(arg: Value) -> (bool, Value) {
    if let Value::Apply(op, args) = arg {
        match op.as_ref() {
            value::OP_OR => (true, Value::Apply(value::OP_AND.into(), args.into_iter().map(|v| {
                let (_, v) = do_cnf_transform(v);
                Value::Apply(value::OP_NEGATE.into(), vec![v])
            }).collect())),
            value::OP_AND => (true, Value::Apply(value::OP_OR.into(), args.into_iter().map(|v| {
                let (_, v) = do_cnf_transform(v);
                Value::Apply(value::OP_NEGATE.into(), vec![v])
            }).collect())),
            value::OP_NEGATE => {
                let (_, v) = do_cnf_transform(args.into_iter().next().unwrap());
                (true, v)
            },
            _ => (false, Value::Apply(value::OP_NEGATE.into(), vec![Value::Apply(op, args)]))
        }
    } else {
        let (transformed, arg) = do_cnf_transform(arg);
        (transformed, Value::Apply(value::OP_NEGATE.into(), vec![arg]))
    }
}