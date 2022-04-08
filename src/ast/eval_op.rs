use crate::ast::Expr;
use crate::ast::Expr::*;
use crate::ast::op::Op;
use crate::error::CozoError;
use crate::error::CozoError::*;
use crate::value::Value::*;

pub fn add_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            Ok(Const(match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(va), Int(vb)) => Int(va + vb),
                        (Float(va), Int(vb)) => Float(va + vb as f64),
                        (Int(va), Float(vb)) => Float(va as f64 + vb),
                        (Float(va), Float(vb)) => Float(va + vb),
                        (OwnString(va), OwnString(vb)) => OwnString(Box::new(*va + &*vb)),
                        (OwnString(va), RefString(vb)) => OwnString(Box::new(*va + &*vb)),
                        (RefString(va), OwnString(vb)) => OwnString(Box::new(va.to_string() + &*vb)),
                        (RefString(va), RefString(vb)) => OwnString(Box::new(va.to_string() + &*vb)),
                        (_, _) => return Err(CozoError::TypeError)
                    }
                }
                (a, b) => return Ok(Apply(Op::Add, vec![a, b]))
            }))
        }
        _ => unreachable!()
    }
}

pub fn sub_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            Ok(Const(match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(va), Int(vb)) => Int(va - vb),
                        (Float(va), Int(vb)) => Float(va - vb as f64),
                        (Int(va), Float(vb)) => Float(va as f64 - vb),
                        (Float(va), Float(vb)) => Float(va - vb),
                        (_, _) => return Err(CozoError::TypeError)
                    }
                }
                (a, b) => return Ok(Apply(Op::Sub, vec![a, b]))
            }))
        }
        _ => unreachable!()
    }
}

pub fn mul_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            Ok(Const(match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(va), Int(vb)) => Int(va * vb),
                        (Float(va), Int(vb)) => Float(va * vb as f64),
                        (Int(va), Float(vb)) => Float(va as f64 * vb),
                        (Float(va), Float(vb)) => Float(va * vb),
                        (_, _) => return Err(CozoError::TypeError)
                    }
                }
                (a, b) => return Ok(Apply(Op::Mul, vec![a, b]))
            }))
        }
        _ => unreachable!()
    }
}


pub fn div_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            Ok(Const(match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(va), Int(vb)) => Float(va as f64 / vb as f64),
                        (Float(va), Int(vb)) => Float(va / vb as f64),
                        (Int(va), Float(vb)) => Float(va as f64 / vb),
                        (Float(va), Float(vb)) => Float(va / vb),
                        (_, _) => return Err(CozoError::TypeError)
                    }
                }
                (a, b) => return Ok(Apply(Op::Div, vec![a, b]))
            }))
        }
        _ => unreachable!()
    }
}

pub fn mod_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            Ok(Const(match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Int(a % b),
                        (_, _) => return Err(CozoError::TypeError)
                    }
                }
                (a, b) => return Ok(Apply(Op::Mod, vec![a, b]))
            }))
        }
        _ => unreachable!()
    }
}


pub fn eq_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => Ok(Const(Bool(a == b))),
                (a, b) => Ok(Apply(Op::Eq, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}


pub fn ne_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => Ok(Const(Bool(a == b))),
                (a, b) => Ok(Apply(Op::Neq, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}


pub fn gt_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Ok(Const(Bool(a > b))),
                        (Float(a), Int(b)) => Ok(Const(Bool(a > b as f64))),
                        (Int(a), Float(b)) => Ok(Const(Bool(a as f64 > b))),
                        (Float(a), Float(b)) => Ok(Const(Bool(a > b))),
                        (_, _) => Err(CozoError::TypeError)
                    }
                }
                (a, b) => Ok(Apply(Op::Gt, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}


pub fn ge_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Ok(Const(Bool(a >= b))),
                        (Float(a), Int(b)) => Ok(Const(Bool(a >= b as f64))),
                        (Int(a), Float(b)) => Ok(Const(Bool(a as f64 >= b))),
                        (Float(a), Float(b)) => Ok(Const(Bool(a >= b))),
                        (_, _) => Err(CozoError::TypeError)
                    }
                }
                (a, b) => Ok(Apply(Op::Ge, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}


pub fn lt_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Ok(Const(Bool(a < b))),
                        (Float(a), Int(b)) => Ok(Const(Bool(a < b as f64))),
                        (Int(a), Float(b)) => Ok(Const(Bool((a as f64) < b))),
                        (Float(a), Float(b)) => Ok(Const(Bool(a < b))),
                        (_, _) => Err(CozoError::TypeError)
                    }
                }
                (a, b) => Ok(Apply(Op::Lt, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}

pub fn le_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Ok(Const(Bool(a <= b))),
                        (Float(a), Int(b)) => Ok(Const(Bool(a <= b as f64))),
                        (Int(a), Float(b)) => Ok(Const(Bool((a as f64) <= b))),
                        (Float(a), Float(b)) => Ok(Const(Bool(a <= b))),
                        (_, _) => Err(CozoError::TypeError)
                    }
                }
                (a, b) => Ok(Apply(Op::Le, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}


pub fn pow_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) || b == Const(Null) {
                return Ok(Const(Null));
            }
            match (a, b) {
                (Const(a), Const(b)) => {
                    match (a, b) {
                        (Int(a), Int(b)) => Ok(Const(Float((a as f64).powf(b as f64)))),
                        (Float(a), Int(b)) => Ok(Const(Float(a.powi(b as i32)))),
                        (Int(a), Float(b)) => Ok(Const(Float((a as f64).powf(b)))),
                        (Float(a), Float(b)) => Ok(Const(Float(a.powf(b)))),
                        (_, _) => Err(CozoError::TypeError)
                    }
                }
                (a, b) => Ok(Apply(Op::Pow, vec![a, b]))
            }
        }
        _ => unreachable!()
    }
}

pub fn coalesce_exprs<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    match exprs {
        [a, b] => {
            let a = a.eval()?;
            let b = b.eval()?;
            if a == Const(Null) {
                return Ok(b);
            }
            if b == Const(Null) {
                return Ok(a);
            }
            if let a @ Const(_) = a {
                return Ok(a);
            }
            return Ok(Apply(Op::Coalesce, vec![a, b]));
        }
        _ => unreachable!()
    }
}

pub fn negate_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    Ok(match exprs {
        [a] => {
            match a.eval()? {
                Const(Null) => Const(Null),
                Const(Bool(b)) => Const(Bool(!b)),
                Const(_) => return Err(TypeError),
                Apply(Op::Neg, v) => v.into_iter().next().unwrap(),
                Apply(Op::IsNull, v) => Apply(Op::NotNull, v),
                Apply(Op::NotNull, v) => Apply(Op::IsNull, v),
                Apply(Op::Eq, v) => Apply(Op::Neq, v),
                Apply(Op::Neq, v) => Apply(Op::Eq, v),
                Apply(Op::Gt, v) => Apply(Op::Le, v),
                Apply(Op::Ge, v) => Apply(Op::Lt, v),
                Apply(Op::Le, v) => Apply(Op::Gt, v),
                Apply(Op::Lt, v) => Apply(Op::Ge, v),
                v => Apply(Op::Neg, vec![v])
            }
        }
        _ => unreachable!()
    })
}


pub fn minus_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    Ok(match exprs {
        [a] => {
            match a.eval()? {
                Const(Null) => Const(Null),
                Const(Int(i)) => Const(Int(-i)),
                Const(Float(f)) => Const(Float(-f)),
                Const(_) => return Err(TypeError),
                Apply(Op::Minus, v) => v.into_iter().next().unwrap(),
                v => Apply(Op::Minus, vec![v])
            }
        }
        _ => unreachable!()
    })
}


pub fn is_null_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    Ok(match exprs {
        [a] => {
            match a.eval()? {
                Const(Null) => Const(Bool(true)),
                Const(_) => Const(Bool(false)),
                v => Apply(Op::IsNull, vec![v])
            }
        }
        _ => unreachable!()
    })
}

pub fn not_null_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    Ok(match exprs {
        [a] => {
            match a.eval()? {
                Const(Null) => Const(Bool(false)),
                Const(_) => Const(Bool(true)),
                v => Apply(Op::IsNull, vec![v])
            }
        }
        _ => unreachable!()
    })
}

pub fn or_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    let mut unevaluated = vec![];
    let mut has_null = false;
    for expr in exprs {
        match expr.eval()? {
            Const(Bool(true)) => return Ok(Const(Bool(true))),
            Const(Bool(false)) => {}
            Const(Null) => { has_null = true},
            Const(_) => return Err(TypeError),
            Apply(Op::Or, vs) => {
                for el in vs {
                    match el {
                        Const(Null) => has_null = true,
                        Const(_) => unreachable!(),
                        v => unevaluated.push(v)
                    }
                }
            }
            v => unevaluated.push(v)
        }
    }
    match (has_null, unevaluated.len()) {
        (true, 0) => Ok(Const(Null)),
        (false, 0) => Ok(Const(Bool(false))),
        (false, _) => Ok(Apply(Op::Or, unevaluated)),
        (true, _) => {
            unevaluated.push(Const(Null));
            Ok(Apply(Op::Or, unevaluated))
        }
    }
}



pub fn and_expr<'a>(exprs: &[Expr<'a>]) -> Result<Expr<'a>, CozoError> {
    let mut unevaluated = vec![];
    let mut no_null = true;
    for expr in exprs {
        match expr.eval()? {
            Const(Bool(false)) => return Ok(Const(Bool(false))),
            Const(Bool(true)) => {},
            Const(Null) => no_null = false,
            Const(_) => return Err(TypeError),
            Apply(Op::Or, vs) => {
                for el in vs {
                    match el {
                        Const(Null) => no_null = false,
                        Const(_) => unreachable!(),
                        v => unevaluated.push(v)
                    }
                }
            }
            v => unevaluated.push(v)
        }
    }
    match (no_null, unevaluated.len()) {
        (true, 0) => Ok(Const(Bool(true))),
        (false, 0) => Ok(Const(Null)),
        (true, _) => Ok(Apply(Op::Add, unevaluated)),
        (false, _) => {
            unevaluated.push(Const(Null));
            Ok(Apply(Op::And, unevaluated))
        }
    }
}