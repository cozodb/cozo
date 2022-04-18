use std::borrow::{Borrow, Cow};
use std::sync::Arc;
use crate::ast::Expr;
use crate::ast::Expr::*;
use crate::error::Result;
use crate::error::CozoError;
use crate::error::CozoError::*;
use crate::value::Value::*;
use crate::ast::*;
use crate::env::{Env, Environment, LayeredEnv};
use crate::storage::{DummyStorage, RocksStorage, Storage};
use crate::typing::Structured;

pub struct Evaluator<S: Storage> {
    pub env_stack: Vec<Environment>,
    pub storage: S,
}

impl Env<Structured> for Evaluator<DummyStorage> {
    fn define(&mut self, name: String, value: Structured) -> Option<Structured> {
        None
    }

    fn define_new(&mut self, name: String, value: Structured) -> bool {
        false
    }

    fn resolve(&self, name: &str) -> Option<Cow<Structured>> {
        None
    }

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        None
    }

    fn undef(&mut self, name: &str) -> Option<Structured> {
        None
    }
}

impl Env<Structured> for Evaluator<RocksStorage> {
    fn define(&mut self, name: String, value: Structured) -> Option<Structured> {
        self.env_stack.last_mut().unwrap().define(name, value)
    }

    fn define_new(&mut self, name: String, value: Structured) -> bool {
        if self.env_stack.is_empty() {
            self.env_stack.push(Environment::default());
        }
        self.env_stack.last_mut().unwrap().define_new(name, value)
    }

    fn resolve(&self, name: &str) -> Option<Cow<Structured>> {
        let mut res = None;
        for item in self.env_stack.iter().rev() {
            res = item.resolve(name);
            if res.is_some() {
                return res;
            }
        }
        // Unwrap here because read() only fails if lock is poisoned
        let env = self.storage.root_env.read().expect("Root environment is poisoned");
        env.resolve(name).map(|v| Cow::Owned(v.into_owned()))
    }

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        // Cannot obtain root elements this way
        let mut res = None;
        for item in self.env_stack.iter_mut().rev() {
            res = item.resolve_mut(name);
            if res.is_some() {
                return res;
            }
        }
        res
    }

    fn undef(&mut self, name: &str) -> Option<Structured> {
        // Cannot undef root elements this way
        let mut res = None;
        for item in self.env_stack.iter_mut().rev() {
            res = item.undef(name);
            if res.is_some() {
                return res;
            }
        }
        res
    }
}

impl LayeredEnv<Structured> for Evaluator<RocksStorage> {
    fn root_define(&mut self, name: String, value: Structured) -> Option<Structured> {
        self.storage.root_env.write().expect("Root environment is poisoned")
            .define(name, value)
    }

    fn root_define_new(&mut self, name: String, value: Structured) -> bool {
        self.storage.root_env.write().expect("Root environment is poisoned")
            .define_new(name, value)
    }

    fn root_resolve(&self, name: &str) -> Option<Cow<Structured>> {
        let env = self.storage.root_env.read().expect("Root environment is poisoned");
        env.resolve(name).map(|v| Cow::Owned(v.into_owned()))
    }

    fn root_undef(&mut self, name: &str) -> Option<Structured> {
        self.storage.root_env.write().expect("Root environment is poisoned")
            .undef(name)
    }
}

pub type EvaluatorWithStorage = Evaluator<RocksStorage>;
pub type BareEvaluator = Evaluator<DummyStorage>;

impl<S: Storage> Evaluator<S> {
    pub fn new(storage: S) -> Result<Self> {
        Ok(Self {
            env_stack: vec![Environment::default()],
            storage,
        })
    }
}


impl<'a, S: Storage> ExprVisitor<'a, Result<Expr<'a>>> for Evaluator<S>
    where Evaluator<S>: Env<Structured> {
    fn visit_expr(&self, ex: &Expr<'a>) -> Result<Expr<'a>> {
        match ex {
            Apply(op, args) => {
                match op {
                    Op::Add => self.add_exprs(args),
                    Op::Sub => self.sub_exprs(args),
                    Op::Mul => self.mul_exprs(args),
                    Op::Div => self.div_exprs(args),
                    Op::Eq => self.eq_exprs(args),
                    Op::Neq => self.ne_exprs(args),
                    Op::Gt => self.gt_exprs(args),
                    Op::Lt => self.lt_exprs(args),
                    Op::Ge => self.ge_exprs(args),
                    Op::Le => self.le_exprs(args),
                    Op::Neg => self.negate_expr(args),
                    Op::Minus => self.minus_expr(args),
                    Op::Mod => self.mod_exprs(args),
                    Op::Or => self.or_expr(args),
                    Op::And => self.and_expr(args),
                    Op::Coalesce => self.coalesce_exprs(args),
                    Op::Pow => self.pow_exprs(args),
                    Op::IsNull => self.test_null_expr(args),
                    Op::NotNull => self.not_null_expr(args),
                    Op::Call => unimplemented!(),
                }
            }
            Const(v) => Ok(Const(v.clone())),
            Expr::List(_) => { unimplemented!() }
            Expr::Dict(_, _) => { unimplemented!() }
            Ident(ident) => {
                let resolved = self.resolve(ident).ok_or(UndefinedParam)?;
                match resolved.borrow() {
                    Structured::Value(v) => {
                        Ok(Const(v.clone()))
                    }
                    _ => return Err(ValueRequired)
                }
            }
        }
    }
}

impl<S: Storage> Evaluator<S>
    where Evaluator<S>: Env<Structured> {
    fn add_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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
                            (OwnString(va), OwnString(vb)) => OwnString(Arc::new(va.clone().to_string() + &vb)),
                            (OwnString(va), RefString(vb)) => OwnString(Arc::new(va.clone().to_string() + &*vb)),
                            (RefString(va), OwnString(vb)) => OwnString(Arc::new(va.to_string() + &*vb)),
                            (RefString(va), RefString(vb)) => OwnString(Arc::new(va.to_string() + &*vb)),
                            (_, _) => return Err(CozoError::TypeError)
                        }
                    }
                    (a, b) => return Ok(Apply(Op::Add, vec![a, b]))
                }))
            }
            _ => unreachable!()
        }
    }

    fn sub_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn mul_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn div_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn mod_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn eq_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn ne_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
                if a == Const(Null) || b == Const(Null) {
                    return Ok(Const(Null));
                }
                match (a, b) {
                    (Const(a), Const(b)) => Ok(Const(Bool(a != b))),
                    (a, b) => Ok(Apply(Op::Neq, vec![a, b]))
                }
            }
            _ => unreachable!()
        }
    }

    fn gt_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn ge_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn lt_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn le_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn pow_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn coalesce_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        match exprs {
            [a, b] => {
                let a = self.visit_expr(a)?;
                let b = self.visit_expr(b)?;
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

    fn negate_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        Ok(match exprs {
            [a] => {
                match self.visit_expr(a)? {
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

    fn minus_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        Ok(match exprs {
            [a] => {
                match self.visit_expr(a)? {
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

    fn test_null_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        Ok(match exprs {
            [a] => {
                match self.visit_expr(a)? {
                    Const(Null) => Const(Bool(true)),
                    Const(_) => Const(Bool(false)),
                    v => Apply(Op::IsNull, vec![v])
                }
            }
            _ => unreachable!()
        })
    }

    fn not_null_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        Ok(match exprs {
            [a] => {
                match self.visit_expr(a)? {
                    Const(Null) => Const(Bool(false)),
                    Const(_) => Const(Bool(true)),
                    v => Apply(Op::IsNull, vec![v])
                }
            }
            _ => unreachable!()
        })
    }

    fn or_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        let mut unevaluated = vec![];
        let mut has_null = false;
        for expr in exprs {
            match self.visit_expr(expr)? {
                Const(Bool(true)) => return Ok(Const(Bool(true))),
                Const(Bool(false)) => {}
                Const(Null) => { has_null = true }
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

    fn and_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
        let mut unevaluated = vec![];
        let mut no_null = true;
        for expr in exprs {
            match self.visit_expr(expr)? {
                Const(Bool(false)) => return Ok(Const(Bool(false))),
                Const(Bool(true)) => {}
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operators() {
        let ev = Evaluator::new(DummyStorage {}).unwrap();

        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("1/10+(-2+3)*4^5").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && false").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || false").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || null").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("null || true").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && null").unwrap()).unwrap());
    }
}