use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::ast::Expr;
use crate::ast::Expr::*;
use crate::error::Result;
use crate::error::CozoError;
use crate::error::CozoError::*;
use crate::value::Value::*;
use crate::ast::*;
use crate::env::StructuredEnv;
use crate::storage::{DummyStorage, RocksStorage, Storage};

pub struct Evaluator<S: Storage> {
    pub s_envs: StructuredEnv,
    pub storage: S,
    pub last_local_id: Arc<AtomicUsize>,
}

impl <S:Storage> Evaluator<S> {
    pub fn get_next_local_id(&self, is_global: bool) -> usize {
        if is_global {
            0
        } else {
            self.last_local_id.fetch_add(1, Ordering::Relaxed)
        }
    }
}

pub type EvaluatorWithStorage = Evaluator<RocksStorage>;
pub type BareEvaluator = Evaluator<DummyStorage>;

impl EvaluatorWithStorage {
    pub fn new(path: String) -> Result<Self> {
        Ok(Self {
            s_envs: StructuredEnv::new(),
            storage: RocksStorage::new(path)?,
            last_local_id: Arc::new(AtomicUsize::new(1)),
        })
    }
}

impl Default for BareEvaluator {
    fn default() -> Self {
        Self {
            s_envs: StructuredEnv::new(),
            storage: DummyStorage,
            last_local_id: Arc::new(AtomicUsize::new(0)),
        }
    }
}


impl<'a, S: Storage> ExprVisitor<'a, Result<Expr<'a>>> for Evaluator<S> {
    fn visit_expr(&mut self, ex: &Expr<'a>) -> Result<Expr<'a>> {
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
        }
    }
}

impl<S: Storage> Evaluator<S> {
    fn add_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn sub_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn mul_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn div_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn mod_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn eq_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn ne_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn gt_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn ge_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn lt_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn le_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn pow_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn coalesce_exprs<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn negate_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn minus_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn test_null_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn not_null_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn or_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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

    fn and_expr<'a>(&mut self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
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
        let mut ev = BareEvaluator::default();

        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("1/10+(-2+3)*4^5").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && false").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || false").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || null").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("null || true").unwrap()).unwrap());
        println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && null").unwrap()).unwrap());
    }


    #[test]
    fn data() -> Result<()> {
        let data = r#"[{"_src":"AR","_dst":2,"_type":"InRegion"},
{"_src":"AU","_dst":3,"_type":"InRegion"},
{"_src":"BE","_dst":1,"_type":"InRegion"},
{"_src":"BR","_dst":2,"_type":"InRegion"},
{"_src":"CA","_dst":2,"_type":"InRegion"},
{"_src":"CH","_dst":1,"_type":"InRegion"},
{"_src":"CN","_dst":3,"_type":"InRegion"},
{"_src":"DE","_dst":1,"_type":"InRegion"},
{"_src":"DK","_dst":1,"_type":"InRegion"},
{"_src":"EG","_dst":4,"_type":"InRegion"},
{"_src":"FR","_dst":1,"_type":"InRegion"},
{"_src":"HK","_dst":3,"_type":"InRegion"},
{"_src":"IL","_dst":4,"_type":"InRegion"},
{"_src":"IN","_dst":3,"_type":"InRegion"},
{"_src":"IT","_dst":1,"_type":"InRegion"},
{"_src":"JP","_dst":3,"_type":"InRegion"},
{"_src":"KW","_dst":4,"_type":"InRegion"},
{"_src":"MX","_dst":2,"_type":"InRegion"},
{"_src":"NG","_dst":4,"_type":"InRegion"},
{"_src":"NL","_dst":1,"_type":"InRegion"},
{"_src":"SG","_dst":3,"_type":"InRegion"},
{"_src":"UK","_dst":1,"_type":"InRegion"},
{"_src":"US","_dst":2,"_type":"InRegion"},
{"_src":"ZM","_dst":4,"_type":"InRegion"},
{"_src":"ZW","_dst":4,"_type":"InRegion"},
{"_src":1400,"_dst":"US","_type":"InCountry"},
{"_src":1500,"_dst":"US","_type":"InCountry"},
{"_src":1700,"_dst":"US","_type":"InCountry"},
{"_src":1800,"_dst":"CA","_type":"InCountry"},
{"_src":2400,"_dst":"UK","_type":"InCountry"},
{"_src":2500,"_dst":"UK","_type":"InCountry"},
{"_src":2700,"_dst":"DE","_type":"InCountry"},
{"_dst":101,"_src":100,"_type":"Manages"},
{"_dst":102,"_src":100,"_type":"Manages"},
{"_dst":103,"_src":102,"_type":"Manages"},
{"_dst":104,"_src":103,"_type":"Manages"},
{"_dst":105,"_src":103,"_type":"Manages"},
{"_dst":106,"_src":103,"_type":"Manages"},
{"_dst":107,"_src":103,"_type":"Manages"},
{"_dst":108,"_src":101,"_type":"Manages"},
{"_dst":109,"_src":108,"_type":"Manages"},
{"_dst":110,"_src":108,"_type":"Manages"},
{"_dst":111,"_src":108,"_type":"Manages"},
{"_dst":112,"_src":108,"_type":"Manages"},
{"_dst":113,"_src":108,"_type":"Manages"},
{"_dst":114,"_src":100,"_type":"Manages"},
{"_dst":115,"_src":114,"_type":"Manages"},
{"_dst":116,"_src":114,"_type":"Manages"},
{"_dst":117,"_src":114,"_type":"Manages"},
{"_dst":118,"_src":114,"_type":"Manages"},
{"_dst":119,"_src":114,"_type":"Manages"},
{"_dst":120,"_src":100,"_type":"Manages"},
{"_dst":121,"_src":100,"_type":"Manages"},
{"_dst":122,"_src":100,"_type":"Manages"},
{"_dst":123,"_src":100,"_type":"Manages"},
{"_dst":126,"_src":120,"_type":"Manages"},
{"_dst":145,"_src":100,"_type":"Manages"},
{"_dst":146,"_src":100,"_type":"Manages"},
{"_dst":176,"_src":100,"_type":"Manages"},
{"_dst":177,"_src":100,"_type":"Manages"},
{"_dst":178,"_src":100,"_type":"Manages"},
{"_dst":179,"_src":100,"_type":"Manages"},
{"_dst":192,"_src":123,"_type":"Manages"},
{"_dst":193,"_src":123,"_type":"Manages"},
{"_dst":200,"_src":101,"_type":"Manages"},
{"_dst":201,"_src":100,"_type":"Manages"},
{"_dst":202,"_src":201,"_type":"Manages"},
{"_dst":203,"_src":101,"_type":"Manages"},
{"_dst":204,"_src":101,"_type":"Manages"},
{"_dst":205,"_src":101,"_type":"Manages"},
{"_dst":206,"_src":205,"_type":"Manages"},
{"_src":100,"hire_date":"1987-06-17","_dst":4,"salary":24000.0,"_type":"HasJob"},
{"_src":101,"hire_date":"1989-09-21","_dst":5,"salary":17000.0,"_type":"HasJob"},
{"_src":102,"hire_date":"1993-01-13","_dst":5,"salary":17000.0,"_type":"HasJob"},
{"_src":103,"hire_date":"1990-01-03","_dst":9,"salary":9000.0,"_type":"HasJob"},
{"_src":104,"hire_date":"1991-05-21","_dst":9,"salary":6000.0,"_type":"HasJob"},
{"_src":105,"hire_date":"1997-06-25","_dst":9,"salary":4800.0,"_type":"HasJob"},
{"_src":106,"hire_date":"1998-02-05","_dst":9,"salary":4800.0,"_type":"HasJob"},
{"_src":107,"hire_date":"1999-02-07","_dst":9,"salary":4200.0,"_type":"HasJob"},
{"_src":108,"hire_date":"1994-08-17","_dst":7,"salary":12000.0,"_type":"HasJob"},
{"_src":109,"hire_date":"1994-08-16","_dst":6,"salary":9000.0,"_type":"HasJob"},
{"_src":110,"hire_date":"1997-09-28","_dst":6,"salary":8200.0,"_type":"HasJob"},
{"_src":111,"hire_date":"1997-09-30","_dst":6,"salary":7700.0,"_type":"HasJob"},
{"_src":112,"hire_date":"1998-03-07","_dst":6,"salary":7800.0,"_type":"HasJob"},
{"_src":113,"hire_date":"1999-12-07","_dst":6,"salary":6900.0,"_type":"HasJob"},
{"_src":114,"hire_date":"1994-12-07","_dst":14,"salary":11000.0,"_type":"HasJob"},
{"_src":115,"hire_date":"1995-05-18","_dst":13,"salary":3100.0,"_type":"HasJob"},
{"_src":116,"hire_date":"1997-12-24","_dst":13,"salary":2900.0,"_type":"HasJob"},
{"_src":117,"hire_date":"1997-07-24","_dst":13,"salary":2800.0,"_type":"HasJob"},
{"_src":118,"hire_date":"1998-11-15","_dst":13,"salary":2600.0,"_type":"HasJob"},
{"_src":119,"hire_date":"1999-08-10","_dst":13,"salary":2500.0,"_type":"HasJob"},
{"_src":120,"hire_date":"1996-07-18","_dst":19,"salary":8000.0,"_type":"HasJob"},
{"_src":121,"hire_date":"1997-04-10","_dst":19,"salary":8200.0,"_type":"HasJob"},
{"_src":122,"hire_date":"1995-05-01","_dst":19,"salary":7900.0,"_type":"HasJob"},
{"_src":123,"hire_date":"1997-10-10","_dst":19,"salary":6500.0,"_type":"HasJob"},
{"_src":126,"hire_date":"1998-09-28","_dst":18,"salary":2700.0,"_type":"HasJob"},
{"_src":145,"hire_date":"1996-10-01","_dst":15,"salary":14000.0,"_type":"HasJob"},
{"_src":146,"hire_date":"1997-01-05","_dst":15,"salary":13500.0,"_type":"HasJob"},
{"_src":176,"hire_date":"1998-03-24","_dst":16,"salary":8600.0,"_type":"HasJob"},
{"_src":177,"hire_date":"1998-04-23","_dst":16,"salary":8400.0,"_type":"HasJob"},
{"_src":178,"hire_date":"1999-05-24","_dst":16,"salary":7000.0,"_type":"HasJob"},
{"_src":179,"hire_date":"2000-01-04","_dst":16,"salary":6200.0,"_type":"HasJob"},
{"_src":192,"hire_date":"1996-02-04","_dst":17,"salary":4000.0,"_type":"HasJob"},
{"_src":193,"hire_date":"1997-03-03","_dst":17,"salary":3900.0,"_type":"HasJob"},
{"_src":200,"hire_date":"1987-09-17","_dst":3,"salary":4400.0,"_type":"HasJob"},
{"_src":201,"hire_date":"1996-02-17","_dst":10,"salary":13000.0,"_type":"HasJob"},
{"_src":202,"hire_date":"1997-08-17","_dst":11,"salary":6000.0,"_type":"HasJob"},
{"_src":203,"hire_date":"1994-06-07","_dst":8,"salary":6500.0,"_type":"HasJob"},
{"_src":204,"hire_date":"1994-06-07","_dst":12,"salary":10000.0,"_type":"HasJob"},
{"_src":205,"hire_date":"1994-06-07","_dst":2,"salary":12000.0,"_type":"HasJob"},
{"_src":206,"hire_date":"1994-06-07","_dst":1,"salary":8300.0,"_type":"HasJob"},
{"_src":1,"_dst":1700,"_type":"InLocation"},
{"_src":2,"_dst":1800,"_type":"InLocation"},
{"_src":3,"_dst":1700,"_type":"InLocation"},
{"_src":4,"_dst":2400,"_type":"InLocation"},
{"_src":5,"_dst":1500,"_type":"InLocation"},
{"_src":6,"_dst":1400,"_type":"InLocation"},
{"_src":7,"_dst":2700,"_type":"InLocation"},
{"_src":8,"_dst":2500,"_type":"InLocation"},
{"_src":9,"_dst":1700,"_type":"InLocation"},
{"_src":10,"_dst":1700,"_type":"InLocation"},
{"_src":11,"_dst":1700,"_type":"InLocation"},
{"id":1,"title":"Public Accountant","min_salary":4200.0,"max_salary":9000.0,"_type":"Job"},
{"id":2,"title":"Accounting Manager","min_salary":8200.0,"max_salary":16000.0,"_type":"Job"},
{"id":3,"title":"Administration Assistant","min_salary":3000.0,"max_salary":6000.0,"_type":"Job"},
{"id":4,"title":"President","min_salary":20000.0,"max_salary":40000.0,"_type":"Job"},
{"id":5,"title":"Administration Vice President","min_salary":15000.0,"max_salary":30000.0,"_type":"Job"},
{"id":6,"title":"Accountant","min_salary":4200.0,"max_salary":9000.0,"_type":"Job"},
{"id":7,"title":"Finance Manager","min_salary":8200.0,"max_salary":16000.0,"_type":"Job"},
{"id":8,"title":"Human Resources Representative","min_salary":4000.0,"max_salary":9000.0,"_type":"Job"},
{"id":9,"title":"Programmer","min_salary":4000.0,"max_salary":10000.0,"_type":"Job"},
{"id":10,"title":"Marketing Manager","min_salary":9000.0,"max_salary":15000.0,"_type":"Job"},
{"id":11,"title":"Marketing Representative","min_salary":4000.0,"max_salary":9000.0,"_type":"Job"},
{"id":12,"title":"Public Relations Representative","min_salary":4500.0,"max_salary":10500.0,"_type":"Job"},
{"id":13,"title":"Purchasing Clerk","min_salary":2500.0,"max_salary":5500.0,"_type":"Job"},
{"id":14,"title":"Purchasing Manager","min_salary":8000.0,"max_salary":15000.0,"_type":"Job"},
{"id":15,"title":"Sales Manager","min_salary":10000.0,"max_salary":20000.0,"_type":"Job"},
{"id":16,"title":"Sales Representative","min_salary":6000.0,"max_salary":12000.0,"_type":"Job"},
{"id":17,"title":"Shipping Clerk","min_salary":2500.0,"max_salary":5500.0,"_type":"Job"},
{"id":18,"title":"Stock Clerk","min_salary":2000.0,"max_salary":5000.0,"_type":"Job"},
{"id":19,"title":"Stock Manager","min_salary":5500.0,"max_salary":8500.0,"_type":"Job"},
{"id":1,"first_name":"Penelope","last_name":"Gietz","_type":"Dependent"},
{"id":2,"first_name":"Nick","last_name":"Higgins","_type":"Dependent"},
{"id":3,"first_name":"Ed","last_name":"Whalen","_type":"Dependent"},
{"id":4,"first_name":"Jennifer","last_name":"King","_type":"Dependent"},
{"id":5,"first_name":"Johnny","last_name":"Kochhar","_type":"Dependent"},
{"id":6,"first_name":"Bette","last_name":"De Haan","_type":"Dependent"},
{"id":7,"first_name":"Grace","last_name":"Faviet","_type":"Dependent"},
{"id":8,"first_name":"Matthew","last_name":"Chen","_type":"Dependent"},
{"id":9,"first_name":"Joe","last_name":"Sciarra","_type":"Dependent"},
{"id":10,"first_name":"Christian","last_name":"Urman","_type":"Dependent"},
{"id":11,"first_name":"Zero","last_name":"Popp","_type":"Dependent"},
{"id":12,"first_name":"Karl","last_name":"Greenberg","_type":"Dependent"},
{"id":13,"first_name":"Uma","last_name":"Mavris","_type":"Dependent"},
{"id":14,"first_name":"Vivien","last_name":"Hunold","_type":"Dependent"},
{"id":15,"first_name":"Cuba","last_name":"Ernst","_type":"Dependent"},
{"id":16,"first_name":"Fred","last_name":"Austin","_type":"Dependent"},
{"id":17,"first_name":"Helen","last_name":"Pataballa","_type":"Dependent"},
{"id":18,"first_name":"Dan","last_name":"Lorentz","_type":"Dependent"},
{"id":19,"first_name":"Bob","last_name":"Hartstein","_type":"Dependent"},
{"id":20,"first_name":"Lucille","last_name":"Fay","_type":"Dependent"},
{"id":21,"first_name":"Kirsten","last_name":"Baer","_type":"Dependent"},
{"id":22,"first_name":"Elvis","last_name":"Khoo","_type":"Dependent"},
{"id":23,"first_name":"Sandra","last_name":"Baida","_type":"Dependent"},
{"id":24,"first_name":"Cameron","last_name":"Tobias","_type":"Dependent"},
{"id":25,"first_name":"Kevin","last_name":"Himuro","_type":"Dependent"},
{"id":26,"first_name":"Rip","last_name":"Colmenares","_type":"Dependent"},
{"id":27,"first_name":"Julia","last_name":"Raphaely","_type":"Dependent"},
{"id":28,"first_name":"Woody","last_name":"Russell","_type":"Dependent"},
{"id":29,"first_name":"Alec","last_name":"Partners","_type":"Dependent"},
{"id":30,"first_name":"Sandra","last_name":"Taylor","_type":"Dependent"},
{"id":1,"name":"Administration","_type":"Department"},
{"id":2,"name":"Marketing","_type":"Department"},
{"id":3,"name":"Purchasing","_type":"Department"},
{"id":4,"name":"Human Resources","_type":"Department"},
{"id":5,"name":"Shipping","_type":"Department"},
{"id":6,"name":"IT","_type":"Department"},
{"id":7,"name":"Public Relations","_type":"Department"},
{"id":8,"name":"Sales","_type":"Department"},
{"id":9,"name":"Executive","_type":"Department"},
{"id":10,"name":"Finance","_type":"Department"},
{"id":11,"name":"Accounting","_type":"Department"},
{"id":1,"name":"Europe","_type":"Region"},
{"id":2,"name":"Americas","_type":"Region"},
{"id":3,"name":"Asia","_type":"Region"},
{"id":4,"name":"Middle East and Africa","_type":"Region"},
{"id":"AR","name":"Argentina","_type":"Country"},
{"id":"AU","name":"Australia","_type":"Country"},
{"id":"BE","name":"Belgium","_type":"Country"},
{"id":"BR","name":"Brazil","_type":"Country"},
{"id":"CA","name":"Canada","_type":"Country"},
{"id":"CH","name":"Switzerland","_type":"Country"},
{"id":"CN","name":"China","_type":"Country"},
{"id":"DE","name":"Germany","_type":"Country"},
{"id":"DK","name":"Denmark","_type":"Country"},
{"id":"EG","name":"Egypt","_type":"Country"},
{"id":"FR","name":"France","_type":"Country"},
{"id":"HK","name":"HongKong","_type":"Country"},
{"id":"IL","name":"Israel","_type":"Country"},
{"id":"IN","name":"India","_type":"Country"},
{"id":"IT","name":"Italy","_type":"Country"},
{"id":"JP","name":"Japan","_type":"Country"},
{"id":"KW","name":"Kuwait","_type":"Country"},
{"id":"MX","name":"Mexico","_type":"Country"},
{"id":"NG","name":"Nigeria","_type":"Country"},
{"id":"NL","name":"Netherlands","_type":"Country"},
{"id":"SG","name":"Singapore","_type":"Country"},
{"id":"UK","name":"United Kingdom","_type":"Country"},
{"id":"US","name":"United States of America","_type":"Country"},
{"id":"ZM","name":"Zambia","_type":"Country"},
{"id":"ZW","name":"Zimbabwe","_type":"Country"},
{"_src":1,"relationship":"Child","_dst":206,"_type":"HasDependent"},
{"_src":2,"relationship":"Child","_dst":205,"_type":"HasDependent"},
{"_src":3,"relationship":"Child","_dst":200,"_type":"HasDependent"},
{"_src":4,"relationship":"Child","_dst":100,"_type":"HasDependent"},
{"_src":5,"relationship":"Child","_dst":101,"_type":"HasDependent"},
{"_src":6,"relationship":"Child","_dst":102,"_type":"HasDependent"},
{"_src":7,"relationship":"Child","_dst":109,"_type":"HasDependent"},
{"_src":8,"relationship":"Child","_dst":110,"_type":"HasDependent"},
{"_src":9,"relationship":"Child","_dst":111,"_type":"HasDependent"},
{"_src":10,"relationship":"Child","_dst":112,"_type":"HasDependent"},
{"_src":11,"relationship":"Child","_dst":113,"_type":"HasDependent"},
{"_src":12,"relationship":"Child","_dst":108,"_type":"HasDependent"},
{"_src":13,"relationship":"Child","_dst":203,"_type":"HasDependent"},
{"_src":14,"relationship":"Child","_dst":103,"_type":"HasDependent"},
{"_src":15,"relationship":"Child","_dst":104,"_type":"HasDependent"},
{"_src":16,"relationship":"Child","_dst":105,"_type":"HasDependent"},
{"_src":17,"relationship":"Child","_dst":106,"_type":"HasDependent"},
{"_src":18,"relationship":"Child","_dst":107,"_type":"HasDependent"},
{"_src":19,"relationship":"Child","_dst":201,"_type":"HasDependent"},
{"_src":20,"relationship":"Child","_dst":202,"_type":"HasDependent"},
{"_src":21,"relationship":"Child","_dst":204,"_type":"HasDependent"},
{"_src":22,"relationship":"Child","_dst":115,"_type":"HasDependent"},
{"_src":23,"relationship":"Child","_dst":116,"_type":"HasDependent"},
{"_src":24,"relationship":"Child","_dst":117,"_type":"HasDependent"},
{"_src":25,"relationship":"Child","_dst":118,"_type":"HasDependent"},
{"_src":26,"relationship":"Child","_dst":119,"_type":"HasDependent"},
{"_src":27,"relationship":"Child","_dst":114,"_type":"HasDependent"},
{"_src":28,"relationship":"Child","_dst":145,"_type":"HasDependent"},
{"_src":29,"relationship":"Child","_dst":146,"_type":"HasDependent"},
{"_src":30,"relationship":"Child","_dst":176,"_type":"HasDependent"},
{"id":1400,"street_address":"2014 Jabberwocky Rd","postal_code":"26192","city":"Southlake","state_province":"Texas","_type":"Location"},
{"id":1500,"street_address":"2011 Interiors Blvd","postal_code":"99236","city":"South San Francisco","state_province":"California","_type":"Location"},
{"id":1700,"street_address":"2004 Charade Rd","postal_code":"98199","city":"Seattle","state_province":"Washington","_type":"Location"},
{"id":1800,"street_address":"147 Spadina Ave","postal_code":"M5V 2L7","city":"Toronto","state_province":"Ontario","_type":"Location"},
{"id":2400,"street_address":"8204 Arthur St","postal_code":null,"city":"London","state_province":null,"_type":"Location"},
{"id":2500,"street_address":"Magdalen Centre  The Oxford Science Park","postal_code":"OX9 9ZB","city":"Oxford","state_province":"Oxford","_type":"Location"},
{"id":2700,"street_address":"Schwanthalerstr. 7031","postal_code":"80925","city":"Munich","state_province":"Bavaria","_type":"Location"},
{"id":100,"first_name":"Steven","last_name":"King","email":"steven.king@sqltutorial.org","phone_number":"515.123.4567","_type":"Employee"},
{"id":101,"first_name":"Neena","last_name":"Kochhar","email":"neena.kochhar@sqltutorial.org","phone_number":"515.123.4568","_type":"Employee"},
{"id":102,"first_name":"Lex","last_name":"De Haan","email":"lex.de haan@sqltutorial.org","phone_number":"515.123.4569","_type":"Employee"},
{"id":103,"first_name":"Alexander","last_name":"Hunold","email":"alexander.hunold@sqltutorial.org","phone_number":"590.423.4567","_type":"Employee"},
{"id":104,"first_name":"Bruce","last_name":"Ernst","email":"bruce.ernst@sqltutorial.org","phone_number":"590.423.4568","_type":"Employee"},
{"id":105,"first_name":"David","last_name":"Austin","email":"david.austin@sqltutorial.org","phone_number":"590.423.4569","_type":"Employee"},
{"id":106,"first_name":"Valli","last_name":"Pataballa","email":"valli.pataballa@sqltutorial.org","phone_number":"590.423.4560","_type":"Employee"},
{"id":107,"first_name":"Diana","last_name":"Lorentz","email":"diana.lorentz@sqltutorial.org","phone_number":"590.423.5567","_type":"Employee"},
{"id":108,"first_name":"Nancy","last_name":"Greenberg","email":"nancy.greenberg@sqltutorial.org","phone_number":"515.124.4569","_type":"Employee"},
{"id":109,"first_name":"Daniel","last_name":"Faviet","email":"daniel.faviet@sqltutorial.org","phone_number":"515.124.4169","_type":"Employee"},
{"id":110,"first_name":"John","last_name":"Chen","email":"john.chen@sqltutorial.org","phone_number":"515.124.4269","_type":"Employee"},
{"id":111,"first_name":"Ismael","last_name":"Sciarra","email":"ismael.sciarra@sqltutorial.org","phone_number":"515.124.4369","_type":"Employee"},
{"id":112,"first_name":"Jose Manuel","last_name":"Urman","email":"jose manuel.urman@sqltutorial.org","phone_number":"515.124.4469","_type":"Employee"},
{"id":113,"first_name":"Luis","last_name":"Popp","email":"luis.popp@sqltutorial.org","phone_number":"515.124.4567","_type":"Employee"},
{"id":114,"first_name":"Den","last_name":"Raphaely","email":"den.raphaely@sqltutorial.org","phone_number":"515.127.4561","_type":"Employee"},
{"id":115,"first_name":"Alexander","last_name":"Khoo","email":"alexander.khoo@sqltutorial.org","phone_number":"515.127.4562","_type":"Employee"},
{"id":116,"first_name":"Shelli","last_name":"Baida","email":"shelli.baida@sqltutorial.org","phone_number":"515.127.4563","_type":"Employee"},
{"id":117,"first_name":"Sigal","last_name":"Tobias","email":"sigal.tobias@sqltutorial.org","phone_number":"515.127.4564","_type":"Employee"},
{"id":118,"first_name":"Guy","last_name":"Himuro","email":"guy.himuro@sqltutorial.org","phone_number":"515.127.4565","_type":"Employee"},
{"id":119,"first_name":"Karen","last_name":"Colmenares","email":"karen.colmenares@sqltutorial.org","phone_number":"515.127.4566","_type":"Employee"},
{"id":120,"first_name":"Matthew","last_name":"Weiss","email":"matthew.weiss@sqltutorial.org","phone_number":"650.123.1234","_type":"Employee"},
{"id":121,"first_name":"Adam","last_name":"Fripp","email":"adam.fripp@sqltutorial.org","phone_number":"650.123.2234","_type":"Employee"},
{"id":122,"first_name":"Payam","last_name":"Kaufling","email":"payam.kaufling@sqltutorial.org","phone_number":"650.123.3234","_type":"Employee"},
{"id":123,"first_name":"Shanta","last_name":"Vollman","email":"shanta.vollman@sqltutorial.org","phone_number":"650.123.4234","_type":"Employee"},
{"id":126,"first_name":"Irene","last_name":"Mikkilineni","email":"irene.mikkilineni@sqltutorial.org","phone_number":"650.124.1224","_type":"Employee"},
{"id":145,"first_name":"John","last_name":"Russell","email":"john.russell@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":146,"first_name":"Karen","last_name":"Partners","email":"karen.partners@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":176,"first_name":"Jonathon","last_name":"Taylor","email":"jonathon.taylor@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":177,"first_name":"Jack","last_name":"Livingston","email":"jack.livingston@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":178,"first_name":"Kimberely","last_name":"Grant","email":"kimberely.grant@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":179,"first_name":"Charles","last_name":"Johnson","email":"charles.johnson@sqltutorial.org","phone_number":null,"_type":"Employee"},
{"id":192,"first_name":"Sarah","last_name":"Bell","email":"sarah.bell@sqltutorial.org","phone_number":"650.501.1876","_type":"Employee"},
{"id":193,"first_name":"Britney","last_name":"Everett","email":"britney.everett@sqltutorial.org","phone_number":"650.501.2876","_type":"Employee"},
{"id":200,"first_name":"Jennifer","last_name":"Whalen","email":"jennifer.whalen@sqltutorial.org","phone_number":"515.123.4444","_type":"Employee"},
{"id":201,"first_name":"Michael","last_name":"Hartstein","email":"michael.hartstein@sqltutorial.org","phone_number":"515.123.5555","_type":"Employee"},
{"id":202,"first_name":"Pat","last_name":"Fay","email":"pat.fay@sqltutorial.org","phone_number":"603.123.6666","_type":"Employee"},
{"id":203,"first_name":"Susan","last_name":"Mavris","email":"susan.mavris@sqltutorial.org","phone_number":"515.123.7777","_type":"Employee"},
{"id":204,"first_name":"Hermann","last_name":"Baer","email":"hermann.baer@sqltutorial.org","phone_number":"515.123.8888","_type":"Employee"},
{"id":205,"first_name":"Shelley","last_name":"Higgins","email":"shelley.higgins@sqltutorial.org","phone_number":"515.123.8080","_type":"Employee"},
{"id":206,"first_name":"William","last_name":"Gietz","email":"william.gietz@sqltutorial.org","phone_number":"515.123.8181","_type":"Employee"},
{"_src":100,"_dst":9,"_type":"InDepartment"},
{"_src":101,"_dst":9,"_type":"InDepartment"},
{"_src":102,"_dst":9,"_type":"InDepartment"},
{"_src":103,"_dst":6,"_type":"InDepartment"},
{"_src":104,"_dst":6,"_type":"InDepartment"},
{"_src":105,"_dst":6,"_type":"InDepartment"},
{"_src":106,"_dst":6,"_type":"InDepartment"},
{"_src":107,"_dst":6,"_type":"InDepartment"},
{"_src":108,"_dst":10,"_type":"InDepartment"},
{"_src":109,"_dst":10,"_type":"InDepartment"},
{"_src":110,"_dst":10,"_type":"InDepartment"},
{"_src":111,"_dst":10,"_type":"InDepartment"},
{"_src":112,"_dst":10,"_type":"InDepartment"},
{"_src":113,"_dst":10,"_type":"InDepartment"},
{"_src":114,"_dst":3,"_type":"InDepartment"},
{"_src":115,"_dst":3,"_type":"InDepartment"},
{"_src":116,"_dst":3,"_type":"InDepartment"},
{"_src":117,"_dst":3,"_type":"InDepartment"},
{"_src":118,"_dst":3,"_type":"InDepartment"},
{"_src":119,"_dst":3,"_type":"InDepartment"},
{"_src":120,"_dst":5,"_type":"InDepartment"},
{"_src":121,"_dst":5,"_type":"InDepartment"},
{"_src":122,"_dst":5,"_type":"InDepartment"},
{"_src":123,"_dst":5,"_type":"InDepartment"},
{"_src":126,"_dst":5,"_type":"InDepartment"},
{"_src":145,"_dst":8,"_type":"InDepartment"},
{"_src":146,"_dst":8,"_type":"InDepartment"},
{"_src":176,"_dst":8,"_type":"InDepartment"},
{"_src":177,"_dst":8,"_type":"InDepartment"},
{"_src":178,"_dst":8,"_type":"InDepartment"},
{"_src":179,"_dst":8,"_type":"InDepartment"},
{"_src":192,"_dst":5,"_type":"InDepartment"},
{"_src":193,"_dst":5,"_type":"InDepartment"},
{"_src":200,"_dst":1,"_type":"InDepartment"},
{"_src":201,"_dst":2,"_type":"InDepartment"},
{"_src":202,"_dst":2,"_type":"InDepartment"},
{"_src":203,"_dst":4,"_type":"InDepartment"},
{"_src":204,"_dst":7,"_type":"InDepartment"},
{"_src":205,"_dst":11,"_type":"InDepartment"},
{"_src":206,"_dst":11,"_type":"InDepartment"}]"#;
        let parsed = parse_expr_from_str(data)?;
        let mut ev = BareEvaluator::default();
        let evaluated = ev.visit_expr(&parsed)?;
        println!("{:#?}", evaluated);
        Ok(())
    }
}