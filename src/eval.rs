// impl<S: Storage> Evaluator<S>
//     where Evaluator<S>: Env<Structured> {
//
//     fn mod_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 Ok(Const(match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Int(a % b),
//                             (_, _) => return Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => return Ok(Apply(Op::Mod, vec![a, b]))
//                 }))
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn eq_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => Ok(Const(Bool(a == b))),
//                     (a, b) => Ok(Apply(Op::Eq, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn ne_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => Ok(Const(Bool(a != b))),
//                     (a, b) => Ok(Apply(Op::Neq, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn gt_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Ok(Const(Bool(a > b))),
//                             (Float(a), Int(b)) => Ok(Const(Bool(a > b as f64))),
//                             (Int(a), Float(b)) => Ok(Const(Bool(a as f64 > b))),
//                             (Float(a), Float(b)) => Ok(Const(Bool(a > b))),
//                             (_, _) => Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => Ok(Apply(Op::Gt, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn ge_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Ok(Const(Bool(a >= b))),
//                             (Float(a), Int(b)) => Ok(Const(Bool(a >= b as f64))),
//                             (Int(a), Float(b)) => Ok(Const(Bool(a as f64 >= b))),
//                             (Float(a), Float(b)) => Ok(Const(Bool(a >= b))),
//                             (_, _) => Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => Ok(Apply(Op::Ge, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn lt_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Ok(Const(Bool(a < b))),
//                             (Float(a), Int(b)) => Ok(Const(Bool(a < b as f64))),
//                             (Int(a), Float(b)) => Ok(Const(Bool((a as f64) < b))),
//                             (Float(a), Float(b)) => Ok(Const(Bool(a < b))),
//                             (_, _) => Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => Ok(Apply(Op::Lt, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn le_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Ok(Const(Bool(a <= b))),
//                             (Float(a), Int(b)) => Ok(Const(Bool(a <= b as f64))),
//                             (Int(a), Float(b)) => Ok(Const(Bool((a as f64) <= b))),
//                             (Float(a), Float(b)) => Ok(Const(Bool(a <= b))),
//                             (_, _) => Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => Ok(Apply(Op::Le, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn pow_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) || b == Const(Null) {
//                     return Ok(Const(Null));
//                 }
//                 match (a, b) {
//                     (Const(a), Const(b)) => {
//                         match (a, b) {
//                             (Int(a), Int(b)) => Ok(Const(Float((a as f64).powf(b as f64)))),
//                             (Float(a), Int(b)) => Ok(Const(Float(a.powi(b as i32)))),
//                             (Int(a), Float(b)) => Ok(Const(Float((a as f64).powf(b)))),
//                             (Float(a), Float(b)) => Ok(Const(Float(a.powf(b)))),
//                             (_, _) => Err(CozoError::TypeError)
//                         }
//                     }
//                     (a, b) => Ok(Apply(Op::Pow, vec![a, b]))
//                 }
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn coalesce_exprs<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         match exprs {
//             [a, b] => {
//                 let a = self.visit_expr(a)?;
//                 let b = self.visit_expr(b)?;
//                 if a == Const(Null) {
//                     return Ok(b);
//                 }
//                 if b == Const(Null) {
//                     return Ok(a);
//                 }
//                 if let a @ Const(_) = a {
//                     return Ok(a);
//                 }
//                 return Ok(Apply(Op::Coalesce, vec![a, b]));
//             }
//             _ => unreachable!()
//         }
//     }
//
//     fn negate_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         Ok(match exprs {
//             [a] => {
//                 match self.visit_expr(a)? {
//                     Const(Null) => Const(Null),
//                     Const(Bool(b)) => Const(Bool(!b)),
//                     Const(_) => return Err(TypeError),
//                     Apply(Op::Neg, v) => v.into_iter().next().unwrap(),
//                     Apply(Op::IsNull, v) => Apply(Op::NotNull, v),
//                     Apply(Op::NotNull, v) => Apply(Op::IsNull, v),
//                     Apply(Op::Eq, v) => Apply(Op::Neq, v),
//                     Apply(Op::Neq, v) => Apply(Op::Eq, v),
//                     Apply(Op::Gt, v) => Apply(Op::Le, v),
//                     Apply(Op::Ge, v) => Apply(Op::Lt, v),
//                     Apply(Op::Le, v) => Apply(Op::Gt, v),
//                     Apply(Op::Lt, v) => Apply(Op::Ge, v),
//                     v => Apply(Op::Neg, vec![v])
//                 }
//             }
//             _ => unreachable!()
//         })
//     }
//
//     fn minus_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         Ok(match exprs {
//             [a] => {
//                 match self.visit_expr(a)? {
//                     Const(Null) => Const(Null),
//                     Const(Int(i)) => Const(Int(-i)),
//                     Const(Float(f)) => Const(Float(-f)),
//                     Const(_) => return Err(TypeError),
//                     Apply(Op::Minus, v) => v.into_iter().next().unwrap(),
//                     v => Apply(Op::Minus, vec![v])
//                 }
//             }
//             _ => unreachable!()
//         })
//     }
//
//     fn test_null_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         Ok(match exprs {
//             [a] => {
//                 match self.visit_expr(a)? {
//                     Const(Null) => Const(Bool(true)),
//                     Const(_) => Const(Bool(false)),
//                     v => Apply(Op::IsNull, vec![v])
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
//                     v => Apply(Op::IsNull, vec![v])
//                 }
//             }
//             _ => unreachable!()
//         })
//     }
//
//     fn or_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         let mut unevaluated = vec![];
//         let mut has_null = false;
//         for expr in exprs {
//             match self.visit_expr(expr)? {
//                 Const(Bool(true)) => return Ok(Const(Bool(true))),
//                 Const(Bool(false)) => {}
//                 Const(Null) => { has_null = true }
//                 Const(_) => return Err(TypeError),
//                 Apply(Op::Or, vs) => {
//                     for el in vs {
//                         match el {
//                             Const(Null) => has_null = true,
//                             Const(_) => unreachable!(),
//                             v => unevaluated.push(v)
//                         }
//                     }
//                 }
//                 v => unevaluated.push(v)
//             }
//         }
//         match (has_null, unevaluated.len()) {
//             (true, 0) => Ok(Const(Null)),
//             (false, 0) => Ok(Const(Bool(false))),
//             (false, _) => Ok(Apply(Op::Or, unevaluated)),
//             (true, _) => {
//                 unevaluated.push(Const(Null));
//                 Ok(Apply(Op::Or, unevaluated))
//             }
//         }
//     }
//
//     fn and_expr<'a>(&self, exprs: &[Expr<'a>]) -> Result<Expr<'a>> {
//         let mut unevaluated = vec![];
//         let mut no_null = true;
//         for expr in exprs {
//             match self.visit_expr(expr)? {
//                 Const(Bool(false)) => return Ok(Const(Bool(false))),
//                 Const(Bool(true)) => {}
//                 Const(Null) => no_null = false,
//                 Const(_) => return Err(TypeError),
//                 Apply(Op::Or, vs) => {
//                     for el in vs {
//                         match el {
//                             Const(Null) => no_null = false,
//                             Const(_) => unreachable!(),
//                             v => unevaluated.push(v)
//                         }
//                     }
//                 }
//                 v => unevaluated.push(v)
//             }
//         }
//         match (no_null, unevaluated.len()) {
//             (true, 0) => Ok(Const(Bool(true))),
//             (false, 0) => Ok(Const(Null)),
//             (true, _) => Ok(Apply(Op::Add, unevaluated)),
//             (false, _) => {
//                 unevaluated.push(Const(Null));
//                 Ok(Apply(Op::And, unevaluated))
//             }
//         }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn operators() {
//         let ev = Evaluator::new(DummyStorage {}).unwrap();
//
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("1/10+(-2+3)*4^5").unwrap()).unwrap());
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && false").unwrap()).unwrap());
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || false").unwrap()).unwrap());
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true || null").unwrap()).unwrap());
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("null || true").unwrap()).unwrap());
//         println!("{:#?}", ev.visit_expr(&parse_expr_from_str("true && null").unwrap()).unwrap());
//     }
// }