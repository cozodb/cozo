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
//
// }
//
