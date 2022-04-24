// use std::borrow::Cow;
// use std::sync::Arc;
// use pest::iterators::{Pair};
// use pest::Parser as PestParser;
// use pest::prec_climber::{Assoc, PrecClimber, Operator};
// use crate::parser::Parser;
// use crate::parser::Rule;
// use lazy_static::lazy_static;
// use crate::ast::Expr::{Apply, Const};
// use crate::error::CozoError;
// use crate::error::Result;
// use crate::value::Value;
//
//
// #[derive(PartialEq, Debug)]
// pub enum Op {
//     Add,
//     Sub,
//     Mul,
//     Div,
//     Eq,
//     Neq,
//     Gt,
//     Lt,
//     Ge,
//     Le,
//     Neg,
//     Minus,
//     Mod,
//     Or,
//     And,
//     Coalesce,
//     Pow,
//     Call,
//     IsNull,
//     NotNull,
// }
//
//
// lazy_static! {
//     static ref PREC_CLIMBER: PrecClimber<Rule> = {
//         use Assoc::*;
//
//         PrecClimber::new(vec![
//             Operator::new(Rule::op_or, Left),
//             Operator::new(Rule::op_and, Left),
//             Operator::new(Rule::op_gt, Left) | Operator::new(Rule::op_lt, Left) | Operator::new(Rule::op_ge,Left) | Operator::new(Rule::op_le, Left),
//             Operator::new(Rule::op_mod, Left),
//             Operator::new(Rule::op_eq, Left) | Operator::new(Rule::op_ne, Left),
//             Operator::new(Rule::op_add, Left) | Operator::new(Rule::op_sub, Left),
//             Operator::new(Rule::op_mul, Left) | Operator::new(Rule::op_div, Left),
//             Operator::new(Rule::op_pow, Assoc::Right),
//             Operator::new(Rule::op_coalesce, Assoc::Left)
//         ])
//     };
// }
//
//
// #[derive(PartialEq, Debug)]
// pub enum Expr<'a> {
//     List(Vec<Expr<'a>>),
//     Dict(Vec<String>, Vec<Expr<'a>>),
//     Apply(Op, Vec<Expr<'a>>),
//     Ident(String),
//     Const(Value<'a>),
// }
//
// pub trait ExprVisitor<'a, T> {
//     fn visit_expr(&self, ex: &Expr<'a>) -> T;
// }
//
//
// fn build_expr_infix<'a>(lhs: Result<Expr<'a>>, op: Pair<Rule>, rhs: Result<Expr<'a>>) -> Result<Expr<'a>> {
//     let lhs = lhs?;
//     let rhs = rhs?;
//     let op = match op.as_rule() {
//         Rule::op_add => Op::Add,
//         Rule::op_sub => Op::Sub,
//         Rule::op_mul => Op::Mul,
//         Rule::op_div => Op::Div,
//         Rule::op_eq => Op::Eq,
//         Rule::op_ne => Op::Neq,
//         Rule::op_or => Op::Or,
//         Rule::op_and => Op::And,
//         Rule::op_mod => Op::Mod,
//         Rule::op_gt => Op::Gt,
//         Rule::op_ge => Op::Ge,
//         Rule::op_lt => Op::Lt,
//         Rule::op_le => Op::Le,
//         Rule::op_pow => Op::Pow,
//         Rule::op_coalesce => Op::Coalesce,
//         _ => unreachable!()
//     };
//     Ok(Apply(op, vec![lhs, rhs]))
// }
//
//
// fn build_expr_primary(pair: Pair<Rule>) -> Result<Expr> {
//     match pair.as_rule() {
//         Rule::expr => build_expr_primary(pair.into_inner().next().unwrap()),
//         Rule::term => build_expr_primary(pair.into_inner().next().unwrap()),
//         Rule::grouping => build_expr(pair.into_inner().next().unwrap()),
//
//         Rule::unary => {
//             let mut inner = pair.into_inner();
//             let op = inner.next().unwrap().as_rule();
//             let term = build_expr_primary(inner.next().unwrap())?;
//             Ok(Apply(match op {
//                 Rule::negate => Op::Neg,
//                 Rule::minus => Op::Minus,
//                 _ => unreachable!()
//             }, vec![term]))
//         }
//
//         Rule::pos_int => Ok(Const(Value::Int(pair.as_str().replace('_', "").parse::<i64>()?))),
//         Rule::hex_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 16)))),
//         Rule::octo_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 8)))),
//         Rule::bin_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 2)))),
//         Rule::dot_float | Rule::sci_float => Ok(Const(Value::Float(pair.as_str().replace('_', "").parse::<f64>()?))),
//         Rule::null => Ok(Const(Value::Null)),
//         Rule::boolean => Ok(Const(Value::Bool(pair.as_str() == "true"))),
//         Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => Ok(
//             Const(Value::Text(Arc::new(Cow::Owned(parse_string(pair)?))))),
//         Rule::list => {
//             let mut vals = vec![];
//             let mut has_apply = false;
//             for p in pair.into_inner() {
//                 let res = build_expr_primary(p)?;
//                 match res {
//                     v @ Const(_) => { vals.push(v) }
//                     v => {
//                         has_apply = true;
//                         vals.push(v);
//                     }
//                 }
//             }
//             if has_apply {
//                 Ok(Expr::List(vals))
//             } else {
//                 Ok(Const(Value::List(Arc::new(vals.into_iter().map(|v| {
//                     match v {
//                         Apply(_, _) => { unreachable!() }
//                         Expr::List(_) => { unreachable!() }
//                         Expr::Dict(_, _) => { unreachable!() }
//                         Const(v) => { v }
//                         Expr::Ident(_) => unimplemented!()
//                     }
//                 }).collect()))))
//             }
//         }
//         Rule::dict => {
//             // let mut res = BTreeMap::new();
//             let mut keys = vec![];
//             let mut vals = vec![];
//             let mut has_apply = false;
//             for p in pair.into_inner() {
//                 match p.as_rule() {
//                     Rule::dict_pair => {
//                         let mut inner = p.into_inner();
//                         let name = parse_string(inner.next().unwrap())?;
//                         keys.push(name);
//                         match build_expr_primary(inner.next().unwrap())? {
//                             v @ Const(_) => {
//                                 vals.push(v);
//                             }
//                             v => {
//                                 has_apply = true;
//                                 vals.push(v);
//                             }
//                         }
//                     }
//                     _ => todo!()
//                 }
//             }
//             if has_apply {
//                 Ok(Expr::Dict(keys, vals))
//             } else {
//                 Ok(Const(Value::Dict(Arc::new(keys.into_iter().zip(vals.into_iter()).map(|(k, v)| {
//                     match v {
//                         Expr::List(_) => { unreachable!() }
//                         Expr::Dict(_, _) => { unreachable!() }
//                         Apply(_, _) => { unreachable!() }
//                         Const(v) => {
//                             (k.into(), v)
//                         }
//                         Expr::Ident(_) => unimplemented!()
//                     }
//                 }).collect()))))
//             }
//         }
//         Rule::param => {
//             Ok(Expr::Ident(pair.as_str().to_string()))
//         }
//         _ => {
//             println!("Unhandled rule {:?}", pair.as_rule());
//             unimplemented!()
//         }
//     }
// }
//
// pub fn build_expr(pair: Pair<Rule>) -> Result<Expr> {
//     PREC_CLIMBER.climb(pair.into_inner(), build_expr_primary, build_expr_infix)
// }
//
// pub fn parse_expr_from_str(inp: &str) -> Result<Expr> {
//     let expr_tree = Parser::parse(Rule::expr, inp)?.next().unwrap();
//     build_expr(expr_tree)
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn raw_string() {
//         println!("{:#?}", parse_expr_from_str(r#####"r#"x"#"#####))
//     }
//
//     #[test]
//     fn parse_literals() {
//         assert_eq!(parse_expr_from_str("1").unwrap(), Const(Value::Int(1)));
//         assert_eq!(parse_expr_from_str("12_3").unwrap(), Const(Value::Int(123)));
//         assert_eq!(parse_expr_from_str("0xaf").unwrap(), Const(Value::Int(0xaf)));
//         assert_eq!(parse_expr_from_str("0xafcE_f").unwrap(), Const(Value::Int(0xafcef)));
//         assert_eq!(parse_expr_from_str("0o1234_567").unwrap(), Const(Value::Int(0o1234567)));
//         assert_eq!(parse_expr_from_str("0o0001234_567").unwrap(), Const(Value::Int(0o1234567)));
//         assert_eq!(parse_expr_from_str("0b101010").unwrap(), Const(Value::Int(0b101010)));
//
//         assert_eq!(parse_expr_from_str("0.0").unwrap(), Const(Value::Float(0.)));
//         assert_eq!(parse_expr_from_str("10.022_3").unwrap(), Const(Value::Float(10.0223)));
//         assert_eq!(parse_expr_from_str("10.022_3e-100").unwrap(), Const(Value::Float(10.0223e-100)));
//
//         assert_eq!(parse_expr_from_str("null").unwrap(), Const(Value::Null));
//         assert_eq!(parse_expr_from_str("true").unwrap(), Const(Value::Bool(true)));
//         assert_eq!(parse_expr_from_str("false").unwrap(), Const(Value::Bool(false)));
//         assert_eq!(parse_expr_from_str(r#""x \n \ty \"""#).unwrap(), Const(Value::Text(Arc::new(Cow::Borrowed("x \n \ty \"")))));
//         // assert_eq!(parse_expr_from_str(r#""x'""#).unwrap(), Const(Value::RefString("x'")));
//         // assert_eq!(parse_expr_from_str(r#"'"x"'"#).unwrap(), Const(Value::RefString(r##""x""##)));
//         // assert_eq!(parse_expr_from_str(r#####"r###"x"yz"###"#####).unwrap(), Const(Value::RefString(r##"x"yz"##)));
//     }
// }