use std::borrow::Cow;
use std::collections::BTreeMap;
use pest::prec_climber::{Assoc, Operator, PrecClimber};
use std::result;
use std::sync::Arc;
use lazy_static::lazy_static;
use pest::iterators::Pair;
use crate::data::expr::{Expr, ExprError};
use crate::data::op::{Op, OpAdd, OpAnd, OpCoalesce, OpConcat, OpDiv, OpEq, OpGe, OpGt, OpLe, OpLt, OpMerge, OpMinus, OpMod, OpMul, OpNe, OpNegate, OpOr, OpPow, OpStrCat, OpSub, UnresolvedOp};
use crate::data::value::Value;
use crate::parser::number::parse_int;
use crate::parser::Rule;
use crate::parser::text_identifier::{parse_string};

#[derive(thiserror::Error, Debug)]
pub(crate) enum ExprParseError {
    #[error(transparent)]
    TextParser(#[from] crate::parser::text_identifier::TextParseError),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Cannot spread {0}")]
    SpreadingError(String),
}

type Result<T> = result::Result<T, ExprParseError>;

impl<'a> TryFrom<Pair<'a, Rule>> for Expr<'a> {
    type Error = ExprParseError;

    fn try_from(pair: Pair<'a, Rule>) -> Result<Self> {
        PREC_CLIMBER.climb(pair.into_inner(), build_expr_primary, build_expr_infix)
    }
}

lazy_static! {
    static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use Assoc::*;

        PrecClimber::new(vec![
            Operator::new(Rule::op_or, Left),
            Operator::new(Rule::op_and, Left),
            Operator::new(Rule::op_gt, Left)
                | Operator::new(Rule::op_lt, Left)
                | Operator::new(Rule::op_ge, Left)
                | Operator::new(Rule::op_le, Left),
            Operator::new(Rule::op_mod, Left),
            Operator::new(Rule::op_eq, Left) | Operator::new(Rule::op_ne, Left),
            Operator::new(Rule::op_add, Left)
                | Operator::new(Rule::op_sub, Left)
                | Operator::new(Rule::op_str_cat, Left),
            Operator::new(Rule::op_mul, Left) | Operator::new(Rule::op_div, Left),
            Operator::new(Rule::op_pow, Assoc::Right),
            Operator::new(Rule::op_coalesce, Assoc::Left),
        ])
    };
}

fn build_expr_primary(pair: Pair<Rule>) -> Result<Expr> {
    match pair.as_rule() {
        Rule::expr => build_expr_primary(pair.into_inner().next().unwrap()),
        Rule::term => {
            let mut pairs = pair.into_inner();
            let mut head = build_expr_primary(pairs.next().unwrap())?;
            for p in pairs {
                match p.as_rule() {
                    Rule::accessor => {
                        let accessor_key = p.into_inner().next().unwrap().as_str();
                        head = Expr::FieldAcc(accessor_key.into(), head.into());
                    }
                    Rule::index_accessor => {
                        let accessor_key = p.into_inner().next().unwrap();
                        let accessor_idx = parse_int(accessor_key.as_str(), 10);
                        head = Expr::IdxAcc(accessor_idx as usize, head.into());
                    }
                    Rule::call => {
                        let mut pairs = p.into_inner();
                        let method_name = pairs.next().unwrap().as_str();
                        let op = Arc::new(UnresolvedOp(method_name.to_string()));
                        let mut args = vec![head];
                        args.extend(pairs.map(Expr::try_from).collect::<Result<Vec<_>>>()?);
                        head = Expr::Apply(op, args);
                    }
                    _ => todo!(),
                }
            }
            Ok(head)
        }
        Rule::grouping => Expr::try_from(pair.into_inner().next().unwrap()),

        Rule::unary => {
            let mut inner = pair.into_inner();
            let p = inner.next().unwrap();
            let op = p.as_rule();
            let op: Arc<dyn Op> = match op {
                Rule::term => return build_expr_primary(p),
                Rule::negate => Arc::new(OpNegate),
                Rule::minus => Arc::new(OpMinus),
                _ => unreachable!(),
            };
            let term = build_expr_primary(inner.next().unwrap())?;
            Ok(Expr::Apply(op, vec![term]))
        }

        Rule::pos_int => Ok(Expr::Const(Value::Int(pair.as_str().replace('_', "").parse::<i64>()?))),
        Rule::hex_pos_int => Ok(Expr::Const(Value::Int(parse_int(pair.as_str(), 16)))),
        Rule::octo_pos_int => Ok(Expr::Const(Value::Int(parse_int(pair.as_str(), 8)))),
        Rule::bin_pos_int => Ok(Expr::Const(Value::Int(parse_int(pair.as_str(), 2)))),
        Rule::dot_float | Rule::sci_float => Ok(Expr::Const(Value::Float(
            pair.as_str().replace('_', "").parse::<f64>()?.into(),
        ))),
        Rule::null => Ok(Expr::Const(Value::Null)),
        Rule::boolean => Ok(Expr::Const(Value::Bool(pair.as_str() == "true"))),
        Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => {
            Ok(Expr::Const(Value::Text(Cow::Owned(parse_string(pair)?))))
        }
        Rule::list => {
            let mut spread_collected = vec![];
            let mut collected = vec![];
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::expr => collected.push(Expr::try_from(p)?),
                    Rule::spreading => {
                        let el = p.into_inner().next().unwrap();
                        let to_concat = Expr::try_from(el)?;
                        if !matches!(
                            to_concat,
                            Expr::List(_)
                                | Expr::Variable(_)
                                | Expr::IdxAcc(_, _)
                                | Expr::FieldAcc(_, _)
                                | Expr::Apply(_, _)
                        ) {
                            return Err(ExprParseError::SpreadingError(format!("{:?}", to_concat)));
                        }
                        if !collected.is_empty() {
                            spread_collected.push(Expr::List(collected));
                            collected = vec![];
                        }
                        spread_collected.push(to_concat);
                    }
                    _ => unreachable!(),
                }
            }
            if spread_collected.is_empty() {
                return Ok(Expr::List(collected));
            }
            if !collected.is_empty() {
                spread_collected.push(Expr::List(collected));
            }
            Ok(Expr::Apply(Arc::new(OpConcat), spread_collected))
        }
        Rule::dict => {
            let mut spread_collected = vec![];
            let mut collected = BTreeMap::new();
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::dict_pair => {
                        let mut inner = p.into_inner();
                        let name = parse_string(inner.next().unwrap())?;
                        let val = Expr::try_from(inner.next().unwrap())?;
                        collected.insert(name.into(), val);
                    }
                    Rule::scoped_accessor => {
                        let name = parse_string(p.into_inner().next().unwrap())?;
                        let val = Expr::FieldAcc(
                            name.clone().into(),
                            Expr::Variable("_".into()).into(),
                        );
                        collected.insert(name.into(), val);
                    }
                    Rule::spreading => {
                        let el = p.into_inner().next().unwrap();
                        let to_concat = build_expr_primary(el)?;
                        if !matches!(
                            to_concat,
                            Expr::Dict(_)
                                | Expr::Variable(_)
                                | Expr::IdxAcc(_, _)
                                | Expr::FieldAcc(_, _)
                                | Expr::Apply(_, _)
                        ) {
                            return Err(ExprParseError::SpreadingError(format!("{:?}", to_concat)));
                        }
                        if !collected.is_empty() {
                            spread_collected.push(Expr::Dict(collected));
                            collected = BTreeMap::new();
                        }
                        spread_collected.push(to_concat);
                    }
                    _ => unreachable!(),
                }
            }

            if spread_collected.is_empty() {
                return Ok(Expr::Dict(collected));
            }

            if !collected.is_empty() {
                spread_collected.push(Expr::Dict(collected));
            }
            Ok(Expr::Apply(Arc::new(OpMerge), spread_collected))
        }
        Rule::param => Ok(Expr::Variable(pair.as_str().into())),
        Rule::ident => Ok(Expr::Variable(pair.as_str().into())),
        _ => {
            println!("Unhandled rule {:?}", pair.as_rule());
            unimplemented!()
        }
    }
}


fn build_expr_infix<'a>(
    lhs: Result<Expr<'a>>,
    op: Pair<Rule>,
    rhs: Result<Expr<'a>>,
) -> Result<Expr<'a>> {
    let lhs = lhs?;
    let rhs = rhs?;
    let op: Arc<dyn Op> = match op.as_rule() {
        Rule::op_add => Arc::new(OpAdd),
        Rule::op_str_cat => Arc::new(OpStrCat),
        Rule::op_sub => Arc::new(OpSub),
        Rule::op_mul => Arc::new(OpMul),
        Rule::op_div => Arc::new(OpDiv),
        Rule::op_eq => Arc::new(OpEq),
        Rule::op_ne => Arc::new(OpNe),
        Rule::op_or => Arc::new(OpOr),
        Rule::op_and => Arc::new(OpAnd),
        Rule::op_mod => Arc::new(OpMod),
        Rule::op_gt => Arc::new(OpGt),
        Rule::op_ge => Arc::new(OpGe),
        Rule::op_lt => Arc::new(OpLt),
        Rule::op_le => Arc::new(OpLe),
        Rule::op_pow => Arc::new(OpPow),
        Rule::op_coalesce => Arc::new(OpCoalesce),
        _ => unreachable!(),
    };
    Ok(Expr::Apply(op, vec![lhs, rhs]))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::CozoParser;
    use pest::Parser;
    use crate::data::expr::StaticExpr;

    fn parse_expr_from_str(s: &str) -> Result<Expr> {
        let pair = CozoParser::parse(Rule::expr, s.as_ref())
            .unwrap()
            .next()
            .unwrap();
        Expr::try_from(pair)
    }

    #[test]
    fn raw_string() {
        assert!(dbg!(parse_expr_from_str(r#####"r#"x"#"#####)).is_ok());
    }

    #[test]
    fn unevaluated() {
        assert!(dbg!(parse_expr_from_str("a+b*c+d")).is_ok());
    }

    #[test]
    fn parse_literals() {
        assert_eq!(parse_expr_from_str("1").unwrap(), Expr::Const(Value::Int(1)));
        assert_eq!(parse_expr_from_str("12_3").unwrap(), Expr::Const(Value::Int(123)));
        assert_eq!(parse_expr_from_str("0xaf").unwrap(), Expr::Const(Value::Int(0xaf)));
        assert_eq!(
            parse_expr_from_str("0xafcE_f").unwrap(),
            Expr::Const(Value::Int(0xafcef)
            ));
        assert_eq!(
            parse_expr_from_str("0o1234_567").unwrap(),
            Expr::Const(Value::Int(0o1234567)
            ));
        assert_eq!(
            parse_expr_from_str("0o0001234_567").unwrap(),
            Expr::Const(Value::Int(0o1234567)
            ));
        assert_eq!(
            parse_expr_from_str("0b101010").unwrap(),
            Expr::Const(Value::Int(0b101010)
            ));

        assert_eq!(
            parse_expr_from_str("0.0").unwrap(),
            Expr::Const(Value::Float((0.).into())
            ));
        assert_eq!(
            parse_expr_from_str("10.022_3").unwrap(),
            Expr::Const(Value::Float(10.0223.into())
            ));
        assert_eq!(
            parse_expr_from_str("10.022_3e-100").unwrap(),
            Expr::Const(Value::Float(10.0223e-100.into())
            ));

        assert_eq!(parse_expr_from_str("null").unwrap(), Expr::Const(Value::Null));
        assert_eq!(parse_expr_from_str("true").unwrap(), Expr::Const(Value::Bool(true)));
        assert_eq!(parse_expr_from_str("false").unwrap(), Expr::Const(Value::Bool(false)));
        assert_eq!(
            parse_expr_from_str(r#""x \n \ty \"""#).unwrap(),
            Expr::Const(Value::Text(Cow::Borrowed("x \n \ty \""))
            ));
        assert_eq!(
            parse_expr_from_str(r#""x'""#).unwrap(),
            Expr::Const(Value::Text("x'".into())
            ));
        assert_eq!(
            parse_expr_from_str(r#"'"x"'"#).unwrap(),
            Expr::Const(Value::Text(r##""x""##.into())
            ));
        assert_eq!(
            parse_expr_from_str(r#####"r###"x"yz"###"#####).unwrap(),
            (Expr::Const(Value::Text(r##"x"yz"##.into()))
            ));
    }

    #[test]
    fn complex_cases() -> Result<()> {
        dbg!(parse_expr_from_str("{}")?);
        dbg!(parse_expr_from_str("{b:1,a,c:2,d,...e,}")?);
        dbg!(parse_expr_from_str("{...a,...b,c:1,d:2,...e,f:3}")?);
        dbg!(parse_expr_from_str("[]")?);
        dbg!(parse_expr_from_str("[...a,...b,1,2,...e,3]")?);
        Ok(())
    }
}
