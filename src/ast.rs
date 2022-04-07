use pest::iterators::{Pair, Pairs};
use pest::Parser as PestParser;
use pest::prec_climber::{Assoc, PrecClimber, Operator};
use crate::parser::Parser;
use crate::parser::Rule;
use lazy_static::lazy_static;
use crate::ast::Expr::Const;
use crate::value::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CozoError {
    #[error("Invalid UTF code")]
    InvalidUtfCode,

    #[error("Type mismatch")]
    InfixTypeMismatch {
        op: Rule,
        lhs: Value<'static>,
        rhs: Value<'static>,
    },

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    Parse(#[from] pest::error::Error<Rule>),
}


lazy_static! {
    static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use Assoc::*;

        PrecClimber::new(vec![
            Operator::new(Rule::op_or, Left),
            Operator::new(Rule::op_and, Left),
            Operator::new(Rule::op_gt, Left) | Operator::new(Rule::op_lt, Left) | Operator::new(Rule::op_ge,Left) | Operator::new(Rule::op_le, Left),
            Operator::new(Rule::op_mod, Left),
            Operator::new(Rule::op_eq, Left) | Operator::new(Rule::op_ne, Left),
            Operator::new(Rule::op_add, Left) | Operator::new(Rule::op_sub, Left),
            Operator::new(Rule::op_mul, Left) | Operator::new(Rule::op_div, Left),
            Operator::new(Rule::op_pow, Assoc::Right),
            Operator::new(Rule::op_coalesce, Assoc::Left)
        ])
    };
}

#[derive(PartialEq, Debug)]
pub enum Expr<'a> {
    UnaryOp,
    BinaryOp,
    AssocOp,
    Accessor,
    FnCall,
    Const(Value<'a>),
}

fn build_expr_infix<'a>(lhs: Result<Expr<'a>, CozoError>, op: Pair<Rule>, rhs: Result<Expr<'a>, CozoError>) -> Result<Expr<'a>, CozoError> {
    let lhs = lhs?;
    let rhs = rhs?;
    if let (Const(a), Const(b)) = (lhs, rhs) {
        let rule = op.as_rule();
        return match rule {
            Rule::op_add => {
                match (a, b) {
                    (Value::Null, _) => Ok(Const(Value::Null)),
                    (_, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Int(va), Value::Int(vb)) => Ok(Const(Value::Int(va + vb))),
                    (Value::Float(va), Value::Int(vb)) => Ok(Const(Value::Float(va + vb as f64))),
                    (Value::Int(va), Value::Float(vb)) => Ok(Const(Value::Float(va as f64 + vb))),
                    (Value::Float(va), Value::Float(vb)) => Ok(Const(Value::Float(va + vb))),
                    (Value::OwnString(va), Value::OwnString(vb)) => Ok(Const(Value::OwnString(Box::new(*va + &*vb)))),
                    (Value::OwnString(va), Value::RefString(vb)) => Ok(Const(Value::OwnString(Box::new(*va + &*vb)))),
                    (Value::RefString(va), Value::OwnString(vb)) => Ok(Const(Value::OwnString(Box::new(va.to_string() + &*vb)))),
                    (Value::RefString(va), Value::RefString(vb)) => Ok(Const(Value::OwnString(Box::new(va.to_string() + &*vb)))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_sub => {
                match (a, b) {
                    (Value::Null, _) => Ok(Const(Value::Null)),
                    (_, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Int(va), Value::Int(vb)) => Ok(Const(Value::Int(va - vb))),
                    (Value::Float(va), Value::Int(vb)) => Ok(Const(Value::Float(va - vb as f64))),
                    (Value::Int(va), Value::Float(vb)) => Ok(Const(Value::Float(va as f64 - vb))),
                    (Value::Float(va), Value::Float(vb)) => Ok(Const(Value::Float(va - vb))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_mul => {
                match (a, b) {
                    (Value::Null, _) => Ok(Const(Value::Null)),
                    (_, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Int(va), Value::Int(vb)) => Ok(Const(Value::Int(va * vb))),
                    (Value::Float(va), Value::Int(vb)) => Ok(Const(Value::Float(va * vb as f64))),
                    (Value::Int(va), Value::Float(vb)) => Ok(Const(Value::Float(va as f64 * vb))),
                    (Value::Float(va), Value::Float(vb)) => Ok(Const(Value::Float(va * vb))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_div => {
                match (a, b) {
                    (Value::Null, _) => Ok(Const(Value::Null)),
                    (_, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Int(va), Value::Int(vb)) => Ok(Const(Value::Float(va as f64 / vb as f64))),
                    (Value::Float(va), Value::Int(vb)) => Ok(Const(Value::Float(va / vb as f64))),
                    (Value::Int(va), Value::Float(vb)) => Ok(Const(Value::Float(va as f64 / vb))),
                    (Value::Float(va), Value::Float(vb)) => Ok(Const(Value::Float(va / vb))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_eq => Ok(Const(Value::Bool(a == b))),
            Rule::op_ne => Ok(Const(Value::Bool(a != b))),
            Rule::op_or => {
                match (a, b) {
                    (Value::Null, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Null, Value::Bool(b)) => Ok(Const(Value::Bool(b))),
                    (Value::Bool(b), Value::Null) => Ok(Const(Value::Bool(b))),
                    (Value::Bool(a), Value::Bool(b)) => Ok(Const(Value::Bool(a || b))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_and => {
                match (a, b) {
                    (Value::Null, Value::Null) => Ok(Const(Value::Null)),
                    (Value::Null, Value::Bool(_)) => Ok(Const(Value::Null)),
                    (Value::Bool(_), Value::Null) => Ok(Const(Value::Null)),
                    (Value::Bool(a), Value::Bool(b)) => Ok(Const(Value::Bool(a && b))),
                    (a, b) => Err(CozoError::InfixTypeMismatch { op: rule, lhs: a.into_owned(), rhs: b.into_owned() })
                }
            }
            Rule::op_coalesce => Ok(if a == Value::Null { Const(b) } else { Const(a) }),
            _ => { unimplemented!() }
        };
    } else {
        unimplemented!()
    }
}

#[inline]
fn parse_int(s: &str, radix: u32) -> i64 {
    i64::from_str_radix(&s[2..].replace('_', ""), radix).unwrap()
}

#[inline]
fn parse_quoted_string(pairs: Pairs<Rule>) -> Result<String, CozoError> {
    let mut ret = String::new();
    for pair in pairs {
        let s = pair.as_str();
        match s {
            r#"\""# => ret.push('"'),
            r"\\" => ret.push('\\'),
            r"\/" => ret.push('/'),
            r"\b" => ret.push('\x08'),
            r"\f" => ret.push('\x0c'),
            r"\n" => ret.push('\n'),
            r"\r" => ret.push('\r'),
            r"\t" => ret.push('\t'),
            s if s.starts_with(r"\u") => {
                let code = parse_int(s, 16) as u32;
                let ch = char::from_u32(code).ok_or(CozoError::InvalidUtfCode)?;
                ret.push(ch);
            }
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}


#[inline]
fn parse_s_quoted_string(pairs: Pairs<Rule>) -> Result<String, CozoError> {
    let mut ret = String::new();
    for pair in pairs {
        let s = pair.as_str();
        match s {
            r#"\'"# => ret.push('\''),
            r"\\" => ret.push('\\'),
            r"\/" => ret.push('/'),
            r"\b" => ret.push('\x08'),
            r"\f" => ret.push('\x0c'),
            r"\n" => ret.push('\n'),
            r"\r" => ret.push('\r'),
            r"\t" => ret.push('\t'),
            s if s.starts_with(r"\u") => {
                let code = parse_int(s, 16) as u32;
                let ch = char::from_u32(code).ok_or(CozoError::InvalidUtfCode)?;
                ret.push(ch);
            }
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}

fn build_expr_primary(pair: Pair<Rule>) -> Result<Expr, CozoError> {
    match pair.as_rule() {
        Rule::expr => build_expr_primary(pair.into_inner().next().unwrap()),
        Rule::term => build_expr_primary(pair.into_inner().next().unwrap()),

        Rule::pos_int => Ok(Const(Value::Int(pair.as_str().replace('_', "").parse::<i64>()?))),
        Rule::hex_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 16)))),
        Rule::octo_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 8)))),
        Rule::bin_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 2)))),
        Rule::dot_float | Rule::sci_float => Ok(Const(Value::Float(pair.as_str().replace('_', "").parse::<f64>()?))),
        Rule::null => Ok(Const(Value::Null)),
        Rule::boolean => Ok(Const(Value::Bool(pair.as_str() == "true"))),
        Rule::quoted_string => Ok(Const(Value::OwnString(Box::new(parse_quoted_string(pair.into_inner().next().unwrap().into_inner())?)))),
        Rule::s_quoted_string => Ok(Const(Value::OwnString(Box::new(parse_s_quoted_string(pair.into_inner().next().unwrap().into_inner())?)))),
        _ => {
            println!("{:#?}", pair);
            unimplemented!()
        }
    }
}

fn build_expr(pair: Pair<Rule>) -> Result<Expr, CozoError> {
    PREC_CLIMBER.climb(pair.into_inner(), build_expr_primary, build_expr_infix)
}

pub fn parse_expr_from_str(inp: &str) -> Result<Expr, CozoError> {
    let expr_tree = Parser::parse(Rule::expr, inp)?.next().unwrap();
    build_expr(expr_tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_literals() {
        assert_eq!(parse_expr_from_str("1").unwrap(), Const(Value::Int(1)));
        assert_eq!(parse_expr_from_str("12_3").unwrap(), Const(Value::Int(123)));
        assert_eq!(parse_expr_from_str("0xaf").unwrap(), Const(Value::Int(0xaf)));
        assert_eq!(parse_expr_from_str("0xafcE_f").unwrap(), Const(Value::Int(0xafcef)));
        assert_eq!(parse_expr_from_str("0o1234_567").unwrap(), Const(Value::Int(0o1234567)));
        assert_eq!(parse_expr_from_str("0o0001234_567").unwrap(), Const(Value::Int(0o1234567)));
        assert_eq!(parse_expr_from_str("0b101010").unwrap(), Const(Value::Int(0b101010)));

        assert_eq!(parse_expr_from_str("0.0").unwrap(), Const(Value::Float(0.)));
        assert_eq!(parse_expr_from_str("10.022_3").unwrap(), Const(Value::Float(10.0223)));
        assert_eq!(parse_expr_from_str("10.022_3e-100").unwrap(), Const(Value::Float(10.0223e-100)));

        assert_eq!(parse_expr_from_str("null").unwrap(), Const(Value::Null));
        assert_eq!(parse_expr_from_str("true").unwrap(), Const(Value::Bool(true)));
        assert_eq!(parse_expr_from_str("false").unwrap(), Const(Value::Bool(false)));
        assert_eq!(parse_expr_from_str(r#""x \n \ty \"""#).unwrap(), Const(Value::RefString("x \n \ty \"")));
        assert_eq!(parse_expr_from_str(r#""x'""#).unwrap(), Const(Value::RefString("x'")));
        assert_eq!(parse_expr_from_str(r#"'"x"'"#).unwrap(), Const(Value::RefString(r##""x""##)));
    }

    #[test]
    fn operators() {
        println!("{:#?}", parse_expr_from_str("1/10+2+3*4").unwrap());
    }
}