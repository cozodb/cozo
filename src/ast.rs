use std::iter;
use pest::iterators::{Pair, Pairs};
use pest::Parser as PestParser;
use pest::prec_climber::{Assoc, PrecClimber, Operator};
use crate::parser::Parser;
use crate::parser::Rule;
use anyhow::Result;
use lazy_static::lazy_static;
use crate::ast::Expr::Const;
use crate::value::Value;


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

fn parse_expr_infix<'a>(_lhs: Expr<'a>, _op: Pair<Rule>, _rhs: Expr<'a>) -> Expr<'a> {
    unimplemented!()
}

#[inline]
fn parse_int(s: &str, radix: u32) -> i64 {
    i64::from_str_radix(&s[2..].replace('_', ""), radix).unwrap()
}

#[inline]
fn parse_quoted_string(pairs: Pairs<Rule>) -> String {
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
                let ch = char::from_u32(code).unwrap_or('\u{FFFD}');
                ret.push(ch);
            }
            s => ret.push_str(s)
        }
    }
    ret
}


#[inline]
fn parse_s_quoted_string(pairs: Pairs<Rule>) -> String {
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
                let ch = char::from_u32(code).unwrap_or('\u{FFFD}');
                ret.push(ch);
            }
            s => ret.push_str(s)
        }
    }
    ret
}

fn parse_expr_primary(pair: Pair<Rule>) -> Expr {
    match pair.as_rule() {
        Rule::expr => parse_expr_primary(pair.into_inner().next().unwrap()),
        Rule::term => parse_expr_primary(pair.into_inner().next().unwrap()),

        Rule::pos_int => Const(Value::Int(pair.as_str().replace('_', "").parse::<i64>().unwrap())),
        Rule::hex_pos_int => Const(Value::Int(parse_int(pair.as_str(), 16))),
        Rule::octo_pos_int => Const(Value::Int(parse_int(pair.as_str(), 8))),
        Rule::bin_pos_int => Const(Value::Int(parse_int(pair.as_str(), 2))),
        Rule::dot_float | Rule::sci_float => Const(Value::Float(pair.as_str().replace('_', "").parse::<f64>().unwrap())),
        Rule::null => Const(Value::Null),
        Rule::boolean => Const(Value::Bool(pair.as_str() == "true")),
        Rule::quoted_string => Const(Value::OwnString(Box::new(parse_quoted_string(pair.into_inner().next().unwrap().into_inner())))),
        Rule::s_quoted_string => Const(Value::OwnString(Box::new(parse_s_quoted_string(pair.into_inner().next().unwrap().into_inner())))),
        _ => {
            println!("{:#?}", pair);
            unimplemented!()
        }
    }
}

fn parse_expr(pair: Pair<Rule>) -> Expr {
    PREC_CLIMBER.climb(iter::once(pair), parse_expr_primary, parse_expr_infix)
}

pub fn parse_expr_from_str(inp: &str) -> Result<Expr> {
    let expr_tree = Parser::parse(Rule::expr, inp)?.next().unwrap();
    Ok(parse_expr(expr_tree))
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
}