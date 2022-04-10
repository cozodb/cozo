use pest::iterators::{Pair, Pairs};
use pest::Parser as PestParser;
use pest::prec_climber::{Assoc, PrecClimber, Operator};
use crate::parser::Parser;
use crate::parser::Rule;
use lazy_static::lazy_static;
use crate::ast::Expr::{Apply, Const};
use crate::error::CozoError;
use crate::error::CozoError::ReservedIdent;
use crate::typing::{BaseType, Typing};
use crate::value::Value;



#[derive(PartialEq, Debug)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    Neg,
    Minus,
    Mod,
    Or,
    And,
    Coalesce,
    Pow,
    Call,
    IsNull,
    NotNull
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

#[derive(Debug, PartialEq)]
pub struct Col {
    pub name: String,
    pub typ: Typing,
    pub default: Option<Value<'static>>,
}

#[derive(Debug, PartialEq)]
pub enum TableDef {
    Node {
        is_local: bool,
        name: String,
        keys: Vec<Col>,
        cols: Vec<Col>,
    },
    Edge {
        is_local: bool,
        src: String,
        dst: String,
        name: String,
        keys: Vec<Col>,
        cols: Vec<Col>,
    },
    Columns {
        is_local: bool,
        attached: String,
        name: String,
        cols: Vec<Col>,
    },
    Index {
        is_local: bool,
        name: String,
        attached: String,
        cols: Vec<String>,
    },
}


#[derive(PartialEq, Debug)]
pub enum Expr<'a> {
    Apply(Op, Vec<Expr<'a>>),
    Const(Value<'a>),
}

pub trait ExprVisitor<'a, T> {
    fn visit_expr(&mut self, ex: &Expr<'a>) -> T;
}


fn build_expr_infix<'a>(lhs: Result<Expr<'a>, CozoError>, op: Pair<Rule>, rhs: Result<Expr<'a>, CozoError>) -> Result<Expr<'a>, CozoError> {
    let lhs = lhs?;
    let rhs = rhs?;
    let op = match op.as_rule() {
        Rule::op_add => Op::Add,
        Rule::op_sub => Op::Sub,
        Rule::op_mul => Op::Mul,
        Rule::op_div => Op::Div,
        Rule::op_eq => Op::Eq,
        Rule::op_ne => Op::Neq,
        Rule::op_or => Op::Or,
        Rule::op_and => Op::And,
        Rule::op_mod => Op::Mod,
        Rule::op_gt => Op::Gt,
        Rule::op_ge => Op::Ge,
        Rule::op_lt => Op::Lt,
        Rule::op_le => Op::Le,
        Rule::op_pow => Op::Pow,
        Rule::op_coalesce => Op::Coalesce,
        _ => unreachable!()
    };
    Ok(Apply(op, vec![lhs, rhs]))
}

#[inline]
fn parse_int(s: &str, radix: u32) -> i64 {
    i64::from_str_radix(&s[2..].replace('_', ""), radix).unwrap()
}

#[inline]
fn parse_raw_string(pair: Pair<Rule>) -> Result<String, CozoError> {
    Ok(pair.into_inner().into_iter().next().unwrap().as_str().to_string())
}

#[inline]
fn parse_quoted_string(pair: Pair<Rule>) -> Result<String, CozoError> {
    let pairs = pair.into_inner().next().unwrap().into_inner();
    let mut ret = String::with_capacity(pairs.as_str().len());
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
            s if s.starts_with('\\') => return Err(CozoError::InvalidEscapeSequence),
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}


#[inline]
fn parse_s_quoted_string(pair: Pair<Rule>) -> Result<String, CozoError> {
    let pairs = pair.into_inner().next().unwrap().into_inner();
    let mut ret = String::with_capacity(pairs.as_str().len());
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
            s if s.starts_with('\\') => return Err(CozoError::InvalidEscapeSequence),
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}

fn build_expr_primary(pair: Pair<Rule>) -> Result<Expr, CozoError> {
    match pair.as_rule() {
        Rule::expr => build_expr_primary(pair.into_inner().next().unwrap()),
        Rule::term => build_expr_primary(pair.into_inner().next().unwrap()),
        Rule::grouping => build_expr(pair.into_inner().next().unwrap()),

        Rule::unary => {
            let mut inner = pair.into_inner();
            let op = inner.next().unwrap().as_rule();
            let term = build_expr_primary(inner.next().unwrap())?;
            Ok(Apply(match op {
                Rule::negate => Op::Neg,
                Rule::minus => Op::Minus,
                _ => unreachable!()
            }, vec![term]))
        }

        Rule::pos_int => Ok(Const(Value::Int(pair.as_str().replace('_', "").parse::<i64>()?))),
        Rule::hex_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 16)))),
        Rule::octo_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 8)))),
        Rule::bin_pos_int => Ok(Const(Value::Int(parse_int(pair.as_str(), 2)))),
        Rule::dot_float | Rule::sci_float => Ok(Const(Value::Float(pair.as_str().replace('_', "").parse::<f64>()?))),
        Rule::null => Ok(Const(Value::Null)),
        Rule::boolean => Ok(Const(Value::Bool(pair.as_str() == "true"))),
        Rule::quoted_string => Ok(Const(Value::OwnString(Box::new(parse_quoted_string(pair)?)))),
        Rule::s_quoted_string => Ok(Const(Value::OwnString(Box::new(parse_s_quoted_string(pair)?)))),
        Rule::raw_string => Ok(Const(Value::OwnString(Box::new(parse_raw_string(pair)?)))),
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

fn parse_ident(pair: Pair<Rule>) -> String {
    pair.as_str().to_string()
}

fn build_name_in_def(pair: Pair<Rule>, forbid_underscore: bool) -> Result<String, CozoError> {
    let inner = pair.into_inner().next().unwrap();
    let name = match inner.as_rule() {
        Rule::ident => parse_ident(inner),
        Rule::raw_string => parse_raw_string(inner)?,
        Rule::s_quoted_string => parse_s_quoted_string(inner)?,
        Rule::quoted_string => parse_quoted_string(inner)?,
        _ => unreachable!()
    };
    if forbid_underscore && name.starts_with('_') {
        Err(ReservedIdent)
    } else {
        Ok(name)
    }
}

fn parse_col_name(pair: Pair<Rule>) -> Result<(String, bool), CozoError> {
    let mut pairs = pair.into_inner();
    let mut is_key = false;
    let mut nxt_pair = pairs.next().unwrap();
    if nxt_pair.as_rule() == Rule::key_marker {
        is_key = true;
        nxt_pair = pairs.next().unwrap();
    }

    Ok((build_name_in_def(nxt_pair, true)?, is_key))
}

fn build_col_entry(pair: Pair<Rule>) -> Result<(Col, bool), CozoError> {
    let mut pairs = pair.into_inner();
    let (name, is_key) = parse_col_name(pairs.next().unwrap())?;
    Ok((Col {
        name,
        typ: Typing::Base(BaseType::Int),
        default: None,
    }, is_key))
}

fn build_col_defs(pair: Pair<Rule>) -> Result<(Vec<Col>, Vec<Col>), CozoError> {
    let mut keys = vec![];
    let mut cols = vec![];
    for pair in pair.into_inner() {
        let (col, is_key) = build_col_entry(pair)?;
        if is_key {
            keys.push(col)
        } else {
            cols.push(col)
        }
    }

    Ok((keys, cols))
}

fn build_node_def(pair: Pair<Rule>, is_local: bool) -> Result<TableDef, CozoError> {
    let mut inner = pair.into_inner();
    let name = build_name_in_def(inner.next().unwrap(), true)?;
    let (keys, cols) = build_col_defs(inner.next().unwrap())?;
    Ok(TableDef::Node {
        is_local,
        name,
        keys,
        cols,
    })
}

pub fn build_statements(pairs: Pairs<Rule>) -> Result<Vec<TableDef>, CozoError> {
    let mut ret = vec![];
    for pair in pairs {
        match pair.as_rule() {
            r @ (Rule::global_def | Rule::local_def) => {
                let inner = pair.into_inner().next().unwrap();
                let is_local = r == Rule::local_def;
                // println!("{:?} {:?}", r, inner.as_rule());
                match inner.as_rule() {
                    Rule::node_def => {
                        ret.push(build_node_def(inner, is_local)?);
                    }
                    _ => todo!()
                }
            }
            Rule::EOI => {}
            _ => unreachable!()
        }
    }
    Ok(ret)
}

pub fn build_statements_from_str(inp: &str) -> Result<Vec<TableDef>, CozoError> {
    let expr_tree = Parser::parse(Rule::file, inp)?;
    build_statements(expr_tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_string() {
        println!("{:#?}", parse_expr_from_str(r#####"r#"x"#"#####))
    }

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
        assert_eq!(parse_expr_from_str(r#####"r###"x"yz"###"#####).unwrap(), Const(Value::RefString(r##"x"yz"##)));
    }

    #[test]
    fn definitions() {
        println!("{:#?}", build_statements_from_str(r#"
            local node "Person" {
                *id: Int,
                name: String,
                email: ?String,
                habits: [String]
            }
        "#).unwrap());
    }
}