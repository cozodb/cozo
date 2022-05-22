use crate::data::expr::Expr;
use crate::data::op::*;
use crate::data::value::Value;
use crate::parser::number::parse_int;
use crate::parser::text_identifier::parse_string;
use crate::parser::{Pair, Rule};
use lazy_static::lazy_static;
use pest::prec_climber::{Assoc, Operator, PrecClimber};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::result;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum ExprParseError {
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

impl<'a> TryFrom<Pair<'a>> for Expr<'a> {
    type Error = ExprParseError;

    fn try_from(pair: Pair<'a>) -> Result<Self> {
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

fn build_if_expr(pair: Pair) -> Result<Expr> {
    let mut if_parts = vec![];
    let mut else_part = Expr::Const(Value::Null);
    for pair in pair.into_inner() {
        if pair.as_rule() == Rule::else_clause {
            else_part = Expr::try_from(pair)?;
        } else {
            if_parts.push(build_if_clause(pair)?)
        }
    }
    Ok(if_parts
        .into_iter()
        .rev()
        .fold(else_part, |accum, (cond, expr)| {
            Expr::IfExpr((cond, expr, accum).into())
        }))
}

fn build_cond_expr(pair: Pair) -> Result<Expr> {
    let mut res = Expr::Const(Value::Null);
    for pair in pair.into_inner().rev() {
        let (cond, expr) = build_switch_pattern(pair)?;
        res = Expr::IfExpr((cond, expr, res).into());
    }
    Ok(res)
}

fn build_call_expr(pair: Pair) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let method = get_method(pairs.next().unwrap().as_str());
    let args = pairs.map(Expr::try_from).collect::<Result<Vec<_>>>()?;
    Ok(Expr::Apply(method, args))
}

fn build_switch_expr(pair: Pair) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let expr = pairs.next().unwrap();
    let expr = Expr::try_from(expr)?;
    let mut collected = vec![(expr, Expr::Const(Value::Null))];
    for pair in pairs {
        match pair.as_rule() {
            Rule::switch_pattern => {
                collected.push(build_switch_pattern(pair)?);
            }
            Rule::default_pattern => {
                collected[0].1 = Expr::try_from(pair.into_inner().next().unwrap())?;
                break;
            }
            _ => unreachable!(),
        }
    }
    Ok(Expr::SwitchExpr(collected))
}

fn build_switch_pattern(pair: Pair) -> Result<(Expr, Expr)> {
    let mut pairs = pair.into_inner();
    Ok((
        Expr::try_from(pairs.next().unwrap())?,
        Expr::try_from(pairs.next().unwrap())?,
    ))
}

fn build_if_clause(pair: Pair) -> Result<(Expr, Expr)> {
    let mut pairs = pair.into_inner();
    let cond = pairs.next().unwrap();
    let cond = Expr::try_from(cond)?;
    let expr = pairs.next().unwrap();
    let expr = Expr::try_from(expr)?;
    Ok((cond, expr))
}

fn build_expr_primary(pair: Pair) -> Result<Expr> {
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
                        let op: Arc<dyn Op + Send + Sync> =
                            get_method(pairs.next().unwrap().as_str());
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
            let op: Arc<dyn Op + Send + Sync> = match op {
                Rule::term => return build_expr_primary(p),
                Rule::negate => Arc::new(OpNot),
                Rule::minus => Arc::new(OpMinus),
                Rule::if_expr => return build_if_expr(p),
                Rule::cond_expr => return build_cond_expr(p),
                Rule::switch_expr => return build_switch_expr(p),
                r => unreachable!("Encountered unknown op {:?}", r),
            };
            let term = build_expr_primary(inner.next().unwrap())?;
            Ok(Expr::Apply(op, vec![term]))
        }

        Rule::pos_int => Ok(Expr::Const(Value::Int(
            pair.as_str().replace('_', "").parse::<i64>()?,
        ))),
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
                        let val =
                            Expr::FieldAcc(name.clone().into(), Expr::Variable("_".into()).into());
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
        Rule::call_expr => build_call_expr(pair),
        _ => {
            println!("Unhandled rule {:?}", pair.as_rule());
            unimplemented!()
        }
    }
}

fn get_method(name: &str) -> Arc<dyn Op + Send + Sync> {
    match name {
        NAME_OP_IS_NULL => Arc::new(OpIsNull),
        NAME_OP_NOT_NULL => Arc::new(OpNotNull),
        NAME_OP_CONCAT => Arc::new(OpConcat),
        NAME_OP_MERGE => Arc::new(OpMerge),
        method_name => Arc::new(UnresolvedOp(method_name.to_string())),
    }
}

fn build_expr_infix<'a>(
    lhs: Result<Expr<'a>>,
    op: Pair,
    rhs: Result<Expr<'a>>,
) -> Result<Expr<'a>> {
    let lhs = lhs?;
    let rhs = rhs?;
    let op: Arc<dyn Op + Send + Sync> = match op.as_rule() {
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


pub(crate) fn parse_scoped_dict(pair: Pair) -> Result<(String, BTreeMap<String, Expr>, Expr)> {
    let mut pairs = pair.into_inner();
    let binding = pairs.next().unwrap().as_str().to_string();
    let keyed_dict = pairs.next().unwrap();
    let mut keys = BTreeMap::new();
    let mut spread_collected = vec![];
    let mut collected = BTreeMap::new();
    for p in keyed_dict.into_inner() {
        match p.as_rule() {
            Rule::keyed_pair => {
                let mut inner = p.into_inner();
                let name = parse_string(inner.next().unwrap())?;
                let val = Expr::try_from(inner.next().unwrap())?;
                keys.insert(name.into(), val);
            }
            Rule::dict_pair => {
                let mut inner = p.into_inner();
                let name = parse_string(inner.next().unwrap())?;
                let val = Expr::try_from(inner.next().unwrap())?;
                collected.insert(name.into(), val);
            }
            Rule::scoped_accessor => {
                let name = parse_string(p.into_inner().next().unwrap())?;
                let val =
                    Expr::FieldAcc(name.clone().into(), Expr::Variable("_".into()).into());
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

    let vals = if spread_collected.is_empty() {
        Expr::Dict(collected)
    } else {
        if !collected.is_empty() {
            spread_collected.push(Expr::Dict(collected));
        }
        Expr::Apply(Arc::new(OpMerge), spread_collected)
    };
    Ok((binding, keys, vals))
}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::parser::CozoParser;
    use pest::Parser;

    pub(crate) fn str2expr(s: &str) -> Result<Expr> {
        let pair = CozoParser::parse(Rule::expr_all, s)
            .unwrap()
            .next()
            .unwrap();
        Expr::try_from(pair)
    }

    #[test]
    fn raw_string() {
        assert!(dbg!(str2expr(r#####"r#"x"#"#####)).is_ok());
    }

    #[test]
    fn unevaluated() {
        assert!(dbg!(str2expr("a+b*c+d")).is_ok());
    }

    #[test]
    fn parse_literals() {
        assert_eq!(str2expr("1").unwrap(), Expr::Const(Value::Int(1)));
        assert_eq!(str2expr("12_3").unwrap(), Expr::Const(Value::Int(123)));
        assert_eq!(str2expr("0xaf").unwrap(), Expr::Const(Value::Int(0xaf)));
        assert_eq!(
            str2expr("0xafcE_f").unwrap(),
            Expr::Const(Value::Int(0xafcef))
        );
        assert_eq!(
            str2expr("0o1234_567").unwrap(),
            Expr::Const(Value::Int(0o1234567))
        );
        assert_eq!(
            str2expr("0o0001234_567").unwrap(),
            Expr::Const(Value::Int(0o1234567))
        );
        assert_eq!(
            str2expr("0b101010").unwrap(),
            Expr::Const(Value::Int(0b101010))
        );

        assert_eq!(
            str2expr("0.0").unwrap(),
            Expr::Const(Value::Float((0.).into()))
        );
        assert_eq!(
            str2expr("10.022_3").unwrap(),
            Expr::Const(Value::Float(10.0223.into()))
        );
        assert_eq!(
            str2expr("10.022_3e-100").unwrap(),
            Expr::Const(Value::Float(10.0223e-100.into()))
        );

        assert_eq!(str2expr("null").unwrap(), Expr::Const(Value::Null));
        assert_eq!(str2expr("true").unwrap(), Expr::Const(Value::Bool(true)));
        assert_eq!(str2expr("false").unwrap(), Expr::Const(Value::Bool(false)));
        assert_eq!(
            str2expr(r#""x \n \ty \"""#).unwrap(),
            Expr::Const(Value::Text(Cow::Borrowed("x \n \ty \"")))
        );
        assert_eq!(
            str2expr(r#""x'""#).unwrap(),
            Expr::Const(Value::Text("x'".into()))
        );
        assert_eq!(
            str2expr(r#"'"x"'"#).unwrap(),
            Expr::Const(Value::Text(r##""x""##.into()))
        );
        assert_eq!(
            str2expr(r#####"r###"x"yz"###"#####).unwrap(),
            (Expr::Const(Value::Text(r##"x"yz"##.into())))
        );
    }

    #[test]
    fn complex_cases() -> Result<()> {
        dbg!(str2expr("{}")?);
        dbg!(str2expr("{b:1,a,c:2,d,...e,}")?);
        dbg!(str2expr("{...a,...b,c:1,d:2,...e,f:3}")?);
        dbg!(str2expr("[]")?);
        dbg!(str2expr("[...a,...b,1,2,...e,3]")?);
        Ok(())
    }

    #[test]
    fn conditionals() -> Result<()> {
        let s = r#"if a { b + c * d } else if (x) { y } else {z}"#;
        dbg!(str2expr(s))?;
        let s = r#"if a { b + c * d }"#;
        dbg!(str2expr(s))?;

        let s = r#"(if a { b + c * d } else if (x) { y } else {z})+1"#;
        dbg!(str2expr(s))?;

        let s = r#"cond {
            a > 1 => 1,
            a == 1 => 2,
            true => 3
        }"#;
        dbg!(str2expr(s))?;

        let s = r#"switch(a) {
            1 => 1,
            2 => 2,
            .. => 3
        }"#;
        dbg!(str2expr(s))?;

        let s = r#"switch(a) {
            1 => 1,
            2 => 2,
        }"#;
        dbg!(str2expr(s))?;

        Ok(())
    }
}
