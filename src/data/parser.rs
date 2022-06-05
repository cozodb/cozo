use crate::data::expr::Expr;
use crate::data::op::*;
use crate::data::op_agg::*;
use crate::data::value::Value;
use crate::parser::number::parse_int;
use crate::parser::text_identifier::parse_string;
use crate::parser::{Pair, Rule};
use anyhow::Result;
use lazy_static::lazy_static;
use pest::prec_climber::{Assoc, Operator, PrecClimber};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[derive(thiserror::Error, Debug)]
pub enum ExprParseError {
    #[error("Cannot spread {0}")]
    SpreadingError(String),
}

impl<'a> TryFrom<Pair<'a>> for Expr {
    type Error = anyhow::Error;

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
                        let op_name = pairs.next().unwrap().as_str();
                        let mut args = vec![head];
                        args.extend(pairs.map(Expr::try_from).collect::<Result<Vec<_>>>()?);
                        head = build_method_call(op_name, args)?;
                    }
                    Rule::aggr => {
                        let mut pairs = p.into_inner();
                        let op_name = pairs.next().unwrap().as_str();
                        let mut args = vec![head];
                        let mut a_args = vec![];
                        for pair in pairs {
                            match pair.as_rule() {
                                Rule::expr => args.push(Expr::try_from(pair)?),
                                Rule::aggr_params => {
                                    for pair in pair.into_inner() {
                                        a_args.push(Expr::try_from(pair)?);
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                        head = build_aggr_call(op_name, a_args, args)?;
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
            Ok(match op {
                Rule::term => return build_expr_primary(p),
                Rule::minus => {
                    Expr::BuiltinFn(OP_MINUS, vec![build_expr_primary(inner.next().unwrap())?])
                }
                Rule::negate => {
                    Expr::BuiltinFn(OP_NOT, vec![build_expr_primary(inner.next().unwrap())?])
                }
                Rule::if_expr => return build_if_expr(p),
                Rule::cond_expr => return build_cond_expr(p),
                Rule::switch_expr => return build_switch_expr(p),
                r => unreachable!("Encountered unknown op {:?}", r),
            })
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
                                | Expr::BuiltinFn(_, _)
                                | Expr::OpConcat(_)
                                | Expr::OpMerge(_)
                                | Expr::OpCoalesce(_)
                        ) {
                            return Err(
                                ExprParseError::SpreadingError(format!("{:?}", to_concat)).into()
                            );
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
            Ok(Expr::OpConcat(spread_collected))
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
                        collected.insert(name, val);
                    }
                    Rule::scoped_accessor => {
                        let name = parse_string(p.into_inner().next().unwrap())?;
                        let val = Expr::FieldAcc(name.clone(), Expr::Variable("_".into()).into());
                        collected.insert(name, val);
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
                                | Expr::BuiltinFn(_, _)
                                | Expr::OpConcat(_)
                                | Expr::OpMerge(_)
                                | Expr::OpCoalesce(_)
                        ) {
                            return Err(
                                ExprParseError::SpreadingError(format!("{:?}", to_concat)).into()
                            );
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
            Ok(Expr::OpMerge(spread_collected))
        }
        Rule::param => Ok(Expr::Variable(pair.as_str().into())),
        Rule::ident => Ok(Expr::Variable(pair.as_str().into())),
        Rule::call_expr => {
            let mut pairs = pair.into_inner();
            let op_name = pairs.next().unwrap().as_str();
            let args = pairs.map(Expr::try_from).collect::<Result<Vec<_>>>()?;
            build_method_call(op_name, args)
        }
        Rule::aggr_expr => {
            let mut pairs = pair.into_inner();
            let op_name = pairs.next().unwrap().as_str();
            let mut args = vec![];
            let mut a_args = vec![];
            for pair in pairs {
                match pair.as_rule() {
                    Rule::expr => args.push(Expr::try_from(pair)?),
                    Rule::aggr_params => {
                        for pair in pair.into_inner() {
                            a_args.push(Expr::try_from(pair)?);
                        }
                    }
                    _ => unreachable!(),
                }
            }
            build_aggr_call(op_name, a_args, args)
        }
        _ => {
            println!("Unhandled rule {:?}", pair.as_rule());
            unimplemented!()
        }
    }
}

fn build_method_call(name: &str, args: Vec<Expr>) -> Result<Expr> {
    Ok(match name {
        NAME_OP_IS_NULL => Expr::BuiltinFn(OP_IS_NULL, args),
        NAME_OP_NOT_NULL => Expr::BuiltinFn(OP_NOT_NULL, args),
        NAME_OP_CONCAT => Expr::OpConcat(args),
        NAME_OP_MERGE => Expr::OpMerge(args),
        method_name => unimplemented!("{}", method_name),
    })
}

fn build_aggr_call(name: &str, a_args: Vec<Expr>, args: Vec<Expr>) -> Result<Expr> {
    Ok(match name {
        NAME_OP_SUM => build_op_sum(a_args, args),
        NAME_OP_AVG => build_op_avg(a_args, args),
        NAME_OP_VAR => build_op_var(a_args, args),
        NAME_OP_COUNT_WITH => build_op_count_with(a_args, args),
        NAME_OP_COUNT => build_op_count(a_args, args),
        NAME_OP_COUNT_NON_NULL => build_op_count_non_null(a_args, args),
        NAME_OP_LAG => build_op_lag(a_args, args),
        NAME_OP_COLLECT_IF => build_op_collect_if(a_args, args),
        NAME_OP_COLLECT => build_op_collect(a_args, args),
        NAME_OP_MIN => build_op_min(a_args, args),
        NAME_OP_MAX => build_op_max(a_args, args),
        method_name => unimplemented!("{}", method_name),
    })
}

fn build_expr_infix(lhs: Result<Expr>, op: Pair, rhs: Result<Expr>) -> Result<Expr> {
    let args = vec![lhs?, rhs?];
    Ok(match op.as_rule() {
        Rule::op_add => Expr::BuiltinFn(OP_ADD, args),
        Rule::op_sub => Expr::BuiltinFn(OP_SUB, args),
        Rule::op_mul => Expr::BuiltinFn(OP_MUL, args),
        Rule::op_div => Expr::BuiltinFn(OP_DIV, args),
        Rule::op_mod => Expr::BuiltinFn(OP_MOD, args),
        Rule::op_pow => Expr::BuiltinFn(OP_POW, args),
        Rule::op_eq => Expr::BuiltinFn(OP_EQ, args),
        Rule::op_ne => Expr::BuiltinFn(OP_NE, args),
        Rule::op_gt => Expr::BuiltinFn(OP_GT, args),
        Rule::op_ge => Expr::BuiltinFn(OP_GE, args),
        Rule::op_lt => Expr::BuiltinFn(OP_LT, args),
        Rule::op_le => Expr::BuiltinFn(OP_LE, args),
        Rule::op_str_cat => Expr::BuiltinFn(OP_STR_CAT, args),
        Rule::op_or => Expr::OpOr(args),
        Rule::op_and => Expr::OpAnd(args),
        Rule::op_coalesce => Expr::OpCoalesce(args),
        _ => unreachable!(),
    })
}

pub(crate) fn parse_scoped_dict(pair: Pair) -> Result<(String, BTreeMap<String, Expr>, Expr)> {
    let mut pairs = pair.into_inner();
    let binding = pairs.next().unwrap().as_str().to_string();
    let keyed_dict = pairs.next().unwrap();
    let (keys, vals) = parse_keyed_dict(keyed_dict)?;
    Ok((binding, keys, vals))
}

pub(crate) fn parse_keyed_dict(keyed_dict: Pair) -> Result<(BTreeMap<String, Expr>, Expr)> {
    let mut keys = BTreeMap::new();
    let mut spread_collected = vec![];
    let mut collected = BTreeMap::new();
    for p in keyed_dict.into_inner() {
        match p.as_rule() {
            Rule::keyed_pair => {
                let mut inner = p.into_inner();
                let name = parse_string(inner.next().unwrap())?;
                let val = Expr::try_from(inner.next().unwrap())?;
                keys.insert(name, val);
            }
            Rule::dict_pair => {
                let mut inner = p.into_inner();
                let name = parse_string(inner.next().unwrap())?;
                let val = Expr::try_from(inner.next().unwrap())?;
                collected.insert(name, val);
            }
            Rule::scoped_accessor => {
                let name = parse_string(p.into_inner().next().unwrap())?;
                let val = Expr::FieldAcc(name.clone(), Expr::Variable("_".into()).into());
                collected.insert(name, val);
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
                        | Expr::BuiltinFn(_, _)
                        | Expr::OpConcat(_)
                        | Expr::OpMerge(_)
                        | Expr::OpCoalesce(_)
                ) {
                    return Err(ExprParseError::SpreadingError(format!("{:?}", to_concat)).into());
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
        Expr::OpMerge(spread_collected)
    };
    Ok((keys, vals))
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
