use crate::data::op::*;
use crate::data::op_agg::OpAgg;
use crate::data::tuple_set::TupleSetIdx;
use crate::data::value::{StaticValue, Value};
use crate::parser::{CozoParser, Rule};
use anyhow::Result;
use pest::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::result;

#[derive(thiserror::Error, Debug)]
pub enum ExprError {
    #[error("Cannot convert from {0}")]
    ConversionFailure(StaticValue),

    #[error("Unknown expr tag {0}")]
    UnknownExprTag(String),

    #[error("List extraction failed for {0}")]
    ListExtractionFailed(StaticValue),

    #[error("Failed to parse {0} into expr")]
    Parse(String),
}

#[derive(Clone)]
pub struct BuiltinFn {
    pub(crate) name: &'static str,
    pub(crate) arity: Option<u8>,
    pub(crate) non_null_args: bool,
    pub(crate) func: for<'a> fn(&[Value<'a>]) -> Result<Value<'a>>,
}

impl PartialEq for BuiltinFn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for BuiltinFn {}

#[derive(Clone, PartialEq)]
pub enum Expr {
    Const(StaticValue),
    List(Vec<Expr>),
    Dict(BTreeMap<String, Expr>),
    Variable(String),
    TupleSetIdx(TupleSetIdx),
    ApplyAgg(OpAgg, Vec<Expr>, Vec<Expr>),
    FieldAcc(String, Box<Expr>),
    IdxAcc(usize, Box<Expr>),
    IfExpr(Box<(Expr, Expr, Expr)>),
    SwitchExpr(Vec<(Expr, Expr)>),
    OpAnd(Vec<Expr>),
    OpOr(Vec<Expr>),
    OpCoalesce(Vec<Expr>),
    OpMerge(Vec<Expr>),
    OpConcat(Vec<Expr>),
    BuiltinFn(BuiltinFn, Vec<Expr>),
}

impl Expr {
    pub(crate) fn is_const(&self) -> bool {
        matches!(self, Expr::Const(_))
    }
    pub(crate) fn extract_const(self) -> Option<StaticValue> {
        match self {
            Expr::Const(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn all_variables(&self) -> BTreeSet<String> {
        let mut ret = BTreeSet::new();
        fn collect(ex: &Expr, accum: &mut BTreeSet<String>) {
            match ex {
                Expr::Const(_) => {}
                Expr::List(ls) => {
                    for el in ls {
                        collect(el, accum);
                    }
                }
                Expr::Dict(d) => {
                    for el in d.values() {
                        collect(el, accum);
                    }
                }
                Expr::Variable(v) => {
                    accum.insert(v.clone());
                }
                Expr::TupleSetIdx(_) => {}
                Expr::BuiltinFn(_, args) => {
                    for el in args {
                        collect(el, accum);
                    }
                }
                Expr::ApplyAgg(_, a_args, args) => {
                    for el in args {
                        collect(el, accum);
                    }
                    for el in a_args {
                        collect(el, accum);
                    }
                }
                Expr::FieldAcc(_, arg) => {
                    collect(arg, accum);
                }
                Expr::IdxAcc(_, arg) => {
                    collect(arg, accum);
                }
                Expr::IfExpr(args) => {
                    let (if_p, then_p, else_p) = args.as_ref();
                    collect(if_p, accum);
                    collect(then_p, accum);
                    collect(else_p, accum);
                }
                Expr::SwitchExpr(args) => {
                    for (cond, el) in args {
                        collect(cond, accum);
                        collect(el, accum);
                    }
                }
                ex => panic!("Unsupported on optimized expression: {:?}", ex),
            }
        }
        collect(self, &mut ret);
        ret
    }
}

fn write_fn_call<'a, T: IntoIterator<Item = &'a [Expr]>>(
    f: &mut Formatter,
    name: &str,
    args_iter: T,
) -> std::fmt::Result {
    write!(f, "({}", name)?;
    for (i, args) in args_iter.into_iter().enumerate() {
        if i == 0 {
            write!(f, " ")?;
        } else {
            write!(f, "; ")?;
        }
        write!(
            f,
            "{}",
            args.iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join(" ")
        )?
    }
    write!(f, ")")
}

impl<'a> Debug for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Const(c) => write!(f, "{}", c),
            Expr::List(l) => write!(f, "{:?}", l),
            Expr::Dict(d) => write!(f, "{:?}", d),
            Expr::Variable(v) => write!(f, "`{}`", v),
            Expr::TupleSetIdx(sid) => write!(f, "{:?}", sid),
            Expr::ApplyAgg(op, a_args, args) => write!(
                f,
                "[|{} {} | {}|]",
                op.name(),
                a_args
                    .iter()
                    .map(|v| format!("{:?}", v))
                    .collect::<Vec<_>>()
                    .join(" "),
                args.iter()
                    .map(|v| format!("{:?}", v))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            Expr::IfExpr(args) => {
                let args = args.as_ref();
                write!(f, "(if {:?} {:?} {:?})", args.0, args.1, args.2)
            }
            Expr::SwitchExpr(args) => {
                let mut args = args.iter();
                let (expr, default) = args.next().unwrap();
                write!(f, "(switch {:?}", expr)?;
                for (cond, expr) in args {
                    write!(f, ", {:?} => {:?}", cond, expr)?;
                }
                write!(f, ", .. => {:?})", default)
            }
            Expr::FieldAcc(field, arg) => write!(f, "(.{} {:?})", field, arg),
            Expr::IdxAcc(i, arg) => write!(f, "(.{} {:?})", i, arg),
            Expr::OpAnd(args) => write_fn_call(f, "&&", [args.as_ref()]),
            Expr::OpOr(args) => write_fn_call(f, "||", [args.as_ref()]),
            Expr::OpCoalesce(args) => write_fn_call(f, "~", [args.as_ref()]),
            Expr::OpMerge(args) => write_fn_call(f, "merge", [args.as_ref()]),
            Expr::OpConcat(args) => write_fn_call(f, "concat", [args.as_ref()]),
            Expr::BuiltinFn(op, args) => write_fn_call(f, op.name, [args.as_ref()]),
        }
    }
}

fn extract_list_from_value(value: Value, n: usize) -> Result<Vec<Value>> {
    if let Value::List(l) = value {
        if n > 0 && l.len() != n {
            return Err(ExprError::ListExtractionFailed(Value::List(l).into_static()).into());
        }
        Ok(l)
    } else {
        return Err(ExprError::ListExtractionFailed(value.into_static()).into());
    }
}

impl TryFrom<StaticValue> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: StaticValue) -> Result<Self> {
        if let Value::Dict(d) = value {
            if d.len() != 1 {
                return Err(ExprError::ConversionFailure(Value::Dict(d).into_static()).into());
            }
            let (k, v) = d.into_iter().next().unwrap();
            match k.as_ref() {
                EXPR_TAG_CONST => Ok(Expr::Const(v)),
                EXPR_TAG_LIST => {
                    let l = extract_list_from_value(v, 0)?;
                    Ok(Expr::List(
                        l.into_iter()
                            .map(Expr::try_from)
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
                EXPR_TAG_DICT => match v {
                    Value::Dict(d) => Ok(Expr::Dict(
                        d.into_iter()
                            .map(|(k, v)| -> Result<(String, Expr)> {
                                Ok((k.to_string(), Expr::try_from(v)?))
                            })
                            .collect::<Result<BTreeMap<_, _>>>()?,
                    )),
                    v => {
                        return Err(ExprError::ConversionFailure(
                            Value::Dict(BTreeMap::from([(k, v)])).into_static(),
                        )
                        .into());
                    }
                },
                EXPR_TAG_VARIABLE => {
                    if let Value::Text(t) = v {
                        Ok(Expr::Variable(t.to_string()))
                    } else {
                        return Err(ExprError::ConversionFailure(
                            Value::Dict(BTreeMap::from([(k, v)])).into_static(),
                        )
                        .into());
                    }
                }
                EXPR_TAG_TUPLE_SET_IDX => {
                    let mut l = extract_list_from_value(v, 3)?.into_iter();
                    let is_key = match l.next().unwrap() {
                        Value::Bool(b) => b,
                        v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    };
                    let tid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    };
                    let cid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    };
                    Ok(Expr::TupleSetIdx(TupleSetIdx {
                        is_key,
                        t_set: tid as usize,
                        col_idx: cid as usize,
                    }))
                }
                EXPR_TAG_APPLY => {
                    // let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    // let name = match ll.next().unwrap() {
                    //     Value::Text(t) => t,
                    //     v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    // };
                    // let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    // let _args = l
                    //     .into_iter()
                    //     .map(Expr::try_from)
                    //     .collect::<Result<Vec<_>>>()?;
                    todo!()
                }
                EXPR_TAG_APPLY_AGG => {
                    // let mut ll = extract_list_from_value(v, 3)?.into_iter();
                    // let name = match ll.next().unwrap() {
                    //     Value::Text(t) => t,
                    //     v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    // };
                    // let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    // let a_args = l
                    //     .into_iter()
                    //     .map(Expr::try_from)
                    //     .collect::<Result<Vec<_>>>()?;
                    // let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    // let _args = l
                    //     .into_iter()
                    //     .map(Expr::try_from)
                    //     .collect::<Result<Vec<_>>>()?;
                    todo!()
                }
                EXPR_TAG_FIELD_ACC => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let field = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::FieldAcc(field.to_string(), arg.into()))
                }
                EXPR_TAG_IDX_ACC => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let idx = match ll.next().unwrap() {
                        Value::Int(i) => i as usize,
                        v => return Err(ExprError::ConversionFailure(v.into_static()).into()),
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::IdxAcc(idx, arg.into()))
                }
                k => Err(ExprError::UnknownExprTag(k.to_string()).into()),
            }
        } else {
            Err(ExprError::ConversionFailure(value.into_static()).into())
        }
    }
}

pub(crate) const EXPR_TAG_CONST: &str = "Const";
pub(crate) const EXPR_TAG_LIST: &str = "List";
pub(crate) const EXPR_TAG_DICT: &str = "Dict";
pub(crate) const EXPR_TAG_VARIABLE: &str = "Variable";
pub(crate) const EXPR_TAG_TUPLE_SET_IDX: &str = "TupleSetIdx";
pub(crate) const EXPR_TAG_APPLY: &str = "Apply";
pub(crate) const EXPR_TAG_APPLY_AGG: &str = "ApplyAgg";
pub(crate) const EXPR_TAG_FIELD_ACC: &str = "FieldAcc";
pub(crate) const EXPR_TAG_IDX_ACC: &str = "IndexAcc";

impl<'a> From<Expr> for Value<'a> {
    fn from(expr: Expr) -> Self {
        match expr {
            Expr::Const(c) => build_tagged_value(EXPR_TAG_CONST, c),
            Expr::List(l) => build_tagged_value(
                EXPR_TAG_LIST,
                l.into_iter().map(Value::from).collect::<Vec<_>>().into(),
            ),
            Expr::Dict(d) => build_tagged_value(
                EXPR_TAG_DICT,
                d.into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect::<BTreeMap<_, _>>()
                    .into(),
            ),
            Expr::Variable(v) => build_tagged_value(EXPR_TAG_VARIABLE, v.into()),
            Expr::TupleSetIdx(sid) => build_tagged_value(
                EXPR_TAG_TUPLE_SET_IDX,
                vec![
                    sid.is_key.into(),
                    Value::from(sid.t_set as i64),
                    Value::from(sid.col_idx as i64),
                ]
                .into(),
            ),
            Expr::BuiltinFn(op, args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(op.name.to_string()),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::OpAnd(args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(NAME_OP_ADD),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::OpOr(args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(NAME_OP_OR),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::OpCoalesce(args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(NAME_OP_COALESCE),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::OpMerge(args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(NAME_OP_MERGE),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::OpConcat(args) => build_tagged_value(
                EXPR_TAG_APPLY,
                vec![
                    Value::from(NAME_OP_CONCAT),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::IfExpr(_) => {
                todo!()
            }
            Expr::SwitchExpr(_) => {
                todo!()
            }
            Expr::ApplyAgg(_op, _a_args, _args) => {
                todo!()
                // build_tagged_value(
                //     // EXPR_TAG_APPLY_AGG,
                //     // vec![
                //     //     Value::from(todo!()),
                //     //     a_args
                //     //         .into_iter()
                //     //         .map(Value::from)
                //     //         .collect::<Vec<_>>()
                //     //         .into(),
                //     //     args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                //     // ]
                //     // .into(),
                //     todo!()
                // )
            },
            Expr::FieldAcc(f, v) => {
                build_tagged_value(EXPR_TAG_FIELD_ACC, vec![f.into(), Value::from(*v)].into())
            }
            Expr::IdxAcc(idx, v) => build_tagged_value(
                EXPR_TAG_IDX_ACC,
                vec![(idx as i64).into(), Value::from(*v)].into(),
            ),
        }
    }
}

fn build_tagged_value<'a>(tag: &'static str, val: Value<'a>) -> Value<'a> {
    Value::Dict(BTreeMap::from([(tag.into(), val)]))
}

impl<'a> TryFrom<&'a str> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: &'a str) -> result::Result<Self, Self::Error> {
        let pair = CozoParser::parse(Rule::expr_all, value)
            .map_err(|_| ExprError::Parse(value.to_string()))?
            .next()
            .ok_or_else(|| ExprError::Parse(value.to_string()))?;
        Expr::try_from(pair)
    }
}
