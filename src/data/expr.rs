use crate::data::op::{
    AggOp, Op, OpAdd, OpAnd, OpCoalesce, OpDiv, OpEq, OpGe, OpGt, OpIsNull, OpLe, OpLt, OpMinus,
    OpMod, OpMul, OpNe, OpNot, OpNotNull, OpOr, OpPow, OpStrCat, OpSub, UnresolvedOp,
};
use crate::data::parser::ExprParseError;
use crate::data::tuple_set::TupleSetIdx;
use crate::data::value::{StaticValue, Value};
use crate::parser::{CozoParser, Rule};
use pest::Parser;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::result;
use std::sync::Arc;

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

    #[error(transparent)]
    ParseInner(#[from] ExprParseError),
}

type Result<T> = result::Result<T, ExprError>;

#[derive(Clone)]
pub(crate) enum Expr<'a> {
    Const(Value<'a>),
    List(Vec<Expr<'a>>),
    Dict(BTreeMap<String, Expr<'a>>),
    Variable(String),
    TupleSetIdx(TupleSetIdx),
    Apply(Arc<dyn Op + Send + Sync>, Vec<Expr<'a>>),
    ApplyAgg(Arc<dyn AggOp + Send + Sync>, Vec<Expr<'a>>, Vec<Expr<'a>>),
    FieldAcc(String, Box<Expr<'a>>),
    IdxAcc(usize, Box<Expr<'a>>),
    IfExpr(Box<(Expr<'a>, Expr<'a>, Expr<'a>)>),
    SwitchExpr(Vec<(Expr<'a>, Expr<'a>)>),
    // optimized
    Add(Box<(Expr<'a>, Expr<'a>)>),
    Sub(Box<(Expr<'a>, Expr<'a>)>),
    Mul(Box<(Expr<'a>, Expr<'a>)>),
    Div(Box<(Expr<'a>, Expr<'a>)>),
    Pow(Box<(Expr<'a>, Expr<'a>)>),
    Mod(Box<(Expr<'a>, Expr<'a>)>),
    StrCat(Box<(Expr<'a>, Expr<'a>)>),
    Eq(Box<(Expr<'a>, Expr<'a>)>),
    Ne(Box<(Expr<'a>, Expr<'a>)>),
    Gt(Box<(Expr<'a>, Expr<'a>)>),
    Ge(Box<(Expr<'a>, Expr<'a>)>),
    Lt(Box<(Expr<'a>, Expr<'a>)>),
    Le(Box<(Expr<'a>, Expr<'a>)>),
    Not(Box<Expr<'a>>),
    Minus(Box<Expr<'a>>),
    IsNull(Box<Expr<'a>>),
    NotNull(Box<Expr<'a>>),
    Coalesce(Box<(Expr<'a>, Expr<'a>)>),
    Or(Box<(Expr<'a>, Expr<'a>)>),
    And(Box<(Expr<'a>, Expr<'a>)>),
}

impl<'a> Expr<'a> {
    pub(crate) fn is_const(&self) -> bool {
        matches!(self, Expr::Const(_))
    }
    pub(crate) fn extract_const(self) -> Option<Value<'a>> {
        match self {
            Expr::Const(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn to_static(self) -> StaticExpr {
        match self {
            Expr::Const(v) => Expr::Const(v.to_static()),
            Expr::List(l) => Expr::List(l.into_iter().map(|v| v.to_static()).collect()),
            Expr::Dict(d) => Expr::Dict(d.into_iter().map(|(k, v)| (k, v.to_static())).collect()),
            Expr::Variable(v) => Expr::Variable(v),
            Expr::TupleSetIdx(idx) => Expr::TupleSetIdx(idx),
            Expr::Apply(op, args) => {
                Expr::Apply(op, args.into_iter().map(|v| v.to_static()).collect())
            }
            Expr::ApplyAgg(op, a_args, args) => Expr::ApplyAgg(
                op,
                a_args.into_iter().map(|v| v.to_static()).collect(),
                args.into_iter().map(|v| v.to_static()).collect(),
            ),
            Expr::FieldAcc(f, arg) => Expr::FieldAcc(f, arg.to_static().into()),
            Expr::IdxAcc(i, arg) => Expr::IdxAcc(i, arg.to_static().into()),
            Expr::IfExpr(args) => {
                let (a, b, c) = *args;
                Expr::IfExpr((a.to_static(), b.to_static(), c.to_static()).into())
            }
            Expr::SwitchExpr(args) => Expr::SwitchExpr(
                args.into_iter()
                    .map(|(a, b)| (a.to_static(), b.to_static()))
                    .collect(),
            ),
            Expr::Add(args) => {
                let (a, b) = *args;
                Expr::Add((a.to_static(), b.to_static()).into())
            }
            Expr::Sub(args) => {
                let (a, b) = *args;
                Expr::Sub((a.to_static(), b.to_static()).into())
            }
            Expr::Mul(args) => {
                let (a, b) = *args;
                Expr::Mul((a.to_static(), b.to_static()).into())
            }
            Expr::Div(args) => {
                let (a, b) = *args;
                Expr::Div((a.to_static(), b.to_static()).into())
            }
            Expr::Pow(args) => {
                let (a, b) = *args;
                Expr::Pow((a.to_static(), b.to_static()).into())
            }
            Expr::Mod(args) => {
                let (a, b) = *args;
                Expr::Mod((a.to_static(), b.to_static()).into())
            }
            Expr::StrCat(args) => {
                let (a, b) = *args;
                Expr::StrCat((a.to_static(), b.to_static()).into())
            }
            Expr::Eq(args) => {
                let (a, b) = *args;
                Expr::Eq((a.to_static(), b.to_static()).into())
            }
            Expr::Ne(args) => {
                let (a, b) = *args;
                Expr::Ne((a.to_static(), b.to_static()).into())
            }
            Expr::Gt(args) => {
                let (a, b) = *args;
                Expr::Gt((a.to_static(), b.to_static()).into())
            }
            Expr::Ge(args) => {
                let (a, b) = *args;
                Expr::Ge((a.to_static(), b.to_static()).into())
            }
            Expr::Lt(args) => {
                let (a, b) = *args;
                Expr::Lt((a.to_static(), b.to_static()).into())
            }
            Expr::Le(args) => {
                let (a, b) = *args;
                Expr::Le((a.to_static(), b.to_static()).into())
            }
            Expr::Not(arg) => Expr::Not(arg.to_static().into()),
            Expr::Minus(arg) => Expr::Minus(arg.to_static().into()),
            Expr::IsNull(arg) => Expr::IsNull(arg.to_static().into()),
            Expr::NotNull(arg) => Expr::NotNull(arg.to_static().into()),
            Expr::Coalesce(args) => {
                let (a, b) = *args;
                Expr::Coalesce((a.to_static(), b.to_static()).into())
            }
            Expr::Or(args) => {
                let (a, b) = *args;
                Expr::Or((a.to_static(), b.to_static()).into())
            }
            Expr::And(args) => {
                let (a, b) = *args;
                Expr::And((a.to_static(), b.to_static()).into())
            }
        }
    }
}

impl<'a> PartialEq for Expr<'a> {
    fn eq(&self, other: &Self) -> bool {
        use Expr::*;

        match (self, other) {
            (Const(l), Const(r)) => l == r,
            (List(l), List(r)) => l == r,
            (Dict(l), Dict(r)) => l == r,
            (Variable(l), Variable(r)) => l == r,
            (TupleSetIdx(l), TupleSetIdx(r)) => l == r,
            (Apply(lo, la), Apply(ro, ra)) => (lo.name() == ro.name()) && (la == ra),
            (ApplyAgg(lo, laa, la), ApplyAgg(ro, raa, ra)) => {
                (lo.name() == ro.name()) && (laa == raa) && (la == ra)
            }
            (FieldAcc(lf, la), FieldAcc(rf, ra)) => (lf == rf) && (la == ra),
            (IdxAcc(li, la), IdxAcc(ri, ra)) => (li == ri) && (la == ra),
            _ => false,
        }
    }
}

impl<'a> Debug for Expr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Const(c) => write!(f, "{}", c),
            Expr::List(l) => write!(f, "{:?}", l),
            Expr::Dict(d) => write!(f, "{:?}", d),
            Expr::Variable(v) => write!(f, "`{}`", v),
            Expr::TupleSetIdx(sid) => write!(f, "{:?}", sid),
            Expr::Apply(op, args) => write!(
                f,
                "({} {})",
                op.name(),
                args.iter()
                    .map(|v| format!("{:?}", v))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            Expr::Add(args) => write!(f, "(`+ {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Sub(args) => write!(f, "(`- {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Mul(args) => write!(f, "(`* {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Div(args) => write!(f, "(`/ {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Pow(args) => write!(f, "(`** {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Mod(args) => write!(f, "(`% {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::StrCat(args) => write!(f, "(`++ {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Eq(args) => write!(f, "(`== {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Ne(args) => write!(f, "(`!= {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Gt(args) => write!(f, "(`> {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Ge(args) => write!(f, "(`>= {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Lt(args) => write!(f, "(`< {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Le(args) => write!(f, "(`<= {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Not(arg) => write!(f, "(`! {:?})", arg.as_ref()),
            Expr::Minus(arg) => write!(f, "(`-- {:?})", arg.as_ref()),
            Expr::IsNull(arg) => write!(f, "(`is_null {:?})", arg.as_ref()),
            Expr::NotNull(arg) => write!(f, "(`not_null {:?})", arg.as_ref()),
            Expr::Coalesce(args) => write!(f, "(`~ {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::Or(args) => write!(f, "(`|| {:?} {:?})", args.as_ref().0, args.as_ref().1),
            Expr::And(args) => write!(f, "(`&& {:?} {:?})", args.as_ref().0, args.as_ref().1),
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
        }
    }
}

pub(crate) type StaticExpr = Expr<'static>;

fn extract_list_from_value(value: Value, n: usize) -> Result<Vec<Value>> {
    if let Value::List(l) = value {
        if n > 0 && l.len() != n {
            return Err(ExprError::ListExtractionFailed(Value::List(l).to_static()));
        }
        Ok(l)
    } else {
        return Err(ExprError::ListExtractionFailed(value.to_static()));
    }
}

impl<'a> TryFrom<Value<'a>> for Expr<'a> {
    type Error = ExprError;

    fn try_from(value: Value<'a>) -> Result<Self> {
        if let Value::Dict(d) = value {
            if d.len() != 1 {
                return Err(ExprError::ConversionFailure(Value::Dict(d).to_static()));
            }
            let (k, v) = d.into_iter().next().unwrap();
            match k.as_ref() {
                "Const" => Ok(Expr::Const(v)),
                "List" => {
                    let l = extract_list_from_value(v, 0)?;
                    Ok(Expr::List(
                        l.into_iter()
                            .map(Expr::try_from)
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
                "Dict" => match v {
                    Value::Dict(d) => Ok(Expr::Dict(
                        d.into_iter()
                            .map(|(k, v)| -> Result<(String, Expr)> {
                                Ok((k.to_string(), Expr::try_from(v)?))
                            })
                            .collect::<Result<BTreeMap<_, _>>>()?,
                    )),
                    v => {
                        return Err(ExprError::ConversionFailure(
                            Value::Dict(BTreeMap::from([(k, v)])).to_static(),
                        ));
                    }
                },
                "Variable" => {
                    if let Value::Text(t) = v {
                        Ok(Expr::Variable(t.to_string()))
                    } else {
                        return Err(ExprError::ConversionFailure(
                            Value::Dict(BTreeMap::from([(k, v)])).to_static(),
                        ));
                    }
                }
                "TupleSetIdx" => {
                    let mut l = extract_list_from_value(v, 3)?.into_iter();
                    let is_key = match l.next().unwrap() {
                        Value::Bool(b) => b,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let tid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let cid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    Ok(Expr::TupleSetIdx(TupleSetIdx {
                        is_key,
                        t_set: tid as usize,
                        col_idx: cid as usize,
                    }))
                }
                "Apply" => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let name = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let op = Arc::new(UnresolvedOp(name.to_string()));
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let args = l
                        .into_iter()
                        .map(Expr::try_from)
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Expr::Apply(op, args))
                }
                "ApplyAgg" => {
                    let mut ll = extract_list_from_value(v, 3)?.into_iter();
                    let name = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let op = Arc::new(UnresolvedOp(name.to_string()));
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let a_args = l
                        .into_iter()
                        .map(Expr::try_from)
                        .collect::<Result<Vec<_>>>()?;
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let args = l
                        .into_iter()
                        .map(Expr::try_from)
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Expr::ApplyAgg(op, a_args, args))
                }
                "FieldAcc" => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let field = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::FieldAcc(field.to_string(), arg.into()))
                }
                "IdxAcc" => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let idx = match ll.next().unwrap() {
                        Value::Int(i) => i as usize,
                        v => return Err(ExprError::ConversionFailure(v.to_static())),
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::IdxAcc(idx, arg.into()))
                }
                k => Err(ExprError::UnknownExprTag(k.to_string())),
            }
        } else {
            Err(ExprError::ConversionFailure(value.to_static()))
        }
    }
}

fn build_value_from_binop<'a>(name: &str, (left, right): (Expr<'a>, Expr<'a>)) -> Value<'a> {
    build_tagged_value(
        "Apply",
        vec![
            Value::from(name.to_string()),
            Value::from(vec![Value::from(left), Value::from(right)]),
        ]
        .into(),
    )
}

fn build_value_from_uop<'a>(name: &str, arg: Expr<'a>) -> Value<'a> {
    build_tagged_value(
        "Apply",
        vec![
            Value::from(name.to_string()),
            Value::from(vec![Value::from(arg)]),
        ]
        .into(),
    )
}

impl<'a> From<Expr<'a>> for Value<'a> {
    fn from(expr: Expr<'a>) -> Self {
        match expr {
            Expr::Const(c) => build_tagged_value("Const", c),
            Expr::List(l) => build_tagged_value(
                "List",
                l.into_iter().map(Value::from).collect::<Vec<_>>().into(),
            ),
            Expr::Dict(d) => build_tagged_value(
                "Dict",
                d.into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect::<BTreeMap<_, _>>()
                    .into(),
            ),
            Expr::Variable(v) => build_tagged_value("Variable", v.into()),
            Expr::TupleSetIdx(sid) => build_tagged_value(
                "TupleSetIdx",
                vec![
                    sid.is_key.into(),
                    Value::from(sid.t_set as i64),
                    Value::from(sid.col_idx as i64),
                ]
                .into(),
            ),
            Expr::Add(arg) => build_value_from_binop(OpAdd.name(), *arg),
            Expr::Sub(arg) => build_value_from_binop(OpSub.name(), *arg),
            Expr::Mul(arg) => build_value_from_binop(OpMul.name(), *arg),
            Expr::Div(arg) => build_value_from_binop(OpDiv.name(), *arg),
            Expr::Pow(arg) => build_value_from_binop(OpPow.name(), *arg),
            Expr::Mod(arg) => build_value_from_binop(OpMod.name(), *arg),
            Expr::StrCat(arg) => build_value_from_binop(OpStrCat.name(), *arg),
            Expr::Eq(arg) => build_value_from_binop(OpEq.name(), *arg),
            Expr::Ne(arg) => build_value_from_binop(OpNe.name(), *arg),
            Expr::Gt(arg) => build_value_from_binop(OpGt.name(), *arg),
            Expr::Ge(arg) => build_value_from_binop(OpGe.name(), *arg),
            Expr::Lt(arg) => build_value_from_binop(OpLt.name(), *arg),
            Expr::Le(arg) => build_value_from_binop(OpLe.name(), *arg),
            Expr::Not(arg) => build_value_from_uop(OpNot.name(), *arg),
            Expr::Minus(arg) => build_value_from_uop(OpMinus.name(), *arg),
            Expr::IsNull(arg) => build_value_from_uop(OpIsNull.name(), *arg),
            Expr::NotNull(arg) => build_value_from_uop(OpNotNull.name(), *arg),
            Expr::Coalesce(arg) => build_value_from_binop(OpCoalesce.name(), *arg),
            Expr::Or(arg) => build_value_from_binop(OpOr.name(), *arg),
            Expr::And(arg) => build_value_from_binop(OpAnd.name(), *arg),
            Expr::Apply(op, args) => build_tagged_value(
                "Apply",
                vec![
                    Value::from(op.name().to_string()),
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
            Expr::ApplyAgg(op, a_args, args) => build_tagged_value(
                "ApplyAgg",
                vec![
                    Value::from(op.name().to_string()),
                    a_args
                        .into_iter()
                        .map(Value::from)
                        .collect::<Vec<_>>()
                        .into(),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                .into(),
            ),
            Expr::FieldAcc(f, v) => {
                build_tagged_value("FieldAcc", vec![f.into(), Value::from(*v)].into())
            }
            Expr::IdxAcc(idx, v) => {
                build_tagged_value("IdxAcc", vec![(idx as i64).into(), Value::from(*v)].into())
            }
        }
    }
}

fn build_tagged_value<'a>(tag: &'static str, val: Value<'a>) -> Value<'a> {
    Value::Dict(BTreeMap::from([(tag.into(), val)]))
}

impl<'a> TryFrom<&'a str> for Expr<'a> {
    type Error = ExprError;

    fn try_from(value: &'a str) -> result::Result<Self, Self::Error> {
        let pair = CozoParser::parse(Rule::expr_all, value)
            .map_err(|_| ExprError::Parse(value.to_string()))?
            .next()
            .ok_or_else(|| ExprError::Parse(value.to_string()))?;
        Ok(Expr::try_from(pair)?)
    }
}
