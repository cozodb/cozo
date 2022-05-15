use crate::data::op::{AggOp, Op, UnresolvedOp};
use crate::data::tuple_set::{ColId, TableId, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use std::collections::BTreeMap;
use std::result;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ExprError {
    #[error("Cannot convert from {0}")]
    ConversionFailure(StaticValue),

    #[error("Unknown expr tag {0}")]
    UnknownExprTag(String),

    #[error("List extraction failed for {0}")]
    ListExtractionFailed(StaticValue),
}

type Result<T> = result::Result<T, ExprError>;

pub(crate) enum Expr<'a> {
    Const(Value<'a>),
    List(Vec<Expr<'a>>),
    Dict(BTreeMap<String, Expr<'a>>),
    Variable(String),
    TableCol(TableId, ColId),
    TupleSetIdx(TupleSetIdx),
    Apply(Arc<dyn Op>, Vec<Expr<'a>>),
    ApplyAgg(Arc<dyn AggOp>, Vec<Expr<'a>>, Vec<Expr<'a>>),
    FieldAcc(String, Box<Expr<'a>>),
    IdxAcc(usize, Box<Expr<'a>>),
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
                    Ok(Expr::List(l.into_iter().map(Expr::try_from).collect::<Result<Vec<_>>>()?))
                }
                "Dict" => {
                    match v {
                        Value::Dict(d) => {
                            Ok(Expr::Dict(d.into_iter().map(|(k, v)| -> Result<(String, Expr)> {
                                Ok((k.to_string(), Expr::try_from(v)?))
                            }).collect::<Result<BTreeMap<_, _>>>()?))
                        }
                        v => return Err(ExprError::ConversionFailure(Value::Dict(BTreeMap::from([(k, v)])).to_static()))
                    }
                }
                "Variable" => {
                    if let Value::Text(t) = v {
                        Ok(Expr::Variable(t.to_string()))
                    } else {
                        return Err(ExprError::ConversionFailure(Value::Dict(BTreeMap::from([(k, v)])).to_static()));
                    }
                }
                "TableCol" => {
                    let mut l = extract_list_from_value(v, 4)?.into_iter();
                    let in_root = match l.next().unwrap() {
                        Value::Bool(b) => b,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let tid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let is_key = match l.next().unwrap() {
                        Value::Bool(b) => b,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let cid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    Ok(Expr::TableCol((in_root, tid as u32).into(), (is_key, cid as usize).into()))
                }
                "TupleSetIdx" => {
                    let mut l = extract_list_from_value(v, 3)?.into_iter();
                    let is_key = match l.next().unwrap() {
                        Value::Bool(b) => b,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let tid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let cid = match l.next().unwrap() {
                        Value::Int(i) => i,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
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
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let op = Arc::new(UnresolvedOp(name.to_string()));
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let args = l.into_iter().map(Expr::try_from).collect::<Result<Vec<_>>>()?;
                    Ok(Expr::Apply(op, args))
                }
                "ApplyAgg" => {
                    let mut ll = extract_list_from_value(v, 3)?.into_iter();
                    let name = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let op = Arc::new(UnresolvedOp(name.to_string()));
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let a_args = l.into_iter().map(Expr::try_from).collect::<Result<Vec<_>>>()?;
                    let l = extract_list_from_value(ll.next().unwrap(), 0)?;
                    let args = l.into_iter().map(Expr::try_from).collect::<Result<Vec<_>>>()?;
                    Ok(Expr::ApplyAgg(op, a_args, args))
                }
                "FieldAcc" => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let field = match ll.next().unwrap() {
                        Value::Text(t) => t,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::FieldAcc(field.to_string(), arg.into()))
                }
                "IdxAcc" => {
                    let mut ll = extract_list_from_value(v, 2)?.into_iter();
                    let idx = match ll.next().unwrap() {
                        Value::Int(i) => i as usize,
                        v => return Err(ExprError::ConversionFailure(v.to_static()))
                    };
                    let arg = Expr::try_from(ll.next().unwrap())?;
                    Ok(Expr::IdxAcc(idx, arg.into()))
                }
                k => Err(ExprError::UnknownExprTag(k.to_string()))
            }
        } else {
            Err(ExprError::ConversionFailure(value.to_static()))
        }
    }
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
            Expr::TableCol(tid, cid) => build_tagged_value(
                "TableCol",
                vec![
                    tid.in_root.into(),
                    Value::from(tid.id as i64),
                    cid.is_key.into(),
                    Value::from(cid.id as i64),
                ]
                    .into(),
            ),
            Expr::TupleSetIdx(sid) => build_tagged_value(
                "TupleSetIdx",
                vec![
                    sid.is_key.into(),
                    Value::from(sid.t_set as i64),
                    Value::from(sid.col_idx as i64),
                ]
                    .into(),
            ),
            Expr::Apply(op, args) => build_tagged_value(
                "Apply",
                vec![
                    Value::from(op.name().to_string()),
                    args.into_iter().map(Value::from).collect::<Vec<_>>().into(),
                ]
                    .into(),
            ),
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
