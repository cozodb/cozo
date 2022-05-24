use crate::data::value::{StaticValue, Value};
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::{CozoParser, Pair, Rule};
use anyhow::Result;
use pest::Parser;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum TypingError {
    #[error("Not null constraint violated for {0}")]
    NotNullViolated(Typing),

    #[error("Type mismatch: {1} cannot be interpreted as {0}")]
    TypeMismatch(Typing, StaticValue),

    #[error("Undefined type '{0}'")]
    UndefinedType(String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Typing {
    Any,
    Bool,
    Int,
    Float,
    Text,
    Uuid,
    Bytes,
    Nullable(Box<Typing>),
    Homogeneous(Box<Typing>),
    UnnamedTuple(Vec<Typing>),
    NamedTuple(Vec<(String, Typing)>),
}

impl Display for Typing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Typing::Any => write!(f, "Any"),
            Typing::Bool => write!(f, "Bool"),
            Typing::Int => write!(f, "Int"),
            Typing::Float => write!(f, "Float"),
            Typing::Text => write!(f, "Text"),
            Typing::Uuid => write!(f, "Uuid"),
            Typing::Nullable(t) => write!(f, "?{}", t),
            Typing::Homogeneous(h) => write!(f, "[{}]", h),
            Typing::UnnamedTuple(u) => {
                let collected = u.iter().map(|v| v.to_string()).collect::<Vec<_>>();
                let joined = collected.join(",");
                write!(f, "({})", joined)
            }
            Typing::NamedTuple(n) => {
                let collected = n
                    .iter()
                    .map(|(k, v)| format!(r##""{}":{}"##, k, v))
                    .collect::<Vec<_>>();
                let joined = collected.join(",");
                write!(f, "{{")?;
                write!(f, "{}", joined)?;
                write!(f, "}}")
            }
            Typing::Bytes => write!(f, "Bytes"),
        }
    }
}

impl Debug for Typing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Typing<{}>", self)
    }
}

impl Typing {
    pub(crate) fn representative_value(&self) -> StaticValue {
        match self {
            Typing::Any => Value::Bottom,
            Typing::Bool => Value::Bool(false),
            Typing::Int => Value::Int(0),
            Typing::Float => Value::Float((0.).into()),
            Typing::Text => Value::Text("".into()),
            Typing::Uuid => Value::Uuid(Uuid::nil()),
            Typing::Nullable(n) => n.representative_value(),
            Typing::Homogeneous(h) => vec![h.representative_value()].into(),
            Typing::UnnamedTuple(v) => v
                .iter()
                .map(|v| v.representative_value())
                .collect::<Vec<_>>()
                .into(),
            Typing::NamedTuple(nt) => {
                let map = nt
                    .iter()
                    .map(|(k, v)| (k.clone().into(), v.representative_value()))
                    .collect::<BTreeMap<_, _>>();
                Value::from(map)
            }
            Typing::Bytes => Value::from(b"".as_ref()),
        }
    }
    pub(crate) fn coerce_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        if *self == Typing::Any {
            return Ok(Cow::Borrowed(v));
        }
        if *v == Value::Null {
            return if matches!(self, Typing::Nullable(_)) {
                Ok(Cow::Borrowed(v))
            } else {
                Err(TypingError::NotNullViolated(self.clone()).into())
            };
        }

        if let Typing::Nullable(t) = self {
            return t.coerce_ref(v);
        }

        match self {
            Typing::Bool => self.coerce_bool_ref(v),
            Typing::Int => self.coerce_int_ref(v),
            Typing::Float => self.coerce_float_ref(v),
            Typing::Text => self.coerce_text_ref(v),
            Typing::Uuid => self.coerce_uuid_ref(v),
            Typing::Bytes => self.coerce_bytes_ref(v),
            Typing::Homogeneous(t) => match v {
                Value::List(vs) => Ok(Cow::Owned(Value::List(
                    vs.into_iter()
                        .map(|v| t.coerce(v.clone()))
                        .collect::<Result<Vec<_>>>()?,
                ))),
                _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
            },
            Typing::UnnamedTuple(_ut) => {
                todo!()
            }
            Typing::NamedTuple(_nt) => {
                todo!()
            }
            Typing::Any => unreachable!(),
            Typing::Nullable(_) => unreachable!(),
        }
    }
    pub(crate) fn coerce<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        if *self == Typing::Any {
            return Ok(v);
        }
        if v == Value::Null {
            return if matches!(self, Typing::Nullable(_)) {
                Ok(Value::Null)
            } else {
                Err(TypingError::NotNullViolated(self.clone()).into())
            };
        }

        if let Typing::Nullable(t) = self {
            return t.coerce(v);
        }

        match self {
            Typing::Bool => self.coerce_bool(v),
            Typing::Int => self.coerce_int(v),
            Typing::Float => self.coerce_float(v),
            Typing::Text => self.coerce_text(v),
            Typing::Uuid => self.coerce_uuid(v),
            Typing::Bytes => self.coerce_bytes(v),
            Typing::Homogeneous(t) => match v {
                Value::List(vs) => Ok(Value::List(
                    vs.into_iter()
                        .map(|v| t.coerce(v))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
            },
            Typing::UnnamedTuple(_ut) => {
                todo!()
            }
            Typing::NamedTuple(_nt) => {
                todo!()
            }
            Typing::Any => unreachable!(),
            Typing::Nullable(_) => unreachable!(),
        }
    }
    fn coerce_bool<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Bool(_) => Ok(v),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_bool_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Bool(_) => Ok(Cow::Borrowed(v)),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
    fn coerce_int<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Int(_) => Ok(v),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_int_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Int(_) => Ok(Cow::Borrowed(v)),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
    fn coerce_float<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Float(_) => Ok(v),
            Value::Int(i) => Ok((i as f64).into()),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_float_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Float(_) => Ok(Cow::Borrowed(v)),
            Value::Int(i) => Ok(Cow::Owned((*i as f64).into())),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
    fn coerce_text<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Text(_) => Ok(v),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_text_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Text(_) => Ok(Cow::Borrowed(v)),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
    fn coerce_uuid<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Uuid(_) => Ok(v),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_uuid_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Uuid(_) => Ok(Cow::Borrowed(v)),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
    fn coerce_bytes<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Bytes(_) => Ok(v),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.to_static()).into()),
        }
    }
    fn coerce_bytes_ref<'a>(&self, v: &'a Value<'a>) -> Result<Cow<'a, Value<'a>>> {
        match v {
            v @ Value::Bytes(_) => Ok(Cow::Borrowed(v)),
            _ => Err(TypingError::TypeMismatch(self.clone(), v.clone().to_static()).into()),
        }
    }
}

impl<'a> Value<'a> {
    pub(crate) fn deduce_typing(&self) -> Typing {
        match self {
            Value::Null => Typing::Any,
            Value::Bool(_) => Typing::Bool,
            Value::Int(_) => Typing::Int,
            Value::Float(_) => Typing::Float,
            Value::Uuid(_) => Typing::Uuid,
            Value::Text(_) => Typing::Text,
            Value::Bytes(_) => Typing::Bytes,
            Value::List(_) => Typing::Any,
            Value::Dict(_) => Typing::Any,
            Value::DescVal(_) => Typing::Any,
            Value::Bottom => Typing::Any,
        }
    }
}

impl TryFrom<&str> for Typing {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        let pair = CozoParser::parse(Rule::typing_all, value)?.next().unwrap();
        Typing::try_from(pair)
    }
}

impl TryFrom<Pair<'_>> for Typing {
    type Error = anyhow::Error;

    fn try_from(pair: Pair) -> Result<Self> {
        Ok(match pair.as_rule() {
            Rule::simple_type => match pair.as_str() {
                "Any" => Typing::Any,
                "Bool" => Typing::Bool,
                "Int" => Typing::Int,
                "Float" => Typing::Float,
                "Text" => Typing::Text,
                "Uuid" => Typing::Uuid,
                t => return Err(TypingError::UndefinedType(t.to_string()).into()),
            },
            Rule::nullable_type => {
                let inner_type = Typing::try_from(pair.into_inner().next().unwrap())?;
                Typing::Nullable(Box::new(inner_type))
            }
            Rule::homogeneous_list_type => Typing::Homogeneous(Box::new(Typing::try_from(
                pair.into_inner().next().unwrap(),
            )?)),
            Rule::unnamed_tuple_type => {
                let types = pair
                    .into_inner()
                    .map(Typing::try_from)
                    .collect::<Result<Vec<Typing>>>()?;
                Typing::UnnamedTuple(types)
            }
            Rule::named_tuple_type => {
                let types = pair
                    .into_inner()
                    .map(|p| -> Result<(String, Typing)> {
                        let mut ps = p.into_inner();
                        let name_pair = ps.next().unwrap();
                        let name = build_name_in_def(name_pair, true)?;
                        let typ_pair = ps.next().unwrap();
                        let typ = Typing::try_from(typ_pair)?;
                        Ok((name, typ))
                    })
                    .collect::<Result<Vec<(String, Typing)>>>()?;
                Typing::NamedTuple(types)
            }
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string() {
        assert_eq!(
            format!(
                "{}",
                Typing::Nullable(Box::new(Typing::Homogeneous(Box::new(Typing::Text))))
            ),
            "?[Text]"
        );
    }

    #[test]
    fn from_string() {
        assert!(dbg!(Typing::try_from("?[Text]")).is_ok());
        assert!(dbg!(Typing::try_from("?(Text, [Int], ?Uuid)")).is_ok());
        assert!(dbg!(Typing::try_from("{xzzx : Text}")).is_ok());
        assert!(dbg!(Typing::try_from("?({x : Text, ppqp: ?Int}, [Int], ?Uuid)")).is_ok());
        assert!(dbg!(Typing::try_from("??Int")).is_err());
    }
}
