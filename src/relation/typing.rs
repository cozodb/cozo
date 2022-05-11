use crate::db::engine::Session;
use crate::error::{CozoError, Result};
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::Parser;
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::value::Value;
use pest::iterators::Pair;
use pest::Parser as PestParser;
use std::fmt::{Display, Formatter};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Typing {
    Any,
    Bool,
    Int,
    Float,
    Text,
    Uuid,
    Nullable(Box<Typing>),
    Homogeneous(Box<Typing>),
    UnnamedTuple(Vec<Typing>),
    NamedTuple(Vec<(String, Typing)>),
    Function(Vec<Typing>, Box<Typing>),
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
            Typing::Function(args, ret) => {
                let args_display = args
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "<{}>->{}", args_display, ret)
            }
        }
    }
}

impl Typing {
    #[inline]
    pub fn to_storage(&self, _v: Value) -> Option<Value> {
        todo!()
    }
    #[inline]
    pub fn to_display(&self, _v: Value) -> Option<Value> {
        todo!()
    }
}

impl Typing {
    pub fn from_pair<'a>(pair: Pair<Rule>, env: Option<&Session<'a>>) -> Result<Self> {
        Ok(match pair.as_rule() {
            Rule::simple_type => match pair.as_str() {
                "Any" => Typing::Any,
                "Bool" => Typing::Bool,
                "Int" => Typing::Int,
                "Float" => Typing::Float,
                "Text" => Typing::Text,
                "Uuid" => Typing::Uuid,
                t => match env {
                    None => return Err(CozoError::UndefinedType(t.to_string())),
                    Some(env) => {
                        let resolved = env.resolve(t)?;
                        let resolved =
                            resolved.ok_or_else(|| CozoError::UndefinedType(t.to_string()))?;
                        match resolved.data_kind()? {
                            DataKind::Type => resolved.interpret_as_type()?,
                            _ => return Err(CozoError::UndefinedType(t.to_string())),
                        }
                    }
                },
            },
            Rule::nullable_type => Typing::Nullable(Box::new(Typing::from_pair(
                pair.into_inner().next().unwrap(),
                env,
            )?)),
            Rule::homogeneous_list_type => Typing::Homogeneous(Box::new(Typing::from_pair(
                pair.into_inner().next().unwrap(),
                env,
            )?)),
            Rule::unnamed_tuple_type => {
                let types = pair
                    .into_inner()
                    .map(|p| Typing::from_pair(p, env))
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
                        let typ = Typing::from_pair(typ_pair, env)?;
                        Ok((name, typ))
                    })
                    .collect::<Result<Vec<(String, Typing)>>>()?;
                Typing::NamedTuple(types)
            }
            Rule::function_type => {
                let mut pairs = pair.into_inner();
                let args = pairs
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(|p| Typing::from_pair(p, env))
                    .collect::<Result<Vec<_>>>()?;
                let ret = Typing::from_pair(pairs.next().unwrap(), env)?;
                Typing::Function(args, ret.into())
            }
            _ => unreachable!(),
        })
    }

    pub fn extract_named_tuple(self) -> Option<Vec<(String, Typing)>> {
        match self {
            Typing::NamedTuple(t) => Some(t),
            _ => None,
        }
    }

    pub fn coerce<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        if *self == Typing::Any {
            return Ok(v);
        }
        if v == Value::Null {
            return if matches!(self, Typing::Nullable(_)) {
                Ok(Value::Null)
            } else {
                Err(CozoError::NotNullViolated(v.to_static()))
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
            Typing::Homogeneous(t) => match v {
                Value::List(vs) => Ok(Value::List(
                    vs.into_iter()
                        .map(|v| t.coerce(v))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => Err(CozoError::TypeMismatch),
            },
            Typing::UnnamedTuple(_ut) => {
                todo!()
            }
            Typing::NamedTuple(_nt) => {
                todo!()
            }
            Typing::Any => unreachable!(),
            Typing::Nullable(_) => unreachable!(),
            Typing::Function(_, _) => Err(CozoError::LogicError(
                "Cannot coerce function types".to_string(),
            )),
        }
    }
    fn coerce_bool<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Bool(_) => Ok(v),
            _ => Err(CozoError::TypeMismatch),
        }
    }
    fn coerce_int<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Int(_) => Ok(v),
            _ => Err(CozoError::TypeMismatch),
        }
    }
    fn coerce_float<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Float(_) => Ok(v),
            _ => Err(CozoError::TypeMismatch),
        }
    }
    fn coerce_text<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Text(_) => Ok(v),
            _ => Err(CozoError::TypeMismatch),
        }
    }
    fn coerce_uuid<'a>(&self, v: Value<'a>) -> Result<Value<'a>> {
        match v {
            v @ Value::Uuid(_) => Ok(v),
            _ => Err(CozoError::TypeMismatch),
        }
    }
}

impl TryFrom<&str> for Typing {
    type Error = CozoError;

    fn try_from(value: &str) -> Result<Self> {
        let pair = Parser::parse(Rule::typing, value)?.next().unwrap();
        Typing::from_pair(pair, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

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
        let res: Result<Typing> = "?[Text]".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
        let res: Result<Typing> = "?(Text, [Int], ?Uuid)".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
        let res: Result<Typing> = "{xzzx : Text}".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
        let res: Result<Typing> = "?({x : Text, ppqp: ?Int}, [Int], ?Uuid)".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
        let res: Result<Typing> = "??Int".try_into();
        println!("{:#?}", res);
        assert!(res.is_err());
        let res: Result<Typing> = "<Int, Int, ?Int>->Any".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
        let res: Result<Typing> = "<>->Any".try_into();
        println!("{:#?}", res);
        assert!(res.is_ok());
    }
}
