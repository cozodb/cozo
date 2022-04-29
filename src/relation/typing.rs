use std::fmt::{Display, Formatter};
use pest::iterators::Pair;
use crate::error::{Result, CozoError};
use crate::relation::value::Value;
use pest::Parser as PestParser;
use cozorocks::SlicePtr;
use crate::db::engine::Session;
use crate::db::eval::{Environment};
use crate::parser::Parser;
use crate::parser::Rule;
use crate::parser::text_identifier::build_name_in_def;
use crate::relation::data::DataKind;


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
                let collected = n.iter().map(|(k, v)|
                    format!(r##""{}":{}"##, k, v)).collect::<Vec<_>>();
                let joined = collected.join(",");
                write!(f, "({})", joined)
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
    pub fn from_pair<'t, T: AsRef<[u8]>, E: Environment<'t, T>>(pair: Pair<Rule>, env: Option<&E>) -> Result<Self> {
        Ok(match pair.as_rule() {
            Rule::simple_type => match pair.as_str() {
                "Any" => Typing::Any,
                "Bool" => Typing::Bool,
                "Int" => Typing::Int,
                "Float" => Typing::Float,
                "Text" => Typing::Text,
                "Uuid" => Typing::Uuid,
                t => {
                    match env {
                        None => return Err(CozoError::UndefinedType(t.to_string())),
                        Some(env) => {
                            let resolved = env.resolve(t)?;
                            let resolved = resolved.ok_or_else(|| CozoError::UndefinedType(t.to_string()))?;
                            match resolved.data_kind()? {
                                DataKind::Type => resolved.interpret_as_type()?,
                                _ => return Err(CozoError::UndefinedType(t.to_string()))
                            }
                        }
                    }
                }
            },
            Rule::nullable_type => Typing::Nullable(Box::new(Typing::from_pair(pair.into_inner().next().unwrap(), env)?)),
            Rule::homogeneous_list_type => Typing::Homogeneous(Box::new(Typing::from_pair(pair.into_inner().next().unwrap(), env)?)),
            Rule::unnamed_tuple_type => {
                let types = pair.into_inner().map(|p| Typing::from_pair(p, env)).collect::<Result<Vec<Typing>>>()?;
                Typing::UnnamedTuple(types)
            }
            Rule::named_tuple_type => {
                let types = pair.into_inner().map(|p| -> Result<(String, Typing)> {
                    let mut ps = p.into_inner();
                    let name_pair = ps.next().unwrap();
                    let name = build_name_in_def(name_pair, true)?;
                    let typ_pair = ps.next().unwrap();
                    let typ = Typing::from_pair(typ_pair, env)?;
                    Ok((name, typ))
                }).collect::<Result<Vec<(String, Typing)>>>()?;
                Typing::NamedTuple(types)
            }
            _ => unreachable!()
        })
    }
}

impl TryFrom<&str> for Typing {
    type Error = CozoError;

    fn try_from(value: &str) -> Result<Self> {
        let pair = Parser::parse(Rule::typing, value)?.next().unwrap();
        Typing::from_pair::<SlicePtr, Session>(pair, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn to_string() {
        assert_eq!(
            format!("{}", Typing::Nullable(Box::new(Typing::Homogeneous(Box::new(Typing::Text))))),
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
    }
}