use ordered_float::OrderedFloat;
use std::borrow::Cow;
use std::cmp::{min, Reverse};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter, Write};
use uuid::Uuid;

#[derive(Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    Uuid(Uuid),
    Text(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),

    List(Vec<Value<'a>>),
    Dict(BTreeMap<Cow<'a, str>, Value<'a>>),

    DescVal(Reverse<Box<Value<'a>>>),

    Bottom, // Acts as "any" in type inference, end value in sorting
}

impl<'a> Value<'a> {
    pub(crate) fn is_null(&self) -> bool {
        *self == Value::Null
    }
    pub(crate) fn get_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            Value::Int(b) => Some(*b),
            _ => None,
        }
    }
    pub(crate) fn get_float(&self) -> Option<f64> {
        match self {
            Value::Float(b) => Some(b.into_inner()),
            _ => None,
        }
    }
    pub(crate) fn get_str(&self) -> Option<&str> {
        match self {
            Value::Text(b) => Some(b.as_ref()),
            _ => None,
        }
    }
    pub(crate) fn get_slice(&self) -> Option<&[Value<'a>]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }
    pub(crate) fn get_map(&self) -> Option<&BTreeMap<Cow<str>, Value>> {
        match self {
            Value::Dict(m) => Some(m),
            _ => None,
        }
    }
}

pub(crate) type StaticValue = Value<'static>;

impl<'a> Debug for Value<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value {{ {} }}", self)
    }
}

impl From<()> for StaticValue {
    #[inline]
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<bool> for StaticValue {
    #[inline]
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for StaticValue {
    #[inline]
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for StaticValue {
    #[inline]
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}

impl From<f64> for StaticValue {
    #[inline]
    fn from(f: f64) -> Self {
        Value::Float(f.into())
    }
}

impl From<OrderedFloat<f64>> for StaticValue {
    #[inline]
    fn from(f: OrderedFloat<f64>) -> Self {
        Value::Float(f)
    }
}

impl<'a> From<&'a str> for Value<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Value::Text(Cow::Borrowed(s))
    }
}

impl<'a> From<&'a [u8]> for Value<'a> {
    #[inline]
    fn from(v: &'a [u8]) -> Self {
        Value::Bytes(Cow::Borrowed(v))
    }
}

impl From<String> for StaticValue {
    #[inline]
    fn from(s: String) -> Self {
        Value::Text(Cow::Owned(s))
    }
}

impl From<Vec<u8>> for StaticValue {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(Cow::Owned(v))
    }
}

impl From<Uuid> for StaticValue {
    #[inline]
    fn from(u: Uuid) -> Self {
        Value::Uuid(u)
    }
}

impl<'a> From<Vec<Value<'a>>> for Value<'a> {
    #[inline]
    fn from(v: Vec<Value<'a>>) -> Self {
        Value::List(v)
    }
}

impl<'a> From<BTreeMap<Cow<'a, str>, Value<'a>>> for Value<'a> {
    #[inline]
    fn from(m: BTreeMap<Cow<'a, str>, Value<'a>>) -> Self {
        Value::Dict(m)
    }
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => {
                write!(f, "null")?;
            }
            Value::Bool(b) => {
                write!(f, "{}", if *b { "true" } else { "false" })?;
            }
            Value::Int(i) => {
                write!(f, "{}", i)?;
            }
            Value::Float(n) => {
                write!(f, "{}", n.into_inner())?;
            }
            Value::Uuid(u) => {
                write!(f, "{}", u)?;
            }
            Value::Bytes(b) => {
                write!(f, "<{} bytes: {:?} ..>", b.len(), &b[..min(8, b.len())])?;
            }
            Value::Text(t) => {
                f.write_char('"')?;
                for char in t.chars() {
                    match char {
                        '"' => {
                            f.write_str("\\\"")?;
                        }
                        '\\' => {
                            f.write_str("\\\\")?;
                        }
                        '/' => {
                            f.write_str("\\/")?;
                        }
                        '\x08' => {
                            f.write_str("\\b")?;
                        }
                        '\x0c' => {
                            f.write_str("\\f")?;
                        }
                        '\n' => {
                            f.write_str("\\n")?;
                        }
                        '\r' => {
                            f.write_str("\\r")?;
                        }
                        '\t' => {
                            f.write_str("\\t")?;
                        }
                        c => {
                            f.write_char(c)?;
                        }
                    }
                }
                f.write_char('"')?;
            }
            Value::List(l) => {
                f.write_char('[')?;
                let mut first = true;
                for v in l.iter() {
                    if !first {
                        f.write_char(',')?;
                    }
                    Display::fmt(v, f)?;
                    first = false;
                }
                f.write_char(']')?;
            }
            Value::Dict(d) => {
                f.write_char('{')?;
                let mut first = true;
                for (k, v) in d.iter() {
                    if !first {
                        f.write_char(',')?;
                    }
                    Display::fmt(&Value::Text(k.clone()), f)?;
                    f.write_char(':')?;
                    Display::fmt(v, f)?;
                    first = false;
                }
                f.write_char('}')?;
            }
            Value::Bottom => write!(f, "Sentinel")?,
            Value::DescVal(Reverse(v)) => {
                write!(f, "~{}", v)?;
            }
        }
        Ok(())
    }
}

impl<'a> Value<'a> {
    #[inline]
    pub fn to_static(self) -> StaticValue {
        match self {
            Value::Null => Value::from(()),
            Value::Bool(b) => Value::from(b),
            Value::Int(i) => Value::from(i),
            Value::Float(f) => Value::from(f),
            Value::Uuid(u) => Value::from(u),
            Value::Text(t) => Value::from(t.into_owned()),
            Value::List(l) => l
                .into_iter()
                .map(|v| v.to_static())
                .collect::<Vec<StaticValue>>()
                .into(),
            Value::Dict(d) => d
                .into_iter()
                .map(|(k, v)| (Cow::Owned(k.into_owned()), v.to_static()))
                .collect::<BTreeMap<Cow<'static, str>, StaticValue>>()
                .into(),
            Value::Bottom => panic!("Cannot process sentinel value"),
            Value::Bytes(t) => Value::from(t.into_owned()),
            Value::DescVal(Reverse(val)) => Value::DescVal(Reverse(val.to_static().into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::Value;
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;
    use uuid::Uuid;

    #[test]
    fn print_sizes() {
        dbg!(size_of::<usize>());
        dbg!(size_of::<Uuid>());
        dbg!(size_of::<Cow<str>>());
        dbg!(size_of::<BTreeMap<(), ()>>());
        dbg!(size_of::<HashMap<(), ()>>());
        dbg!(size_of::<Value>());
    }

    #[test]
    fn conversions() {
        assert!(matches!(Value::from(()), Value::Null));
        assert!(matches!(Value::from(10i64), Value::Int(_)));
        assert!(matches!(Value::from(10.1f64), Value::Float(_)));
        assert!(matches!(Value::from("abc"), Value::Text(_)));
        assert!(matches!(Value::from("abc".to_string()), Value::Text(_)));
        assert!(matches!(Value::from(vec![Value::Null]), Value::List(_)));
        assert!(matches!(Value::from(BTreeMap::new()), Value::Dict(_)));
    }
}
