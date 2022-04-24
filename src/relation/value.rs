use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Write};
use ordered_float::OrderedFloat;
use uuid::Uuid;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Tag {
    BoolFalse = 1,
    Null = 2,
    BoolTrue = 3,
    Int = 4,
    Float = 5,
    Text = 6,
    Uuid = 7,
    UInt = 8,
    List = 9,
    Dict = 10,
    // Timestamp = 23,
    // Datetime = 25,
    // Timezone = 27,
    // Date = 27,
    // Time = 29,
    // Duration = 31,
    // BigInt = 51,
    // BigDecimal = 53,
    // Inet = 55,
    // Crs = 57,
    // BitArr = 60,
    // U8Arr = 61,
    // I8Arr = 62,
    // U16Arr = 63,
    // I16Arr = 64,
    // U32Arr = 65,
    // I32Arr = 66,
    // U64Arr = 67,
    // I64Arr = 68,
    // F16Arr = 69,
    // F32Arr = 70,
    // F64Arr = 71,
    // C32Arr = 72,
    // C64Arr = 73,
    // C128Arr = 74,
    MaxTag = u8::MAX
}

impl TryFrom<u8> for Tag {
    type Error = u8;
    #[inline]
    fn try_from(u: u8) -> std::result::Result<Tag, u8> {
        use self::Tag::*;
        Ok(match u {
            1 => BoolFalse,
            2 => Null,
            3 => BoolTrue,
            4 => Int,
            5 => Float,
            6 => Text,
            7 => Uuid,
            8 => UInt,
            9 => List,
            10 => Dict,
            u8::MAX => MaxTag,
            v => return Err(v)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    Uuid(Uuid),
    Text(Cow<'a, str>),
    List(Vec<Value<'a>>),
    Dict(BTreeMap<Cow<'a, str>, Value<'a>>),
    End_Sentinel
}

pub type StaticValue = Value<'static>;

impl<'a> Value<'a> {
    #[inline]
    pub fn to_static(self) -> StaticValue {
        match self {
            Value::Null => Value::from(()),
            Value::Bool(b) => Value::from(b),
            Value::UInt(u) => Value::from(u),
            Value::Int(i) => Value::from(i),
            Value::Float(f) => Value::from(f),
            Value::Uuid(u) => Value::from(u),
            Value::Text(t) => Value::from(t.into_owned()),
            Value::List(l) => l.into_iter().map(|v| v.to_static()).collect::<Vec<StaticValue>>().into(),
            Value::Dict(d) => d.into_iter()
                .map(|(k, v)| (Cow::Owned(k.into_owned()), v.to_static()))
                .collect::<BTreeMap<Cow<'static, str>, StaticValue>>().into(),
            Value::End_Sentinel => panic!("Cannot process sentinel value")
        }
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

impl From<u64> for StaticValue {
    #[inline]
    fn from(u: u64) -> Self {
        Value::UInt(u)
    }
}


impl From<u32> for StaticValue {
    #[inline]
    fn from(u: u32) -> Self {
        Value::UInt(u as u64)
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

impl From<String> for StaticValue {
    #[inline]
    fn from(s: String) -> Self {
        Value::Text(Cow::Owned(s))
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
            Value::Null => { f.write_str("null")?; }
            Value::Bool(b) => { f.write_str(if *b { "true" } else { "false" })?; }
            Value::UInt(u) => {
                f.write_str(&u.to_string())?;
                f.write_str("u")?;
            }
            Value::Int(i) => { f.write_str(&i.to_string())?; }
            Value::Float(n) => { f.write_str(&format!("{:e}", n.into_inner()))?; }
            Value::Uuid(u) => { f.write_str(&u.to_string())?; }
            Value::Text(t) => {
                f.write_char('"')?;
                for char in t.chars() {
                    match char {
                        '"' => { f.write_str("\\\"")?; }
                        '\\' => { f.write_str("\\\\")?; }
                        '/' => { f.write_str("\\/")?; }
                        '\x08' => { f.write_str("\\b")?; }
                        '\x0c' => { f.write_str("\\f")?; }
                        '\n' => { f.write_str("\\n")?; }
                        '\r' => { f.write_str("\\r")?; }
                        '\t' => { f.write_str("\\t")?; }
                        c => { f.write_char(c)?; }
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
            Value::End_Sentinel => {
                write!(f, "Sentinel")?
            }
        }
        Ok(())
    }
}