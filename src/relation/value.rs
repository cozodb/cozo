use std::borrow::Cow;
use std::collections::BTreeMap;
use uuid::Uuid;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Tag {
    BoolFalse = 0,
    Null = 2,
    BoolTrue = 4,
    FwdEdge = 6,
    BwdEdge = 8,
    Int = 11,
    Float = 13,
    Text = 15,
    Uuid = 17,
    UInt = 21,
    List = 101,
    Dict = 103,
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
}

impl From<u8> for Tag {
    #[inline]
    fn from(u: u8) -> Self {
        use self::Tag::*;
        match u {
            0 => BoolFalse,
            2 => Null,
            4 => BoolTrue,
            6 => FwdEdge,
            8 => BwdEdge,
            11 => Int,
            13 => Float,
            15 => Text,
            17 => Uuid,
            21 => UInt,
            101 => List,
            103 => Dict,
            _ => panic!("Unexpected value tag {}", u)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EdgeDir {
    Fwd,
    Bwd,
}


#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    EdgeDir(EdgeDir),
    UInt(u64),
    Int(i64),
    Float(f64),
    Uuid(Uuid),
    Text(Cow<'a, str>),
    List(Vec<Value<'a>>),
    Dict(BTreeMap<Cow<'a, str>, Value<'a>>),
}

pub type StaticValue = Value<'static>;

impl <'a> Value<'a> {
    #[inline]
    pub fn to_static(self) -> StaticValue {
        match self {
            Value::Null => Value::from(()),
            Value::Bool(b) => Value::from(b),
            Value::EdgeDir(e) => Value::from(e),
            Value::UInt(u) => Value::from(u),
            Value::Int(i) => Value::from(i),
            Value::Float(f) => Value::from(f),
            Value::Uuid(u) => Value::from(u),
            Value::Text(t) => Value::from(t.into_owned()),
            Value::List(l) => l.into_iter().map(|v| v.to_static()).collect::<Vec<StaticValue>>().into(),
            Value::Dict(d) => d.into_iter()
                .map(|(k, v)| (Cow::Owned(k.into_owned()), v.to_static()))
                .collect::<BTreeMap<Cow<'static, str>, StaticValue>>().into()
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

impl From<EdgeDir> for StaticValue {
    #[inline]
    fn from(e: EdgeDir) -> Self {
        Value::EdgeDir(e)
    }
}

impl From<u64> for StaticValue {
    #[inline]
    fn from(u: u64) -> Self {
        Value::UInt(u)
    }
}

impl From<i64> for StaticValue {
    #[inline]
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<f64> for StaticValue {
    #[inline]
    fn from(f: f64) -> Self {
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
