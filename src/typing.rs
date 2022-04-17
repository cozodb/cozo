use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter, Write};
use crate::env::{Env};
use crate::value::Value;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BaseType {
    Bool,
    Int,
    UInt,
    Float,
    String,
    BitArr,
    U8Arr,
    I8Arr,
    I16Arr,
    U16Arr,
    I32Arr,
    U32Arr,
    I64Arr,
    U64Arr,
    F16Arr,
    F32Arr,
    F64Arr,
    C32Arr,
    C64Arr,
    C128Arr,
    Uuid,
    Timestamp,
    Datetime,
    Timezone,
    Date,
    Time,
    Duration,
    BigInt,
    BigDecimal,
    Inet,
    Crs,
}


#[derive(Debug, PartialEq, Clone)]
pub struct Col {
    pub name: String,
    pub typ: Typing,
    pub default: Value<'static>,
}


#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub enum StorageStatus {
    Planned,
    Verified,
    Stored,
}

#[derive(PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct TableId {
    pub name: String,
    pub local_id: usize,
}

impl TableId {
    pub fn is_global(&self) -> bool {
        self.local_id == 0
    }
}


impl Debug for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)?;
        if self.local_id > 0 {
            f.write_str(&format!("({})", self.local_id))?;
        }
        Ok(())
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct ColumnId(TableId, i64);

impl Debug for ColumnId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.0))?;
        f.write_str(&format!("~{}", self.1))?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Node {
    pub status: StorageStatus,
    pub id: TableId,
    pub keys: Vec<Col>,
    pub cols: Vec<Col>,
    pub out_e: Vec<TableId>,
    pub in_e: Vec<TableId>,
    pub attached: Vec<TableId>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Edge {
    pub status: StorageStatus,
    pub src: TableId,
    pub dst: TableId,
    pub id: TableId,
    pub keys: Vec<Col>,
    pub cols: Vec<Col>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Columns {
    pub status: StorageStatus,
    pub attached: TableId,
    pub id: TableId,
    pub cols: Vec<Col>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Index {
    pub status: StorageStatus,
    pub id: TableId,
    pub attached: TableId,
    pub cols: Vec<String>,
}


#[derive(Eq, PartialEq, Clone)]
pub enum Typing {
    Any,
    Base(BaseType),
    HList(Box<Typing>),
    Nullable(Box<Typing>),
    Tuple(Vec<Typing>),
    NamedTuple(BTreeMap<String, Typing>),
}

impl Display for Typing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Typing::Any => f.write_str("Any")?,
            Typing::Base(b) => {
                match b {
                    BaseType::Bool => f.write_str("Bool")?,
                    BaseType::Int => f.write_str("Int")?,
                    BaseType::UInt => f.write_str("UInt")?,
                    BaseType::Float => f.write_str("Float")?,
                    BaseType::String => f.write_str("String")?,
                    BaseType::BitArr => f.write_str("BitArr")?,
                    BaseType::U8Arr => f.write_str("U8Arr")?,
                    BaseType::I8Arr => f.write_str("I8Arr")?,
                    BaseType::I16Arr => f.write_str("I16Arr")?,
                    BaseType::U16Arr => f.write_str("U16Arr")?,
                    BaseType::I32Arr => f.write_str("I32Arr")?,
                    BaseType::U32Arr => f.write_str("U32Arr")?,
                    BaseType::I64Arr => f.write_str("I64Arr")?,
                    BaseType::U64Arr => f.write_str("U64Arr")?,
                    BaseType::F16Arr => f.write_str("F16Arr")?,
                    BaseType::F32Arr => f.write_str("F32Arr")?,
                    BaseType::F64Arr => f.write_str("F64Arr")?,
                    BaseType::C32Arr => f.write_str("C32Arr")?,
                    BaseType::C64Arr => f.write_str("C64Arr")?,
                    BaseType::C128Arr => f.write_str("C128Arr")?,
                    BaseType::Uuid => f.write_str("Uuid")?,
                    BaseType::Timestamp => f.write_str("Timestamp")?,
                    BaseType::Datetime => f.write_str("Datetime")?,
                    BaseType::Timezone => f.write_str("Timezone")?,
                    BaseType::Date => f.write_str("Date")?,
                    BaseType::Time => f.write_str("Time")?,
                    BaseType::Duration => f.write_str("Duration")?,
                    BaseType::BigInt => f.write_str("BigInt")?,
                    BaseType::BigDecimal => f.write_str("BigDecimal")?,
                    BaseType::Inet => f.write_str("Inet")?,
                    BaseType::Crs => f.write_str("Crs")?
                }
            }
            Typing::HList(l) => {
                f.write_char('[')?;
                Display::fmt(l, f)?;
                f.write_char(']')?;
            }
            Typing::Nullable(d) => {
                f.write_char('?')?;
                Display::fmt(d, f)?;
            }
            Typing::Tuple(_) => todo!(),
            Typing::NamedTuple(_) => todo!()
        }
        Ok(())
    }
}

impl Debug for Typing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Structured {
    Typing(Typing),
    Node(Node),
    Edge(Edge),
    Columns(Columns),
    Index(Index),
    Value(Value<'static>)
}

impl Structured {
    pub fn storage_id(&self) -> Option<TableId> {
        match self {
            Structured::Typing(_) => None,
            Structured::Node(n) => Some(n.id.clone()),
            Structured::Edge(e) => Some(e.id.clone()),
            Structured::Columns(c) => Some(c.id.clone()),
            Structured::Index(i) => Some(i.id.clone()),
            Structured::Value(_) => None
        }
    }
}

pub fn define_base_types<T: Env<Structured>>(env: &mut T) {
    env.define("Any".to_string(), Structured::Typing(Typing::Any));
    env.define("Bool".to_string(), Structured::Typing(Typing::Base(BaseType::Bool)));
    env.define("Int".to_string(), Structured::Typing(Typing::Base(BaseType::Int)));
    env.define("UInt".to_string(), Structured::Typing(Typing::Base(BaseType::UInt)));
    env.define("Float".to_string(), Structured::Typing(Typing::Base(BaseType::Float)));
    env.define("String".to_string(), Structured::Typing(Typing::Base(BaseType::String)));
    env.define("Bytes".to_string(), Structured::Typing(Typing::Base(BaseType::U8Arr)));
    env.define("U8Arr".to_string(), Structured::Typing(Typing::Base(BaseType::U8Arr)));
    env.define("Uuid".to_string(), Structured::Typing(Typing::Base(BaseType::Uuid)));
    env.define("Timestamp".to_string(), Structured::Typing(Typing::Base(BaseType::Timestamp)));
    env.define("Datetime".to_string(), Structured::Typing(Typing::Base(BaseType::Datetime)));
    env.define("Timezone".to_string(), Structured::Typing(Typing::Base(BaseType::Timezone)));
    env.define("Date".to_string(), Structured::Typing(Typing::Base(BaseType::Date)));
    env.define("Time".to_string(), Structured::Typing(Typing::Base(BaseType::Time)));
    env.define("Duration".to_string(), Structured::Typing(Typing::Base(BaseType::Duration)));
    env.define("BigInt".to_string(), Structured::Typing(Typing::Base(BaseType::BigInt)));
    env.define("BigDecimal".to_string(), Structured::Typing(Typing::Base(BaseType::BigDecimal)));
    env.define("Int".to_string(), Structured::Typing(Typing::Base(BaseType::Int)));
    env.define("Crs".to_string(), Structured::Typing(Typing::Base(BaseType::Crs)));
}