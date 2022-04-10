use std::collections::BTreeMap;
use crate::ast::Op;
use crate::env::{Env, LayeredEnv};
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

pub struct StructuredEnvItem {
    map: BTreeMap<String, Structured>,
}


pub struct StructuredEnv {
    stack: Vec<StructuredEnvItem>,
}


impl StructuredEnv {
    pub fn new() -> Self {
        let mut root = StructuredEnvItem { map: BTreeMap::new() };
        define_base_types(&mut root);
        Self { stack: vec![root] }
    }

    pub fn root(&self) -> &StructuredEnvItem {
        &self.stack[0]
    }

    pub fn root_mut(&mut self) -> &mut StructuredEnvItem {
        &mut self.stack[0]
    }

    pub fn cur(&self) -> &StructuredEnvItem {
        self.stack.last().unwrap()
    }

    pub fn cur_mut(&mut self) -> &mut StructuredEnvItem {
        self.stack.last_mut().unwrap()
    }

    pub fn push(&mut self) {
        self.stack.push(StructuredEnvItem { map: BTreeMap::new() })
    }
    pub fn pop(&mut self) -> bool {
        if self.stack.len() <= 1 {
            false
        } else {
            self.stack.pop();
            true
        }
    }

    pub fn get_next_table_id(&self, local: bool) -> TableId {
        let mut id = 0;
        let persistence = if local { Persistence::Local } else { Persistence::Global };
        for env in &self.stack {
            for item in env.map.values() {
                if let Some(TableId(p, eid)) = item.storage_id() {
                    if p == persistence {
                        id = id.max(eid);
                    }
                }
            }
        }
        TableId(persistence, id + 1)
    }
}

impl LayeredEnv<Structured> for StructuredEnv {
    fn root_define(&mut self, name: String, value: Structured) -> Option<Structured> {
        self.root_mut().define(name, value)
    }

    fn root_define_new(&mut self, name: String, value: Structured) -> bool {
        self.root_mut().define_new(name, value)
    }

    fn root_resolve(&self, name: &str) -> Option<&Structured> {
        self.root().resolve(name)
    }

    fn root_resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        self.root_mut().resolve_mut(name)
    }

    fn root_undef(&mut self, name: &str) -> Option<Structured> {
        self.root_mut().undef(name)
    }
}

impl Env<Structured> for StructuredEnv {
    fn define(&mut self, name: String, value: Structured) -> Option<Structured> {
        self.stack.last_mut().unwrap().define(name, value)
    }

    fn define_new(&mut self, name: String, value: Structured) -> bool {
        self.stack.last_mut().unwrap().define_new(name, value)
    }

    fn resolve(&self, name: &str) -> Option<&Structured> {
        let mut res = None;
        for item in self.stack.iter().rev() {
            res = item.resolve(name);
            if res.is_some() {
                return res;
            }
        }
        res
    }

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        let mut res = None;
        for item in self.stack.iter_mut().rev() {
            res = item.resolve_mut(name);
            if res.is_some() {
                return res;
            }
        }
        res
    }

    fn undef(&mut self, name: &str) -> Option<Structured> {
        let mut res = None;
        for item in self.stack.iter_mut().rev() {
            res = item.undef(name);
            if res.is_some() {
                return res;
            }
        }
        res
    }
}

impl Env<Structured> for StructuredEnvItem {
    fn define(&mut self, name: String, value: Structured) -> Option<Structured> {
        let old = self.map.remove(&name);
        self.map.insert(name, value);
        old
    }

    fn define_new(&mut self, name: String, value: Structured) -> bool {
        if let std::collections::btree_map::Entry::Vacant(e) = self.map.entry(name) {
            e.insert(value);
            true
        } else {
            false
        }
    }

    fn resolve(&self, name: &str) -> Option<&Structured> {
        self.map.get(name)
    }

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        self.map.get_mut(name)
    }


    fn undef(&mut self, name: &str) -> Option<Structured> {
        self.map.remove(name)
    }
}


impl Default for StructuredEnv {
    fn default() -> Self {
        StructuredEnv::new()
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct Col {
    pub name: String,
    pub typ: Typing,
    pub default: Option<Value<'static>>,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
pub enum Persistence {
    Global,
    Local,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub enum StorageStatus {
    Planned,
    Verified,
    Stored,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
pub struct TableId(pub Persistence, pub usize);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ColumnId(TableId, usize);

#[derive(Debug, PartialEq, Clone)]
pub struct Node {
    pub id: TableId,
    pub keys: Vec<Col>,
    pub cols: Vec<Col>,
    pub out_e: Vec<TableId>,
    pub in_e: Vec<TableId>,
    pub attached: Vec<TableId>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Edge {
    pub src: TableId,
    pub dst: TableId,
    pub id: TableId,
    pub keys: Vec<Col>,
    pub cols: Vec<Col>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Columns {
    pub attached: TableId,
    pub id: TableId,
    pub cols: Vec<Col>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Index {
    pub id: TableId,
    pub attached: TableId,
    pub cols: Vec<String>,
}


#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Typing {
    Any,
    Base(BaseType),
    HList(Box<Typing>),
    Nullable(Box<Typing>),
    Tuple(Vec<Typing>),
    NamedTuple(BTreeMap<String, Typing>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Structured {
    Typing(Typing),
    Node(Node, StorageStatus),
    Edge(Edge, StorageStatus),
    Columns(Columns, StorageStatus),
    Index(Index, StorageStatus),
}

impl Structured {
    fn storage_id(&self) -> Option<TableId> {
        match self {
            Structured::Typing(_) => None,
            Structured::Node(n, _) => Some(n.id),
            Structured::Edge(e, _) => Some(e.id),
            Structured::Columns(c, _) => Some(c.id),
            Structured::Index(i, _) => Some(i.id)
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