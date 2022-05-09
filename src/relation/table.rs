use crate::relation::tuple::{CowTuple};
use crate::relation::typing::Typing;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct StorageId {
    pub cf: String,
    pub tid: u32,
}

pub struct Column {
    pub name: String,
    pub typ: Typing,
}

pub struct StoredRelation {
    pub keys: Vec<Column>,
    pub vals: Vec<Column>,
}

pub enum Table {
    NodeTable {
        name: String,
        stored: StoredRelation,
    },
    EdgeTable {
        name: String,
        src: Box<Table>,
        dst: Box<Table>,
        stored: StoredRelation,
    },
    AssociateTable {
        name: String,
        src: Box<Table>,
        stored: StoredRelation,
    },
    IndexTable {
        name: String,
        src: Box<Table>,
        stored: StoredRelation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MegaTuple {
    pub keys: Vec<CowTuple>,
    pub vals: Vec<CowTuple>,
}

impl MegaTuple {
    pub fn empty_tuple() -> Self {
        MegaTuple { keys: vec![], vals: vec![] }
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}