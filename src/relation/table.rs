use crate::relation::tuple::CowTuple;
use crate::relation::typing::Typing;
use std::cmp::Ordering;

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
        MegaTuple {
            keys: vec![],
            vals: vec![],
        }
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn extend(&mut self, other: Self) {
        self.keys.extend(other.keys);
        self.vals.extend(other.vals);
    }
    pub fn all_keys_eq(&self, other: &Self) -> bool {
        if self.keys.len() != other.keys.len() {
            return false;
        }
        for (l, r) in self.keys.iter().zip(&other.keys) {
            if !l.key_part_eq(r) {
                return false;
            }
        }
        true
    }
    pub fn all_keys_cmp(&self, other: &Self) -> Ordering {
        for (l, r) in self.keys.iter().zip(&other.keys) {
            match l.key_part_cmp(r) {
                Ordering::Equal => {}
                v => return v,
            }
        }
        Ordering::Equal
    }
}
