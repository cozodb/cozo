use crate::relation::typing::Typing;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum DataKind {
    Node = 1,
    Edge = 2,
    Associate = 3,
    Index = 4,
    Value = 5,
    TypeAlias = 6
}
// In storage, key layout is `[0, name, stack_depth]` where stack_depth is a non-positive number as zigzag
// Also has inverted index `[0, stack_depth, name]` for easy popping of stacks


#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct StorageId {
    cf: String,
    tid: u32,
}

pub struct Column {
    name: String,
    typ: Typing,
}

pub struct StoredRelation {
    keys: Vec<Column>,
    vals: Vec<Column>,
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