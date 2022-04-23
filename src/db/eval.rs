use crate::db::engine::{Engine, Session};
use crate::relation::table::Table;
use crate::relation::tuple::Tuple;
use crate::relation::typing::Typing;
use crate::relation::value::Value;

pub trait Environment {
    fn push_env(&mut self);
    fn pop_env(&mut self);
    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool);
    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool);
    fn define_table(&mut self, table: &Table, in_root: bool);
    fn resolve(&mut self, name: &str);
    fn delete_defined(&mut self, name: &str, in_root: bool);
}


#[repr(u8)]
enum DefinableTag {
    Value = 1,
    Typing = 2,
    Node = 3,
    Edge = 4,
    Associate = 5,
    Index = 6,
}


impl<'a> Session<'a> {
    fn encode_definable_key(&self, name: &str, in_root: bool) -> Tuple<Vec<u8>> {
        let depth_code = if in_root { 0 } else { self.stack_depth as i64 };
        let mut tuple = Tuple::with_prefix(0);
        tuple.push_str(name);
        tuple.push_int(depth_code);
        tuple
    }
}


impl<'a> Environment for Session<'a> {
    fn push_env(&mut self) {
        self.stack_depth -= 1;
    }

    fn pop_env(&mut self) {
        if self.stack_depth == 0 {
            return;
        }
        // Remove all stuff starting with the stack depth from the temp session
        self.stack_depth += 1;
    }

    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) {
        if in_root {
            todo!()
        } else {
            let key = self.encode_definable_key(name, in_root);
            let mut data = Tuple::with_prefix(0);
            data.push_uint(DefinableTag::Value as u8 as u64);
            data.push_value(val);
        }
    }

    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool) {
        todo!()
    }

    fn define_table(&mut self, table: &Table, in_root: bool) {
        todo!()
    }

    fn resolve(&mut self, name: &str) {
        todo!()
    }

    fn delete_defined(&mut self, name: &str, in_root: bool) {
        todo!()
    }
}