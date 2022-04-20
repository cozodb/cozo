use crate::relation::table::Table;
use crate::relation::typing::Typing;
use crate::relation::value::Value;

pub trait Environment {
    fn push_env(&mut self) {

    }
    fn pop_env(&mut self) {

    }
    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) {

    }
    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool) {

    }
    fn define_table(&mut self, table: &Table, in_root: bool) {

    }
    fn resolve(&mut self, name: &str) {

    }
    fn delete_defined(&mut self, name: &str, in_root: bool) {

    }
}
