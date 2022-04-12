use std::collections::BTreeMap;
use crate::typing::{define_base_types, Structured};

pub trait Env<V> {
    fn define(&mut self, name: String, value: V) -> Option<V>;
    fn define_new(&mut self, name: String, value: V) -> bool;
    fn resolve(&self, name: &str) -> Option<&V>;
    fn resolve_mut(&mut self, name: &str) -> Option<&mut V>;
    fn undef(&mut self, name: &str) -> Option<V>;
}

pub trait LayeredEnv<V>: Env<V> {
    fn root_define(&mut self, name: String, value: V) -> Option<V>;
    fn root_define_new(&mut self, name: String, value: V) -> bool;
    fn root_resolve(&self, name: &str) -> Option<&V>;
    fn root_resolve_mut(&mut self, name: &str) -> Option<&mut V>;
    fn root_undef(&mut self, name: &str) -> Option<V>;
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
