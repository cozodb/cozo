use std::borrow::Cow;
use std::collections::BTreeMap;
use crate::typing::{Structured};

pub trait Env<V> where V: Clone {
    fn define(&mut self, name: String, value: V) -> Option<V>;
    fn define_new(&mut self, name: String, value: V) -> bool;
    fn resolve(&self, name: &str) -> Option<Cow<V>>;
    fn resolve_mut(&mut self, name: &str) -> Option<&mut V>;
    fn undef(&mut self, name: &str) -> Option<V>;
}

pub trait LayeredEnv<V>: Env<V> where V: Clone {
    fn root_define(&mut self, name: String, value: V) -> Option<V>;
    fn root_define_new(&mut self, name: String, value: V) -> bool;
    fn root_resolve(&self, name: &str) -> Option<Cow<V>>;
    fn root_undef(&mut self, name: &str) -> Option<V>;
}


pub struct Environment {
    map: BTreeMap<String, Structured>,
}


impl Default for Environment {
    fn default() -> Self {
        Self { map: BTreeMap::new() }
    }
}

impl Env<Structured> for Environment {
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

    fn resolve(&self, name: &str) -> Option<Cow<Structured>> {
        self.map.get(name)
            .map(|v| Cow::Borrowed(v))
    }

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Structured> {
        self.map.get_mut(name)
    }


    fn undef(&mut self, name: &str) -> Option<Structured> {
        self.map.remove(name)
    }
}
