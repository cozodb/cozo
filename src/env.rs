pub trait Env<V> {
    fn define(&mut self, name: String, value: V) -> Option<V>;
    fn define_new(&mut self, name: String, value: V) -> bool;
    fn resolve(&self, name: &str) -> Option<&V>;
    fn resolve_mut(&mut self, name: &str) -> Option<&mut V>;
    fn undef(&mut self, name: &str) -> Option<V>;
}

pub trait LayeredEnv<V> : Env<V> {
    fn root_define(&mut self, name: String, value: V) -> Option<V>;
    fn root_define_new(&mut self, name: String, value: V) -> bool;
    fn root_resolve(&self, name: &str) -> Option<&V>;
    fn root_resolve_mut(&mut self, name: &str) -> Option<&mut V>;
    fn root_undef(&mut self, name: &str) -> Option<V>;
}