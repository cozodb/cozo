pub trait Env<V> {
    fn define(&mut self, name: &str, value: V) -> Option<V>;
    fn resolve(&self, name: &str) -> Option<V>;
    fn undef(&mut self, name: &str) -> Option<V>;
}
