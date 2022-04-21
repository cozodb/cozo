use std::cmp::Ordering;
use crate::relation::tuple::Tuple;

pub fn compare(a: &[u8], b: &[u8]) -> i8 {
    let ta = Tuple::new(a);
    let tb = Tuple::new(b);
    match ta.get_prefix().cmp(&tb.get_prefix()) {
        Ordering::Less => return -1,
        Ordering::Greater => return 1,
        Ordering::Equal => {}
    }
    match ta.iter().cmp(tb.iter()) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::relation::key_order::compare;
    use crate::relation::tuple::Tuple;
    use crate::relation::value::Value;

    #[test]
    fn ordering() {
        let mut t = Tuple::with_prefix(0);
        let t2 = Tuple::with_prefix(123);
        assert_eq!(compare(t.as_ref(), t.as_ref()), 0);
        assert_eq!(compare(t.as_ref(), t2.as_ref()), -1);
        assert_eq!(compare(t2.as_ref(), t.as_ref()), 1);
        let mut t2 = Tuple::with_prefix(0);
        t.push_str("aaa");
        t2.push_str("aaac");
        assert_eq!(compare(t.as_ref(), t2.as_ref()), -1);
        let mut t2 = Tuple::with_prefix(0);
        t2.push_str("aaa");
        t2.push_null();
        assert_eq!(compare(t.as_ref(), t2.as_ref()), -1);
        t.push_null();
        assert_eq!(compare(t.as_ref(), t2.as_ref()), 0);
        t.push_int(-123);
        t2.push_int(123);
        assert_eq!(compare(t.as_ref(), t2.as_ref()), -1);
        assert_eq!(compare(t.as_ref(), t.as_ref()), 0);
        let vals: Value = vec![().into(), BTreeMap::new().into(), 1e23.into(), false.into(), "xxyx".into()].into();
        t.push_value(&vals);
        assert_eq!(compare(t.as_ref(), t.as_ref()), 0);
    }
}