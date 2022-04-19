use std::borrow::{Cow};
use std::cell::RefCell;
use std::collections::BTreeMap;
use uuid::Uuid;
use crate::relation::value::{EdgeDir, Tag, Value};

#[derive(Debug, Clone)]
pub struct Tuple<T>
    where T: AsRef<[u8]>
{
    pub data: T,
    idx_cache: RefCell<Vec<usize>>,
}

impl<T> AsRef<[u8]> for Tuple<T> where T: AsRef<[u8]> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

const PREFIX_LEN: usize = 4;

impl<T: AsRef<[u8]>> Tuple<T> {
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            data,
            idx_cache: RefCell::new(vec![]),
        }
    }

    #[inline]
    pub fn get_prefix(&self) -> u32 {
        u32::from_be_bytes(self.data.as_ref()[0..4].try_into().unwrap())
    }

    #[inline]
    fn all_cached(&self) -> bool {
        match self.idx_cache.borrow().last() {
            None => self.data.as_ref().len() == PREFIX_LEN,
            Some(l) => *l == self.data.as_ref().len()
        }
    }
    #[inline]
    fn get_pos(&self, idx: usize) -> Option<usize> {
        if idx == 0 {
            if self.data.as_ref().len() > PREFIX_LEN {
                Some(PREFIX_LEN)
            } else {
                None
            }
        } else {
            self.cache_until(idx);
            self.idx_cache.borrow().get(idx - 1).cloned()
        }
    }
    #[inline]
    fn cache_until(&self, idx: usize) {
        while self.idx_cache.borrow().len() < idx && !self.all_cached() {
            self.skip_and_cache();
        }
    }
    #[inline]
    fn skip_and_cache(&self) {
        let data = self.data.as_ref();
        let tag_start = *self.idx_cache.borrow().last().unwrap_or(&PREFIX_LEN);
        let start = tag_start + 1;
        let nxt = match Tag::from(data[tag_start]) {
            Tag::Null | Tag::BoolTrue | Tag::BoolFalse | Tag::FwdEdge | Tag::BwdEdge => start,
            Tag::Int | Tag::UInt => start + self.parse_varint(start).1,
            Tag::Float => start + 8,
            Tag::Uuid => start + 16,
            Tag::Text => {
                let (slen, offset) = self.parse_varint(start);
                let slen = slen as usize;
                start + slen + offset
            }
            Tag::List => start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize,
            Tag::Dict => start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize
        };
        self.idx_cache.borrow_mut().push(nxt);
    }

    #[inline]
    fn parse_varint(&self, idx: usize) -> (u64, usize) {
        let data = self.data.as_ref();
        let mut cur = idx;
        let mut u: u64 = 0;
        let mut shift = 0;
        loop {
            let buf = data[cur];
            cur += 1;
            u |= ((buf & 0b01111111) as u64) << shift;
            if buf & 0b10000000 == 0 {
                break;
            }
            shift += 7;
        }
        (u, cur - idx)
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<Value> {
        match self.get_pos(idx) {
            Some(v) => {
                if v == self.data.as_ref().len() {
                    return None;
                }
                let (val, nxt) = self.parse_value_at(v);
                if idx == self.idx_cache.borrow().len() {
                    self.idx_cache.borrow_mut().push(nxt);
                }
                Some(val)
            }
            None => None
        }
    }
    #[inline]
    fn parse_value_at(&self, pos: usize) -> (Value, usize) {
        let data = self.data.as_ref();
        let start = pos + 1;
        let (nxt, val): (usize, Value) = match Tag::from(data[pos]) {
            Tag::Null => (start, ().into()),
            Tag::BoolTrue => (start, true.into()),
            Tag::BoolFalse => (start, false.into()),
            Tag::FwdEdge => (start, EdgeDir::Fwd.into()),
            Tag::BwdEdge => (start, EdgeDir::Bwd.into()),
            Tag::UInt => {
                let (u, offset) = self.parse_varint(start);
                (start + offset, u.into())
            }
            Tag::Int => {
                let (u, offset) = self.parse_varint(start);
                let val = if u & 1 == 0 {
                    (u >> 1) as i64
                } else {
                    -((u >> 1) as i64) - 1
                };
                (start + offset, val.into())
            }
            Tag::Float => (start + 8, f64::from_be_bytes(data[start..start + 8].try_into().unwrap()).into()),
            Tag::Uuid => (start + 16, Uuid::from_slice(&data[start..start + 16]).unwrap().into()),
            Tag::Text => {
                let (slen, offset) = self.parse_varint(start);
                let slen = slen as usize;
                let s = unsafe {
                    std::str::from_utf8_unchecked(&data[start + offset..start + offset + slen])
                };

                (start + slen + offset, s.into())
            }
            Tag::List => {
                let end_pos = start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize;
                let mut start_pos = start + 4;
                let mut collected = vec![];
                while start_pos < end_pos {
                    let (val, new_pos) = self.parse_value_at(start_pos);
                    collected.push(val);
                    start_pos = new_pos;
                }
                (end_pos, collected.into())
            }
            Tag::Dict => {
                let end_pos = start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize;
                let mut start_pos = start + 4;
                let mut collected: BTreeMap<Cow<str>, Value> = BTreeMap::new();
                while start_pos < end_pos {
                    let (slen, offset) = self.parse_varint(start_pos);
                    start_pos += offset;
                    let key = unsafe {
                        std::str::from_utf8_unchecked(&data[start_pos..start_pos + slen as usize])
                    };
                    start_pos += slen as usize;
                    let (val, new_pos) = self.parse_value_at(start_pos);
                    collected.insert(key.into(), val);
                    start_pos = new_pos;
                }
                (end_pos, collected.into())
            }
        };
        (val, nxt)
    }
    pub fn iter(&self) -> TupleIter<T> {
        TupleIter {
            tuple: self,
            pos: 4,
        }
    }
}

pub struct TupleIter<'a, T: AsRef<[u8]>> {
    tuple: &'a Tuple<T>,
    pos: usize,
}

impl<'a, T: AsRef<[u8]>> Iterator for TupleIter<'a, T> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.tuple.data.as_ref().len() {
            return None;
        }
        let (v, pos) = self.tuple.parse_value_at(self.pos);
        self.pos = pos;
        Some(v)
    }
}

impl Tuple<Vec<u8>> {
    #[inline]
    pub fn with_prefix(prefix: u32) -> Self {
        let data = Vec::from(prefix.to_be_bytes());
        Self {
            data,
            idx_cache: RefCell::new(vec![]),
        }
    }
    #[inline]
    fn push_tag(&mut self, tag: Tag) {
        self.data.push(tag as u8);
    }
    #[inline]
    pub fn push_null(&mut self) {
        self.push_tag(Tag::Null);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_bool(&mut self, b: bool) {
        self.push_tag(if b { Tag::BoolTrue } else { Tag::BoolFalse });
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_edge_dir(&mut self, d: EdgeDir) {
        self.push_tag(match d {
            EdgeDir::Fwd => { Tag::FwdEdge }
            EdgeDir::Bwd => { Tag::BwdEdge }
        });
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_int(&mut self, i: i64) {
        self.push_tag(Tag::Int);
        self.push_zigzag(i);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_float(&mut self, f: f64) {
        self.push_tag(Tag::Float);
        self.data.extend(f.to_be_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_uint(&mut self, u: u64) {
        self.push_tag(Tag::UInt);
        self.push_varint(u);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_uuid(&mut self, u: Uuid) {
        self.push_tag(Tag::Uuid);
        self.data.extend(u.as_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_str(&mut self, s: impl AsRef<str>) {
        let s = s.as_ref();
        self.push_tag(Tag::Text);
        self.push_varint(s.len() as u64);
        self.data.extend_from_slice(s.as_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub fn push_value(&mut self, v: &Value) {
        match v {
            Value::Null => self.push_null(),
            Value::Bool(b) => self.push_bool(*b),
            Value::EdgeDir(e) => self.push_edge_dir(*e),
            Value::UInt(u) => self.push_uint(*u),
            Value::Int(i) => self.push_int(*i),
            Value::Float(f) => self.push_float(f.into_inner()),
            Value::Uuid(u) => self.push_uuid(*u),
            Value::Text(t) => self.push_str(t),
            Value::List(l) => {
                self.push_tag(Tag::List);
                let start_pos = self.data.len();
                let start_len = self.idx_cache.borrow().len();
                self.data.extend(0u32.to_be_bytes());
                for val in l {
                    self.push_value(val);
                }
                let length = (self.data.len() - start_pos) as u32;
                let length_bytes = length.to_be_bytes();
                for i in 0..4 {
                    self.data[start_pos + i] = length_bytes[i]
                }
                let mut cache = self.idx_cache.borrow_mut();
                cache.truncate(start_len);
                cache.push(self.data.len());
            }
            Value::Dict(d) => {
                self.push_tag(Tag::Dict);
                let start_pos = self.data.len();
                let start_len = self.idx_cache.borrow().len();
                self.data.extend(0u32.to_be_bytes());
                for (k, v) in d {
                    self.push_varint(k.len() as u64);
                    self.data.extend_from_slice(k.as_bytes());
                    self.push_value(v);
                }
                let length = (self.data.len() - start_pos) as u32;
                let length_bytes = length.to_be_bytes();
                for i in 0..4 {
                    self.data[start_pos + i] = length_bytes[i]
                }
                let mut cache = self.idx_cache.borrow_mut();
                cache.truncate(start_len);
                cache.push(self.data.len());
            }
        }
    }


    #[inline]
    fn push_varint(&mut self, u: u64) {
        let mut u = u;
        while u > 0b01111111 {
            self.data.push(0b10000000 | (u as u8 & 0b01111111));
            u >>= 7;
        }
        self.data.push(u as u8);
    }

    #[inline]
    fn push_zigzag(&mut self, i: i64) {
        let u: u64 = if i >= 0 {
            (i as u64) << 1
        } else {
            // Convoluted, to prevent overflow when calling .abs()
            (((i + 1).abs() as u64) << 1) + 1
        };
        self.push_varint(u);
    }
}

impl<T: AsRef<[u8]>> PartialEq for Tuple<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data.as_ref() == other.data.as_ref()
    }
}

impl<T: AsRef<[u8]>> Eq for Tuple<T> {}


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use super::*;

    #[test]
    fn serde() {
        let mut t = Tuple::with_prefix(0);
        t.push_null();
        t.push_bool(true);
        t.push_bool(false);
        t.push_edge_dir(EdgeDir::Fwd);
        t.push_str("abcdef");
        t.push_edge_dir(EdgeDir::Bwd);
        t.push_value(&vec![
            true.into(),
            1e236.into(),
            "xxyyzz".into(),
        ].into());
        t.push_int(-123345);
        t.push_value(&BTreeMap::from([]).into());
        t.push_uint(12121212);
        t.push_value(&BTreeMap::from([("yzyz".into(), "fifo".into())]).into());
        t.push_float(1e245);
        t.push_bool(false);
        assert!(t.all_cached());
        assert_eq!(t.idx_cache.borrow().len(), 13);
        let ot = t;
        let t = Tuple::new(ot.data.as_slice());
        let t3 = Tuple::new(ot.data.as_slice());
        assert_eq!(Value::from(()), t.get(0).unwrap());
        t3.get_pos(1);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(true), t.get(1).unwrap());
        t3.get_pos(2);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(false), t.get(2).unwrap());
        t3.get_pos(3);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(EdgeDir::Fwd), t.get(3).unwrap());
        t3.get_pos(4);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from("abcdef"), t.get(4).unwrap());
        t3.get_pos(5);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(EdgeDir::Bwd), t.get(5).unwrap());
        t3.get_pos(6);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(Value::from(vec![
            true.into(),
            1e236.into(),
            "xxyyzz".into(),
        ])), t.get(6).unwrap());
        t3.get_pos(7);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(-123345i64), t.get(7).unwrap());
        t3.get_pos(8);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(BTreeMap::new()), t.get(8).unwrap());
        t3.get_pos(9);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(12121212u64), t.get(9).unwrap());
        t3.get_pos(10);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(BTreeMap::from([("yzyz".into(), "fifo".into())])), t.get(10).unwrap());
        t3.get_pos(11);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(1e245), t.get(11).unwrap());
        t3.get_pos(12);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(Value::from(false), t.get(12).unwrap());
        t3.get_pos(13);
        assert_eq!(t.idx_cache.borrow().last(), t3.idx_cache.borrow().last());
        assert_eq!(None, t.get(13));
        assert_eq!(None, t.get(13131));
        let t = Tuple::new(ot.data.as_slice());
        assert_eq!(Value::from(EdgeDir::Bwd), t.get(5).unwrap());
        assert_eq!(Value::from(true), t.get(1).unwrap());
        assert_eq!(Value::from(true), t.get(1).unwrap());
        assert_eq!(Value::from(1e245), t.get(11).unwrap());
        assert_eq!(Value::from(false), t.get(12).unwrap());
        assert_eq!(Value::from(()), t.get(0).unwrap());
        assert_eq!(Value::from(false), t.get(2).unwrap());
        assert_eq!(Value::from(12121212u64), t.get(9).unwrap());
        assert_eq!(Value::from(BTreeMap::new()), t.get(8).unwrap());
        assert_eq!(Value::from(EdgeDir::Fwd), t.get(3).unwrap());
        assert_eq!(Value::from("abcdef"), t.get(4).unwrap());
        assert_eq!(Value::from(Value::from(vec![
            true.into(),
            1e236.into(),
            "xxyyzz".into(),
        ])), t.get(6).unwrap());
        assert_eq!(None, t.get(13));
        assert_eq!(Value::from(-123345i64), t.get(7).unwrap());
        assert_eq!(Value::from(BTreeMap::from([("yzyz".into(), "fifo".into())])), t.get(10).unwrap());
        assert_eq!(None, t.get(13131));

        println!("{:?}", t.iter().collect::<Vec<Value>>());
        for v in t.iter() {
            println!("{}", v);
        }
    }

    /*
    #[test]
    fn lifetime() {
        let v;
        {
            let s : Vec<u8> = vec![];
            let s = s.as_slice();
            let p = Tuple::new(s);
            v = p.get(0);
        }
        println!("{:?}", v);
    }
     */
}