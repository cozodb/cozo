use std::borrow::Cow;
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::io::{Write};
use ordered_float::OrderedFloat;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueTag {
    Null = 0,
    BoolTrue = 2,
    BoolFalse = 4,
    FwdEdge = 6,
    BwdEdge = 8,
    Int = 11,
    Float = 13,
    String = 15,
    UInt = 21,
    List = 101,
    Dict = 103,
}

impl From<u64> for ValueTag {
    fn from(u: u64) -> Self {
        match u {
            u if u == ValueTag::Null as u64 => ValueTag::Null,
            u if u == ValueTag::BoolTrue as u64 => ValueTag::BoolTrue,
            u if u == ValueTag::BoolFalse as u64 => ValueTag::BoolFalse,
            u if u == ValueTag::FwdEdge as u64 => ValueTag::FwdEdge,
            u if u == ValueTag::BwdEdge as u64 => ValueTag::BwdEdge,
            u if u == ValueTag::Int as u64 => ValueTag::Int,
            u if u == ValueTag::Float as u64 => ValueTag::Float,
            u if u == ValueTag::String as u64 => ValueTag::String,
            u if u == ValueTag::UInt as u64 => ValueTag::UInt,
            u if u == ValueTag::List as u64 => ValueTag::List,
            u if u == ValueTag::Dict as u64 => ValueTag::Dict,
            _ => {
                panic!()
            }
        }
    }
}


#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EdgeDir {
    FwdEdge,
    BwdEdge,
}

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    EdgeDir(EdgeDir),
    UInt(u64),
    Int(i64),
    Float(f64),
    String(Cow<'a, str>),
    List(Vec<Value<'a>>),
    Dict(HashMap<Cow<'a, str>, Value<'a>>),
}

pub struct ByteArrayParser<'a> {
    bytes: &'a [u8],
    current: usize,
}

pub struct ByteArrayBuilder<T: Write> {
    byte_writer: T,
}

impl<'a> ByteArrayParser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, current: 0 }
    }

    fn advance(&mut self, n: usize) -> Option<&'a [u8]> {
        let cur = self.current;
        if n + cur > self.bytes.len() {
            None
        } else {
            self.current += n;
            Some(&self.bytes[cur..cur + n])
        }
    }
    fn at_end(&self) -> bool {
        self.current == self.bytes.len()
    }
    pub fn parse_varint(&mut self) -> Option<u64> {
        let mut u: u64 = 0;
        let mut shift = 0;
        loop {
            let buf = self.advance(1)?[0];
            u |= ((buf & 0b01111111) as u64) << shift;
            if buf & 0b10000000 == 0 {
                break;
            }
            shift += 7;
        }
        Some(u)
    }

    pub fn compare_varint(&mut self, other: &mut Self) -> Ordering {
        self.parse_varint().unwrap().cmp(&other.parse_varint().unwrap())
    }
    pub fn parse_zigzag(&mut self) -> Option<i64> {
        let u = self.parse_varint()?;
        Some(if u & 1 == 0 {
            (u >> 1) as i64
        } else {
            -((u >> 1) as i64) - 1
        })
    }
    pub fn compare_zigzag(&mut self, other: &mut Self) -> Ordering {
        self.parse_zigzag().unwrap().cmp(&other.parse_zigzag().unwrap())
    }
    pub fn parse_float(&mut self) -> Option<f64> {
        let buf = self.advance(8)?;
        let buf: [u8; 8] = buf.try_into().unwrap();
        Some(f64::from_be_bytes(buf))
    }
    pub fn compare_float(&mut self, other: &mut Self) -> Ordering {
        OrderedFloat(self.parse_float().unwrap()).cmp(&OrderedFloat(other.parse_float().unwrap()))
    }
    pub fn parse_string(&mut self) -> Option<&'a str> {
        let l = self.parse_varint()?;
        let bytes = self.advance(l as usize)?;
        unsafe {
            Some(std::str::from_utf8_unchecked(bytes))
        }
    }
    pub fn compare_string(&mut self, other: &mut Self) -> Ordering {
        let len_a = self.parse_varint().unwrap();
        let len_b = self.parse_varint().unwrap();
        for _ in 0..min(len_a, len_b) {
            let byte_a = self.advance(1).unwrap()[0];
            let byte_b = other.advance(1).unwrap()[0];
            match byte_a.cmp(&byte_b) {
                Ordering::Less => { return Ordering::Less; }
                Ordering::Greater => { return Ordering::Greater; }
                Ordering::Equal => {}
            }
        }
        len_a.cmp(&len_b)
    }
    pub fn parse_list(&mut self) -> Option<Vec<Value<'a>>> {
        let l = self.parse_varint()?;
        let mut ret = Vec::with_capacity(l as usize);
        for _ in 0..l {
            let val = self.parse_value()?;
            ret.push(val);
        }
        Some(ret)
    }
    pub fn parse_value(&mut self) -> Option<Value<'a>> {
        let tag_id = self.parse_varint()?;
        match ValueTag::from(tag_id) {
            ValueTag::Null => {
                Some(Value::Null)
            }
            ValueTag::BoolTrue => {
                Some(Value::Bool(true))
            }
            ValueTag::BoolFalse => {
                Some(Value::Bool(false))
            }
            ValueTag::FwdEdge => {
                Some(Value::EdgeDir(EdgeDir::FwdEdge))
            }
            ValueTag::BwdEdge => {
                Some(Value::EdgeDir(EdgeDir::BwdEdge))
            }
            ValueTag::Int => {
                Some(Value::Int(self.parse_zigzag()?))
            }
            ValueTag::Float => {
                Some(Value::Float(self.parse_float()?))
            }
            ValueTag::String => {
                Some(Value::String(Cow::from(self.parse_string()?)))
            }
            ValueTag::UInt => {
                Some(Value::UInt(self.parse_varint()?))
            }
            ValueTag::List => {
                Some(Value::List(self.parse_list()?))
            }
            ValueTag::Dict => {
                Some(Value::Dict(self.parse_dict()?))
            }
        }
    }
    pub fn compare_value(&mut self, other: &mut Self) -> Ordering {
        match (self.parse_varint(), other.parse_varint()) {
            (None, None) => { return Ordering::Equal; }
            (None, Some(_)) => { return Ordering::Less; }
            (Some(_), None) => { return Ordering::Greater; }
            (Some(ta), Some(tb)) => {
                let type_a = ValueTag::from(ta);
                let type_b = ValueTag::from(tb);
                match type_a.cmp(&type_b) {
                    Ordering::Less => { return Ordering::Less; }
                    Ordering::Greater => { return Ordering::Greater; }
                    Ordering::Equal => {}
                }
                match type_a {
                    ValueTag::Int => { self.compare_zigzag(other) }
                    ValueTag::Float => { self.compare_float(other) }
                    ValueTag::String => { self.compare_string(other) }
                    ValueTag::UInt => { self.compare_varint(other) }
                    ValueTag::List => { self.compare_list(other) }
                    ValueTag::Dict => { self.compare_dict(other) }
                    ValueTag::Null => { Ordering::Equal }
                    ValueTag::BoolTrue => { Ordering::Equal }
                    ValueTag::BoolFalse => { Ordering::Equal }
                    ValueTag::FwdEdge => { Ordering::Equal }
                    ValueTag::BwdEdge => { Ordering::Equal }
                }
            }
        }
    }
    pub fn compare_list(&mut self, other: &mut Self) -> Ordering {
        let len_a = self.parse_varint().unwrap();
        let len_b = self.parse_varint().unwrap();
        for _ in 0..min(len_a, len_b) {
            match self.compare_value(other) {
                Ordering::Less => { return Ordering::Less; }
                Ordering::Greater => { return Ordering::Greater; }
                Ordering::Equal => {}
            }
        }
        len_a.cmp(&len_b)
    }
    pub fn parse_dict(&mut self) -> Option<HashMap<Cow<'a, str>, Value<'a>>> {
        let l = self.parse_varint()?;
        let mut ret = HashMap::with_capacity(l as usize);

        for _ in 0..l {
            let key = Cow::from(self.parse_string()?);
            let val = self.parse_value()?;
            ret.insert(key, val);
        }
        Some(ret)
    }
    pub fn compare_dict(&mut self, other: &mut Self) -> Ordering {
        let len_a = self.parse_varint().unwrap();
        let len_b = self.parse_varint().unwrap();
        for _ in 0..min(len_a, len_b) {
            match self.compare_string(other) {
                Ordering::Less => { return Ordering::Less; }
                Ordering::Greater => { return Ordering::Greater; }
                Ordering::Equal => {}
            }
            match self.compare_value(other) {
                Ordering::Less => { return Ordering::Less; }
                Ordering::Greater => { return Ordering::Greater; }
                Ordering::Equal => {}
            }
        }
        len_a.cmp(&len_b)
    }
}

impl <T:Write> ByteArrayBuilder<T> {
    pub fn new(byte_writer: T) -> Self {
        Self { byte_writer }
    }
    pub fn build_varint(&mut self, u: u64) {
        let mut u = u;
        while u > 0b01111111 {
            self.byte_writer.write_all(&[0b10000000 | (u as u8 & 0b01111111)]).unwrap();
            u >>= 7;
        }
        self.byte_writer.write_all(&[u as u8]).unwrap();
    }
    pub fn build_zigzag(&mut self, i: i64) {
        let u: u64 = if i >= 0 {
            (i as u64) << 1
        } else {
            // Convoluted, to prevent overflow when calling .abs()
            (((i + 1).abs() as u64) << 1) + 1
        };
        self.build_varint(u);
    }
    pub fn build_float(&mut self, f: f64) {
        self.byte_writer.write_all(&f.to_be_bytes()).unwrap();
    }
    pub fn build_string(&mut self, s: &str) {
        self.build_varint(s.len() as u64);
        self.byte_writer.write_all(s.as_bytes()).unwrap();
    }
    pub fn build_tag(&mut self, t: ValueTag) {
        self.byte_writer.write_all(&[t as u8]).unwrap();
    }
    pub fn build_value(&mut self, v: &Value) {
        match v {
            Value::Null => {
                self.build_tag(ValueTag::Null)
            }
            Value::Bool(b) => {
                self.build_tag(if *b { ValueTag::BoolTrue } else { ValueTag::BoolFalse })
            }
            Value::EdgeDir(e) => {
                self.build_tag(match e {
                    EdgeDir::FwdEdge => { ValueTag::FwdEdge }
                    EdgeDir::BwdEdge => { ValueTag::BwdEdge }
                })
            }
            Value::UInt(u) => {
                self.build_tag(ValueTag::UInt);
                self.build_varint(*u);
            }
            Value::Int(i) => {
                self.build_tag(ValueTag::Int);
                self.build_zigzag(*i);
            }
            Value::Float(f) => {
                self.build_tag(ValueTag::Float);
                self.build_float(*f);
            }
            Value::String(s) => {
                self.build_tag(ValueTag::String);
                self.build_string(s);
            }
            Value::List(l) => {
                self.build_tag(ValueTag::List);
                self.build_list(l);
            }
            Value::Dict(d) => {
                self.build_tag(ValueTag::Dict);
                self.build_dict(d);
            }
        }
    }
    pub fn build_list(&mut self, l: &[Value]) {
        self.build_varint(l.len() as u64);
        for el in l {
            self.build_value(el);
        }
    }
    pub fn build_dict(&mut self, d: &HashMap<Cow<str>, Value>) {
        self.build_varint(d.len() as u64);
        for (k, v) in d {
            self.build_string(k);
            self.build_value(v);
        }
    }
}

pub fn cmp_keys<'a>(pa: &mut ByteArrayParser<'a>, pb: &mut ByteArrayParser<'a>) -> Ordering {
    match pa.compare_varint(pb) {
        Ordering::Less => { return Ordering::Less; }
        Ordering::Greater => { return Ordering::Greater; }
        Ordering::Equal => {}
    }
    cmp_data(pa, pb)
}

pub fn cmp_data<'a>(pa: &mut ByteArrayParser<'a>, pb: &mut ByteArrayParser<'a>) -> Ordering {
    loop {
        match (pa.at_end(), pb.at_end()) {
            (true, true) => { return Ordering::Equal; }
            (true, false) => { return Ordering::Less; }
            (false, true) => { return Ordering::Greater; }
            (false, false) => {}
        }
        match pa.compare_value(pb) {
            Ordering::Less => { return Ordering::Less; }
            Ordering::Greater => { return Ordering::Greater; }
            Ordering::Equal => {}
        }
    }
}


impl<'a> Value<'a> {
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Null => {
                Value::Null
            }
            Value::Bool(b) => {
                Value::Bool(b)
            }
            Value::EdgeDir(dir) => {
                Value::EdgeDir(dir)
            }
            Value::UInt(u) => {
                Value::UInt(u)
            }
            Value::Int(i) => {
                Value::Int(i)
            }
            Value::Float(f) => {
                Value::Float(f)
            }
            Value::String(s) => {
                Value::String(Cow::from(s.into_owned()))
            }
            Value::List(l) => {
                let mut inner = Vec::with_capacity(l.len());

                for el in l {
                    inner.push(el.into_owned())
                }
                Value::List(inner)
            }
            Value::Dict(d) => {
                let mut inner = HashMap::with_capacity(d.len());
                for (k, v) in d {
                    let new_k = Cow::from(k.into_owned());
                    inner.insert(new_k, v.into_owned());
                }
                Value::Dict(inner)
            }
        }
    }
}


pub struct CozoKey<'a> {
    pub table_id: u64,
    pub values: Vec<Value<'a>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint() {
        for u in 126..(2u64).pow(9) {
            let mut x = vec![];
            let mut builder = ByteArrayBuilder::new(&mut x);
            builder.build_varint(u);
            let mut parser = ByteArrayParser::new(&x);
            let u2 = parser.parse_varint().unwrap();
            assert_eq!(u, u2);
        }

        let u = u64::MIN;
        let mut x = vec![];
        let mut builder = ByteArrayBuilder::new(&mut x);
        builder.build_varint(u);
        let mut parser = ByteArrayParser::new(&x);
        let u2 = parser.parse_varint().unwrap();
        assert_eq!(u, u2);

        let u = u64::MAX;
        let mut x = vec![];
        let mut builder = ByteArrayBuilder::new(&mut x);
        builder.build_varint(u);
        let mut parser = ByteArrayParser::new(&x);
        let u2 = parser.parse_varint().unwrap();
        assert_eq!(u, u2);
    }

    #[test]
    fn zigzag() {
        for i in 126..(2i64).pow(9) {
            let mut x = vec![];
            let mut builder = ByteArrayBuilder::new(&mut x);
            builder.build_zigzag(i);
            let mut parser = ByteArrayParser::new(&x);
            let i2 = parser.parse_zigzag().unwrap();
            assert_eq!(i, i2);
        }
        for i in 126..(2i64).pow(9) {
            let i = -i;
            let mut x = vec![];
            let mut builder = ByteArrayBuilder::new(&mut x);
            builder.build_zigzag(i);
            let mut parser = ByteArrayParser::new(&x);
            let i2 = parser.parse_zigzag().unwrap();
            assert_eq!(i, i2);
        }

        let i = i64::MIN;
        let mut x = vec![];
        let mut builder = ByteArrayBuilder::new(&mut x);
        builder.build_zigzag(i);
        let mut parser = ByteArrayParser::new(&x);
        let i2 = parser.parse_zigzag().unwrap();
        assert_eq!(i, i2);

        let i = i64::MAX;
        let mut x = vec![];
        let mut builder = ByteArrayBuilder::new(&mut x);
        builder.build_zigzag(i);
        let mut parser = ByteArrayParser::new(&x);
        let i2 = parser.parse_zigzag().unwrap();
        assert_eq!(i, i2);
    }
}
