use std::borrow::Cow;
use std::cmp::{min, Ordering};
use std::collections::{BTreeMap};
use std::io::{Write};
use ordered_float::OrderedFloat;
use uuid::Uuid;

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
    Uuid = 17,
    UInt = 21,
    // Timestamp = 23,
    // Datetime = 25,
    // Timezone = 27,
    // Date = 27,
    // Time = 29,
    // Duration = 31,
    // BigInt = 51,
    // BigDecimal = 53,
    // Inet = 55,
    // Crs = 57,
    // Bytes = 99,
    List = 101,
    Dict = 103,
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
    Uuid(Uuid),
    RefString(&'a str),
    OwnString(Box<String>),
    List(Box<Vec<Value<'a>>>),
    Dict(Box<BTreeMap<Cow<'a, str>, Value<'a>>>),
}

pub struct ByteArrayParser<'a> {
    bytes: &'a [u8],
    current: usize,
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

    pub fn parse_value_tag(&mut self) -> Option<ValueTag> {
        let u = self.parse_varint()?;
        match u {
            u if u == ValueTag::Null as u64 => Some(ValueTag::Null),
            u if u == ValueTag::BoolTrue as u64 => Some(ValueTag::BoolTrue),
            u if u == ValueTag::BoolFalse as u64 => Some(ValueTag::BoolFalse),
            u if u == ValueTag::FwdEdge as u64 => Some(ValueTag::FwdEdge),
            u if u == ValueTag::BwdEdge as u64 => Some(ValueTag::BwdEdge),
            u if u == ValueTag::Int as u64 => Some(ValueTag::Int),
            u if u == ValueTag::Float as u64 => Some(ValueTag::Float),
            u if u == ValueTag::String as u64 => Some(ValueTag::String),
            u if u == ValueTag::UInt as u64 => Some(ValueTag::UInt),
            u if u == ValueTag::List as u64 => Some(ValueTag::List),
            u if u == ValueTag::Dict as u64 => Some(ValueTag::Dict),
            u if u == ValueTag::Uuid as u64 => Some(ValueTag::Uuid),
            _ => {
                None
            }
        }
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
        let buf = self.advance(8)?.try_into().ok()?;
        Some(f64::from_be_bytes(buf))
    }
    pub fn parse_uuid(&mut self) -> Option<Uuid> {
        Uuid::from_slice(self.advance(16)?).ok()
    }
    pub fn compare_float(&mut self, other: &mut Self) -> Ordering {
        OrderedFloat(self.parse_float().unwrap()).cmp(&OrderedFloat(other.parse_float().unwrap()))
    }
    // This should first compare UUID version, then for V1, compare the timestamps
    pub fn compare_uuid(&mut self, other: &mut Self) -> Ordering {
        let ua = self.parse_uuid().unwrap();
        let ub = other.parse_uuid().unwrap();
        let (a3, a2, a1, a4) = ua.as_fields();
        let (b3, b2, b1, b4) = ub.as_fields();
        match a1.cmp(&b1) {
            Ordering::Equal => {}
            x => { return x; }
        }
        match a2.cmp(&b2) {
            Ordering::Equal => {}
            x => { return x; }
        }
        match a3.cmp(&b3) {
            Ordering::Equal => {}
            x => { return x; }
        }
        a4.cmp(b4)
    }
    pub fn parse_string(&mut self) -> Option<&'a str> {
        let l = self.parse_varint()?;
        let bytes = self.advance(l as usize)?;
        // unsafe {
        //     Some(std::str::from_utf8_unchecked(bytes))
        // }
        std::str::from_utf8(bytes).ok()
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
        match self.parse_value_tag()? {
            ValueTag::Null => { Some(Value::Null) }
            ValueTag::BoolTrue => { Some(Value::Bool(true)) }
            ValueTag::BoolFalse => { Some(Value::Bool(false)) }
            ValueTag::FwdEdge => { Some(Value::EdgeDir(EdgeDir::FwdEdge)) }
            ValueTag::BwdEdge => { Some(Value::EdgeDir(EdgeDir::BwdEdge)) }
            ValueTag::Int => { Some(Value::Int(self.parse_zigzag()?)) }
            ValueTag::Float => { Some(Value::Float(self.parse_float()?)) }
            ValueTag::String => { Some(Value::RefString(self.parse_string()?)) }
            ValueTag::UInt => { Some(Value::UInt(self.parse_varint()?)) }
            ValueTag::List => { Some(Value::List(Box::new(self.parse_list()?))) }
            ValueTag::Dict => { Some(Value::Dict(Box::new(self.parse_dict()?))) }
            ValueTag::Uuid => { Some(Value::Uuid(self.parse_uuid()?)) }
        }
    }
    pub fn compare_value(&mut self, other: &mut Self) -> Ordering {
        match (self.parse_value_tag(), other.parse_value_tag()) {
            (None, None) => { Ordering::Equal }
            (None, Some(_)) => { Ordering::Less }
            (Some(_), None) => { Ordering::Greater }
            (Some(type_a), Some(type_b)) => {
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
                    ValueTag::Uuid => { self.compare_uuid(other) }
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
    pub fn parse_dict(&mut self) -> Option<BTreeMap<Cow<'a, str>, Value<'a>>> {
        let l = self.parse_varint()?;
        let mut ret = BTreeMap::new();

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

pub struct ByteArrayBuilder<T: Write> {
    byte_writer: T,
}

impl<T: Write> ByteArrayBuilder<T> {
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
    pub fn build_uuid(&mut self, u: Uuid) {
        self.byte_writer.write_all(u.as_bytes()).unwrap();
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
            Value::OwnString(s) => {
                self.build_tag(ValueTag::String);
                self.build_string(s);
            }
            Value::RefString(s) => {
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
            Value::Uuid(u) => {
                self.build_tag(ValueTag::Uuid);
                self.build_uuid(*u);
            }
        }
    }
    pub fn build_list(&mut self, l: &[Value]) {
        self.build_varint(l.len() as u64);
        for el in l {
            self.build_value(el);
        }
    }
    pub fn build_dict(&mut self, d: &BTreeMap<Cow<str>, Value>) {
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
            Value::RefString(s) => {
                Value::OwnString(Box::new(s.to_string()))
            }
            Value::OwnString(s) => {
                Value::OwnString(s)
            }
            Value::List(l) => {
                let mut inner = Vec::with_capacity(l.len());

                for el in *l {
                    inner.push(el.into_owned())
                }
                Value::List(Box::new(inner))
            }
            Value::Dict(d) => {
                let mut inner = BTreeMap::new();
                for (k, v) in *d {
                    let new_k = Cow::from(k.into_owned());
                    inner.insert(new_k, v.into_owned());
                }
                Value::Dict(Box::new(inner))
            }
            Value::Uuid(u) => {
                Value::Uuid(u)
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


    #[test]
    fn size() {
        println!("{:?}", std::mem::size_of::<Value>());
        println!("{:?}", std::mem::size_of::<i64>());
        println!("{:?}", std::mem::size_of::<Uuid>());
        println!("{:?}", std::mem::size_of::<BTreeMap<Cow<str>, Value>>());
        println!("{:?}", std::mem::size_of::<Vec<Value>>());
        println!("{:?}", std::mem::size_of::<Cow<str>>());
        println!("{:?}", std::mem::size_of::<Box<Cow<str>>>());
        println!("{:?}", std::mem::size_of::<Box<Vec<Value>>>());
        println!("{:?}", std::mem::size_of::<String>());
        println!("{:?}", std::mem::size_of::<&str>());
    }
}
