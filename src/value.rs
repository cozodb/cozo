use std::borrow::Cow;
use std::cmp::{min, Ordering};
use std::collections::{BTreeMap};
use std::io::{Write};
use ordered_float::OrderedFloat;
use uuid::Uuid;
use crate::typing::Typing;
use Ordering::{Greater, Less, Equal};


#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueTag {
    NullTag = 0,
    BoolTrueTag = 2,
    BoolFalseTag = 4,
    FwdEdgeTag = 6,
    BwdEdgeTag = 8,
    IntTag = 11,
    FloatTag = 13,
    StringTag = 15,
    UuidTag = 17,
    UIntTag = 21,
    // TimestampTag = 23,
    // DatetimeTag = 25,
    // TimezoneTag = 27,
    // DateTag = 27,
    // TimeTag = 29,
    // DurationTag = 31,
    // BigIntTag = 51,
    // BigDecimalTag = 53,
    // InetTag = 55,
    // CrsTag = 57,
    // BytesTag = 99,
    ListTag = 101,
    DictTag = 103,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EdgeDirKind {
    FwdEdgeDir,
    BwdEdgeDir,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    EdgeDir(EdgeDirKind),
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
        use ValueTag::*;

        let u = self.parse_varint()?;
        match u {
            u if u == NullTag as u64 => Some(NullTag),
            u if u == BoolTrueTag as u64 => Some(BoolTrueTag),
            u if u == BoolFalseTag as u64 => Some(BoolFalseTag),
            u if u == FwdEdgeTag as u64 => Some(FwdEdgeTag),
            u if u == BwdEdgeTag as u64 => Some(BwdEdgeTag),
            u if u == IntTag as u64 => Some(IntTag),
            u if u == FloatTag as u64 => Some(FloatTag),
            u if u == StringTag as u64 => Some(StringTag),
            u if u == UIntTag as u64 => Some(UIntTag),
            u if u == ListTag as u64 => Some(ListTag),
            u if u == DictTag as u64 => Some(DictTag),
            u if u == UuidTag as u64 => Some(UuidTag),
            _ => None
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
            Equal => (),
            x => return x
        }
        match a2.cmp(&b2) {
            Equal => (),
            x => return x
        }
        match a3.cmp(&b3) {
            Equal => (),
            x => return x
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
                x @ (Less | Greater) => return x,
                Equal => ()
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
        use ValueTag::*;
        use Value::*;
        use EdgeDirKind::*;

        match self.parse_value_tag()? {
            NullTag => Some(Null),
            BoolTrueTag => Some(Bool(true)),
            BoolFalseTag => Some(Bool(false)),
            FwdEdgeTag => Some(EdgeDir(FwdEdgeDir)),
            BwdEdgeTag => Some(EdgeDir(BwdEdgeDir)),
            IntTag => Some(Int(self.parse_zigzag()?)),
            FloatTag => Some(Float(self.parse_float()?)),
            StringTag => Some(RefString(self.parse_string()?)),
            UIntTag => Some(UInt(self.parse_varint()?)),
            ListTag => Some(List(Box::new(self.parse_list()?))),
            DictTag => Some(Dict(Box::new(self.parse_dict()?))),
            UuidTag => Some(Uuid(self.parse_uuid()?))
        }
    }
    pub fn compare_value(&mut self, other: &mut Self) -> Ordering {
        use ValueTag::*;

        match (self.parse_value_tag(), other.parse_value_tag()) {
            (None, None) => Equal,
            (None, Some(_)) => Less,
            (Some(_), None) => Greater,
            (Some(type_a), Some(type_b)) => {
                match type_a.cmp(&type_b) {
                    x @ (Less | Greater) => return x,
                    Equal => ()
                }
                match type_a {
                    IntTag => self.compare_zigzag(other),
                    FloatTag => self.compare_float(other),
                    StringTag => self.compare_string(other),
                    UIntTag => self.compare_varint(other),
                    ListTag => self.compare_list(other),
                    DictTag => self.compare_dict(other),
                    UuidTag => self.compare_uuid(other),
                    NullTag | BoolTrueTag | BoolFalseTag | FwdEdgeTag | BwdEdgeTag => Equal
                }
            }
        }
    }
    pub fn compare_list(&mut self, other: &mut Self) -> Ordering {
        let len_a = self.parse_varint().unwrap();
        let len_b = self.parse_varint().unwrap();
        for _ in 0..min(len_a, len_b) {
            match self.compare_value(other) {
                x @ (Less | Greater) => return x,
                Equal => ()
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
                x @ (Less | Greater) => return x,
                Equal => ()
            }
            match self.compare_value(other) {
                x @ (Less | Greater) => return x,
                Equal => ()
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
        use ValueTag::*;

        match v {
            Value::Null => self.build_tag(NullTag),
            Value::Bool(b) => self.build_tag(if *b { BoolTrueTag } else { BoolFalseTag }),
            Value::EdgeDir(e) => self.build_tag(match e {
                EdgeDirKind::FwdEdgeDir => { FwdEdgeTag }
                EdgeDirKind::BwdEdgeDir => { BwdEdgeTag }
            }),
            Value::UInt(u) => {
                self.build_tag(UIntTag);
                self.build_varint(*u);
            }
            Value::Int(i) => {
                self.build_tag(IntTag);
                self.build_zigzag(*i);
            }
            Value::Float(f) => {
                self.build_tag(FloatTag);
                self.build_float(*f);
            }
            Value::OwnString(s) => {
                self.build_tag(StringTag);
                self.build_string(s);
            }
            Value::RefString(s) => {
                self.build_tag(StringTag);
                self.build_string(s);
            }
            Value::List(l) => {
                self.build_tag(ListTag);
                self.build_list(l);
            }
            Value::Dict(d) => {
                self.build_tag(DictTag);
                self.build_dict(d);
            }
            Value::Uuid(u) => {
                self.build_tag(UuidTag);
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
        x @ (Less | Greater) => return x,
        Equal => ()
    }
    cmp_data(pa, pb)
}

pub fn cmp_data<'a>(pa: &mut ByteArrayParser<'a>, pb: &mut ByteArrayParser<'a>) -> Ordering {
    loop {
        match (pa.at_end(), pb.at_end()) {
            (true, true) => return Equal,
            (true, false) => return Less,
            (false, true) => return Greater,
            (false, false) => ()
        }
        match pa.compare_value(pb) {
            x @ (Less | Greater) => return x,
            Equal => ()
        }
    }
}


impl<'a> Value<'a> {
    pub fn into_owned(self) -> Value<'static> {
        use Value::*;

        match self {
            Null => Null,
            Bool(b) => Bool(b),
            EdgeDir(dir) => EdgeDir(dir),
            UInt(u) => UInt(u),
            Int(i) => Int(i),
            Float(f) => Float(f),
            RefString(s) => OwnString(Box::new(s.to_string())),
            OwnString(s) => OwnString(s),
            List(l) => {
                let mut inner = Vec::with_capacity(l.len());

                for el in *l {
                    inner.push(el.into_owned())
                }
                List(Box::new(inner))
            }
            Dict(d) => {
                let mut inner = BTreeMap::new();
                for (k, v) in *d {
                    let new_k = Cow::from(k.into_owned());
                    inner.insert(new_k, v.into_owned());
                }
                Dict(Box::new(inner))
            }
            Uuid(u) => Uuid(u),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CoercionError<'a> {
    pub msg: String,
    pub val: Value<'a>,
}

impl Typing {
    pub fn coerce<'a>(&self, v: Value<'a>) -> Result<Value<'a>, CoercionError<'a>> {
        // TODO
        Ok(v)
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
