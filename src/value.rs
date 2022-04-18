use std::borrow::{Cow};
use std::cmp::{min, Ordering};
use std::collections::{BTreeMap};
use std::io::{Write};
use ordered_float::OrderedFloat;
use uuid::Uuid;
use crate::typing::{Typing};
use Ordering::{Greater, Less, Equal};
use std::sync::Arc;

// TODO: array types, alignment of values
#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueTag {
    BoolFalseTag = 0,
    NullTag = 2,
    BoolTrueTag = 4,
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
    // BitArrTag = 60,
    // U8ArrTag = 61,
    // I8ArrTag = 62,
    // U16ArrTag = 63,
    // I16ArrTag = 64,
    // U32ArrTag = 65,
    // I32ArrTag = 66,
    // U64ArrTag = 67,
    // I64ArrTag = 68,
    // F16ArrTag = 69,
    // F32ArrTag = 70,
    // F64ArrTag = 71,
    // C32ArrTag = 72,
    // C64ArrTag = 73,
    // C128ArrTag = 74,
    ListTag = 101,
    DictTag = 103,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EdgeDirKind {
    FwdEdgeDir,
    BwdEdgeDir,
}

#[derive(Debug, Clone)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    EdgeDir(EdgeDirKind),
    UInt(u64),
    Int(i64),
    Float(f64),
    Uuid(Uuid),
    RefString(&'a str),
    OwnString(Arc<String>),
    List(Arc<Vec<Value<'a>>>),
    Dict(Arc<BTreeMap<Cow<'a, str>, Value<'a>>>),
}

pub type StaticValue = Value<'static>;

impl<'a> Value<'a> {
    pub fn get_list(&self) -> Option<Arc<Vec<Self>>> {
        match self {
            Value::List(v) => Some(v.clone()),
            _ => None
        }
    }
    pub fn get_string(&self) -> Option<Arc<String>> {
        match self {
            Value::OwnString(v) => Some(v.clone()),
            Value::RefString(v) => Some(Arc::new(v.to_string())),
            _ => None
        }
    }
}

impl<'a> PartialEq for Value<'a> {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;

        match (self, other) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (EdgeDir(a), EdgeDir(b)) => a == b,
            (UInt(a), UInt(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            (RefString(a), RefString(b)) => a == b,
            (RefString(a), OwnString(b)) => *a == **b,
            (OwnString(a), RefString(b)) => **a == *b,
            (OwnString(a), OwnString(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Dict(a), Dict(b)) => a == b,
            _ => false
        }
    }
}

pub struct ByteArrayParser<'a> {
    bytes: &'a [u8],
    current: usize,
}

impl<'a> ByteArrayParser<'a> {
    pub fn new<T: AsRef<[u8]>>(source: &'a T) -> Self {
        Self { bytes: source.as_ref(), current: 0 }
    }

    #[inline]
    fn advance(&mut self, n: usize) -> Option<&'a [u8]> {
        let cur = self.current;
        if n + cur > self.bytes.len() {
            None
        } else {
            self.current += n;
            Some(&self.bytes[cur..cur + n])
        }
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.current == self.bytes.len()
    }

    #[inline]
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

    #[inline]
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

    #[inline]
    pub fn compare_varint(&mut self, other: &mut Self) -> Ordering {
        self.parse_varint().expect(
            "Failed to parse VarInt when comparing"
        ).cmp(&other.parse_varint().expect(
            "Failed to parse VarInt when comparing"
        ))
    }

    #[inline]
    pub fn parse_zigzag(&mut self) -> Option<i64> {
        let u = self.parse_varint()?;
        Some(if u & 1 == 0 {
            (u >> 1) as i64
        } else {
            -((u >> 1) as i64) - 1
        })
    }

    #[inline]
    pub fn compare_zigzag(&mut self, other: &mut Self) -> Ordering {
        self.parse_zigzag().expect(
            "Failed to parse ZigZag when comparing"
        ).cmp(&other.parse_zigzag().expect(
            "Failed to parse ZigZag when comparing"
        ))
    }

    #[inline]
    pub fn parse_float(&mut self) -> Option<f64> {
        let buf = self.advance(8)?.try_into().ok()?;
        Some(f64::from_be_bytes(buf))
    }

    #[inline]
    pub fn parse_uuid(&mut self) -> Option<Uuid> {
        Uuid::from_slice(self.advance(16)?).ok()
    }

    #[inline]
    pub fn compare_float(&mut self, other: &mut Self) -> Ordering {
        OrderedFloat(self.parse_float().expect(
            "Failed to parse Float when comparing"
        )).cmp(&OrderedFloat(other.parse_float().expect(
            "Failed to parse Float when comparing"
        )))
    }
    // This should first compare UUID version, then for V1, compare the timestamps
    #[inline]
    pub fn compare_uuid(&mut self, other: &mut Self) -> Ordering {
        let ua = self.parse_uuid().expect(
            "Failed to parse Uuid when comparing"
        );
        let (a3, a2, a1, a4) = ua.as_fields();
        let ub = other.parse_uuid().expect(
            "Failed to parse Uuid when comparing"
        );
        let (b3, b2, b1, b4) = ub.as_fields();
        if let x @ (Greater | Less) = a1.cmp(&b1) { return x; }
        if let x @ (Greater | Less) = a2.cmp(&b2) { return x; }
        if let x @ (Greater | Less) = a3.cmp(&b3) { return x; }
        a4.cmp(b4)
    }

    #[inline]
    pub fn parse_string(&mut self) -> Option<&'a str> {
        let l = self.parse_varint()?;
        let bytes = self.advance(l as usize)?;
        // unsafe {
        //     Some(std::str::from_utf8_unchecked(bytes))
        // }
        std::str::from_utf8(bytes).ok()
    }

    #[inline]
    pub fn compare_string(&mut self, other: &mut Self) -> Ordering {
        let len_a = self.parse_varint().expect("Failed to get String length when comparing");
        let len_b = other.parse_varint().expect("Failed to get String length when comparing");
        for _ in 0..min(len_a, len_b) {
            let byte_a = self.advance(1).expect("Unexpected end of String when comparing")[0];
            let byte_b = other.advance(1).expect("Unexpected end of String when comparing")[0];
            if let x @ (Greater | Less) = byte_a.cmp(&byte_b) { return x; }
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
            ListTag => Some(List(Arc::new(self.parse_list()?))),
            DictTag => Some(Dict(Arc::new(self.parse_dict()?))),
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
                if let x @ (Greater | Less) = type_a.cmp(&type_b) { return x; }
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
        let len_a = self.parse_varint().expect("Failed to get List length when comparing");
        let len_b = other.parse_varint().expect("Failed to get List length when comparing");
        for _ in 0..min(len_a, len_b) {
            if let x @ (Greater | Less) = self.compare_value(other) { return x; }
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
        let len_a = self.parse_varint().expect("Failed to get Dict length when comparing");
        let len_b = other.parse_varint().expect("Failed to get Dict length when comparing");
        for _ in 0..min(len_a, len_b) {
            if let x @ (Greater | Less) = self.compare_string(other) { return x; }
            if let x @ (Greater | Less) = self.compare_value(other) { return x; }
        }
        len_a.cmp(&len_b)
    }
}

pub struct ByteArrayBuilder<T: Write + AsRef<[u8]>> {
    byte_writer: T,
}

impl<T: Write + AsRef<[u8]>> AsRef<[u8]> for ByteArrayBuilder<T> {
    fn as_ref(&self) -> &[u8] {
        self.byte_writer.as_ref()
    }
}

impl ByteArrayBuilder<Vec<u8>> {
    pub fn default() -> Self { Self { byte_writer: vec![] } }
    pub fn with_capacity(size: usize) -> Self {
        Self::new(Vec::with_capacity(size))
    }
}

impl<T: Write + AsRef<[u8]>> ByteArrayBuilder<T> {
    pub fn get(self) -> T {
        self.byte_writer
    }

    pub fn new(byte_writer: T) -> Self {
        Self { byte_writer }
    }

    #[inline]
    pub fn build_varint(&mut self, u: u64) -> &mut Self {
        let mut u = u;
        while u > 0b01111111 {
            self.byte_writer.write_all(&[0b10000000 | (u as u8 & 0b01111111)]).expect(
                "Failed to write when building VarInt"
            );
            u >>= 7;
        }
        self.byte_writer.write_all(&[u as u8]).expect(
            "Failed to write when building Varint"
        );
        self
    }

    #[inline]
    pub fn build_zigzag(&mut self, i: i64) -> &mut Self {
        let u: u64 = if i >= 0 {
            (i as u64) << 1
        } else {
            // Convoluted, to prevent overflow when calling .abs()
            (((i + 1).abs() as u64) << 1) + 1
        };
        self.build_varint(u);
        self
    }

    #[inline]
    pub fn build_float(&mut self, f: f64) -> &mut Self {
        self.byte_writer.write_all(&f.to_be_bytes()).expect(
            "Failed to write when building Float"
        );
        self
    }

    #[inline]
    pub fn build_uuid(&mut self, u: Uuid) -> &mut Self {
        self.byte_writer.write_all(u.as_bytes()).expect(
            "Failed to write when building Uuid"
        );
        self
    }

    #[inline]
    pub fn build_string(&mut self, s: &str) -> &mut Self {
        self.build_varint(s.len() as u64);
        self.byte_writer.write_all(s.as_bytes()).expect("Failed to write when building String");
        self
    }

    #[inline]
    pub fn build_tag(&mut self, t: ValueTag) -> &mut Self {
        self.byte_writer.write_all(&[t as u8]).expect("Failed to write when building Tag");
        self
    }

    pub fn build_value(&mut self, v: &Value) -> &mut Self {
        use ValueTag::*;

        match v {
            Value::Null => self.build_tag(NullTag),
            Value::Bool(b) => self.build_tag(if *b { BoolTrueTag } else { BoolFalseTag }),
            Value::EdgeDir(e) => self.build_tag(match e {
                EdgeDirKind::FwdEdgeDir => { FwdEdgeTag }
                EdgeDirKind::BwdEdgeDir => { BwdEdgeTag }
            }),
            Value::UInt(u) => {
                self.build_tag(UIntTag).build_varint(*u)
            }
            Value::Int(i) => {
                self.build_tag(IntTag).build_zigzag(*i)
            }
            Value::Float(f) => {
                self.build_tag(FloatTag).build_float(*f)
            }
            Value::OwnString(s) => {
                self.build_tag(StringTag).build_string(s)
            }
            Value::RefString(s) => {
                self.build_tag(StringTag).build_string(s)
            }
            Value::List(l) => {
                self.build_tag(ListTag).build_list(l)
            }
            Value::Dict(d) => {
                self.build_tag(DictTag).build_dict(d)
            }
            Value::Uuid(u) => {
                self.build_tag(UuidTag).build_uuid(*u)
            }
        }
    }

    pub fn build_list(&mut self, l: &[Value]) -> &mut Self {
        self.build_varint(l.len() as u64);
        for el in l {
            self.build_value(el);
        }
        self
    }

    pub fn build_dict(&mut self, d: &BTreeMap<Cow<str>, Value>) -> &mut Self {
        self.build_varint(d.len() as u64);
        for (k, v) in d {
            self.build_string(k).build_value(v);
        }
        self
    }
}


pub fn cozo_comparator_v1(a: &[u8], b: &[u8]) -> i8 {
    let mut ba = &mut ByteArrayParser { bytes: a, current: 0 };
    let mut bb = &mut ByteArrayParser { bytes: b, current: 0 };
    match ba.compare_varint(&mut bb) {
        Less => return -1,
        Greater => return 1,
        Equal => {}
    }
    match cmp_data(&mut ba, &mut bb) {
        Less => -1,
        Equal => 0,
        Greater => 1
    }
}

pub fn cmp_data<'a>(pa: &mut ByteArrayParser<'a>, pb: &mut ByteArrayParser<'a>) -> Ordering {
    loop {
        match (pa.at_end(), pb.at_end()) {
            (true, true) => return Equal,
            (true, false) => return Less,
            (false, true) => return Greater,
            (false, false) => ()
        }
        if let x @ (Greater | Less) = pa.compare_value(pb) { return x; }
    }
}


impl<'a> Value<'a> {
    pub fn owned_clone(&self) -> StaticValue {
        use Value::*;

        match self {
            Null => Null,
            Bool(b) => Bool(*b),
            EdgeDir(dir) => EdgeDir(*dir),
            UInt(u) => UInt(*u),
            Int(i) => Int(*i),
            Float(f) => Float(*f),
            RefString(s) => OwnString(Arc::new(s.to_string())),
            OwnString(s) => OwnString(s.clone()),
            List(l) => {
                let mut inner = Vec::with_capacity(l.len());

                for el in l.iter() {
                    inner.push(el.owned_clone())
                }
                List(Arc::new(inner))
            }
            Dict(d) => {
                let mut inner = BTreeMap::new();
                for (k, v) in d.iter() {
                    let new_k = Cow::from(k.clone().into_owned());
                    inner.insert(new_k, v.owned_clone());
                }
                Dict(Arc::new(inner))
            }
            Uuid(u) => Uuid(*u),
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
    pub table_id: i64,
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
            let mut builder = ByteArrayBuilder::default();
            builder.build_zigzag(i);
            let mut parser = ByteArrayParser::new(&builder);
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
