use crate::data::tuple::TupleError::UndefinedDataTag;
use crate::data::value::Value;
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::result;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub(crate) enum TupleError {
    #[error("Undefined data kind {0}")]
    UndefinedDataKind(u32),

    #[error("Undefined data tag {0}")]
    UndefinedDataTag(u8),

    #[error("Index {0} out of bound for tuple {1:?}")]
    IndexOutOfBound(usize, OwnTuple),

    #[error("Type mismatch: {1:?} is not {0}")]
    TypeMismatch(&'static str, OwnTuple),
}

type Result<T> = result::Result<T, TupleError>;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum StorageTag {
    BoolFalse = 1,
    Null = 2,
    BoolTrue = 3,
    Int = 4,
    Float = 5,
    Text = 6,
    Uuid = 7,

    Bytes = 64,

    List = 128,
    Dict = 129,

    DescVal = 192,

    Max = 255,
}

impl TryFrom<u8> for StorageTag {
    type Error = u8;
    #[inline]
    fn try_from(u: u8) -> std::result::Result<StorageTag, u8> {
        use self::StorageTag::*;
        Ok(match u {
            1 => BoolFalse,
            2 => Null,
            3 => BoolTrue,
            4 => Int,
            5 => Float,
            6 => Text,
            7 => Uuid,

            64 => Bytes,

            128 => List,
            129 => Dict,

            192 => DescVal,

            255 => Max,
            v => return Err(v),
        })
    }
}

#[repr(u32)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum DataKind {
    Data = 0,
    Node = 1,
    Edge = 2,
    Assoc = 3,
    Index = 4,
    Val = 5,
    Type = 6,
    Empty = u32::MAX,
}
// In storage, key layout is `[0, name, stack_depth]` where stack_depth is a non-positive number as zigzag
// Also has inverted index `[0, stack_depth, name]` for easy popping of stacks

pub const EMPTY_DATA: [u8; 4] = u32::MAX.to_be_bytes();

impl<T: AsRef<[u8]>> Tuple<T> {
    pub fn data_kind(&self) -> Result<DataKind> {
        use DataKind::*;
        Ok(match self.get_prefix() {
            0 => Data,
            1 => Node,
            2 => Edge,
            3 => Assoc,
            4 => Index,
            5 => Val,
            6 => Type,
            u32::MAX => Empty,
            v => return Err(TupleError::UndefinedDataKind(v)),
        })
    }
}

#[derive(Clone)]
pub(crate) struct Tuple<T>
    where
        T: AsRef<[u8]>,
{
    pub(crate) data: T,
    idx_cache: RefCell<Vec<usize>>,
}

impl<T> Tuple<T>
    where
        T: AsRef<[u8]>,
{
    pub(crate) fn clear_cache(&self) {
        self.idx_cache.borrow_mut().clear()
    }
}

impl<T> AsRef<[u8]> for Tuple<T>
    where
        T: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

pub(crate) type OwnTuple = Tuple<Vec<u8>>;

pub(crate) const PREFIX_LEN: usize = 4;

impl<T: AsRef<[u8]>> Tuple<T> {
    #[inline]
    pub(crate) fn to_owned(&self) -> OwnTuple {
        OwnTuple {
            data: self.data.as_ref().to_vec(),
            idx_cache: RefCell::new(vec![]),
        }
    }

    #[inline]
    pub(crate) fn starts_with<T2: AsRef<[u8]>>(&self, other: &Tuple<T2>) -> bool {
        self.data.as_ref().starts_with(other.data.as_ref())
    }

    #[inline]
    pub(crate) fn key_part_eq<T2: AsRef<[u8]>>(&self, other: &Tuple<T2>) -> bool {
        self.data.as_ref()[PREFIX_LEN..] == other.data.as_ref()[PREFIX_LEN..]
    }

    #[inline]
    pub(crate) fn key_part_cmp<T2: AsRef<[u8]>>(&self, other: &Tuple<T2>) -> Ordering {
        self.iter()
            .map(|v| v.expect("Key comparison failed"))
            .cmp(other.iter().map(|v| v.expect("Key comparison failed")))
    }

    #[inline]
    pub(crate) fn new(data: T) -> Self {
        Self {
            data,
            idx_cache: RefCell::new(vec![]),
        }
    }

    #[inline]
    pub(crate) fn get_prefix(&self) -> u32 {
        u32::from_be_bytes(self.data.as_ref()[0..4].try_into().unwrap())
    }

    #[inline]
    fn all_cached(&self) -> bool {
        match self.idx_cache.borrow().last() {
            None => self.data.as_ref().len() == PREFIX_LEN,
            Some(l) => *l == self.data.as_ref().len(),
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
        let mut start = tag_start + 1;
        let nxt;
        loop {
            nxt = match StorageTag::try_from(data[tag_start]).unwrap() {
                StorageTag::Null | StorageTag::BoolTrue | StorageTag::BoolFalse => start,
                StorageTag::Int => start + self.parse_varint(start).1,
                StorageTag::Float => start + 8,
                StorageTag::Uuid => start + 16,
                StorageTag::Text | StorageTag::Bytes => {
                    let (slen, offset) = self.parse_varint(start);
                    let slen = slen as usize;
                    start + slen + offset
                }
                StorageTag::List | StorageTag::Dict => {
                    start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize
                }
                StorageTag::DescVal => {
                    start += 1;
                    continue;
                }
                StorageTag::Max => panic!(),
            };
            break;
        }
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
    pub(crate) fn get(&self, idx: usize) -> Result<Value> {
        match self.get_pos(idx) {
            Some(v) => {
                if v == self.data.as_ref().len() {
                    return Err(TupleError::IndexOutOfBound(idx, self.to_owned()));
                }
                let (val, nxt) = self.parse_value_at(v)?;
                if idx == self.idx_cache.borrow().len() {
                    self.idx_cache.borrow_mut().push(nxt);
                }
                Ok(val)
            }
            None => Err(TupleError::IndexOutOfBound(idx, self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_null(&self, idx: usize) -> Result<()> {
        match self.get(idx)? {
            Value::Null => Ok(()),
            _ => Err(TupleError::TypeMismatch("Null", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_int(&self, idx: usize) -> Result<i64> {
        match self.get(idx)? {
            Value::Int(i) => Ok(i),
            _ => Err(TupleError::TypeMismatch("Int", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_text(&self, idx: usize) -> Result<Cow<str>> {
        match self.get(idx)? {
            Value::Text(d) => Ok(d),
            _ => Err(TupleError::TypeMismatch("Text", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_bool(&self, idx: usize) -> Result<bool> {
        match self.get(idx)? {
            Value::Bool(b) => Ok(b),
            _ => Err(TupleError::TypeMismatch("Bool", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_float(&self, idx: usize) -> Result<f64> {
        match self.get(idx)? {
            Value::Float(f) => Ok(f.into_inner()),
            _ => Err(TupleError::TypeMismatch("Float", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_uuid(&self, idx: usize) -> Result<Uuid> {
        match self.get(idx)? {
            Value::Uuid(u) => Ok(u),
            _ => Err(TupleError::TypeMismatch("Uuid", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_list(&self, idx: usize) -> Result<Vec<Value>> {
        match self.get(idx)? {
            Value::List(u) => Ok(u),
            _ => Err(TupleError::TypeMismatch("List", self.to_owned())),
        }
    }

    #[inline]
    pub(crate) fn get_dict(&self, idx: usize) -> Result<BTreeMap<Cow<str>, Value>> {
        match self.get(idx)? {
            Value::Dict(u) => Ok(u),
            _ => Err(TupleError::TypeMismatch("Dict", self.to_owned())),
        }
    }

    #[inline]
    fn parse_value_at(&self, pos: usize) -> Result<(Value, usize)> {
        let data = self.data.as_ref();
        let start = pos + 1;
        let tag = match StorageTag::try_from(data[pos]) {
            Ok(t) => t,
            Err(e) => return Err(TupleError::UndefinedDataTag(e)),
        };
        let (nxt, val): (usize, Value) = match tag {
            StorageTag::Null => (start, ().into()),
            StorageTag::BoolTrue => (start, true.into()),
            StorageTag::BoolFalse => (start, false.into()),
            StorageTag::Int => {
                let (u, offset) = self.parse_varint(start);
                let val = Self::varint_to_zigzag(u);
                (start + offset, val.into())
            }
            StorageTag::Float => (
                start + 8,
                f64::from_be_bytes(data[start..start + 8].try_into().unwrap()).into(),
            ),
            StorageTag::Uuid => (
                start + 16,
                Uuid::from_slice(&data[start..start + 16]).unwrap().into(),
            ),
            StorageTag::Text => {
                let (slen, offset) = self.parse_varint(start);
                let slen = slen as usize;
                let s = unsafe {
                    std::str::from_utf8_unchecked(&data[start + offset..start + offset + slen])
                };

                (start + slen + offset, s.into())
            }
            StorageTag::Bytes => {
                let (slen, offset) = self.parse_varint(start);
                let slen = slen as usize;
                let s = &data[start + offset..start + offset + slen];

                (start + slen + offset, s.into())
            }
            StorageTag::List => {
                let end_pos =
                    start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize;
                let mut start_pos = start + 4;
                let mut collected = vec![];
                while start_pos < end_pos {
                    let (val, new_pos) = self.parse_value_at(start_pos)?;
                    collected.push(val);
                    start_pos = new_pos;
                }
                (end_pos, collected.into())
            }
            StorageTag::Dict => {
                let end_pos =
                    start + u32::from_be_bytes(data[start..start + 4].try_into().unwrap()) as usize;
                let mut start_pos = start + 4;
                let mut collected: BTreeMap<Cow<str>, Value> = BTreeMap::new();
                while start_pos < end_pos {
                    let (slen, offset) = self.parse_varint(start_pos);
                    start_pos += offset;
                    let key = unsafe {
                        std::str::from_utf8_unchecked(&data[start_pos..start_pos + slen as usize])
                    };
                    start_pos += slen as usize;
                    let (val, new_pos) = self.parse_value_at(start_pos)?;
                    collected.insert(key.into(), val);
                    start_pos = new_pos;
                }
                (end_pos, collected.into())
            }
            StorageTag::DescVal => {
                let (val, offset) = self.parse_value_at(pos + 1)?;
                (offset, Value::DescVal(Reverse(val.into())))
            }
            StorageTag::Max => (start, Value::EndSentinel),
        };
        Ok((val, nxt))
    }

    fn varint_to_zigzag(u: u64) -> i64 {
        if u & 1 == 0 {
            (u >> 1) as i64
        } else {
            -((u >> 1) as i64) - 1
        }
    }
    pub(crate) fn iter(&self) -> TupleIter<T> {
        TupleIter {
            tuple: self,
            pos: 4,
        }
    }
}

impl<T: AsRef<[u8]>> Debug for Tuple<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.data_kind() {
            Ok(data_kind) => {
                write!(f, "Tuple<{:?}>{{", data_kind)?;
            }
            Err(_) => {
                write!(f, "Tuple<{}>{{", self.get_prefix())?;
            }
        }
        let strings = self
            .iter()
            .enumerate()
            .map(|(i, v)| match v {
                Ok(v) => {
                    format!("{}: {}", i, v)
                }
                Err(err) => {
                    format!("{}: {:?}", i, err)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}}}", strings)
    }
}

pub(crate) struct TupleIter<'a, T: AsRef<[u8]>> {
    tuple: &'a Tuple<T>,
    pos: usize,
}

impl<'a, T: AsRef<[u8]>> Iterator for TupleIter<'a, T> {
    type Item = Result<Value<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.tuple.data.as_ref().len() {
            return None;
        }
        let (v, pos) = match self.tuple.parse_value_at(self.pos) {
            Ok(vs) => vs,
            Err(e) => return Some(Err(e)),
        };
        self.pos = pos;
        Some(Ok(v))
    }
}

impl OwnTuple {
    #[inline]
    pub(crate) fn truncate_all(&mut self) {
        self.clear_cache();
        self.data.truncate(PREFIX_LEN);
    }
    #[inline]
    pub(crate) fn empty_tuple() -> OwnTuple {
        OwnTuple::with_data_prefix(DataKind::Empty)
    }
    #[inline]
    pub(crate) fn with_null_prefix() -> Self {
        Tuple::with_prefix(0)
    }
    #[inline]
    pub(crate) fn with_data_prefix(prefix: DataKind) -> Self {
        Tuple::with_prefix(prefix as u32)
    }
    #[inline]
    pub(crate) fn with_prefix(prefix: u32) -> Self {
        let data = Vec::from(prefix.to_be_bytes());
        Self {
            data,
            idx_cache: RefCell::new(vec![]),
        }
    }
    #[inline]
    pub(crate) fn overwrite_prefix(&mut self, prefix: u32) {
        let bytes = prefix.to_be_bytes();
        self.data[..4].clone_from_slice(&bytes[..4]);
    }
    #[inline]
    pub(crate) fn max_tuple() -> Self {
        let mut ret = Tuple::with_prefix(u32::MAX);
        ret.seal_with_sentinel();
        ret
    }
    #[inline]
    pub(crate) fn seal_with_sentinel(&mut self) {
        self.push_tag(StorageTag::Max);
    }
    #[inline]
    fn push_tag(&mut self, tag: StorageTag) {
        self.data.push(tag as u8);
    }
    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.push_tag(StorageTag::Null);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_bool(&mut self, b: bool) {
        self.push_tag(if b {
            StorageTag::BoolTrue
        } else {
            StorageTag::BoolFalse
        });
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_int(&mut self, i: i64) {
        self.push_tag(StorageTag::Int);
        self.push_zigzag(i);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_float(&mut self, f: f64) {
        self.push_tag(StorageTag::Float);
        self.data.extend(f.to_be_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_uuid(&mut self, u: Uuid) {
        self.push_tag(StorageTag::Uuid);
        self.data.extend(u.as_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_str(&mut self, s: impl AsRef<str>) {
        let s = s.as_ref();
        self.push_tag(StorageTag::Text);
        self.push_varint(s.len() as u64);
        self.data.extend_from_slice(s.as_bytes());
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_bytes(&mut self, b: impl AsRef<[u8]>) {
        let b = b.as_ref();
        self.push_tag(StorageTag::Bytes);
        self.push_varint(b.len() as u64);
        self.data.extend_from_slice(b);
        self.idx_cache.borrow_mut().push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_reverse_value(&mut self, v: &Value) {
        self.push_tag(StorageTag::DescVal);
        let start_len = self.idx_cache.borrow().len();
        self.push_value(v);
        let mut cache = self.idx_cache.borrow_mut();
        cache.truncate(start_len);
        cache.push(self.data.len());
    }
    #[inline]
    pub(crate) fn push_value(&mut self, v: &Value) {
        match v {
            Value::Null => self.push_null(),
            Value::Bool(b) => self.push_bool(*b),
            Value::Int(i) => self.push_int(*i),
            Value::Float(f) => self.push_float(f.into_inner()),
            Value::Uuid(u) => self.push_uuid(*u),
            Value::Text(t) => self.push_str(t),
            Value::Bytes(b) => self.push_bytes(b),
            Value::List(l) => {
                self.push_tag(StorageTag::List);
                let start_pos = self.data.len();
                let start_len = self.idx_cache.borrow().len();
                self.data.extend(0u32.to_be_bytes());
                for val in l {
                    self.push_value(val);
                }
                let length = (self.data.len() - start_pos) as u32;
                let length_bytes = length.to_be_bytes();
                self.data[start_pos..(4 + start_pos)].clone_from_slice(&length_bytes[..4]);
                let mut cache = self.idx_cache.borrow_mut();
                cache.truncate(start_len);
                cache.push(self.data.len());
            }
            Value::Dict(d) => {
                self.push_tag(StorageTag::Dict);
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
                self.data[start_pos..(4 + start_pos)].clone_from_slice(&length_bytes[..4]);
                let mut cache = self.idx_cache.borrow_mut();
                cache.truncate(start_len);
                cache.push(self.data.len());
            }
            Value::EndSentinel => panic!("Cannot push sentinel value"),
            Value::DescVal(Reverse(v)) => {
                self.push_reverse_value(v);
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

    #[inline]
    pub(crate) fn concat_data<T: AsRef<[u8]>>(&mut self, other: &Tuple<T>) {
        let other_data_part = &other.as_ref()[4..];
        self.data.extend_from_slice(other_data_part);
    }

    #[inline]
    pub(crate) fn insert_values_at<'a, T: AsRef<[Value<'a>]>>(
        &self,
        idx: usize,
        values: T,
    ) -> Result<Self> {
        let mut new_tuple = Tuple::with_prefix(self.get_prefix());
        for v in self.iter().take(idx) {
            new_tuple.push_value(&v?);
        }
        for v in values.as_ref() {
            new_tuple.push_value(v);
        }
        for v in self.iter().skip(idx) {
            new_tuple.push_value(&v?);
        }
        Ok(new_tuple)
    }
}

impl<'a> Extend<Value<'a>> for OwnTuple {
    #[inline]
    fn extend<T: IntoIterator<Item=Value<'a>>>(&mut self, iter: T) {
        for v in iter {
            self.push_value(&v)
        }
    }
}

impl<T: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<Tuple<T2>> for Tuple<T> {
    #[inline]
    fn eq(&self, other: &Tuple<T2>) -> bool {
        self.data.as_ref() == other.data.as_ref()
    }
}

impl<T: AsRef<[u8]>> Hash for Tuple<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.as_ref().hash(state);
    }
}

impl<T: AsRef<[u8]>> Eq for Tuple<T> {}
