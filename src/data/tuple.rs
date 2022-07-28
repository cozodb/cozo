use std::cmp::{min, Ordering};
use std::fmt::{Debug, Formatter};

use anyhow::Result;
use itertools::Itertools;
use rmp_serde::Serializer;
use serde::Serialize;

use crate::data::json::JsonValue;
use crate::data::value::DataValue;
use crate::runtime::temp_store::TempStoreId;

pub(crate) const SCRATCH_DB_KEY_PREFIX_LEN: usize = 6;

#[derive(Debug, thiserror::Error)]
pub enum TupleError {
    #[error("bad data: {0} for {1:x?}")]
    BadData(String, Vec<u8>),
}

pub struct Tuple(pub(crate) Vec<DataValue>);

impl Debug for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            let j = JsonValue::from(v.clone());
            write!(f, "{}", j)?;
        }
        write!(f, "]")
    }
}

pub(crate) type TupleIter<'a> = Box<dyn Iterator<Item=Result<Tuple>> + 'a>;

impl Tuple {
    pub(crate) fn arity(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn encode_as_key(&self, prefix: TempStoreId) -> Vec<u8> {
        self.encode_as_key_for_epoch(prefix, 0)
    }
    pub(crate) fn encode_as_key_for_epoch(&self, prefix: TempStoreId, epoch: u32) -> Vec<u8> {
        let len = self.arity();
        let mut ret = Vec::with_capacity(4 + 4 * len + 10 * len);
        let prefix_bytes = prefix.0.to_be_bytes();
        let epoch_bytes = epoch.to_be_bytes();
        ret.extend([
            prefix_bytes[1],
            prefix_bytes[2],
            prefix_bytes[3],
            epoch_bytes[1],
            epoch_bytes[2],
            epoch_bytes[3],
        ]);
        ret.extend((len as u16).to_be_bytes());
        ret.resize(4 * (len + 1), 0);
        for (idx, val) in self.0.iter().enumerate() {
            if idx > 0 {
                let pos = (ret.len() as u32).to_be_bytes();
                for (i, u) in pos.iter().enumerate() {
                    ret[4 * (1 + idx) + i] = *u;
                }
            }
            val.serialize(&mut Serializer::new(&mut ret)).unwrap();
        }
        ret
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct EncodedTuple<'a>(pub(crate) &'a [u8]);

impl<'a> From<&'a [u8]> for EncodedTuple<'a> {
    fn from(s: &'a [u8]) -> Self {
        EncodedTuple(s)
    }
}

impl<'a> EncodedTuple<'a> {
    pub(crate) fn bounds_for_prefix(prefix: TempStoreId) -> ([u8; 6], [u8; 6]) {
        let prefix_bytes = prefix.0.to_be_bytes();
        let next_prefix_bytes = (prefix.0 + 1).to_be_bytes();
        (
            [
                prefix_bytes[1],
                prefix_bytes[2],
                prefix_bytes[3],
                0,
                0,
                0,
            ],
            [
                next_prefix_bytes[1],
                next_prefix_bytes[2],
                next_prefix_bytes[3],
                0,
                0,
                0,
            ],
        )
    }
    pub(crate) fn bounds_for_prefix_and_epoch(prefix: TempStoreId, epoch: u32) -> ([u8; 6], [u8; 6]) {
        let prefix_bytes = prefix.0.to_be_bytes();
        let epoch_bytes = epoch.to_be_bytes();
        let epoch_bytes_upper = (epoch + 1).to_be_bytes();
        (
            [
                prefix_bytes[1],
                prefix_bytes[2],
                prefix_bytes[3],
                epoch_bytes[1],
                epoch_bytes[2],
                epoch_bytes[3],
            ],
            [
                prefix_bytes[1],
                prefix_bytes[2],
                prefix_bytes[3],
                epoch_bytes_upper[1],
                epoch_bytes_upper[2],
                epoch_bytes_upper[3],
            ],
        )
    }
    pub(crate) fn prefix(&self) -> Result<(TempStoreId, u32), TupleError> {
        if self.0.len() < 6 {
            Err(TupleError::BadData(
                "bad data length".to_string(),
                self.0.to_vec(),
            ))
        } else {
            let id = u32::from_be_bytes([0, self.0[0], self.0[1], self.0[2]]);
            let epoch = u32::from_be_bytes([0, self.0[3], self.0[4], self.0[5]]);
            Ok((TempStoreId(id), epoch))
        }
    }
    pub(crate) fn arity(&self) -> Result<usize, TupleError> {
        if self.0.len() == 6 {
            return Ok(0);
        }
        if self.0.len() < 8 {
            Err(TupleError::BadData(
                "bad data length".to_string(),
                self.0.to_vec(),
            ))
        } else {
            Ok(u16::from_be_bytes([self.0[6], self.0[7]]) as usize)
        }
    }
    fn force_get(&self, idx: usize) -> DataValue {
        let pos = if idx == 0 {
            let arity = u16::from_be_bytes([self.0[6], self.0[7]]) as usize;
            4 * (arity + 1)
        } else {
            let len_pos = (idx + 1) * 4;
            u32::from_be_bytes([
                self.0[len_pos],
                self.0[len_pos + 1],
                self.0[len_pos + 2],
                self.0[len_pos + 3],
            ]) as usize
        };
        rmp_serde::from_slice(&self.0[pos..]).unwrap()
    }
    pub(crate) fn get(&self, idx: usize) -> anyhow::Result<DataValue> {
        let pos = if idx == 0 {
            4 * (self.arity()? + 1)
        } else {
            let len_pos = (idx + 1) * 4;
            if self.0.len() < len_pos + 4 {
                return Err(
                    TupleError::BadData("bad data length".to_string(), self.0.to_vec()).into(),
                );
            }
            u32::from_be_bytes([
                self.0[len_pos],
                self.0[len_pos + 1],
                self.0[len_pos + 2],
                self.0[len_pos + 3],
            ]) as usize
        };
        if pos >= self.0.len() {
            return Err(TupleError::BadData("bad data length".to_string(), self.0.to_vec()).into());
        }
        Ok(rmp_serde::from_slice(&self.0[pos..])?)
    }

    pub(crate) fn iter(&self) -> EncodedTupleIter<'a> {
        EncodedTupleIter {
            tuple: *self,
            size: 0,
            pos: 0,
        }
    }
    pub(crate) fn decode(&self) -> Result<Tuple> {
        let v = self.iter().try_collect()?;
        Ok(Tuple(v))
    }
}

pub(crate) struct EncodedTupleIter<'a> {
    tuple: EncodedTuple<'a>,
    size: usize,
    pos: usize,
}

impl<'a> Iterator for EncodedTupleIter<'a> {
    type Item = anyhow::Result<DataValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.size == 0 {
            let arity = match self.tuple.arity() {
                Ok(a) => a,
                Err(e) => return Some(Err(e.into())),
            };
            self.size = arity;
        }
        if self.pos == self.size {
            None
        } else {
            let pos = self.pos;
            self.pos += 1;
            Some(self.tuple.get(pos))
        }
    }
}

pub(crate) fn rusty_scratch_cmp(a: &[u8], b: &[u8]) -> i8 {
    let a = EncodedTuple(a);
    let b = EncodedTuple(b);
    match a.prefix().unwrap().cmp(&b.prefix().unwrap()) {
        Ordering::Greater => return 1,
        Ordering::Equal => {}
        Ordering::Less => return -1,
    }
    let a_len = a.arity().unwrap();
    let b_len = b.arity().unwrap();
    for idx in 0..min(a_len, b_len) {
        let av = a.force_get(idx);
        let bv = b.force_get(idx);
        match av.cmp(&bv) {
            Ordering::Greater => return 1,
            Ordering::Equal => {}
            Ordering::Less => return -1,
        }
    }
    match a_len.cmp(&b_len) {
        Ordering::Greater => 1,
        Ordering::Equal => 0,
        Ordering::Less => -1,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::data::tuple::{EncodedTuple, Tuple};
    use crate::data::value::DataValue;
    use crate::runtime::temp_store::TempStoreId;

    #[test]
    fn test_serde() {
        let val: Vec<DataValue> = vec![
            json!(1).into(),
            json!(2.0).into(),
            json!("my_name_is").into(),
        ];
        let val = Tuple(val);
        let encoded = val.encode_as_key(TempStoreId(123));
        println!("{:x?}", encoded);
        let encoded_tuple: EncodedTuple<'_> = (&encoded as &[u8]).into();
        println!("{:?}", encoded_tuple.prefix());
        println!("{:?}", encoded_tuple.arity());
        println!("{:?}", encoded_tuple.get(0));
        println!("{:?}", encoded_tuple.get(1));
        println!("{:?}", encoded_tuple.get(2));
        println!("{:?}", encoded_tuple.get(3));
        println!(
            "{:?}",
            encoded_tuple
                .iter()
                .collect::<anyhow::Result<Vec<DataValue>>>()
        )
    }
}
