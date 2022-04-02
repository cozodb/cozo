extern crate pest;
#[macro_use]
extern crate pest_derive;

use std::array::from_mut;
use std::borrow::Cow;
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::io;
use std::io::{Cursor, Read, Write};
use ordered_float::OrderedFloat;
use pest::Parser;


#[derive(Parser)]
#[grammar = "cozo.pest"]
pub struct CozoParser;

#[derive(Copy, Clone)]
pub enum EdgeDir {
    FwdEdge,
    BwdEdge,
}

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

pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    let cur_a = &mut Cursor::new(a);
    let cur_b = &mut Cursor::new(b);


    match parse_varint(cur_a).unwrap().cmp(&parse_varint(cur_b).unwrap()) {
        Ordering::Equal => {
            compare_data(cur_a, cur_b)
        }
        x => x
    }
}

fn compare_data<T: Read>(cur_a: &mut T, cur_b: &mut T) -> Ordering {
    loop {
        let atag = parse_varint(cur_a);
        let btag = parse_varint(cur_b);
        match (atag, btag) {
            (Err(_), Err(_)) => {
                return Ordering::Equal;
            }
            (Err(_), Ok(_)) => {
                return Ordering::Less;
            }
            (Ok(_), Err(_)) => {
                return Ordering::Greater;
            }
            (Ok(atype), Ok(btype)) => {
                match atype.cmp(&btype) {
                    Ordering::Equal => {
                        match ValueTag::from(atype) {
                            ValueTag::Int => {
                                match parse_zigzag(cur_a).unwrap().cmp(&parse_zigzag(cur_b).unwrap()) {
                                    Ordering::Equal => {}
                                    x => { return x; }
                                }
                            }
                            ValueTag::Float => {
                                let av = parse_float(cur_a).unwrap();
                                let bv = parse_float(cur_b).unwrap();

                                match OrderedFloat(av).cmp(&OrderedFloat(bv)) {
                                    Ordering::Equal => {}
                                    x => { return x; }
                                }
                            }
                            ValueTag::String => {
                                let al = parse_varint(cur_a).unwrap();
                                let bl = parse_varint(cur_b).unwrap();
                                for _ in 0..min(al, bl) {
                                    let mut av = 0;
                                    let mut bv = 0;
                                    cur_a.read_exact(from_mut(&mut av)).unwrap();
                                    cur_b.read_exact(from_mut(&mut bv)).unwrap();
                                    match av.cmp(&bv) {
                                        Ordering::Equal => {}
                                        x => { return x; }
                                    }
                                }
                                match al.cmp(&bl) {
                                    Ordering::Equal => {}
                                    x => { return x; }
                                }
                            }
                            ValueTag::UInt => {
                                match parse_varint(cur_a).unwrap().cmp(&parse_varint(cur_b).unwrap()) {
                                    Ordering::Equal => {}
                                    x => { return x; }
                                }
                            }
                            ValueTag::List => {
                                let al = parse_varint(cur_a).unwrap();
                                let bl = parse_varint(cur_b).unwrap();
                                for _ in 0..min(al, bl) {
                                    match compare_data(cur_a, cur_b) {
                                        Ordering::Equal => {}
                                        x => { return x; }
                                    }
                                }
                            }
                            ValueTag::Dict => {
                                let al = parse_varint(cur_a).unwrap();
                                let bl = parse_varint(cur_b).unwrap();
                                for _ in 0..min(al, bl) {
                                    let asl = parse_varint(cur_a).unwrap();
                                    let bsl = parse_varint(cur_b).unwrap();
                                    for _ in 0..min(asl, bsl) {
                                        let mut av = 0;
                                        let mut bv = 0;
                                        cur_a.read_exact(from_mut(&mut av)).unwrap();
                                        cur_b.read_exact(from_mut(&mut bv)).unwrap();
                                        match av.cmp(&bv) {
                                            Ordering::Equal => {}
                                            x => { return x; }
                                        }
                                    }
                                    match al.cmp(&bl) {
                                        Ordering::Equal => {}
                                        x => { return x; }
                                    }

                                    match compare_data(cur_a, cur_b) {
                                        Ordering::Equal => {}
                                        x => { return x; }
                                    }
                                }
                            }
                            // Fall through
                            ValueTag::Null => {}
                            ValueTag::BoolTrue => {}
                            ValueTag::BoolFalse => {}
                            ValueTag::FwdEdge => {}
                            ValueTag::BwdEdge => {}
                        }
                    }
                    x => { return x; }
                }
            }
        }
    }
}

impl<'a> Value<'a> {
    pub fn to_owned(&self) -> Value<'_> {
        match self {
            Value::Null => {
                Value::Null
            }
            Value::Bool(b) => {
                Value::Bool(*b)
            }
            Value::EdgeDir(dir) => {
                Value::EdgeDir(*dir)
            }
            Value::UInt(u) => {
                Value::UInt(*u)
            }
            Value::Int(i) => {
                Value::Int(*i)
            }
            Value::Float(f) => {
                Value::Float(*f)
            }
            Value::String(s) => {
                Value::String(s.to_owned())
            }
            Value::List(l) => {
                return Value::List(l.iter().map(|v| v.to_owned()).collect());
            }
            Value::Dict(d) => {
                return Value::Dict(d.iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect());
            }
        }
    }
}


#[repr(u8)]
enum ValueTag {
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

pub struct CozoRawKey<'a> {
    pub table_id: u64,
    pub values: Vec<Value<'a>>,
}

pub fn build_values(vals: &[Value]) -> Vec<u8> {
    let mut ret = vec![];
    ret.reserve(vals.len() * 4);
    for v in vals {
        build_value(&mut ret, v);
    }
    ret
}

pub fn build_keys(k: &CozoRawKey) -> Vec<u8> {
    let mut ret = vec![];
    ret.reserve(1 + k.values.len() * 4);
    build_varint(k.table_id, &mut ret);
    for v in &k.values {
        build_value(&mut ret, v);
    }
    ret
}

fn build_value<T: Write>(ret: &mut T, val: &Value) {
    match val {
        Value::Null => {
            ret.write_all(&[ValueTag::Null as u8]).unwrap();
        }
        Value::Bool(b) => {
            ret.write_all(&[if *b { ValueTag::BoolTrue } else { ValueTag::BoolFalse }
                as u8]).unwrap();
        }
        Value::EdgeDir(dir) => {
            ret.write_all(&[match *dir {
                EdgeDir::FwdEdge => {
                    ValueTag::FwdEdge
                }
                EdgeDir::BwdEdge => {
                    ValueTag::BwdEdge
                }
            } as u8]).unwrap();
        }
        Value::UInt(u) => {
            ret.write_all(&[ValueTag::UInt as u8]).unwrap();
            build_varint(*u, ret);
        }
        Value::Int(i) => {
            ret.write_all(&[ValueTag::Int as u8]).unwrap();
            build_zigzag(*i, ret);
        }
        Value::Float(f) => {
            ret.write_all(&[ValueTag::Float as u8]).unwrap();
            ret.write_all(&f.to_be_bytes()).unwrap();
        }
        Value::String(s) => {
            ret.write_all(&[ValueTag::String as u8]).unwrap();
            let s_bytes = s.as_bytes();
            build_varint(s_bytes.len() as u64, ret);
            ret.write_all(s_bytes).unwrap();
        }
        Value::List(l) => {
            ret.write_all(&[ValueTag::List as u8]).unwrap();
            build_varint(l.len() as u64, ret);
            for v in l {
                build_value(ret, v);
            }
        }
        Value::Dict(d) => {
            ret.write_all(&[ValueTag::Dict as u8]).unwrap();
            build_varint(d.len() as u64, ret);
            for (k, v) in d {
                let buf = k.as_bytes();
                build_varint(buf.len() as u64, ret);
                ret.write_all(buf).unwrap();
                build_value(ret, v);
            }
        }
    }
}

fn build_varint<T: Write>(mut u: u64, out: &mut T) {
    while u > 0b01111111 {
        out.write_all(&[0b10000000 | (u as u8 & 0b01111111)]).unwrap();
        u >>= 7;
    }
    out.write_all(&[u as u8]).unwrap();
}

fn parse_varint<T: Read>(inp: &mut T) -> io::Result<u64> {
    let mut u: u64 = 0;
    let mut buf = 0u8;
    let mut shift = 0;
    loop {
        inp.read_exact(from_mut(&mut buf))?;
        u |= ((buf & 0b01111111) as u64) << shift;
        if buf & 0b10000000 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(u)
}

fn build_zigzag<T: Write>(i: i64, out: &mut T) {
    let u: u64 = if i >= 0 {
        (i as u64) << 1
    } else {
        // Convoluted, to prevent overflow when calling .abs()
        (((i + 1).abs() as u64) << 1) + 1
    };
    build_varint(u, out)
}

fn parse_zigzag<T: Read>(inp: &mut T) -> io::Result<i64> {
    let u = parse_varint(inp)?;
    Ok(if u & 1 == 0 {
        (u >> 1) as i64
    } else {
        -((u >> 1) as i64) - 1
    })
}

fn parse_float<T: Read>(inp: &mut T) -> io::Result<f64> {
    let mut buf = [0; 8];
    inp.read_exact(&mut buf)?;
    Ok(f64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;

    #[test]
    fn varint() {
        for u in 126..(2u64).pow(9) {
            let mut x = vec![];
            build_varint(u, &mut x);
            let mut cur = Cursor::new(&x);
            let u2 = parse_varint(&mut cur).unwrap();
            assert_eq!(u, u2);
        }

        let u = u64::MIN;
        let mut x = vec![];
        build_varint(u, &mut x);
        let mut cur = Cursor::new(&x);
        let u2 = parse_varint(&mut cur).unwrap();
        assert_eq!(u, u2);

        let u = u64::MAX;
        let mut x = vec![];
        build_varint(u, &mut x);
        let mut cur = Cursor::new(&x);
        let u2 = parse_varint(&mut cur).unwrap();
        assert_eq!(u, u2);
    }

    #[test]
    fn zigzag() {
        for i in 126..(2i64).pow(9) {
            let mut x = vec![];
            build_zigzag(i, &mut x);
            let mut cur = Cursor::new(&x);
            let i2 = parse_zigzag(&mut cur).unwrap();
            assert_eq!(i, i2);
        }
        for i in 126..(2i64).pow(9) {
            let i = -i;
            let mut x = vec![];
            build_zigzag(i, &mut x);
            let mut cur = Cursor::new(&x);
            let i2 = parse_zigzag(&mut cur).unwrap();
            assert_eq!(i, i2);
        }

        let i = i64::MIN;
        let mut x = vec![];
        build_zigzag(i, &mut x);
        let mut cur = Cursor::new(&x);
        let i2 = parse_zigzag(&mut cur).unwrap();
        assert_eq!(i, i2);

        let i = i64::MAX;
        let mut x = vec![];
        build_zigzag(i, &mut x);
        let mut cur = Cursor::new(&x);
        let i2 = parse_zigzag(&mut cur).unwrap();
        assert_eq!(i, i2);
    }

    #[test]
    fn identifiers() {
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x").unwrap().as_str(), "x");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x2").unwrap().as_str(), "x2");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x_y").unwrap().as_str(), "x_y");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "x_").unwrap().as_str(), "x_");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "你好").unwrap().as_str(), "你好");
        assert_eq!(CozoParser::parse(Rule::normal_ident, "你好123").unwrap().as_str(), "你好123");
        assert_ne!(CozoParser::parse(Rule::ident, "x$y").unwrap().as_str(), "x$y");

        assert!(CozoParser::parse(Rule::normal_ident, "_x").is_err());
        assert!(CozoParser::parse(Rule::normal_ident, "_").is_err());
        assert_eq!(CozoParser::parse(Rule::ident, "_x").unwrap().as_str(), "_x");
        assert_eq!(CozoParser::parse(Rule::ident, "_").unwrap().as_str(), "_");

        assert!(CozoParser::parse(Rule::normal_ident, "$x").is_err());
        assert!(CozoParser::parse(Rule::ident, "$").is_err());
        assert_eq!(CozoParser::parse(Rule::ident, "$x").unwrap().as_str(), "$x");

        assert!(CozoParser::parse(Rule::ident, "123x").is_err());
        assert!(CozoParser::parse(Rule::ident, ".x").is_err());
        assert_ne!(CozoParser::parse(Rule::ident, "x.x").unwrap().as_str(), "x.x");
        assert_ne!(CozoParser::parse(Rule::ident, "x~x").unwrap().as_str(), "x~x");
    }

    #[test]
    fn strings() {
        assert_eq!(CozoParser::parse(Rule::string, r#""""#).unwrap().as_str(), r#""""#);
        assert_eq!(CozoParser::parse(Rule::string, r#"" b a c""#).unwrap().as_str(), r#"" b a c""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""你好👋""#).unwrap().as_str(), r#""你好👋""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""\n""#).unwrap().as_str(), r#""\n""#);
        assert_eq!(CozoParser::parse(Rule::string, r#""\u5678""#).unwrap().as_str(), r#""\u5678""#);
        assert!(CozoParser::parse(Rule::string, r#""\ux""#).is_err());
        assert_eq!(CozoParser::parse(Rule::string, r###"r#"a"#"###).unwrap().as_str(), r##"r#"a"#"##);
    }

    #[test]
    fn numbers() {
        assert_eq!(CozoParser::parse(Rule::number, "123").unwrap().as_str(), "123");
        assert_eq!(CozoParser::parse(Rule::number, "-123").unwrap().as_str(), "-123");
        assert_eq!(CozoParser::parse(Rule::number, "0").unwrap().as_str(), "0");
        assert_eq!(CozoParser::parse(Rule::number, "-0").unwrap().as_str(), "-0");
        assert_eq!(CozoParser::parse(Rule::number, "0123").unwrap().as_str(), "0123");
        assert_eq!(CozoParser::parse(Rule::number, "000_1").unwrap().as_str(), "000_1");
        assert!(CozoParser::parse(Rule::number, "_000_1").is_err());
        assert_eq!(CozoParser::parse(Rule::number, "0xAf03").unwrap().as_str(), "0xAf03");
        assert_eq!(CozoParser::parse(Rule::number, "0o0_7067").unwrap().as_str(), "0o0_7067");
        assert_ne!(CozoParser::parse(Rule::number, "0o0_7068").unwrap().as_str(), "0o0_7068");
        assert_eq!(CozoParser::parse(Rule::number, "0b0000_0000_1111").unwrap().as_str(), "0b0000_0000_1111");
        assert_ne!(CozoParser::parse(Rule::number, "0b0000_0000_1112").unwrap().as_str(), "0b0000_0000_1112");

        assert_eq!(CozoParser::parse(Rule::number, "123.45").unwrap().as_str(), "123.45");
        assert_eq!(CozoParser::parse(Rule::number, "1_23.4_5_").unwrap().as_str(), "1_23.4_5_");
        assert_ne!(CozoParser::parse(Rule::number, "123.").unwrap().as_str(), "123.");
        assert_eq!(CozoParser::parse(Rule::number, "-123e-456").unwrap().as_str(), "-123e-456");
        assert_eq!(CozoParser::parse(Rule::number, "123.333e456").unwrap().as_str(), "123.333e456");
        assert_eq!(CozoParser::parse(Rule::number, "1_23.33_3e45_6").unwrap().as_str(), "1_23.33_3e45_6");
    }

    #[test]
    fn expressions() {
        assert!(CozoParser::parse(Rule::expr, r"(a + b) ~ [] + c.d.e(1,2,x=3).f").is_ok());
    }
}
