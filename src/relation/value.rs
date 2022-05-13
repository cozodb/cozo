use crate::db::table::{ColId, TableId};
use crate::error::CozoError::LogicError;
use crate::error::{CozoError, Result};
use crate::parser::number::parse_int;
use crate::parser::text_identifier::parse_string;
use crate::parser::{Parser, Rule};
use lazy_static::lazy_static;
use ordered_float::OrderedFloat;
use pest::iterators::Pair;
use pest::prec_climber::{Assoc, Operator, PrecClimber};
use pest::Parser as PestParser;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter, Write};
use uuid::Uuid;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Tag {
    BoolFalse = 1,
    Null = 2,
    BoolTrue = 3,
    Int = 4,
    Float = 5,
    Text = 6,
    Uuid = 7,

    List = 128,
    Dict = 129,

    DescVal = 192,

    TupleRef = 250,
    IdxAccess = 251,
    FieldAccess = 252,
    Variable = 253,
    Apply = 254,
    MaxTag = 255,
}

impl TryFrom<u8> for Tag {
    type Error = u8;
    #[inline]
    fn try_from(u: u8) -> std::result::Result<Tag, u8> {
        use self::Tag::*;
        Ok(match u {
            1 => BoolFalse,
            2 => Null,
            3 => BoolTrue,
            4 => Int,
            5 => Float,
            6 => Text,
            7 => Uuid,

            128 => List,
            129 => Dict,

            192 => DescVal,

            250 => TupleRef,
            251 => IdxAccess,
            252 => FieldAccess,
            253 => Variable,
            254 => Apply,
            255 => MaxTag,
            v => return Err(v),
        })
    }
}

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
// BitArr = 60,
// U8Arr = 61,
// I8Arr = 62,
// U16Arr = 63,
// I16Arr = 64,
// U32Arr = 65,
// I32Arr = 66,
// U64Arr = 67,
// I64Arr = 68,
// F16Arr = 69,
// F32Arr = 70,
// F64Arr = 71,
// C32Arr = 72,
// C64Arr = 73,
// C128Arr = 74,

#[derive(Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum Value<'a> {
    // evaluated
    Null,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    Uuid(Uuid),
    Text(Cow<'a, str>),
    // maybe evaluated
    List(Vec<Value<'a>>),
    Dict(BTreeMap<Cow<'a, str>, Value<'a>>),
    // not evaluated
    Variable(Cow<'a, str>),
    TupleRef(TableId, ColId),
    Apply(Cow<'a, str>, Vec<Value<'a>>),
    // TODO optimization: special case for small number of args (esp. 0, 1, 2)
    FieldAccess(Cow<'a, str>, Box<Value<'a>>),
    IdxAccess(usize, Box<Value<'a>>),
    DescSort(DescVal<'a>),
    // cannot exist
    EndSentinel,
}

// #[derive(Clone, PartialEq, Ord, PartialOrd, Eq)]
// pub struct DescVal<'a>(pub Box<Value<'a>>);
pub type DescVal<'a> = Reverse<Box<Value<'a>>>;

pub type StaticValue = Value<'static>;

impl<'a> Debug for Value<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value {{ {} }}", self)
    }
}

impl<'a> Value<'a> {
    #[inline]
    pub fn to_static(self) -> StaticValue {
        match self {
            Value::Null => Value::from(()),
            Value::Bool(b) => Value::from(b),
            Value::Int(i) => Value::from(i),
            Value::Float(f) => Value::from(f),
            Value::Uuid(u) => Value::from(u),
            Value::Text(t) => Value::from(t.into_owned()),
            Value::Variable(s) => Value::Variable(Cow::Owned(s.into_owned())),
            Value::List(l) => l
                .into_iter()
                .map(|v| v.to_static())
                .collect::<Vec<StaticValue>>()
                .into(),
            Value::Apply(op, args) => Value::Apply(
                Cow::Owned(op.into_owned()),
                args.into_iter()
                    .map(|v| v.to_static())
                    .collect::<Vec<StaticValue>>(),
            ),
            Value::Dict(d) => d
                .into_iter()
                .map(|(k, v)| (Cow::Owned(k.into_owned()), v.to_static()))
                .collect::<BTreeMap<Cow<'static, str>, StaticValue>>()
                .into(),
            Value::EndSentinel => panic!("Cannot process sentinel value"),
            Value::FieldAccess(field, value) => {
                Value::FieldAccess(Cow::from(field.into_owned()), value.to_static().into())
            }
            Value::IdxAccess(idx, value) => Value::IdxAccess(idx, value.to_static().into()),
            Value::TupleRef(tid, cid) => Value::TupleRef(tid, cid),
            Value::DescSort(Reverse(val)) => Value::DescSort(Reverse(val.to_static().into()))
        }
    }
    #[inline]
    pub fn is_evaluated(&self) -> bool {
        match self {
            Value::Null
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Uuid(_)
            | Value::Text(_)
            | Value::EndSentinel => true,
            Value::List(l) => l.iter().all(|v| v.is_evaluated()),
            Value::Dict(d) => d.values().all(|v| v.is_evaluated()),
            Value::Variable(_) => false,
            Value::Apply(_, _) => false,
            Value::FieldAccess(_, _) => false,
            Value::IdxAccess(_, _) => false,
            Value::TupleRef(_, _) => false,
            Value::DescSort(Reverse(v)) => v.is_evaluated()
        }
    }
    #[inline]
    pub fn from_pair(pair: pest::iterators::Pair<'a, Rule>) -> Result<Self> {
        PREC_CLIMBER.climb(pair.into_inner(), build_expr_primary, build_expr_infix)
    }

    #[inline]
    pub fn parse_str(s: &'a str) -> Result<Self> {
        let pair = Parser::parse(Rule::expr, s)?.next();
        let pair = pair.ok_or_else(|| CozoError::LogicError("Parsing value failed".to_string()))?;
        Value::from_pair(pair)
    }

    pub fn extract_relevant_tables<T: Iterator<Item=Self>>(
        data: T,
    ) -> Result<(Vec<Self>, Vec<TableId>)> {
        let mut coll = vec![];
        let mut res = Vec::with_capacity(data.size_hint().1.unwrap_or(0));
        for v in data {
            res.push(v.do_extract_relevant_tables(&mut coll)?);
        }
        Ok((res, coll))
    }

    fn do_extract_relevant_tables(self, coll: &mut Vec<TableId>) -> Result<Self> {
        Ok(match self {
            v @ (Value::Null
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Uuid(_)
            | Value::Text(_)
            | Value::Variable(_)) => v,
            Value::List(l) => Value::List(
                l.into_iter()
                    .map(|v| v.do_extract_relevant_tables(coll))
                    .collect::<Result<Vec<_>>>()?,
            ),
            Value::Dict(d) => Value::Dict(
                d.into_iter()
                    .map(|(k, v)| v.do_extract_relevant_tables(coll).map(|v| (k, v)))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ),
            Value::TupleRef(tid, cid) => {
                let pos = coll.iter().position(|id| id == &tid).unwrap_or_else(|| {
                    let olen = coll.len();
                    coll.push(tid);
                    olen
                });
                Value::TupleRef((false, pos).into(), cid)
            }
            Value::Apply(op, args) => Value::Apply(
                op,
                args.into_iter()
                    .map(|v| v.do_extract_relevant_tables(coll))
                    .collect::<Result<Vec<_>>>()?,
            ),
            Value::FieldAccess(field, arg) => {
                Value::FieldAccess(field, arg.do_extract_relevant_tables(coll)?.into())
            }
            Value::IdxAccess(idx, arg) => {
                Value::IdxAccess(idx, arg.do_extract_relevant_tables(coll)?.into())
            }
            Value::EndSentinel => {
                return Err(LogicError("Encountered end sentinel".to_string()));
            }
            Value::DescSort(Reverse(v)) => {
                return v.do_extract_relevant_tables(coll);
            }
        })
    }
}

impl From<()> for StaticValue {
    #[inline]
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<bool> for StaticValue {
    #[inline]
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for StaticValue {
    #[inline]
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for StaticValue {
    #[inline]
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}

impl From<f64> for StaticValue {
    #[inline]
    fn from(f: f64) -> Self {
        Value::Float(f.into())
    }
}

impl From<OrderedFloat<f64>> for StaticValue {
    #[inline]
    fn from(f: OrderedFloat<f64>) -> Self {
        Value::Float(f)
    }
}

impl<'a> From<&'a str> for Value<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Value::Text(Cow::Borrowed(s))
    }
}

impl From<String> for StaticValue {
    #[inline]
    fn from(s: String) -> Self {
        Value::Text(Cow::Owned(s))
    }
}

impl From<Uuid> for StaticValue {
    #[inline]
    fn from(u: Uuid) -> Self {
        Value::Uuid(u)
    }
}

impl<'a> From<Vec<Value<'a>>> for Value<'a> {
    #[inline]
    fn from(v: Vec<Value<'a>>) -> Self {
        Value::List(v)
    }
}

impl<'a> From<BTreeMap<Cow<'a, str>, Value<'a>>> for Value<'a> {
    #[inline]
    fn from(m: BTreeMap<Cow<'a, str>, Value<'a>>) -> Self {
        Value::Dict(m)
    }
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => {
                write!(f, "null")?;
            }
            Value::Bool(b) => {
                write!(f, "{}", if *b { "true" } else { "false" })?;
            }
            Value::Int(i) => {
                write!(f, "{}", i)?;
            }
            Value::Float(n) => {
                write!(f, "{}", n.into_inner())?;
            }
            Value::Uuid(u) => {
                write!(f, "{}", u)?;
            }
            Value::Text(t) => {
                f.write_char('"')?;
                for char in t.chars() {
                    match char {
                        '"' => {
                            f.write_str("\\\"")?;
                        }
                        '\\' => {
                            f.write_str("\\\\")?;
                        }
                        '/' => {
                            f.write_str("\\/")?;
                        }
                        '\x08' => {
                            f.write_str("\\b")?;
                        }
                        '\x0c' => {
                            f.write_str("\\f")?;
                        }
                        '\n' => {
                            f.write_str("\\n")?;
                        }
                        '\r' => {
                            f.write_str("\\r")?;
                        }
                        '\t' => {
                            f.write_str("\\t")?;
                        }
                        c => {
                            f.write_char(c)?;
                        }
                    }
                }
                f.write_char('"')?;
            }
            Value::List(l) => {
                f.write_char('[')?;
                let mut first = true;
                for v in l.iter() {
                    if !first {
                        f.write_char(',')?;
                    }
                    Display::fmt(v, f)?;
                    first = false;
                }
                f.write_char(']')?;
            }
            Value::Dict(d) => {
                f.write_char('{')?;
                let mut first = true;
                for (k, v) in d.iter() {
                    if !first {
                        f.write_char(',')?;
                    }
                    Display::fmt(&Value::Text(k.clone()), f)?;
                    f.write_char(':')?;
                    Display::fmt(v, f)?;
                    first = false;
                }
                f.write_char('}')?;
            }
            Value::Variable(s) => write!(f, "`{}`", s)?,
            Value::EndSentinel => write!(f, "Sentinel")?,
            Value::Apply(op, args) => {
                write!(
                    f,
                    "({} {})",
                    op,
                    args.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                )?;
            }
            Value::FieldAccess(field, value) => {
                write!(f, "(.{} {})", field, value)?;
            }
            Value::IdxAccess(idx, value) => {
                write!(f, "(.{} {})", idx, value)?;
            }
            Value::TupleRef(tid, cid) => {
                write!(
                    f,
                    "#{}{}.{}{}",
                    if tid.in_root { 'G' } else { 'L' },
                    tid.id,
                    if cid.is_key { 'K' } else { 'D' },
                    cid.id
                )?;
            }
            Value::DescSort(Reverse(v)) => {
                write!(f, "~{}", v)?;
            }
        }
        Ok(())
    }
}

lazy_static! {
    static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use Assoc::*;

        PrecClimber::new(vec![
            Operator::new(Rule::op_or, Left),
            Operator::new(Rule::op_and, Left),
            Operator::new(Rule::op_gt, Left)
                | Operator::new(Rule::op_lt, Left)
                | Operator::new(Rule::op_ge, Left)
                | Operator::new(Rule::op_le, Left),
            Operator::new(Rule::op_mod, Left),
            Operator::new(Rule::op_eq, Left) | Operator::new(Rule::op_ne, Left),
            Operator::new(Rule::op_add, Left)
                | Operator::new(Rule::op_sub, Left)
                | Operator::new(Rule::op_str_cat, Left),
            Operator::new(Rule::op_mul, Left) | Operator::new(Rule::op_div, Left),
            Operator::new(Rule::op_pow, Assoc::Right),
            Operator::new(Rule::op_coalesce, Assoc::Left),
        ])
    };
}

pub const OP_ADD: &str = "+";
pub const OP_STR_CAT: &str = "++";
pub const OP_SUB: &str = "-";
pub const OP_MUL: &str = "*";
pub const OP_DIV: &str = "/";
pub const OP_EQ: &str = "==";
pub const OP_NE: &str = "!=";
pub const OP_OR: &str = "||";
pub const OP_AND: &str = "&&";
pub const OP_MOD: &str = "%";
pub const OP_GT: &str = ">";
pub const OP_GE: &str = ">=";
pub const OP_LT: &str = "<";
pub const OP_LE: &str = "<=";
pub const OP_POW: &str = "**";
pub const OP_COALESCE: &str = "~~";
pub const OP_NEGATE: &str = "!";
pub const OP_MINUS: &str = "--";
pub const METHOD_IS_NULL: &str = "is_null";
pub const METHOD_NOT_NULL: &str = "not_null";
pub const METHOD_CONCAT: &str = "concat";
pub const METHOD_MERGE: &str = "merge";

fn build_expr_infix<'a>(
    lhs: Result<Value<'a>>,
    op: Pair<Rule>,
    rhs: Result<Value<'a>>,
) -> Result<Value<'a>> {
    let lhs = lhs?;
    let rhs = rhs?;
    let op = match op.as_rule() {
        Rule::op_add => OP_ADD,
        Rule::op_str_cat => OP_STR_CAT,
        Rule::op_sub => OP_SUB,
        Rule::op_mul => OP_MUL,
        Rule::op_div => OP_DIV,
        Rule::op_eq => OP_EQ,
        Rule::op_ne => OP_NE,
        Rule::op_or => OP_OR,
        Rule::op_and => OP_AND,
        Rule::op_mod => OP_MOD,
        Rule::op_gt => OP_GT,
        Rule::op_ge => OP_GE,
        Rule::op_lt => OP_LT,
        Rule::op_le => OP_LE,
        Rule::op_pow => OP_POW,
        Rule::op_coalesce => OP_COALESCE,
        _ => unreachable!(),
    };
    Ok(Value::Apply(op.into(), vec![lhs, rhs]))
}

fn build_expr_primary(pair: Pair<Rule>) -> Result<Value> {
    match pair.as_rule() {
        Rule::expr => build_expr_primary(pair.into_inner().next().unwrap()),
        Rule::term => {
            let mut pairs = pair.into_inner();
            let mut head = build_expr_primary(pairs.next().unwrap())?;
            for p in pairs {
                match p.as_rule() {
                    Rule::accessor => {
                        let accessor_key = p.into_inner().next().unwrap().as_str();
                        head = Value::FieldAccess(accessor_key.into(), head.into());
                    }
                    Rule::index_accessor => {
                        let accessor_key = p.into_inner().next().unwrap();
                        let accessor_idx = parse_int(accessor_key.as_str(), 10);
                        head = Value::IdxAccess(accessor_idx as usize, head.into());
                    }
                    Rule::call => {
                        let mut pairs = p.into_inner();
                        let method_name = pairs.next().unwrap().as_str();
                        let mut args = vec![head];
                        args.extend(pairs.map(Value::from_pair).collect::<Result<Vec<_>>>()?);
                        head = Value::Apply(method_name.into(), args);
                    }
                    _ => todo!(),
                }
            }
            Ok(head)
        }
        Rule::grouping => Value::from_pair(pair.into_inner().next().unwrap()),

        Rule::unary => {
            let mut inner = pair.into_inner();
            let p = inner.next().unwrap();
            let op = p.as_rule();
            let op = match op {
                Rule::term => return build_expr_primary(p),
                Rule::negate => OP_NEGATE,
                Rule::minus => OP_MINUS,
                _ => unreachable!(),
            };
            let term = build_expr_primary(inner.next().unwrap())?;
            Ok(Value::Apply(op.into(), vec![term]))
        }

        Rule::pos_int => Ok(Value::Int(pair.as_str().replace('_', "").parse::<i64>()?)),
        Rule::hex_pos_int => Ok(Value::Int(parse_int(pair.as_str(), 16))),
        Rule::octo_pos_int => Ok(Value::Int(parse_int(pair.as_str(), 8))),
        Rule::bin_pos_int => Ok(Value::Int(parse_int(pair.as_str(), 2))),
        Rule::dot_float | Rule::sci_float => Ok(Value::Float(
            pair.as_str().replace('_', "").parse::<f64>()?.into(),
        )),
        Rule::null => Ok(Value::Null),
        Rule::boolean => Ok(Value::Bool(pair.as_str() == "true")),
        Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => {
            Ok(Value::Text(Cow::Owned(parse_string(pair)?)))
        }
        Rule::list => {
            let mut spread_collected = vec![];
            let mut collected = vec![];
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::expr => collected.push(Value::from_pair(p)?),
                    Rule::spreading => {
                        let el = p.into_inner().next().unwrap();
                        let to_concat = Value::from_pair(el)?;
                        if !matches!(
                            to_concat,
                            Value::List(_)
                                | Value::Variable(_)
                                | Value::IdxAccess(_, _)
                                | Value::FieldAccess(_, _)
                                | Value::Apply(_, _)
                        ) {
                            return Err(CozoError::LogicError("Cannot spread".to_string()));
                        }
                        if !collected.is_empty() {
                            spread_collected.push(Value::List(collected));
                            collected = vec![];
                        }
                        spread_collected.push(to_concat);
                    }
                    _ => unreachable!(),
                }
            }
            if spread_collected.is_empty() {
                return Ok(Value::List(collected));
            }
            if !collected.is_empty() {
                spread_collected.push(Value::List(collected));
            }
            Ok(Value::Apply(METHOD_CONCAT.into(), spread_collected))
        }
        Rule::dict => {
            let mut spread_collected = vec![];
            let mut collected = BTreeMap::new();
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::dict_pair => {
                        let mut inner = p.into_inner();
                        let name = parse_string(inner.next().unwrap())?;
                        let val = Value::from_pair(inner.next().unwrap())?;
                        collected.insert(name.into(), val);
                    }
                    Rule::scoped_accessor => {
                        let name = parse_string(p.into_inner().next().unwrap())?;
                        let val = Value::FieldAccess(
                            name.clone().into(),
                            Value::Variable("_".into()).into(),
                        );
                        collected.insert(name.into(), val);
                    }
                    Rule::spreading => {
                        let el = p.into_inner().next().unwrap();
                        let to_concat = build_expr_primary(el)?;
                        if !matches!(
                            to_concat,
                            Value::Dict(_)
                                | Value::Variable(_)
                                | Value::IdxAccess(_, _)
                                | Value::FieldAccess(_, _)
                                | Value::Apply(_, _)
                        ) {
                            return Err(CozoError::LogicError("Cannot spread".to_string()));
                        }
                        if !collected.is_empty() {
                            spread_collected.push(Value::Dict(collected));
                            collected = BTreeMap::new();
                        }
                        spread_collected.push(to_concat);
                    }
                    _ => unreachable!(),
                }
            }

            if spread_collected.is_empty() {
                return Ok(Value::Dict(collected));
            }

            if !collected.is_empty() {
                spread_collected.push(Value::Dict(collected));
            }
            Ok(Value::Apply(METHOD_MERGE.into(), spread_collected))
        }
        Rule::param => Ok(Value::Variable(pair.as_str().into())),
        Rule::ident => Ok(Value::Variable(pair.as_str().into())),
        _ => {
            println!("Unhandled rule {:?}", pair.as_rule());
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;
    use pest::Parser as PestParser;

    fn parse_expr_from_str<S: AsRef<str>>(s: S) -> Result<StaticValue> {
        let pair = Parser::parse(Rule::expr, s.as_ref())
            .unwrap()
            .next()
            .unwrap();
        Value::from_pair(pair).map(|v| v.to_static())
    }

    #[test]
    fn raw_string() {
        println!("{:#?}", parse_expr_from_str(r#####"r#"x"#"#####))
    }

    #[test]
    fn unevaluated() {
        let val = parse_expr_from_str("a+b*c+d").unwrap();
        println!("{}", val);
        assert!(!val.is_evaluated());
    }

    #[test]
    fn parse_literals() {
        assert_eq!(parse_expr_from_str("1").unwrap(), Value::Int(1));
        assert_eq!(parse_expr_from_str("12_3").unwrap(), Value::Int(123));
        assert_eq!(parse_expr_from_str("0xaf").unwrap(), Value::Int(0xaf));
        assert_eq!(
            parse_expr_from_str("0xafcE_f").unwrap(),
            Value::Int(0xafcef)
        );
        assert_eq!(
            parse_expr_from_str("0o1234_567").unwrap(),
            Value::Int(0o1234567)
        );
        assert_eq!(
            parse_expr_from_str("0o0001234_567").unwrap(),
            Value::Int(0o1234567)
        );
        assert_eq!(
            parse_expr_from_str("0b101010").unwrap(),
            Value::Int(0b101010)
        );

        assert_eq!(
            parse_expr_from_str("0.0").unwrap(),
            Value::Float((0.).into())
        );
        assert_eq!(
            parse_expr_from_str("10.022_3").unwrap(),
            Value::Float(10.0223.into())
        );
        assert_eq!(
            parse_expr_from_str("10.022_3e-100").unwrap(),
            Value::Float(10.0223e-100.into())
        );

        assert_eq!(parse_expr_from_str("null").unwrap(), Value::Null);
        assert_eq!(parse_expr_from_str("true").unwrap(), Value::Bool(true));
        assert_eq!(parse_expr_from_str("false").unwrap(), Value::Bool(false));
        assert_eq!(
            parse_expr_from_str(r#""x \n \ty \"""#).unwrap(),
            Value::Text(Cow::Borrowed("x \n \ty \""))
        );
        assert_eq!(
            parse_expr_from_str(r#""x'""#).unwrap(),
            Value::Text("x'".into())
        );
        assert_eq!(
            parse_expr_from_str(r#"'"x"'"#).unwrap(),
            Value::Text(r##""x""##.into())
        );
        assert_eq!(
            parse_expr_from_str(r#####"r###"x"yz"###"#####).unwrap(),
            (Value::Text(r##"x"yz"##.into()))
        );
    }

    #[test]
    fn complex_cases() -> Result<()> {
        println!("{}", parse_expr_from_str("{}")?);
        println!("{}", parse_expr_from_str("{b:1,a,c:2,d,...e,}")?);
        println!("{}", parse_expr_from_str("{...a,...b,c:1,d:2,...e,f:3}")?);
        println!("{}", parse_expr_from_str("[]")?);
        println!("{}", parse_expr_from_str("[...a,...b,1,2,...e,3]")?);
        Ok(())
    }
}
