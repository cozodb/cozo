/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::mem;

use itertools::Itertools;
use miette::{bail, Diagnostic, Result};
use serde::de::{Error, Visitor};
use serde::{Deserializer, Serializer};
use smartstring::SmartString;
use thiserror::Error;

use crate::data::functions::*;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::parse::SourceSpan;

#[derive(Clone, PartialEq, Eq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) enum Expr {
    Binding {
        var: Symbol,
        tuple_pos: Option<usize>,
    },
    Const {
        val: DataValue,
        #[serde(skip)]
        span: SourceSpan,
    },
    Apply {
        op: &'static Op,
        args: Box<[Expr]>,
        #[serde(skip)]
        span: SourceSpan,
    },
    Cond {
        clauses: Vec<(Expr, Expr)>,
        #[serde(skip)]
        span: SourceSpan,
    },
    Try {
        clauses: Vec<Expr>,
        #[serde(skip)]
        span: SourceSpan,
    },
}

impl Debug for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Binding { var, .. } => {
                write!(f, "{}", var.name)
            }
            Expr::Const { val, .. } => {
                write!(f, "{}", val)
            }
            Expr::Apply { op, args, .. } => {
                let mut writer =
                    f.debug_tuple(op.name.strip_prefix("OP_").unwrap().to_lowercase().as_str());
                for arg in args.iter() {
                    writer.field(arg);
                }
                writer.finish()
            }
            Expr::Cond { clauses, .. } => {
                let mut writer = f.debug_tuple("cond");
                for (cond, expr) in clauses {
                    writer.field(cond);
                    writer.field(expr);
                }
                writer.finish()
            }
            Expr::Try { clauses, .. } => {
                let mut writer = f.debug_tuple("try");
                for clause in clauses {
                    writer.field(clause);
                }
                writer.finish()
            }
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Found value {1:?} where a boolean value is expected")]
#[diagnostic(code(eval::predicate_not_bool))]
struct PredicateTypeError(#[label] SourceSpan, DataValue);

#[derive(Debug, Error, Diagnostic)]
#[error("Cannot build entity ID from {0:?}")]
#[diagnostic(code(parser::bad_eid))]
#[diagnostic(help("Entity ID should be an integer satisfying certain constraints"))]
struct BadEntityId(DataValue, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Evaluation of expression failed")]
#[diagnostic(code(eval::throw))]
struct EvalRaisedError(#[label] SourceSpan, #[help] String);

impl Expr {
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            Expr::Binding { var, .. } => var.span,
            Expr::Const { span, .. }
            | Expr::Apply { span, .. }
            | Expr::Cond { span, .. }
            | Expr::Try { span, .. } => *span,
        }
    }
    pub(crate) fn get_binding(&self) -> Option<&Symbol> {
        if let Expr::Binding { var, .. } = self {
            Some(var)
        } else {
            None
        }
    }
    pub(crate) fn get_const(&self) -> Option<&DataValue> {
        if let Expr::Const { val, .. } = self {
            Some(val)
        } else {
            None
        }
    }
    pub(crate) fn build_equate(exprs: Vec<Expr>, span: SourceSpan) -> Self {
        Expr::Apply {
            op: &OP_EQ,
            args: exprs.into(),
            span,
        }
    }
    pub(crate) fn build_is_in(exprs: Vec<Expr>, span: SourceSpan) -> Self {
        Expr::Apply {
            op: &OP_IS_IN,
            args: exprs.into(),
            span,
        }
    }
    pub(crate) fn negate(self, span: SourceSpan) -> Self {
        Expr::Apply {
            op: &OP_NEGATE,
            args: Box::new([self]),
            span,
        }
    }
    pub(crate) fn to_conjunction(&self) -> Vec<Self> {
        match self {
            Expr::Apply { op, args, .. } if **op == OP_AND => args.to_vec(),
            v => vec![v.clone()],
        }
    }
    pub(crate) fn fill_binding_indices(
        &mut self,
        binding_map: &BTreeMap<Symbol, usize>,
    ) -> Result<()> {
        match self {
            Expr::Binding { var, tuple_pos, .. } => {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Cannot find binding {0}")]
                #[diagnostic(code(eval::bad_binding))]
                #[diagnostic(help("This could indicate a system problem"))]
                struct BadBindingError(String, #[label] SourceSpan);

                let found_idx = *binding_map
                    .get(var)
                    .ok_or_else(|| BadBindingError(var.to_string(), var.span))?;
                *tuple_pos = Some(found_idx)
            }
            Expr::Const { .. } => {}
            Expr::Apply { args, .. } => {
                for arg in args.iter_mut() {
                    arg.fill_binding_indices(binding_map)?;
                }
            }
            Expr::Cond { clauses, .. } => {
                for (cond, val) in clauses {
                    cond.fill_binding_indices(binding_map)?;
                    val.fill_binding_indices(binding_map)?;
                }
            }
            Expr::Try { clauses, .. } => {
                for clause in clauses {
                    clause.fill_binding_indices(binding_map)?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn binding_indices(&self) -> BTreeSet<usize> {
        let mut ret = BTreeSet::default();
        self.do_binding_indices(&mut ret);
        ret
    }
    fn do_binding_indices(&self, coll: &mut BTreeSet<usize>) {
        match self {
            Expr::Binding { tuple_pos, .. } => {
                if let Some(idx) = tuple_pos {
                    coll.insert(*idx);
                }
            }
            Expr::Const { .. } => {}
            Expr::Apply { args, .. } => {
                for arg in args.iter() {
                    arg.do_binding_indices(coll);
                }
            }
            Expr::Cond { clauses, .. } => {
                for (cond, val) in clauses {
                    cond.do_binding_indices(coll);
                    val.do_binding_indices(coll)
                }
            }
            Expr::Try { clauses, .. } => {
                for clause in clauses {
                    clause.do_binding_indices(coll)
                }
            }
        }
    }
    pub(crate) fn eval_to_const(mut self) -> Result<DataValue> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Expression contains unevaluated constant")]
        #[diagnostic(code(eval::not_constant))]
        struct NotConstError;

        self.partial_eval()?;
        match self {
            Expr::Const { val, .. } => Ok(val),
            _ => bail!(NotConstError),
        }
    }
    pub(crate) fn partial_eval(&mut self) -> Result<()> {
        if let Expr::Apply { args, span, .. } = self {
            let span = *span;
            let mut all_evaluated = true;
            for arg in args.iter_mut() {
                arg.partial_eval()?;
                all_evaluated = all_evaluated && matches!(arg, Expr::Const { .. });
            }
            if all_evaluated {
                let result = self.eval(&Tuple(vec![]))?;
                mem::swap(self, &mut Expr::Const { val: result, span });
            }
            // nested not's can accumulate during conversion to normal form
            if let Expr::Apply {
                op: op1,
                args: arg1,
                ..
            } = self
            {
                if op1.name == OP_NEGATE.name {
                    if let Some(Expr::Apply {
                        op: op2,
                        args: arg2,
                        ..
                    }) = arg1.first()
                    {
                        if op2.name == OP_NEGATE.name {
                            let mut new_self = arg2[0].clone();
                            mem::swap(self, &mut new_self);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn bindings(&self) -> BTreeSet<Symbol> {
        let mut ret = BTreeSet::new();
        self.collect_bindings(&mut ret);
        ret
    }
    pub(crate) fn collect_bindings(&self, coll: &mut BTreeSet<Symbol>) {
        match self {
            Expr::Binding { var, .. } => {
                coll.insert(var.clone());
            }
            Expr::Const { .. } => {}
            Expr::Apply { args, .. } => {
                for arg in args.iter() {
                    arg.collect_bindings(coll)
                }
            }
            Expr::Cond { clauses, .. } => {
                for (cond, val) in clauses {
                    cond.collect_bindings(coll);
                    val.collect_bindings(coll)
                }
            }
            Expr::Try { clauses, .. } => {
                for clause in clauses {
                    clause.collect_bindings(coll);
                }
            }
        }
    }
    pub(crate) fn eval(&self, bindings: &Tuple) -> Result<DataValue> {
        match self {
            Expr::Binding { var, tuple_pos, .. } => match tuple_pos {
                None => {
                    #[derive(Error, Diagnostic, Debug)]
                    #[error("The variable '{0}' is unbound")]
                    #[diagnostic(code(eval::unbound))]
                    struct UnboundVariableError(String, #[label] SourceSpan);

                    bail!(UnboundVariableError(var.name.to_string(), var.span))
                }
                Some(i) => {
                    #[derive(Error, Diagnostic, Debug)]
                    #[error("The tuple bound by variable '{0}' is too short: index is {1}, length is {2}")]
                    #[diagnostic(help("This is definitely a bug. Please report it."))]
                    #[diagnostic(code(eval::unbound))]
                    struct TupleTooShortError(String, usize, usize, #[label] SourceSpan);

                    Ok(bindings
                        .0
                        .get(*i)
                        .ok_or_else(|| {
                            TupleTooShortError(var.name.to_string(), *i, bindings.0.len(), var.span)
                        })?
                        .clone())
                }
            },
            Expr::Const { val, .. } => Ok(val.clone()),
            Expr::Apply { op, args, .. } => {
                let args: Box<[DataValue]> = args.iter().map(|v| v.eval(bindings)).try_collect()?;
                Ok((op.inner)(&args)
                    .map_err(|err| EvalRaisedError(self.span(), err.to_string()))?)
            }
            Expr::Cond { clauses, .. } => {
                for (cond, val) in clauses {
                    let cond_val = cond.eval(bindings)?;
                    let cond_val = cond_val
                        .get_bool()
                        .ok_or_else(|| PredicateTypeError(cond.span(), cond_val))?;

                    if cond_val {
                        return val.eval(bindings);
                    }
                }
                Ok(DataValue::Null)
            }
            Expr::Try { clauses, .. } => {
                if clauses.is_empty() {
                    Ok(DataValue::Null)
                } else {
                    for item in clauses.iter().take(clauses.len() - 1) {
                        let res = item.eval(bindings);
                        if res.is_ok() {
                            return res;
                        }
                    }
                    clauses[clauses.len() - 1].eval(bindings)
                }
            }
        }
    }
    pub(crate) fn eval_pred(&self, bindings: &Tuple) -> Result<bool> {
        match self.eval(bindings)? {
            DataValue::Bool(b) => Ok(b),
            v => {
                bail!(PredicateTypeError(self.span(), v))
            }
        }
    }
    pub(crate) fn extract_bound(&self, target: &Symbol) -> Result<ValueRange> {
        Ok(match self {
            Expr::Binding { .. } | Expr::Const { .. } | Expr::Cond { .. } | Expr::Try { .. } => {
                ValueRange::default()
            }
            Expr::Apply { op, args, .. } => match op.name {
                n if n == OP_GE.name || n == OP_GT.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                let tar_val = match val.get_int() {
                                    Some(i) => DataValue::from(i),
                                    None => val.clone(),
                                };
                                return Ok(ValueRange::lower_bound(tar_val));
                            }
                        }
                    }
                    if let Some(symb) = args[1].get_binding() {
                        if let Some(val) = args[0].get_const() {
                            if target == symb {
                                let tar_val = match val.get_float() {
                                    Some(i) => DataValue::from(i),
                                    None => val.clone(),
                                };
                                return Ok(ValueRange::upper_bound(tar_val));
                            }
                        }
                    }
                    ValueRange::default()
                }
                n if n == OP_LE.name || n == OP_LT.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                let tar_val = match val.get_float() {
                                    Some(i) => DataValue::from(i),
                                    None => val.clone(),
                                };

                                return Ok(ValueRange::upper_bound(tar_val));
                            }
                        }
                    }
                    if let Some(symb) = args[1].get_binding() {
                        if let Some(val) = args[0].get_const() {
                            if target == symb {
                                let tar_val = match val.get_int() {
                                    Some(i) => DataValue::from(i),
                                    None => val.clone(),
                                };

                                return Ok(ValueRange::lower_bound(tar_val));
                            }
                        }
                    }
                    ValueRange::default()
                }
                n if n == OP_STARTS_WITH.name => {
                    if let Some(symb) = args[0].get_binding() {
                        if let Some(val) = args[1].get_const() {
                            if target == symb {
                                let s = val.get_string().ok_or_else(|| {
                                    #[derive(Debug, Error, Diagnostic)]
                                    #[error("Cannot prefix scan with {0:?}")]
                                    #[diagnostic(code(eval::bad_string_range_scan))]
                                    #[diagnostic(help("A string argument is required"))]
                                    struct StrRangeScanError(DataValue, #[label] SourceSpan);

                                    StrRangeScanError(val.clone(), symb.span)
                                })?;
                                let lower = DataValue::Str(SmartString::from(s));
                                // let lower = DataValue::Str(s.to_string());
                                let mut upper = SmartString::from(s);
                                // let mut upper = s.to_string();
                                upper.push(LARGEST_UTF_CHAR);
                                let upper = DataValue::Str(upper);
                                return Ok(ValueRange::new(lower, upper));
                            }
                        }
                    }
                    ValueRange::default()
                }
                _ => ValueRange::default(),
            },
        })
    }
}

pub(crate) fn compute_bounds(
    filters: &[Expr],
    symbols: &[Symbol],
) -> Result<(Vec<DataValue>, Vec<DataValue>)> {
    let mut lowers = vec![];
    let mut uppers = vec![];
    for current in symbols {
        let mut cur_bound = ValueRange::default();
        for filter in filters {
            let nxt = filter.extract_bound(current)?;
            cur_bound = cur_bound.merge(nxt);
        }
        lowers.push(cur_bound.lower);
        uppers.push(cur_bound.upper);
    }

    Ok((lowers, uppers))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueRange {
    pub(crate) lower: DataValue,
    pub(crate) upper: DataValue,
}

impl ValueRange {
    fn merge(self, other: Self) -> Self {
        let lower = max(self.lower, other.lower);
        let upper = min(self.upper, other.upper);
        if lower > upper {
            Self::null()
        } else {
            Self { lower, upper }
        }
    }
    fn null() -> Self {
        Self {
            lower: DataValue::Bot,
            upper: DataValue::Bot,
        }
    }
    fn new(lower: DataValue, upper: DataValue) -> Self {
        Self { lower, upper }
    }
    fn lower_bound(val: DataValue) -> Self {
        Self {
            lower: val,
            upper: DataValue::Bot,
        }
    }
    fn upper_bound(val: DataValue) -> Self {
        Self {
            lower: DataValue::Null,
            upper: val,
        }
    }
}

impl Default for ValueRange {
    fn default() -> Self {
        Self {
            lower: DataValue::Null,
            upper: DataValue::Bot,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Op {
    pub(crate) name: &'static str,
    pub(crate) min_arity: usize,
    pub(crate) vararg: bool,
    pub(crate) inner: fn(&[DataValue]) -> Result<DataValue>,
}

impl serde::Serialize for &'_ Op {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name)
    }
}

impl<'de> serde::Deserialize<'de> for &'static Op {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(OpVisitor)
    }
}

struct OpVisitor;

impl<'de> Visitor<'de> for OpVisitor {
    type Value = &'static Op;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("name of the op")
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: Error,
    {
        let name = v.strip_prefix("OP_").unwrap().to_ascii_lowercase();
        get_op(&name).ok_or_else(|| E::custom(format!("op not found in serialized data: {}", v)))
    }
}

impl PartialEq for Op {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Op {}

impl Debug for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

pub(crate) fn get_op(name: &str) -> Option<&'static Op> {
    Some(match name {
        "list" => &OP_LIST,
        "add" => &OP_ADD,
        "sub" => &OP_SUB,
        "mul" => &OP_MUL,
        "div" => &OP_DIV,
        "minus" => &OP_MINUS,
        "abs" => &OP_ABS,
        "signum" => &OP_SIGNUM,
        "floor" => &OP_FLOOR,
        "ceil" => &OP_CEIL,
        "round" => &OP_ROUND,
        "mod" => &OP_MOD,
        "max" => &OP_MAX,
        "min" => &OP_MIN,
        "pow" => &OP_POW,
        "exp" => &OP_EXP,
        "exp2" => &OP_EXP2,
        "ln" => &OP_LN,
        "log2" => &OP_LOG2,
        "log10" => &OP_LOG10,
        "sin" => &OP_SIN,
        "cos" => &OP_COS,
        "tan" => &OP_TAN,
        "asin" => &OP_ASIN,
        "acos" => &OP_ACOS,
        "atan" => &OP_ATAN,
        "atan2" => &OP_ATAN2,
        "sinh" => &OP_SINH,
        "cosh" => &OP_COSH,
        "tanh" => &OP_TANH,
        "asinh" => &OP_ASINH,
        "acosh" => &OP_ACOSH,
        "atanh" => &OP_ATANH,
        "eq" => &OP_EQ,
        "neq" => &OP_NEQ,
        "gt" => &OP_GT,
        "ge" => &OP_GE,
        "lt" => &OP_LT,
        "le" => &OP_LE,
        "or" => &OP_OR,
        "and" => &OP_AND,
        "negate" => &OP_NEGATE,
        "bit_and" => &OP_BIT_AND,
        "bit_or" => &OP_BIT_OR,
        "bit_not" => &OP_BIT_NOT,
        "bit_xor" => &OP_BIT_XOR,
        "pack_bits" => &OP_PACK_BITS,
        "unpack_bits" => &OP_UNPACK_BITS,
        "concat" => &OP_CONCAT,
        "str_includes" => &OP_STR_INCLUDES,
        "lowercase" => &OP_LOWERCASE,
        "uppercase" => &OP_UPPERCASE,
        "trim" => &OP_TRIM,
        "trim_start" => &OP_TRIM_START,
        "trim_end" => &OP_TRIM_END,
        "starts_with" => &OP_STARTS_WITH,
        "ends_with" => &OP_ENDS_WITH,
        "is_null" => &OP_IS_NULL,
        "is_int" => &OP_IS_INT,
        "is_float" => &OP_IS_FLOAT,
        "is_num" => &OP_IS_NUM,
        "is_string" => &OP_IS_STRING,
        "is_list" => &OP_IS_LIST,
        "is_bytes" => &OP_IS_BYTES,
        "is_in" => &OP_IS_IN,
        "is_finite" => &OP_IS_FINITE,
        "is_infinite" => &OP_IS_INFINITE,
        "is_nan" => &OP_IS_NAN,
        "is_uuid" => &OP_IS_UUID,
        "length" => &OP_LENGTH,
        "sorted" => &OP_SORTED,
        "reverse" => &OP_REVERSE,
        "append" => &OP_APPEND,
        "prepend" => &OP_PREPEND,
        "unicode_normalize" => &OP_UNICODE_NORMALIZE,
        "haversine" => &OP_HAVERSINE,
        "haversine_deg_input" => &OP_HAVERSINE_DEG_INPUT,
        "deg_to_rad" => &OP_DEG_TO_RAD,
        "rad_to_deg" => &OP_RAD_TO_DEG,
        "get" => &OP_GET,
        "maybe_get" => &OP_MAYBE_GET,
        "chars" => &OP_CHARS,
        "from_substrings" => &OP_FROM_SUBSTRINGS,
        "slice" => &OP_SLICE,
        "regex_matches" => &OP_REGEX_MATCHES,
        "regex_replace" => &OP_REGEX_REPLACE,
        "regex_replace_all" => &OP_REGEX_REPLACE_ALL,
        "regex_extract" => &OP_REGEX_EXTRACT,
        "regex_extract_first" => &OP_REGEX_EXTRACT_FIRST,
        "encode_base64" => &OP_ENCODE_BASE64,
        "decode_base64" => &OP_DECODE_BASE64,
        "first" => &OP_FIRST,
        "last" => &OP_LAST,
        "chunks" => &OP_CHUNKS,
        "chunks_exact" => &OP_CHUNKS_EXACT,
        "windows" => &OP_WINDOWS,
        "to_float" => &OP_TO_FLOAT,
        "to_string" => &OP_TO_STRING,
        "rand_float" => &OP_RAND_FLOAT,
        "rand_bernoulli" => &OP_RAND_BERNOULLI,
        "rand_int" => &OP_RAND_INT,
        "rand_choose" => &OP_RAND_CHOOSE,
        "assert" => &OP_ASSERT,
        "union" => &OP_UNION,
        "intersection" => &OP_INTERSECTION,
        "difference" => &OP_DIFFERENCE,
        "to_uuid" => &OP_TO_UUID,
        "to_bool" => &OP_TO_BOOL,
        "rand_uuid_v1" => &OP_RAND_UUID_V1,
        "rand_uuid_v4" => &OP_RAND_UUID_V4,
        "uuid_timestamp" => &OP_UUID_TIMESTAMP,
        "now" => &OP_NOW,
        "format_timestamp" => &OP_FORMAT_TIMESTAMP,
        "parse_timestamp" => &OP_PARSE_TIMESTAMP,
        _ => return None,
    })
}

impl Op {
    pub(crate) fn post_process_args(&self, args: &mut [Expr]) {
        if self.name.starts_with("OP_REGEX_") {
            args[1] = Expr::Apply {
                op: &OP_REGEX,
                args: [args[1].clone()].into(),
                span: args[1].span(),
            }
        }
    }
}
