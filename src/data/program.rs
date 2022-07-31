use std::collections::BTreeMap;

use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};

use crate::data::attr::Attribute;
use crate::data::expr::{Expr, Op};
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::{EntityId, Validity};

#[derive(Clone, Debug, Default)]
pub enum Aggregation {
    #[default]
    Todo,
}

pub(crate) struct InputProgram {
    prog: BTreeMap<Keyword, Vec<InputRule>>,
}

pub(crate) struct StratifiedNormalFormProgram(Vec<NormalFormProgram>);

pub(crate) struct NormalFormProgram {
    prog: BTreeMap<Keyword, Vec<NormalFormRule>>,
}

pub(crate) struct StratifiedMagicProgram(Vec<MagicProgram>);

pub(crate) struct MagicProgram {
    prog: BTreeMap<MagicKeyword, Vec<MagicRule>>,
    keep_rules: Vec<Keyword>,
}

enum MagicKeyword {
    Muggle {
        name: SmartString<LazyCompact>,
    },
    Magic {
        name: SmartString<LazyCompact>,
        adornment: SmallVec<[bool; 8]>,
    },
    Input {
        to: SmartString<LazyCompact>,
        adornment: SmallVec<[bool; 8]>,
    },
    Sup {
        deriving: SmartString<LazyCompact>,
        rule_idx: u16,
        sup_idx: u16,
    },
}

pub(crate) struct InputRule {
    head: Vec<Keyword>,
    aggr: Vec<Option<Aggregation>>,
    body: Vec<InputAtom>,
    vld: Validity,
}

pub(crate) struct NormalFormRule {
    head: Vec<Keyword>,
    aggr: Vec<Option<Aggregation>>,
    body: Vec<NormalFormAtom>,
    vld: Validity,
}

pub(crate) struct MagicRule {
    head: Vec<Keyword>,
    aggr: Vec<Option<Aggregation>>,
    body: Vec<MagicAtom>,
    vld: Validity,
}

pub(crate) enum InputAtom {
    AttrTriple(InputAttrTripleAtom),
    Rule(InputRuleApplyAtom),
    Predicate(Expr),
    Negation(Box<InputAtom>),
    Conjunction(Vec<InputAtom>),
    Disjunction(Vec<InputAtom>),
    Unification(Unification),
}

pub(crate) enum NormalFormAtom {
    AttrTriple(NormalFormAttrTripleAtom),
    Rule(NormalFormRuleApplyAtom),
    Predicate(Expr),
    Negation(Box<NormalFormAtom>),
    Unification(Unification),
}

pub(crate) enum MagicAtom {
    AttrTriple(MagicAttrTripleAtom),
    Rule(MagicRuleApplyAtom),
    Predicate(Expr),
    Negation(Box<MagicAtom>),
    Unification(Unification),
}

#[derive(Clone, Debug)]
pub struct InputAttrTripleAtom {
    pub(crate) attr: Attribute,
    pub(crate) entity: InputTerm<EntityId>,
    pub(crate) value: InputTerm<DataValue>,
}

pub struct NormalFormAttrTripleAtom {
    attr: Attribute,
    entity: Keyword,
    value: Keyword,
}

pub(crate) struct MagicAttrTripleAtom {
    attr: Attribute,
    entity: Keyword,
    value: Keyword,
    entity_is_bound: bool,
    value_is_bound: bool,
}

#[derive(Clone, Debug)]
pub struct InputRuleApplyAtom {
    pub(crate) name: Keyword,
    pub(crate) args: Vec<InputTerm<DataValue>>,
}

pub struct NormalFormRuleApplyAtom {
    name: Keyword,
    args: Vec<Keyword>,
}

pub(crate) struct MagicRuleApplyAtom {
    name: MagicKeyword,
    args: Vec<Keyword>,
}

#[derive(Clone, Debug)]
pub enum InputTerm<T> {
    Var(Keyword),
    Const(T),
}

pub struct Unification {
    binding: Keyword,
    expr: Expr,
}
