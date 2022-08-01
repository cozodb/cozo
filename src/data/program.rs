use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use itertools::Itertools;
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};

use crate::data::attr::Attribute;
use crate::data::expr::{Expr, Op};
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::{EntityId, Validity};

#[derive(Default)]
pub(crate) struct TempKwGen {
    last_id: u32,
}

impl TempKwGen {
    pub(crate) fn next(&mut self) -> Keyword {
        self.last_id += 1;
        Keyword::from(&format!("*{}", self.last_id) as &str)
    }
}

#[derive(Clone, Debug, Default)]
pub enum Aggregation {
    #[default]
    Todo,
}

#[derive(Debug, Clone)]
pub(crate) struct InputProgram {
    pub(crate) prog: BTreeMap<Keyword, Vec<InputRule>>,
}

impl InputProgram {
    pub(crate) fn to_normalized_program(self) -> Result<NormalFormProgram> {
        let mut prog: BTreeMap<_, _> = Default::default();
        for (k, rules) in self.prog {
            let mut collected_rules = vec![];
            for rule in rules {
                let normalized_body =
                    InputAtom::Conjunction(rule.body).disjunctive_normal_form()?;
                for conj in normalized_body.0 {
                    let normalized_rule = NormalFormRule {
                        head: rule.head.clone(),
                        aggr: rule.aggr.clone(),
                        body: conj.0,
                        vld: rule.vld,
                    };
                    collected_rules.push(normalized_rule.convert_to_well_ordered_rule()?);
                }
            }
            prog.insert(k, collected_rules);
        }
        Ok(NormalFormProgram { prog })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StratifiedNormalFormProgram(pub(crate) Vec<NormalFormProgram>);

impl StratifiedNormalFormProgram {
    pub(crate) fn magic_sets_rewrite(self) -> Result<StratifiedMagicProgram> {
        Ok(StratifiedMagicProgram(
            self.0
                .into_iter()
                .map(|p| p.magic_sets_rewrite())
                .try_collect()?,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NormalFormProgram {
    pub(crate) prog: BTreeMap<Keyword, Vec<NormalFormRule>>,
}

impl NormalFormProgram {
    pub(crate) fn magic_sets_rewrite(self) -> Result<MagicProgram> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StratifiedMagicProgram(Vec<MagicProgram>);

#[derive(Debug, Clone)]
pub(crate) struct MagicProgram {
    prog: BTreeMap<MagicKeyword, Vec<MagicRule>>,
    keep_rules: Vec<Keyword>,
}

#[derive(Clone, Debug)]
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

#[derive(Debug, Clone)]
pub(crate) struct InputRule {
    pub(crate) head: Vec<Keyword>,
    pub(crate) aggr: Vec<Option<Aggregation>>,
    pub(crate) body: Vec<InputAtom>,
    pub(crate) vld: Validity,
}

#[derive(Debug, Clone)]
pub(crate) struct NormalFormRule {
    pub(crate) head: Vec<Keyword>,
    pub(crate) aggr: Vec<Option<Aggregation>>,
    pub(crate) body: Vec<NormalFormAtom>,
    pub(crate) vld: Validity,
}

#[derive(Debug, Clone)]
pub(crate) struct MagicRule {
    pub(crate) head: Vec<Keyword>,
    pub(crate) aggr: Vec<Option<Aggregation>>,
    pub(crate) body: Vec<MagicAtom>,
    pub(crate) vld: Validity,
}

#[derive(Debug, Clone)]
pub(crate) enum InputAtom {
    AttrTriple(InputAttrTripleAtom),
    Rule(InputRuleApplyAtom),
    Predicate(Expr),
    Negation(Box<InputAtom>),
    Conjunction(Vec<InputAtom>),
    Disjunction(Vec<InputAtom>),
    Unification(Unification),
}

impl InputAtom {
    pub(crate) fn is_negation(&self) -> bool {
        matches!(self, InputAtom::Negation(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum NormalFormAtom {
    AttrTriple(NormalFormAttrTripleAtom),
    Rule(NormalFormRuleApplyAtom),
    NegatedAttrTriple(NormalFormAttrTripleAtom),
    NegatedRule(NormalFormRuleApplyAtom),
    Predicate(Expr),
    Unification(Unification),
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct NormalFormAttrTripleAtom {
    pub(crate) attr: Attribute,
    pub(crate) entity: Keyword,
    pub(crate) value: Keyword,
}

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug)]
pub struct NormalFormRuleApplyAtom {
    pub(crate) name: Keyword,
    pub(crate) args: Vec<Keyword>,
}

#[derive(Clone, Debug)]
pub(crate) struct MagicRuleApplyAtom {
    name: MagicKeyword,
    args: Vec<Keyword>,
}

#[derive(Clone, Debug)]
pub enum InputTerm<T> {
    Var(Keyword),
    Const(T),
}

#[derive(Clone, Debug)]
pub struct Unification {
    pub(crate) binding: Keyword,
    pub(crate) expr: Expr,
}

impl Unification {
    pub(crate) fn is_const(&self) -> bool {
        matches!(self, Expr::Const(_))
    }
    pub(crate) fn bindings_in_expr(&self) -> BTreeSet<Keyword> {
        self.expr.bindings()
    }
}
