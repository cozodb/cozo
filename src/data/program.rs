use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use anyhow::Result;
use smallvec::SmallVec;

use crate::data::attr::Attribute;
use crate::data::expr::Expr;
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

#[derive(Debug, Clone, Default)]
pub(crate) struct NormalFormProgram {
    pub(crate) prog: BTreeMap<Keyword, Vec<NormalFormRule>>,
}

#[derive(Debug, Clone)]
pub(crate) struct StratifiedMagicProgram(pub(crate) Vec<MagicProgram>);

#[derive(Debug, Clone)]
pub(crate) struct MagicProgram {
    pub(crate) prog: BTreeMap<MagicKeyword, Vec<MagicRule>>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum MagicKeyword {
    Muggle {
        inner: Keyword,
    },
    Magic {
        inner: Keyword,
        adornment: SmallVec<[bool; 8]>,
    },
    Input {
        inner: Keyword,
        adornment: SmallVec<[bool; 8]>,
    },
    Sup {
        inner: Keyword,
        adornment: SmallVec<[bool; 8]>,
        rule_idx: u16,
        sup_idx: u16,
    },
}

impl Debug for MagicKeyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MagicKeyword::Muggle { inner } => write!(f, "{}", inner.0),
            MagicKeyword::Magic { inner, adornment } => {
                write!(f, "{}|M", inner.0)?;
                for b in adornment {
                    if *b {
                        write!(f, "b")?
                    } else {
                        write!(f, "f")?
                    }
                }
                Ok(())
            }
            MagicKeyword::Input{ inner, adornment } => {
                write!(f, "{}|I", inner.0)?;
                for b in adornment {
                    if *b {
                        write!(f, "b")?
                    } else {
                        write!(f, "f")?
                    }
                }
                Ok(())
            }
            MagicKeyword::Sup { inner, adornment, rule_idx, sup_idx } => {
                write!(f, "{}|S.{}.{}", inner.0, rule_idx, sup_idx)?;
                for b in adornment {
                    if *b {
                        write!(f, "b")?
                    } else {
                        write!(f, "f")?
                    }
                }
                Ok(())
            }
        }
    }
}

impl MagicKeyword {
    pub(crate) fn as_keyword(&self) -> &Keyword {
        match self {
            MagicKeyword::Muggle { inner, .. }
            | MagicKeyword::Magic { inner, .. }
            | MagicKeyword::Input { inner, .. }
            | MagicKeyword::Sup { inner, .. } => inner,
        }
    }
    pub(crate) fn magic_adornment(&self) -> &[bool] {
        match self {
            MagicKeyword::Muggle { .. } => &[],
            MagicKeyword::Magic { adornment, .. }
            | MagicKeyword::Input { adornment, .. }
            | MagicKeyword::Sup { adornment, .. } => adornment,
        }
    }
    pub(crate) fn has_bound_adornment(&self) -> bool {
        self.magic_adornment().iter().any(|b| *b)
    }
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

impl MagicRule {
    pub(crate) fn contained_rules(&self) -> BTreeSet<MagicKeyword> {
        let mut coll = BTreeSet::new();
        for atom in self.body.iter() {
            match atom {
                MagicAtom::Rule(rule) | MagicAtom::NegatedRule(rule) => {
                    coll.insert(rule.name.clone());
                }
                _ => {}
            }
        }
        coll
    }
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
    NegatedAttrTriple(MagicAttrTripleAtom),
    NegatedRule(MagicRuleApplyAtom),
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
    pub(crate) attr: Attribute,
    pub(crate) entity: Keyword,
    pub(crate) value: Keyword,
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
    pub(crate) name: MagicKeyword,
    pub(crate) args: Vec<Keyword>,
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
        matches!(self.expr, Expr::Const(_))
    }
    pub(crate) fn bindings_in_expr(&self) -> BTreeSet<Keyword> {
        self.expr.bindings()
    }
}
