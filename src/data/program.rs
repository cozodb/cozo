use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::fmt::{Debug, Formatter};

use either::{Left, Right};
use miette::{Diagnostic, ensure, Result};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::algo::{AlgoHandle, AlgoNotFoundError};
use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::symb::{PROG_ENTRY, Symbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::relation::RelationMetadata;
use crate::runtime::transact::SessionTx;

pub(crate) type ConstRules = BTreeMap<MagicSymbol, ConstRule>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConstRule {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) data: Vec<Tuple>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum QueryAssertion {
    AssertNone(SourceSpan),
    AssertSome(SourceSpan),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct QueryOutOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) timeout: Option<u64>,
    pub(crate) sorters: Vec<(Symbol, SortDir)>,
    pub(crate) store_relation: Option<(RelationMetadata, RelationOp)>,
    pub(crate) assertion: Option<QueryAssertion>,
}

impl Default for QueryOutOptions {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
            timeout: None,
            sorters: vec![],
            store_relation: None,
            assertion: None,
        }
    }
}

impl QueryOutOptions {
    pub(crate) fn num_to_take(&self) -> Option<usize> {
        match (self.limit, self.offset) {
            (None, _) => None,
            (Some(i), None) => Some(i),
            (Some(i), Some(j)) => Some(i + j),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum SortDir {
    Asc,
    Dsc,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum RelationOp {
    Create,
    ReDerive,
    Put,
    Retract,
}

#[derive(Default)]
pub(crate) struct TempSymbGen {
    last_id: u32,
}

impl TempSymbGen {
    pub(crate) fn next(&mut self, span: SourceSpan) -> Symbol {
        self.last_id += 1;
        Symbol::new(&format!("*{}", self.last_id) as &str, span)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InputRulesOrAlgo {
    Rules { rules: Vec<InputRule> },
    Algo { algo: AlgoApply },
}

impl InputRulesOrAlgo {
    pub(crate) fn first_span(&self) -> SourceSpan {
        match self {
            InputRulesOrAlgo::Rules { rules, .. } => rules[0].span,
            InputRulesOrAlgo::Algo { algo, .. } => algo.span,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AlgoApply {
    pub(crate) algo: AlgoHandle,
    pub(crate) rule_args: Vec<AlgoRuleArg>,
    pub(crate) options: BTreeMap<SmartString<LazyCompact>, Expr>,
    pub(crate) head: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

impl AlgoApply {
    pub(crate) fn arity(&self) -> Option<usize> {
        self.algo.arity(Left(&self.rule_args), &self.options)
    }
}

impl Debug for AlgoApply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgoApply")
            .field("algo", &self.algo.name)
            .field("rules", &self.rule_args)
            .field("options", &self.options)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct MagicAlgoApply {
    pub(crate) algo: AlgoHandle,
    pub(crate) rule_args: Vec<MagicAlgoRuleArg>,
    pub(crate) options: BTreeMap<SmartString<LazyCompact>, Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Cannot find a required named option '{name}' for '{algo_name}'")]
#[diagnostic(code(algo::arg_not_found))]
pub(crate) struct AlgoOptionNotFoundError {
    name: String,
    #[label]
    span: SourceSpan,
    algo_name: String,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Wrong value for option '{name}' of '{algo_name}'")]
#[diagnostic(code(algo::arg_wrong))]
pub(crate) struct WrongAlgoOptionError {
    pub(crate) name: String,
    #[label]
    pub(crate) span: SourceSpan,
    pub(crate) algo_name: String,
    #[help]
    pub(crate) help: String,
}

impl MagicAlgoApply {
    pub(crate) fn arity(&self) -> Option<usize> {
        self.algo.arity(Right(&self.rule_args), &self.options)
    }
    pub(crate) fn relation_with_min_len(
        &self,
        idx: usize,
        len: usize,
        tx: &SessionTx,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<&MagicAlgoRuleArg> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Input relation to algorithm has insufficient arity")]
        #[diagnostic(help("Arity should be at least {0} but is {1}"))]
        #[diagnostic(code(algo::input_relation_bad_arity))]
        struct InputRelationArityError(usize, usize, #[label] SourceSpan);

        let rel = self.relation(idx)?;
        let arity = rel.arity(tx, stores)?;
        ensure!(
            arity >= len,
            InputRelationArityError(len, arity, rel.span())
        );
        Ok(rel)
    }
    pub(crate) fn relation(&self, idx: usize) -> Result<&MagicAlgoRuleArg> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Cannot find a required positional argument at index {idx} for '{algo_name}'")]
        #[diagnostic(code(algo::not_enough_args))]
        pub(crate) struct AlgoNotEnoughRelationError {
            idx: usize,
            #[label]
            span: SourceSpan,
            algo_name: String,
        }

        Ok(self
            .rule_args
            .get(idx)
            .ok_or_else(|| AlgoNotEnoughRelationError {
                idx,
                span: self.span,
                algo_name: self.algo.name.to_string(),
            })?)
    }
    pub(crate) fn expr_option(&self, name: &str, default: Option<Expr>) -> Result<Expr> {
        match self.options.get(name) {
            Some(ex) => Ok(ex.clone()),
            None => match default {
                Some(ex) => Ok(ex),
                None => Err(AlgoOptionNotFoundError {
                    name: name.to_string(),
                    span: self.span,
                    algo_name: self.algo.name.to_string(),
                }
                    .into()),
            },
        }
    }
    pub(crate) fn pos_integer_option(&self, name: &str, default: Option<usize>) -> Result<usize> {
        match self.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => match n.get_int() {
                    Some(i) => {
                        ensure!(
                            i > 0,
                            WrongAlgoOptionError {
                                name: name.to_string(),
                                span: v.span(),
                                algo_name: self.algo.name.to_string(),
                                help: "a positive integer is required".to_string(),
                            }
                        );
                        Ok(i as usize)
                    }
                    None => Err(AlgoOptionNotFoundError {
                        name: name.to_string(),
                        span: self.span,
                        algo_name: self.algo.name.to_string(),
                    }
                        .into()),
                },
                _ => Err(WrongAlgoOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    algo_name: self.algo.name.to_string(),
                    help: "a positive integer is required".to_string(),
                }
                    .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(AlgoOptionNotFoundError {
                    name: name.to_string(),
                    span: self.span,
                    algo_name: self.algo.name.to_string(),
                }
                    .into()),
            },
        }
    }
    pub(crate) fn non_neg_integer_option(
        &self,
        name: &str,
        default: Option<usize>,
    ) -> Result<usize> {
        match self.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => match n.get_int() {
                    Some(i) => {
                        ensure!(
                            i >= 0,
                            WrongAlgoOptionError {
                                name: name.to_string(),
                                span: v.span(),
                                algo_name: self.algo.name.to_string(),
                                help: "a non-negative integer is required".to_string(),
                            }
                        );
                        Ok(i as usize)
                    }
                    None => Err(AlgoOptionNotFoundError {
                        name: name.to_string(),
                        span: self.span,
                        algo_name: self.algo.name.to_string(),
                    }
                        .into()),
                },
                _ => Err(WrongAlgoOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    algo_name: self.algo.name.to_string(),
                    help: "a non-negative integer is required".to_string(),
                }
                    .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(AlgoOptionNotFoundError {
                    name: name.to_string(),
                    span: self.span,
                    algo_name: self.algo.name.to_string(),
                }
                    .into()),
            },
        }
    }
    pub(crate) fn unit_interval_option(&self, name: &str, default: Option<f64>) -> Result<f64> {
        match self.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => {
                    let f = n.get_float();
                    ensure!(
                        f >= 0. && f <= 1.,
                        WrongAlgoOptionError {
                            name: name.to_string(),
                            span: v.span(),
                            algo_name: self.algo.name.to_string(),
                            help: "a number between 0. and 1. is required".to_string(),
                        }
                    );
                    Ok(f)
                }
                _ => Err(WrongAlgoOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    algo_name: self.algo.name.to_string(),
                    help: "a number between 0. and 1. is required".to_string(),
                }
                    .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(AlgoOptionNotFoundError {
                    name: name.to_string(),
                    span: self.span,
                    algo_name: self.algo.name.to_string(),
                }
                    .into()),
            },
        }
    }
    pub(crate) fn bool_option(&self, name: &str, default: Option<bool>) -> Result<bool> {
        match self.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Bool(b)) => Ok(b),
                _ => Err(WrongAlgoOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    algo_name: self.algo.name.to_string(),
                    help: "a boolean value is required".to_string(),
                }
                    .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(AlgoOptionNotFoundError {
                    name: name.to_string(),
                    span: self.span,
                    algo_name: self.algo.name.to_string(),
                }
                    .into()),
            },
        }
    }
}

impl Debug for MagicAlgoApply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgoApply")
            .field("algo", &self.algo.name)
            .field("rules", &self.rule_args)
            .field("options", &self.options)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum AlgoRuleArg {
    InMem {
        name: Symbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
    Stored {
        name: Symbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum MagicAlgoRuleArg {
    InMem {
        name: MagicSymbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
    Stored {
        name: Symbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
}

impl MagicAlgoRuleArg {
    pub(crate) fn bindings(&self) -> &[Symbol] {
        match self {
            MagicAlgoRuleArg::InMem { bindings, .. }
            | MagicAlgoRuleArg::Stored { bindings, .. } => bindings,
        }
    }
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            MagicAlgoRuleArg::InMem { span, .. }
            | MagicAlgoRuleArg::Stored { span, .. } => *span,
        }
    }
    pub(crate) fn get_binding_map(&self, starting: usize) -> BTreeMap<Symbol, usize> {
        let bindings = match self {
            MagicAlgoRuleArg::InMem { bindings, .. }
            | MagicAlgoRuleArg::Stored { bindings, .. } => bindings,
        };
        bindings
            .iter()
            .enumerate()
            .map(|(idx, symb)| (symb.clone(), idx + starting))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputProgram {
    pub(crate) prog: BTreeMap<Symbol, InputRulesOrAlgo>,
    pub(crate) const_rules: ConstRules,
    pub(crate) out_opts: QueryOutOptions,
}

#[derive(Debug, Diagnostic, Error)]
#[error("Entry head not found")]
#[diagnostic(code(parser::no_entry_head))]
#[diagnostic(help("You need to explicitly name your entry arguments"))]
struct EntryHeadNotExplicitlyDefinedError(#[label] SourceSpan);

#[derive(Debug, Diagnostic, Error)]
#[error("Program has no entry")]
#[diagnostic(code(parser::no_entry))]
#[diagnostic(help("You need to have one rule named '?'"))]
pub(crate) struct NoEntryError;

impl InputProgram {
    pub(crate) fn get_entry_arity(&self) -> Result<usize> {
        if let Some(entry) = self.prog.get(&Symbol::new(PROG_ENTRY, SourceSpan(0, 0))) {
            return match entry {
                InputRulesOrAlgo::Rules { rules } => Ok(rules.last().unwrap().head.len()),
                InputRulesOrAlgo::Algo { algo: algo_apply } => {
                    algo_apply.arity().ok_or_else(|| {
                        AlgoNotFoundError(algo_apply.algo.name.to_string(), algo_apply.span).into()
                    })
                }
            };
        }

        if let Some(ConstRule { data, .. }) = self.const_rules.get(&MagicSymbol::Muggle {
            inner: Symbol::new(PROG_ENTRY, SourceSpan(0, 0)),
        }) {
            return Ok(data.get(0).map(|row| row.0.len()).unwrap_or(0));
        }

        Err(NoEntryError.into())
    }
    pub(crate) fn get_entry_out_head(&self) -> Result<Vec<Symbol>> {
        if let Some(entry) = self.prog.get(&Symbol::new(PROG_ENTRY, SourceSpan(0, 0))) {
            return match entry {
                InputRulesOrAlgo::Rules { rules } => {
                    let head = &rules.last().unwrap().head;
                    let mut ret = Vec::with_capacity(head.len());
                    let aggrs = &rules.last().unwrap().aggr;
                    for (symb, aggr) in head.iter().zip(aggrs.iter()) {
                        if let Some((aggr, _)) = aggr {
                            ret.push(Symbol::new(
                                &format!("{}({})", aggr.name.strip_prefix("AGGR_").unwrap().to_ascii_lowercase(),
                                         symb), symb.span))
                        } else {
                            ret.push(symb.clone())
                        }
                    }
                    Ok(ret)
                }
                InputRulesOrAlgo::Algo { algo: algo_apply } => {
                    if algo_apply.head.is_empty() {
                        Err(EntryHeadNotExplicitlyDefinedError(entry.first_span()).into())
                    } else {
                        Ok(algo_apply.head.to_vec())
                    }
                }
            };
        }

        if let Some(ConstRule { bindings, span, .. }) = self.const_rules.get(&MagicSymbol::Muggle {
            inner: Symbol::new(PROG_ENTRY, SourceSpan(0, 0)),
        }) {
            return if bindings.is_empty() {
                Err(EntryHeadNotExplicitlyDefinedError(*span).into())
            } else {
                Ok(bindings.to_vec())
            };
        }

        Err(NoEntryError.into())
    }
    pub(crate) fn to_normalized_program(
        &self,
        tx: &SessionTx,
    ) -> Result<NormalFormProgram> {
        let mut prog: BTreeMap<Symbol, _> = Default::default();
        for (k, rules_or_algo) in &self.prog {
            match rules_or_algo {
                InputRulesOrAlgo::Rules { rules } => {
                    let mut collected_rules = vec![];
                    for rule in rules {
                        let mut counter = -1;
                        let mut gen_symb = |span| {
                            counter += 1;
                            Symbol::new(&format!("***{}", counter) as &str, span)
                        };
                        let normalized_body = InputAtom::Conjunction {
                            inner: rule.body.clone(),
                            span: rule.span,
                        }
                            .disjunctive_normal_form(tx)?;
                        let mut new_head = Vec::with_capacity(rule.head.len());
                        let mut seen: BTreeMap<&Symbol, Vec<Symbol>> = BTreeMap::default();
                        for symb in rule.head.iter() {
                            match seen.entry(symb) {
                                Entry::Vacant(e) => {
                                    e.insert(vec![]);
                                    new_head.push(symb.clone());
                                }
                                Entry::Occupied(mut e) => {
                                    let new_symb = gen_symb(symb.span);
                                    e.get_mut().push(new_symb.clone());
                                    new_head.push(new_symb);
                                }
                            }
                        }
                        for conj in normalized_body.inner {
                            let mut body = conj.0;
                            for (old_symb, new_symbs) in seen.iter() {
                                for new_symb in new_symbs.iter() {
                                    body.push(NormalFormAtom::Unification(Unification {
                                        binding: new_symb.clone(),
                                        expr: Expr::Binding {
                                            var: (*old_symb).clone(),
                                            tuple_pos: None,
                                        },
                                        one_many_unif: false,
                                        span: new_symb.span,
                                    }))
                                }
                            }
                            let normalized_rule = NormalFormRule {
                                head: new_head.clone(),
                                aggr: rule.aggr.clone(),
                                body,
                            };
                            collected_rules.push(normalized_rule.convert_to_well_ordered_rule()?);
                        }
                    }
                    prog.insert(
                        k.clone(),
                        NormalFormAlgoOrRules::Rules {
                            rules: collected_rules,
                        },
                    );
                }
                InputRulesOrAlgo::Algo { algo: algo_apply } => {
                    prog.insert(
                        k.clone(),
                        NormalFormAlgoOrRules::Algo {
                            algo: algo_apply.clone(),
                        },
                    );
                }
            }
        }
        Ok(NormalFormProgram { prog })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StratifiedNormalFormProgram(pub(crate) Vec<NormalFormProgram>);

#[derive(Debug, Clone)]
pub(crate) enum NormalFormAlgoOrRules {
    Rules { rules: Vec<NormalFormRule> },
    Algo { algo: AlgoApply },
}

impl NormalFormAlgoOrRules {
    pub(crate) fn rules(&self) -> Option<&[NormalFormRule]> {
        match self {
            NormalFormAlgoOrRules::Rules { rules: r } => Some(r),
            NormalFormAlgoOrRules::Algo { algo: _ } => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NormalFormProgram {
    pub(crate) prog: BTreeMap<Symbol, NormalFormAlgoOrRules>,
}

#[derive(Debug, Clone)]
pub(crate) struct StratifiedMagicProgram(pub(crate) Vec<MagicProgram>);

#[derive(Debug, Clone)]
pub(crate) enum MagicRulesOrAlgo {
    Rules { rules: Vec<MagicRule> },
    Algo { algo: MagicAlgoApply },
}

impl Default for MagicRulesOrAlgo {
    fn default() -> Self {
        Self::Rules { rules: vec![] }
    }
}

impl MagicRulesOrAlgo {
    pub(crate) fn arity(&self) -> Option<usize> {
        Some(match self {
            MagicRulesOrAlgo::Rules { rules } => rules.first().unwrap().head.len(),
            MagicRulesOrAlgo::Algo { algo } => algo.arity()?,
        })
    }
    pub(crate) fn mut_rules(&mut self) -> Option<&mut Vec<MagicRule>> {
        match self {
            MagicRulesOrAlgo::Rules { rules } => Some(rules),
            MagicRulesOrAlgo::Algo { algo: _ } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MagicProgram {
    pub(crate) prog: BTreeMap<MagicSymbol, MagicRulesOrAlgo>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum MagicSymbol {
    Muggle {
        inner: Symbol,
    },
    Magic {
        inner: Symbol,
        adornment: SmallVec<[bool; 8]>,
    },
    Input {
        inner: Symbol,
        adornment: SmallVec<[bool; 8]>,
    },
    Sup {
        inner: Symbol,
        adornment: SmallVec<[bool; 8]>,
        rule_idx: u16,
        sup_idx: u16,
    },
}

impl MagicSymbol {
    pub(crate) fn symbol(&self) -> &Symbol {
        match self {
            MagicSymbol::Muggle { inner, .. }
            | MagicSymbol::Magic { inner, .. }
            | MagicSymbol::Input { inner, .. }
            | MagicSymbol::Sup { inner, .. } => inner,
        }
    }
}

impl Debug for MagicSymbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MagicSymbol::Muggle { inner } => write!(f, "{}", inner.name),
            MagicSymbol::Magic { inner, adornment } => {
                write!(f, "{}|M", inner.name)?;
                for b in adornment {
                    if *b {
                        write!(f, "b")?
                    } else {
                        write!(f, "f")?
                    }
                }
                Ok(())
            }
            MagicSymbol::Input { inner, adornment } => {
                write!(f, "{}|I", inner.name)?;
                for b in adornment {
                    if *b {
                        write!(f, "b")?
                    } else {
                        write!(f, "f")?
                    }
                }
                Ok(())
            }
            MagicSymbol::Sup {
                inner,
                adornment,
                rule_idx,
                sup_idx,
            } => {
                write!(f, "{}|S.{}.{}", inner.name, rule_idx, sup_idx)?;
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

impl MagicSymbol {
    pub(crate) fn as_plain_symbol(&self) -> &Symbol {
        match self {
            MagicSymbol::Muggle { inner, .. }
            | MagicSymbol::Magic { inner, .. }
            | MagicSymbol::Input { inner, .. }
            | MagicSymbol::Sup { inner, .. } => inner,
        }
    }
    pub(crate) fn magic_adornment(&self) -> &[bool] {
        match self {
            MagicSymbol::Muggle { .. } => &[],
            MagicSymbol::Magic { adornment, .. }
            | MagicSymbol::Input { adornment, .. }
            | MagicSymbol::Sup { adornment, .. } => adornment,
        }
    }
    pub(crate) fn has_bound_adornment(&self) -> bool {
        self.magic_adornment().iter().any(|b| *b)
    }
    pub(crate) fn is_prog_entry(&self) -> bool {
        if let MagicSymbol::Muggle { inner } = self {
            inner.is_prog_entry()
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<InputAtom>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone)]
pub(crate) struct NormalFormRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<NormalFormAtom>,
}

#[derive(Debug, Clone)]
pub(crate) struct MagicRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<MagicAtom>,
}

impl MagicRule {
    pub(crate) fn contained_rules(&self) -> BTreeSet<MagicSymbol> {
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
    Rule {
        inner: InputRuleApplyAtom,
    },
    Relation {
        inner: InputRelationApplyAtom,
    },
    Predicate {
        inner: Expr,
    },
    Negation {
        inner: Box<InputAtom>,
        span: SourceSpan,
    },
    Conjunction {
        inner: Vec<InputAtom>,
        span: SourceSpan,
    },
    Disjunction {
        inner: Vec<InputAtom>,
        span: SourceSpan,
    },
    Unification {
        inner: Unification,
    },
}

impl InputAtom {
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            InputAtom::Negation { span, .. }
            | InputAtom::Conjunction { span, .. }
            | InputAtom::Disjunction { span, .. } => *span,
            InputAtom::Rule { inner, .. } => inner.span,
            InputAtom::Relation { inner, .. } => inner.span,
            InputAtom::Predicate { inner, .. } => inner.span(),
            InputAtom::Unification { inner, .. } => inner.span,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum NormalFormAtom {
    Rule(NormalFormRuleApplyAtom),
    Relation(NormalFormRelationApplyAtom),
    NegatedRule(NormalFormRuleApplyAtom),
    NegatedRelation(NormalFormRelationApplyAtom),
    Predicate(Expr),
    Unification(Unification),
}

#[derive(Debug, Clone)]
pub(crate) enum MagicAtom {
    Rule(MagicRuleApplyAtom),
    Relation(MagicRelationApplyAtom),
    Predicate(Expr),
    NegatedRule(MagicRuleApplyAtom),
    NegatedRelation(MagicRelationApplyAtom),
    Unification(Unification),
}

#[derive(Clone, Debug)]
pub(crate) struct InputRuleApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<InputTerm<DataValue>>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct InputRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<InputTerm<DataValue>>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalFormRuleApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalFormRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct MagicRuleApplyAtom {
    pub(crate) name: MagicSymbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct MagicRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) enum InputTerm<T> {
    Var { name: Symbol },
    Const { val: T, span: SourceSpan },
}

#[derive(Clone, Debug)]
pub(crate) struct Unification {
    pub(crate) binding: Symbol,
    pub(crate) expr: Expr,
    pub(crate) one_many_unif: bool,
    pub(crate) span: SourceSpan,
}

impl Unification {
    pub(crate) fn is_const(&self) -> bool {
        matches!(self.expr, Expr::Const { .. })
    }
    pub(crate) fn bindings_in_expr(&self) -> BTreeSet<Symbol> {
        self.expr.bindings()
    }
}
