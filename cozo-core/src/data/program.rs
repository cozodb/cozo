/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use miette::{bail, ensure, miette, Diagnostic, Result};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::relation::StoredRelationMetadata;
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::value::{DataValue, ValidityTs};
use crate::fixed_rule::{FixedRule, FixedRuleHandle};
use crate::fts::FtsIndexManifest;
use crate::parse::SourceSpan;
use crate::query::compile::ContainedRuleMultiplicity;
use crate::query::logical::{Disjunction, NamedFieldNotFound};
use crate::runtime::hnsw::HnswIndexManifest;
use crate::runtime::minhash_lsh::{LshSearch, MinHashLshIndexManifest};
use crate::runtime::relation::{
    AccessLevel, InputRelationHandle, InsufficientAccessLevel, RelationHandle,
};
use crate::runtime::temp_store::EpochStore;
use crate::runtime::transact::SessionTx;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum QueryAssertion {
    AssertNone(SourceSpan),
    AssertSome(SourceSpan),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ReturnMutation {
    NotReturning,
    Returning,
}

#[derive(Clone, PartialEq, Default)]
pub(crate) struct QueryOutOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) timeout: Option<f64>,
    pub(crate) sleep: Option<f64>,
    pub(crate) sorters: Vec<(Symbol, SortDir)>,
    pub(crate) store_relation: Option<(InputRelationHandle, RelationOp, ReturnMutation)>,
    pub(crate) assertion: Option<QueryAssertion>,
}

impl Debug for QueryOutOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Display for QueryOutOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(l) = self.limit {
            writeln!(f, ":limit {l};")?;
        }
        if let Some(l) = self.offset {
            writeln!(f, ":offset {l};")?;
        }
        if let Some(l) = self.timeout {
            writeln!(f, ":timeout {l};")?;
        }
        for (symb, dir) in &self.sorters {
            write!(f, ":order ")?;
            if *dir == SortDir::Dsc {
                write!(f, "-")?;
            }
            writeln!(f, "{symb};")?;
        }
        if let Some((
                        InputRelationHandle {
                            name,
                            metadata: StoredRelationMetadata { keys, non_keys },
                            key_bindings,
                            dep_bindings,
                            ..
                        },
                        op,
                        return_mutation,
                    )) = &self.store_relation
        {
            if *return_mutation == ReturnMutation::Returning {
                writeln!(f, ":returning")?;
            }
            match op {
                RelationOp::Create => {
                    write!(f, ":create ")?;
                }
                RelationOp::Replace => {
                    write!(f, ":replace ")?;
                }
                RelationOp::Insert => {
                    write!(f, ":insert ")?;
                }
                RelationOp::Put => {
                    write!(f, ":put ")?;
                }
                RelationOp::Update => {
                    write!(f, ":update ")?;
                }
                RelationOp::Rm => {
                    write!(f, ":rm ")?;
                }
                RelationOp::Delete => {
                    write!(f, ":delete ")?;
                }
                RelationOp::Ensure => {
                    write!(f, ":ensure ")?;
                }
                RelationOp::EnsureNot => {
                    write!(f, ":ensure_not ")?;
                }
            }
            write!(f, "{name} {{")?;
            let mut is_first = true;
            for (col, bind) in keys.iter().zip(key_bindings) {
                if is_first {
                    is_first = false
                } else {
                    write!(f, ", ")?;
                }
                write!(f, "{}: {}", col.name, col.typing)?;
                if let Some(gen) = &col.default_gen {
                    write!(f, " default {gen}")?;
                } else {
                    write!(f, " = {bind}")?;
                }
            }
            write!(f, " => ")?;
            let mut is_first = true;
            for (col, bind) in non_keys.iter().zip(dep_bindings) {
                if is_first {
                    is_first = false
                } else {
                    write!(f, ", ")?;
                }
                write!(f, "{}: {}", col.name, col.typing)?;
                if let Some(gen) = &col.default_gen {
                    write!(f, " default {gen}")?;
                } else {
                    write!(f, " = {bind}")?;
                }
            }
            writeln!(f, "}};")?;
        }

        if let Some(a) = &self.assertion {
            match a {
                QueryAssertion::AssertNone(_) => {
                    writeln!(f, ":assert none;")?;
                }
                QueryAssertion::AssertSome(_) => {
                    writeln!(f, ":assert some;")?;
                }
            }
        }

        Ok(())
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
    Replace,
    Put,
    Insert,
    Update,
    Rm,
    Delete,
    Ensure,
    EnsureNot,
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
    pub(crate) fn next_ignored(&mut self, span: SourceSpan) -> Symbol {
        self.last_id += 1;
        Symbol::new(&format!("~{}", self.last_id) as &str, span)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InputInlineRulesOrFixed {
    Rules { rules: Vec<InputInlineRule> },
    Fixed { fixed: FixedRuleApply },
}

impl InputInlineRulesOrFixed {
    pub(crate) fn first_span(&self) -> SourceSpan {
        match self {
            InputInlineRulesOrFixed::Rules { rules, .. } => rules[0].span,
            InputInlineRulesOrFixed::Fixed { fixed, .. } => fixed.span,
        }
    }
    // pub(crate) fn used_rule(&self, rule_name: &Symbol) -> bool {
    //     match self {
    //         InputInlineRulesOrFixed::Rules { rules, .. } => rules
    //             .iter()
    //             .any(|rule| rule.body.iter().any(|atom| atom.used_rule(rule_name))),
    //         InputInlineRulesOrFixed::Fixed { fixed, .. } => fixed.rule_args.iter().any(|arg| {
    //             if let FixedRuleArg::InMem { name, .. } = arg {
    //                 if name == rule_name {
    //                     return true;
    //                 }
    //             }
    //             false
    //         }),
    //     }
    // }
}

#[derive(Clone)]
pub(crate) struct FixedRuleApply {
    pub(crate) fixed_handle: FixedRuleHandle,
    pub(crate) rule_args: Vec<FixedRuleArg>,
    pub(crate) options: Arc<BTreeMap<SmartString<LazyCompact>, Expr>>,
    pub(crate) head: Vec<Symbol>,
    pub(crate) arity: usize,
    pub(crate) span: SourceSpan,
    pub(crate) fixed_impl: Arc<Box<dyn FixedRule>>,
}

impl FixedRuleApply {
    pub(crate) fn arity(&self) -> Result<usize> {
        self.fixed_impl
            .as_ref()
            .arity(&self.options, &self.head, self.span)
    }
}

impl Debug for FixedRuleApply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedRuleApply")
            .field("name", &self.fixed_handle.name)
            .field("rules", &self.rule_args)
            .field("options", &self.options)
            .finish()
    }
}

pub(crate) struct MagicFixedRuleApply {
    pub(crate) fixed_handle: FixedRuleHandle,
    pub(crate) rule_args: Vec<MagicFixedRuleRuleArg>,
    pub(crate) options: Arc<BTreeMap<SmartString<LazyCompact>, Expr>>,
    pub(crate) span: SourceSpan,
    pub(crate) arity: usize,
    pub(crate) fixed_impl: Arc<Box<dyn FixedRule>>,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Cannot find a required named option '{name}' for '{rule_name}'")]
#[diagnostic(code(fixed_rule::arg_not_found))]
pub(crate) struct FixedRuleOptionNotFoundError {
    pub(crate) name: String,
    #[label]
    pub(crate) span: SourceSpan,
    pub(crate) rule_name: String,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Wrong value for option '{name}' of '{rule_name}'")]
#[diagnostic(code(fixed_rule::arg_wrong))]
pub(crate) struct WrongFixedRuleOptionError {
    pub(crate) name: String,
    #[label]
    pub(crate) span: SourceSpan,
    pub(crate) rule_name: String,
    #[help]
    pub(crate) help: String,
}

impl MagicFixedRuleApply {
    #[allow(dead_code)]
    pub(crate) fn relation_with_min_len(
        &self,
        idx: usize,
        len: usize,
        tx: &SessionTx<'_>,
        stores: &BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<&MagicFixedRuleRuleArg> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Input relation to fixed rule has insufficient arity")]
        #[diagnostic(help("Arity should be at least {0} but is {1}"))]
        #[diagnostic(code(fixed_rule::input_relation_bad_arity))]
        struct InputRelationArityError(usize, usize, #[label] SourceSpan);

        let rel = self.relation(idx)?;
        let arity = rel.arity(tx, stores)?;
        ensure!(
            arity >= len,
            InputRelationArityError(len, arity, rel.span())
        );
        Ok(rel)
    }
    pub(crate) fn relations_count(&self) -> usize {
        self.rule_args.len()
    }
    pub(crate) fn relation(&self, idx: usize) -> Result<&MagicFixedRuleRuleArg> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Cannot find a required positional argument at index {idx} for '{rule_name}'")]
        #[diagnostic(code(fixed_rule::not_enough_args))]
        pub(crate) struct FixedRuleNotEnoughRelationError {
            idx: usize,
            #[label]
            span: SourceSpan,
            rule_name: String,
        }

        Ok(self
            .rule_args
            .get(idx)
            .ok_or_else(|| FixedRuleNotEnoughRelationError {
                idx,
                span: self.span,
                rule_name: self.fixed_handle.name.to_string(),
            })?)
    }
}

impl Debug for MagicFixedRuleApply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedRuleApply")
            .field("name", &self.fixed_handle.name)
            .field("rules", &self.rule_args)
            .field("options", &self.options)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) enum FixedRuleArg {
    InMem {
        name: Symbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
    Stored {
        name: Symbol,
        bindings: Vec<Symbol>,
        valid_at: Option<ValidityTs>,
        span: SourceSpan,
    },
    NamedStored {
        name: Symbol,
        bindings: BTreeMap<SmartString<LazyCompact>, Symbol>,
        valid_at: Option<ValidityTs>,
        span: SourceSpan,
    },
}

impl Debug for FixedRuleArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Display for FixedRuleArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FixedRuleArg::InMem { name, bindings, .. } => {
                write!(f, "{name}")?;
                f.debug_list().entries(bindings).finish()?;
            }
            FixedRuleArg::Stored { name, bindings, .. } => {
                write!(f, ":{name}")?;
                f.debug_list().entries(bindings).finish()?;
            }
            FixedRuleArg::NamedStored { name, bindings, .. } => {
                write!(f, "*")?;
                let mut sf = f.debug_struct(name);
                for (k, v) in bindings {
                    sf.field(k, v);
                }
                sf.finish()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum MagicFixedRuleRuleArg {
    InMem {
        name: MagicSymbol,
        bindings: Vec<Symbol>,
        span: SourceSpan,
    },
    Stored {
        name: Symbol,
        bindings: Vec<Symbol>,
        valid_at: Option<ValidityTs>,
        span: SourceSpan,
    },
}

impl MagicFixedRuleRuleArg {
    #[allow(dead_code)]
    pub(crate) fn bindings(&self) -> &[Symbol] {
        match self {
            MagicFixedRuleRuleArg::InMem { bindings, .. }
            | MagicFixedRuleRuleArg::Stored { bindings, .. } => bindings,
        }
    }
    #[allow(dead_code)]
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            MagicFixedRuleRuleArg::InMem { span, .. }
            | MagicFixedRuleRuleArg::Stored { span, .. } => *span,
        }
    }
    pub(crate) fn get_binding_map(&self, starting: usize) -> BTreeMap<Symbol, usize> {
        let bindings = match self {
            MagicFixedRuleRuleArg::InMem { bindings, .. }
            | MagicFixedRuleRuleArg::Stored { bindings, .. } => bindings,
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
    pub(crate) prog: BTreeMap<Symbol, InputInlineRulesOrFixed>,
    pub(crate) out_opts: QueryOutOptions,
    pub(crate) disable_magic_rewrite: bool,
}

impl Display for InputProgram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (name, rules) in &self.prog {
            match rules {
                InputInlineRulesOrFixed::Rules { rules, .. } => {
                    for InputInlineRule {
                        head, aggr, body, ..
                    } in rules
                    {
                        write!(f, "{name}[")?;

                        for (i, (h, a)) in head.iter().zip(aggr).enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            if let Some((aggr, aggr_args)) = a {
                                write!(f, "{}({}", aggr.name, h)?;
                                for aga in aggr_args {
                                    write!(f, ", {aga}")?;
                                }
                                write!(f, ")")?;
                            } else {
                                write!(f, "{h}")?;
                            }
                        }
                        write!(f, "] := ")?;
                        for (i, atom) in body.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{atom}")?;
                        }
                        writeln!(f, ";")?;
                    }
                }
                InputInlineRulesOrFixed::Fixed {
                    fixed:
                    FixedRuleApply {
                        fixed_handle: handle,
                        rule_args,
                        options,
                        head,
                        ..
                    },
                } => {
                    write!(f, "{name}")?;
                    f.debug_list().entries(head).finish()?;
                    write!(f, " <~ ")?;
                    write!(f, "{}(", handle.name)?;
                    let mut first = true;
                    for rule_arg in rule_args {
                        if first {
                            first = false;
                        } else {
                            write!(f, ", ")?;
                        }
                        write!(f, "{rule_arg}")?;
                    }
                    for (k, v) in options.as_ref() {
                        if first {
                            first = false;
                        } else {
                            write!(f, ", ")?;
                        }
                        write!(f, "{k}: {v}")?;
                    }
                    writeln!(f, ");")?;
                }
            }
        }
        write!(f, "{}", self.out_opts)?;
        Ok(())
    }
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
    pub(crate) fn needs_write_lock(&self) -> Option<SmartString<LazyCompact>> {
        if let Some((h, _, _)) = &self.out_opts.store_relation {
            if !h.name.name.starts_with('_') {
                Some(h.name.name.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn get_entry_arity(&self) -> Result<usize> {
        if let Some(entry) = self.prog.get(&Symbol::new(PROG_ENTRY, SourceSpan(0, 0))) {
            return match entry {
                InputInlineRulesOrFixed::Rules { rules } => Ok(rules.last().unwrap().head.len()),
                InputInlineRulesOrFixed::Fixed { fixed } => fixed.arity(),
            };
        }

        Err(NoEntryError.into())
    }
    pub(crate) fn get_entry_out_head_or_default(&self) -> Result<Vec<Symbol>> {
        match self.get_entry_out_head() {
            Ok(r) => Ok(r),
            Err(_) => {
                let arity = self.get_entry_arity()?;
                Ok((0..arity)
                    .map(|i| Symbol::new(format!("_{i}"), SourceSpan(0, 0)))
                    .collect())
            }
        }
    }
    pub(crate) fn get_entry_out_head(&self) -> Result<Vec<Symbol>> {
        if let Some(entry) = self.prog.get(&Symbol::new(PROG_ENTRY, SourceSpan(0, 0))) {
            return match entry {
                InputInlineRulesOrFixed::Rules { rules } => {
                    let head = &rules.last().unwrap().head;
                    let mut ret = Vec::with_capacity(head.len());
                    let aggrs = &rules.last().unwrap().aggr;
                    for (symb, aggr) in head.iter().zip(aggrs.iter()) {
                        if let Some((aggr, _)) = aggr {
                            ret.push(Symbol::new(
                                format!(
                                    "{}({})",
                                    aggr.name
                                        .strip_prefix("AGGR_")
                                        .unwrap()
                                        .to_ascii_lowercase(),
                                    symb
                                ),
                                symb.span,
                            ))
                        } else {
                            ret.push(symb.clone())
                        }
                    }
                    Ok(ret)
                }
                InputInlineRulesOrFixed::Fixed { fixed } => {
                    if fixed.head.is_empty() {
                        Err(EntryHeadNotExplicitlyDefinedError(entry.first_span()).into())
                    } else {
                        Ok(fixed.head.to_vec())
                    }
                }
            };
        }

        Err(NoEntryError.into())
    }
    pub(crate) fn into_normalized_program(
        self,
        tx: &SessionTx<'_>,
    ) -> Result<(NormalFormProgram, QueryOutOptions)> {
        let mut prog: BTreeMap<Symbol, _> = Default::default();
        for (k, rules_or_fixed) in self.prog {
            match rules_or_fixed {
                InputInlineRulesOrFixed::Rules { rules } => {
                    let mut collected_rules = vec![];
                    for rule in rules {
                        let mut counter = -1;
                        let mut gen_symb = |span| {
                            counter += 1;
                            Symbol::new(&format!("***{counter}") as &str, span)
                        };
                        let normalized_body = InputAtom::Conjunction {
                            inner: rule.body,
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
                            let normalized_rule = NormalFormInlineRule {
                                head: new_head.clone(),
                                aggr: rule.aggr.clone(),
                                body,
                            };
                            collected_rules.push(normalized_rule.convert_to_well_ordered_rule()?);
                        }
                    }
                    prog.insert(
                        k.clone(),
                        NormalFormRulesOrFixed::Rules {
                            rules: collected_rules,
                        },
                    );
                }
                InputInlineRulesOrFixed::Fixed { fixed } => {
                    prog.insert(k.clone(), NormalFormRulesOrFixed::Fixed { fixed });
                }
            }
        }
        Ok((
            NormalFormProgram {
                prog,
                disable_magic_rewrite: self.disable_magic_rewrite,
            },
            self.out_opts,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct StratifiedNormalFormProgram(pub(crate) Vec<NormalFormProgram>);

#[derive(Debug)]
pub(crate) enum NormalFormRulesOrFixed {
    Rules { rules: Vec<NormalFormInlineRule> },
    Fixed { fixed: FixedRuleApply },
}

impl NormalFormRulesOrFixed {
    pub(crate) fn rules(&self) -> Option<&[NormalFormInlineRule]> {
        match self {
            NormalFormRulesOrFixed::Rules { rules: r } => Some(r),
            NormalFormRulesOrFixed::Fixed { fixed: _ } => None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct NormalFormProgram {
    pub(crate) prog: BTreeMap<Symbol, NormalFormRulesOrFixed>,
    pub(crate) disable_magic_rewrite: bool,
}

#[derive(Debug)]
pub(crate) struct StratifiedMagicProgram(pub(crate) Vec<MagicProgram>);

#[derive(Debug)]
pub(crate) enum MagicRulesOrFixed {
    Rules { rules: Vec<MagicInlineRule> },
    Fixed { fixed: MagicFixedRuleApply },
}

impl Default for MagicRulesOrFixed {
    fn default() -> Self {
        Self::Rules { rules: vec![] }
    }
}

impl MagicRulesOrFixed {
    pub(crate) fn arity(&self) -> Result<usize> {
        Ok(match self {
            MagicRulesOrFixed::Rules { rules } => rules.first().unwrap().head.len(),
            MagicRulesOrFixed::Fixed { fixed } => fixed.arity,
        })
    }
    pub(crate) fn mut_rules(&mut self) -> Option<&mut Vec<MagicInlineRule>> {
        match self {
            MagicRulesOrFixed::Rules { rules } => Some(rules),
            MagicRulesOrFixed::Fixed { fixed: _ } => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MagicProgram {
    pub(crate) prog: BTreeMap<MagicSymbol, MagicRulesOrFixed>,
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

impl Display for MagicSymbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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
pub(crate) struct InputInlineRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<InputAtom>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug)]
pub(crate) struct NormalFormInlineRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<NormalFormAtom>,
}

#[derive(Debug)]
pub(crate) struct MagicInlineRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<MagicAtom>,
}

impl MagicInlineRule {
    pub(crate) fn contained_rules(&self) -> BTreeMap<MagicSymbol, ContainedRuleMultiplicity> {
        let mut coll = BTreeMap::new();
        for atom in self.body.iter() {
            match atom {
                MagicAtom::Rule(rule) | MagicAtom::NegatedRule(rule) => {
                    match coll.entry(rule.name.clone()) {
                        Entry::Vacant(ent) => {
                            ent.insert(ContainedRuleMultiplicity::One);
                        }
                        Entry::Occupied(mut ent) => {
                            *ent.get_mut() = ContainedRuleMultiplicity::Many;
                        }
                    }
                }
                _ => {}
            }
        }
        coll
    }
}

#[derive(Clone)]
pub(crate) enum InputAtom {
    Rule {
        inner: InputRuleApplyAtom,
    },
    NamedFieldRelation {
        inner: InputNamedFieldRelationApplyAtom,
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
    Search {
        inner: SearchInput,
    },
}

#[derive(Clone)]
pub(crate) struct SearchInput {
    pub(crate) relation: Symbol,
    pub(crate) index: Symbol,
    pub(crate) bindings: BTreeMap<SmartString<LazyCompact>, Expr>,
    pub(crate) parameters: BTreeMap<SmartString<LazyCompact>, Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct HnswSearch {
    pub(crate) base_handle: RelationHandle,
    pub(crate) idx_handle: RelationHandle,
    pub(crate) manifest: HnswIndexManifest,
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) k: usize,
    pub(crate) ef: usize,
    pub(crate) query: Symbol,
    pub(crate) bind_field: Option<Symbol>,
    pub(crate) bind_field_idx: Option<Symbol>,
    pub(crate) bind_distance: Option<Symbol>,
    pub(crate) bind_vector: Option<Symbol>,
    pub(crate) radius: Option<f64>,
    pub(crate) filter: Option<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum FtsScoreKind {
    TfIdf,
    Tf,
}

#[derive(Clone, Debug)]
pub(crate) struct FtsSearch {
    pub(crate) base_handle: RelationHandle,
    pub(crate) idx_handle: RelationHandle,
    pub(crate) manifest: FtsIndexManifest,
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) k: usize,
    // pub(crate) k1: f64,
    // pub(crate) b: f64,
    pub(crate) query: Symbol,
    pub(crate) score_kind: FtsScoreKind,
    pub(crate) bind_score: Option<Symbol>,
    // pub(crate) lax_mode: bool,
    pub(crate) filter: Option<Expr>,
    pub(crate) span: SourceSpan,
}

impl HnswSearch {
    pub(crate) fn all_bindings(&self) -> impl Iterator<Item=&Symbol> {
        self.bindings
            .iter()
            .chain(self.bind_field.iter())
            .chain(self.bind_field_idx.iter())
            .chain(self.bind_distance.iter())
            .chain(self.bind_vector.iter())
    }
}

impl FtsSearch {
    pub(crate) fn all_bindings(&self) -> impl Iterator<Item=&Symbol> {
        self.bindings.iter().chain(self.bind_score.iter())
    }
}

impl SearchInput {
    fn normalize_lsh(
        mut self,
        base_handle: RelationHandle,
        idx_handle: RelationHandle,
        manifest: MinHashLshIndexManifest,
        gen: &mut TempSymbGen,
    ) -> Result<Disjunction> {
        let mut conj = Vec::with_capacity(self.bindings.len() + 8);
        let mut bindings = Vec::with_capacity(self.bindings.len());
        let mut seen_variables = BTreeSet::new();

        for col in base_handle
            .metadata
            .keys
            .iter()
            .chain(base_handle.metadata.non_keys.iter())
        {
            if let Some(arg) = self.bindings.remove(&col.name) {
                match arg {
                    Expr::Binding { var, .. } => {
                        if var.is_ignored_symbol() {
                            bindings.push(gen.next_ignored(var.span));
                        } else if seen_variables.insert(var.clone()) {
                            bindings.push(var);
                        } else {
                            let span = var.span;
                            let dup = gen.next(span);
                            let unif = NormalFormAtom::Unification(Unification {
                                binding: dup.clone(),
                                expr: Expr::Binding {
                                    var,
                                    tuple_pos: None,
                                },
                                one_many_unif: false,
                                span,
                            });
                            conj.push(unif);
                            bindings.push(dup);
                        }
                    }
                    expr => {
                        let span = expr.span();
                        let kw = gen.next(span);
                        bindings.push(kw.clone());
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: kw,
                            expr,
                            one_many_unif: false,
                            span,
                        });
                        conj.push(unif)
                    }
                }
            } else {
                bindings.push(gen.next_ignored(self.span));
            }
        }

        if let Some((name, _)) = self.bindings.pop_first() {
            bail!(NamedFieldNotFound(
                self.relation.name.to_string(),
                name.to_string(),
                self.span
            ));
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("Field `{0}` is required for LSH search")]
        #[diagnostic(code(parser::hnsw_query_required))]
        struct LshRequiredMissing(String, #[label] SourceSpan);

        #[derive(Debug, Error, Diagnostic)]
        #[error("Expected a list of keys for LSH search")]
        #[diagnostic(code(parser::expected_list_for_lsh_keys))]
        struct ExpectedListForLshKeys(#[label] SourceSpan);

        #[derive(Debug, Error, Diagnostic)]
        #[error("Wrong arity for LSH keys, expected {1}, got {2}")]
        #[diagnostic(code(parser::wrong_arity_for_lsh_keys))]
        struct WrongArityForKeys(#[label] SourceSpan, usize, usize);

        let query = match self
            .parameters
            .remove("query")
            .ok_or_else(|| miette!(LshRequiredMissing("query".to_string(), self.span)))?
        {
            Expr::Binding { var, .. } => var,
            expr => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                kw
            }
        };

        let k = match self.parameters.remove("k") {
            None => None,
            Some(k_expr) => {
                let k = k_expr.eval_to_const()?;
                let k = k.get_int().ok_or(ExpectedPosIntForFtsK(self.span))?;

                #[derive(Debug, Error, Diagnostic)]
                #[error("Expected positive integer for `k`")]
                #[diagnostic(code(parser::expected_int_for_hnsw_k))]
                struct ExpectedPosIntForFtsK(#[label] SourceSpan);

                ensure!(k > 0, ExpectedPosIntForFtsK(self.span));
                Some(k as usize)
            }
        };

        let filter = self.parameters.remove("filter");

        #[derive(Debug, Error, Diagnostic)]
        #[error("Extra parameters for LSH search: {0:?}")]
        #[diagnostic(code(parser::extra_parameters_for_lsh_search))]
        struct ExtraParametersForLshSearch(Vec<String>, #[label] SourceSpan);

        if !self.parameters.is_empty() {
            bail!(ExtraParametersForLshSearch(
                self.parameters.keys().map(|s| s.to_string()).collect(),
                self.span
            ));
        }

        conj.push(NormalFormAtom::LshSearch(LshSearch {
            base_handle,
            idx_handle,
            manifest,
            bindings,
            k,
            query,
            span: self.span,
            filter,
        }));

        Ok(Disjunction::conj(conj))
    }
    fn normalize_fts(
        mut self,
        base_handle: RelationHandle,
        idx_handle: RelationHandle,
        manifest: FtsIndexManifest,
        gen: &mut TempSymbGen,
    ) -> Result<Disjunction> {
        let mut conj = Vec::with_capacity(self.bindings.len() + 8);
        let mut bindings = Vec::with_capacity(self.bindings.len());
        let mut seen_variables = BTreeSet::new();

        for col in base_handle
            .metadata
            .keys
            .iter()
            .chain(base_handle.metadata.non_keys.iter())
        {
            if let Some(arg) = self.bindings.remove(&col.name) {
                match arg {
                    Expr::Binding { var, .. } => {
                        if var.is_ignored_symbol() {
                            bindings.push(gen.next_ignored(var.span));
                        } else if seen_variables.insert(var.clone()) {
                            bindings.push(var);
                        } else {
                            let span = var.span;
                            let dup = gen.next(span);
                            let unif = NormalFormAtom::Unification(Unification {
                                binding: dup.clone(),
                                expr: Expr::Binding {
                                    var,
                                    tuple_pos: None,
                                },
                                one_many_unif: false,
                                span,
                            });
                            conj.push(unif);
                            bindings.push(dup);
                        }
                    }
                    expr => {
                        let span = expr.span();
                        let kw = gen.next(span);
                        bindings.push(kw.clone());
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: kw,
                            expr,
                            one_many_unif: false,
                            span,
                        });
                        conj.push(unif)
                    }
                }
            } else {
                bindings.push(gen.next_ignored(self.span));
            }
        }

        if let Some((name, _)) = self.bindings.pop_first() {
            bail!(NamedFieldNotFound(
                self.relation.name.to_string(),
                name.to_string(),
                self.span
            ));
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("Field `{0}` is required for HNSW search")]
        #[diagnostic(code(parser::hnsw_query_required))]
        struct HnswRequiredMissing(String, #[label] SourceSpan);

        let query = match self
            .parameters
            .remove("query")
            .ok_or_else(|| miette!(HnswRequiredMissing("query".to_string(), self.span)))?
        {
            Expr::Binding { var, .. } => var,
            expr => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                kw
            }
        };

        let k_expr = self
            .parameters
            .remove("k")
            .ok_or_else(|| miette!(HnswRequiredMissing("k".to_string(), self.span)))?;
        let k = k_expr.eval_to_const()?;
        let k = k.get_int().ok_or(ExpectedPosIntForFtsK(self.span))?;

        #[derive(Debug, Error, Diagnostic)]
        #[error("Expected positive integer for `k`")]
        #[diagnostic(code(parser::expected_int_for_hnsw_k))]
        struct ExpectedPosIntForFtsK(#[label] SourceSpan);

        ensure!(k > 0, ExpectedPosIntForFtsK(self.span));

        let score_kind_expr = self.parameters.remove("score_kind");
        let score_kind = match score_kind_expr {
            Some(expr) => {
                let r = expr.eval_to_const()?;
                let r = r
                    .get_str()
                    .ok_or_else(|| miette!("Score kind for FTS must be a string"))?;

                match r {
                    "tf_idf" => FtsScoreKind::TfIdf,
                    "tf" => FtsScoreKind::Tf,
                    s => bail!("Unknown score kind for FTS: {}", s),
                }
            }
            None => FtsScoreKind::TfIdf,
        };

        let filter = self.parameters.remove("filter");

        let bind_score = match self.parameters.remove("bind_score") {
            None => None,
            Some(Expr::Binding { var, .. }) => Some(var),
            Some(expr) => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                Some(kw)
            }
        };

        if !self.parameters.is_empty() {
            bail!("Unknown parameters for FTS: {:?}", self.parameters.keys());
        }

        conj.push(NormalFormAtom::FtsSearch(FtsSearch {
            base_handle,
            idx_handle,
            manifest,
            bindings,
            k: k as usize,
            query,
            score_kind,
            bind_score,
            // lax_mode,
            // k1,
            // b,
            filter,
            span: self.span,
        }));

        Ok(Disjunction::conj(conj))
    }
    fn normalize_hnsw(
        mut self,
        base_handle: RelationHandle,
        idx_handle: RelationHandle,
        manifest: HnswIndexManifest,
        gen: &mut TempSymbGen,
    ) -> Result<Disjunction> {
        let mut conj = Vec::with_capacity(self.bindings.len() + 8);
        let mut bindings = Vec::with_capacity(self.bindings.len());
        let mut seen_variables = BTreeSet::new();

        for col in base_handle
            .metadata
            .keys
            .iter()
            .chain(base_handle.metadata.non_keys.iter())
        {
            if let Some(arg) = self.bindings.remove(&col.name) {
                match arg {
                    Expr::Binding { var, .. } => {
                        if var.is_ignored_symbol() {
                            bindings.push(gen.next_ignored(var.span));
                        } else if seen_variables.insert(var.clone()) {
                            bindings.push(var);
                        } else {
                            let span = var.span;
                            let dup = gen.next(span);
                            let unif = NormalFormAtom::Unification(Unification {
                                binding: dup.clone(),
                                expr: Expr::Binding {
                                    var,
                                    tuple_pos: None,
                                },
                                one_many_unif: false,
                                span,
                            });
                            conj.push(unif);
                            bindings.push(dup);
                        }
                    }
                    expr => {
                        let span = expr.span();
                        let kw = gen.next(span);
                        bindings.push(kw.clone());
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: kw,
                            expr,
                            one_many_unif: false,
                            span,
                        });
                        conj.push(unif)
                    }
                }
            } else {
                bindings.push(gen.next_ignored(self.span));
            }
        }

        if let Some((name, _)) = self.bindings.pop_first() {
            bail!(NamedFieldNotFound(
                self.relation.name.to_string(),
                name.to_string(),
                self.span
            ));
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("Field `{0}` is required for HNSW search")]
        #[diagnostic(code(parser::hnsw_query_required))]
        struct HnswRequiredMissing(String, #[label] SourceSpan);

        let query = match self
            .parameters
            .remove("query")
            .ok_or_else(|| miette!(HnswRequiredMissing("query".to_string(), self.span)))?
        {
            Expr::Binding { var, .. } => var,
            expr => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                kw
            }
        };

        let k_expr = self
            .parameters
            .remove("k")
            .ok_or_else(|| miette!(HnswRequiredMissing("k".to_string(), self.span)))?;
        let k = k_expr.eval_to_const()?;
        let k = k.get_int().ok_or(ExpectedPosIntForHnswK(self.span))?;

        #[derive(Debug, Error, Diagnostic)]
        #[error("Expected positive integer for `k`")]
        #[diagnostic(code(parser::expected_int_for_hnsw_k))]
        struct ExpectedPosIntForHnswK(#[label] SourceSpan);

        ensure!(k > 0, ExpectedPosIntForHnswK(self.span));

        let ef_expr = self
            .parameters
            .remove("ef")
            .ok_or_else(|| miette!(HnswRequiredMissing("ef".to_string(), self.span)))?;
        let ef = ef_expr.eval_to_const()?;
        let ef = ef.get_int().ok_or(ExpectedPosIntForHnswEf(self.span))?;

        #[derive(Debug, Error, Diagnostic)]
        #[error("Expected positive integer for `ef`")]
        #[diagnostic(code(parser::expected_int_for_hnsw_ef))]
        struct ExpectedPosIntForHnswEf(#[label] SourceSpan);

        ensure!(ef > 0, ExpectedPosIntForHnswEf(self.span));

        let radius_expr = self.parameters.remove("radius");
        let radius = match radius_expr {
            Some(expr) => {
                let r = expr.eval_to_const()?;
                let r = r.get_float().ok_or(ExpectedFloatForHnswRadius(self.span))?;

                #[derive(Debug, Error, Diagnostic)]
                #[error("Expected positive float for `radius`")]
                #[diagnostic(code(parser::expected_float_for_hnsw_radius))]
                struct ExpectedFloatForHnswRadius(#[label] SourceSpan);

                ensure!(r > 0.0, ExpectedFloatForHnswRadius(self.span));
                Some(r)
            }
            None => None,
        };

        let filter = self.parameters.remove("filter");

        let bind_field = match self.parameters.remove("bind_field") {
            None => None,
            Some(Expr::Binding { var, .. }) => Some(var),
            Some(expr) => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                Some(kw)
            }
        };

        let bind_field_idx = match self.parameters.remove("bind_field_idx") {
            None => None,
            Some(Expr::Binding { var, .. }) => Some(var),
            Some(expr) => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                Some(kw)
            }
        };

        let bind_distance = match self.parameters.remove("bind_distance") {
            None => None,
            Some(Expr::Binding { var, .. }) => Some(var),
            Some(expr) => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                Some(kw)
            }
        };

        let bind_vector = match self.parameters.remove("bind_vector") {
            None => None,
            Some(Expr::Binding { var, .. }) => Some(var),
            Some(expr) => {
                let span = expr.span();
                let kw = gen.next(span);
                let unif = NormalFormAtom::Unification(Unification {
                    binding: kw.clone(),
                    expr,
                    one_many_unif: false,
                    span,
                });
                conj.push(unif);
                Some(kw)
            }
        };

        if !self.parameters.is_empty() {
            bail!("Unexpected parameters for HNSW: {:?}", self.parameters);
        }

        conj.push(NormalFormAtom::HnswSearch(HnswSearch {
            base_handle,
            idx_handle,
            manifest,
            bindings,
            k: k as usize,
            ef: ef as usize,
            query,
            bind_field,
            bind_field_idx,
            bind_distance,
            bind_vector,
            radius,
            filter,
            span: self.span,
        }));

        Ok(Disjunction::conj(conj))
    }
    pub(crate) fn normalize(
        self,
        gen: &mut TempSymbGen,
        tx: &SessionTx<'_>,
    ) -> Result<Disjunction> {
        let base_handle = tx.get_relation(&self.relation, false)?;
        if base_handle.access_level < AccessLevel::ReadOnly {
            bail!(InsufficientAccessLevel(
                base_handle.name.to_string(),
                "reading rows".to_string(),
                base_handle.access_level
            ));
        }
        if let Some((idx_handle, manifest)) =
            base_handle.hnsw_indices.get(&self.index.name).cloned()
        {
            return self.normalize_hnsw(base_handle, idx_handle, manifest, gen);
        }
        if let Some((idx_handle, manifest)) = base_handle.fts_indices.get(&self.index.name).cloned()
        {
            return self.normalize_fts(base_handle, idx_handle, manifest, gen);
        }
        if let Some((idx_handle, _, manifest)) =
            base_handle.lsh_indices.get(&self.index.name).cloned()
        {
            return self.normalize_lsh(base_handle, idx_handle, manifest, gen);
        }
        #[derive(Debug, Error, Diagnostic)]
        #[error("Index {name} not found on relation {relation}")]
        #[diagnostic(code(eval::hnsw_index_not_found))]
        struct IndexNotFound {
            relation: String,
            name: String,
            #[label]
            span: SourceSpan,
        }
        bail!(IndexNotFound {
            relation: self.relation.to_string(),
            name: self.index.to_string(),
            span: self.span,
        })
    }
}

impl Debug for InputAtom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Display for InputAtom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InputAtom::Rule {
                inner: InputRuleApplyAtom { name, args, .. },
            } => {
                write!(f, "{name}")?;
                f.debug_list().entries(args).finish()?;
            }
            InputAtom::NamedFieldRelation {
                inner: InputNamedFieldRelationApplyAtom { name, args, .. },
            } => {
                f.write_str("*")?;
                let mut sf = f.debug_struct(name);
                for (k, v) in args {
                    sf.field(k, v);
                }
                sf.finish()?;
            }
            InputAtom::Relation {
                inner: InputRelationApplyAtom { name, args, .. },
            } => {
                write!(f, ":{name}")?;
                f.debug_list().entries(args).finish()?;
            }
            InputAtom::Search { inner } => {
                write!(f, "~{}:{}{{", inner.relation, inner.index)?;
                for (binding, expr) in &inner.bindings {
                    write!(f, "{binding}: {expr}, ")?;
                }
                write!(f, "| ")?;
                for (k, v) in inner.parameters.iter() {
                    write!(f, "{k}: {v}, ")?;
                }
                write!(f, "}}")?;
            }
            InputAtom::Predicate { inner } => {
                write!(f, "{inner}")?;
            }
            InputAtom::Negation { inner, .. } => {
                write!(f, "not {inner}")?;
            }
            InputAtom::Conjunction { inner, .. } => {
                for (i, a) in inner.iter().enumerate() {
                    if i > 0 {
                        write!(f, " and ")?;
                    }
                    write!(f, "({a})")?;
                }
            }
            InputAtom::Disjunction { inner, .. } => {
                for (i, a) in inner.iter().enumerate() {
                    if i > 0 {
                        write!(f, " or ")?;
                    }
                    write!(f, "({a})")?;
                }
            }
            InputAtom::Unification {
                inner:
                Unification {
                    binding,
                    expr,
                    one_many_unif,
                    ..
                },
            } => {
                write!(f, "{binding}")?;
                if *one_many_unif {
                    write!(f, " in ")?;
                } else {
                    write!(f, " = ")?;
                }
                write!(f, "{expr}")?;
            }
        }
        Ok(())
    }
}

impl InputAtom {
    // pub(crate) fn used_rule(&self, rule_name: &Symbol) -> bool {
    //     match self {
    //         InputAtom::Rule { inner } => inner.name == *rule_name,
    //         InputAtom::Negation { inner, .. } => inner.used_rule(rule_name),
    //         InputAtom::Conjunction { inner, .. } | InputAtom::Disjunction { inner, .. } => {
    //             inner.iter().any(|a| a.used_rule(rule_name))
    //         }
    //         _ => false,
    //     }
    // }
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            InputAtom::Negation { span, .. }
            | InputAtom::Conjunction { span, .. }
            | InputAtom::Disjunction { span, .. } => *span,
            InputAtom::Rule { inner, .. } => inner.span,
            InputAtom::NamedFieldRelation { inner, .. } => inner.span,
            InputAtom::Relation { inner, .. } => inner.span,
            InputAtom::Predicate { inner, .. } => inner.span(),
            InputAtom::Unification { inner, .. } => inner.span,
            InputAtom::Search { inner, .. } => inner.span,
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
    HnswSearch(HnswSearch),
    FtsSearch(FtsSearch),
    LshSearch(LshSearch),
}

#[derive(Debug, Clone)]
pub(crate) enum MagicAtom {
    Rule(MagicRuleApplyAtom),
    Relation(MagicRelationApplyAtom),
    Predicate(Expr),
    NegatedRule(MagicRuleApplyAtom),
    NegatedRelation(MagicRelationApplyAtom),
    Unification(Unification),
    HnswSearch(HnswSearch),
    FtsSearch(FtsSearch),
    LshSearch(LshSearch),
}

#[derive(Clone, Debug)]
pub(crate) struct InputRuleApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct InputNamedFieldRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: BTreeMap<SmartString<LazyCompact>, Expr>,
    pub(crate) valid_at: Option<ValidityTs>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct InputRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Expr>,
    pub(crate) valid_at: Option<ValidityTs>,
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
    pub(crate) valid_at: Option<ValidityTs>,
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
    pub(crate) valid_at: Option<ValidityTs>,
    pub(crate) span: SourceSpan,
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
    pub(crate) fn bindings_in_expr(&self) -> Result<BTreeSet<Symbol>> {
        self.expr.bindings()
    }
}
