/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeSet;
use std::mem;

use miette::{bail, Diagnostic, Result};
use thiserror::Error;

use crate::data::program::{NormalFormAtom, NormalFormInlineRule};
use crate::parse::SourceSpan;

#[derive(Diagnostic, Debug, Error)]
#[error("Encountered unsafe negation, or empty rule definition")]
#[diagnostic(code(eval::unsafe_negation))]
#[diagnostic(help(
    "Only rule applications that are partially bounded, \
or expressions / unifications that are completely bounded, can be safely negated. \
You may also encounter this error if your rule can never produce any rows."
))]
pub(crate) struct UnsafeNegation(#[label] pub(crate) SourceSpan);

#[derive(Diagnostic, Debug, Error)]
#[error("Atom contains unbound variable, or rule contains no variable at all")]
#[diagnostic(code(eval::unbound_variable))]
pub(crate) struct UnboundVariable(#[label] pub(crate) SourceSpan);

impl NormalFormInlineRule {
    pub(crate) fn convert_to_well_ordered_rule(self) -> Result<Self> {
        let mut seen_variables = BTreeSet::default();
        let mut round_1_collected = vec![];
        let mut pending = vec![];

        // first round: collect all unifications that are completely bounded
        for atom in self.body {
            match atom {
                NormalFormAtom::Unification(u) => {
                    if u.is_const() {
                        seen_variables.insert(u.binding.clone());
                        round_1_collected.push(NormalFormAtom::Unification(u));
                    } else {
                        let unif_vars = u.bindings_in_expr()?;
                        if unif_vars.is_subset(&seen_variables) {
                            seen_variables.insert(u.binding.clone());
                            round_1_collected.push(NormalFormAtom::Unification(u));
                        } else {
                            pending.push(NormalFormAtom::Unification(u));
                        }
                    }
                }
                NormalFormAtom::Rule(mut r) => {
                    for arg in &mut r.args {
                        seen_variables.insert(arg.clone());
                    }
                    round_1_collected.push(NormalFormAtom::Rule(r))
                }
                NormalFormAtom::Relation(v) => {
                    for arg in &v.args {
                        seen_variables.insert(arg.clone());
                    }
                    round_1_collected.push(NormalFormAtom::Relation(v))
                }
                NormalFormAtom::NegatedRule(r) => pending.push(NormalFormAtom::NegatedRule(r)),
                NormalFormAtom::NegatedRelation(v) => {
                    pending.push(NormalFormAtom::NegatedRelation(v))
                }
                NormalFormAtom::Predicate(p) => {
                    pending.push(NormalFormAtom::Predicate(p));
                }
                NormalFormAtom::HnswSearch(s) => {
                    if seen_variables.contains(&s.query) {
                        seen_variables.extend(s.all_bindings().cloned());
                        round_1_collected.push(NormalFormAtom::HnswSearch(s));
                    } else {
                        pending.push(NormalFormAtom::HnswSearch(s));
                    }
                }
                NormalFormAtom::FtsSearch(s) => {
                    if seen_variables.contains(&s.query) {
                        seen_variables.extend(s.all_bindings().cloned());
                        round_1_collected.push(NormalFormAtom::FtsSearch(s));
                    } else {
                        pending.push(NormalFormAtom::FtsSearch(s));
                    }
                }
                NormalFormAtom::LshSearch(s) => {
                    if seen_variables.contains(&s.query) {
                        seen_variables.extend(s.all_bindings().cloned());
                        round_1_collected.push(NormalFormAtom::LshSearch(s));
                    } else {
                        pending.push(NormalFormAtom::LshSearch(s));
                    }
                }
            }
        }

        let mut collected = vec![];
        seen_variables.clear();
        let mut last_pending = vec![];
        // second round: insert pending where possible
        for atom in round_1_collected {
            mem::swap(&mut last_pending, &mut pending);
            pending.clear();
            match atom {
                NormalFormAtom::Rule(r) => {
                    seen_variables.extend(r.args.iter().cloned());
                    collected.push(NormalFormAtom::Rule(r))
                }
                NormalFormAtom::Relation(v) => {
                    seen_variables.extend(v.args.iter().cloned());
                    collected.push(NormalFormAtom::Relation(v))
                }
                NormalFormAtom::NegatedRule(_)
                | NormalFormAtom::NegatedRelation(_)
                | NormalFormAtom::Predicate(_) => {
                    unreachable!()
                }
                NormalFormAtom::Unification(u) => {
                    seen_variables.insert(u.binding.clone());
                    collected.push(NormalFormAtom::Unification(u));
                }
                NormalFormAtom::HnswSearch(s) => {
                    seen_variables.extend(s.all_bindings().cloned());
                    collected.push(NormalFormAtom::HnswSearch(s));
                }
                NormalFormAtom::FtsSearch(s) => {
                    seen_variables.extend(s.all_bindings().cloned());
                    collected.push(NormalFormAtom::FtsSearch(s));
                }
                NormalFormAtom::LshSearch(s) => {
                    seen_variables.extend(s.all_bindings().cloned());
                    collected.push(NormalFormAtom::LshSearch(s));
                }
            }
            for atom in last_pending.iter() {
                match atom {
                    NormalFormAtom::Rule(_) | NormalFormAtom::Relation(_) => unreachable!(),
                    NormalFormAtom::NegatedRule(r) => {
                        if r.args.iter().all(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRule(r.clone()));
                        } else {
                            pending.push(NormalFormAtom::NegatedRule(r.clone()));
                        }
                    }
                    NormalFormAtom::NegatedRelation(v) => {
                        if v.args.iter().all(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRelation(v.clone()));
                        } else {
                            pending.push(NormalFormAtom::NegatedRelation(v.clone()));
                        }
                    }
                    NormalFormAtom::HnswSearch(s) => {
                        if seen_variables.contains(&s.query) {
                            seen_variables.extend(s.all_bindings().cloned());
                            collected.push(NormalFormAtom::HnswSearch(s.clone()));
                        } else {
                            pending.push(NormalFormAtom::HnswSearch(s.clone()));
                        }
                    }
                    NormalFormAtom::FtsSearch(s) => {
                        if seen_variables.contains(&s.query) {
                            seen_variables.extend(s.all_bindings().cloned());
                            collected.push(NormalFormAtom::FtsSearch(s.clone()));
                        } else {
                            pending.push(NormalFormAtom::FtsSearch(s.clone()));
                        }
                    }
                    NormalFormAtom::LshSearch(s) => {
                        if seen_variables.contains(&s.query) {
                            seen_variables.extend(s.all_bindings().cloned());
                            collected.push(NormalFormAtom::LshSearch(s.clone()));
                        } else {
                            pending.push(NormalFormAtom::LshSearch(s.clone()));
                        }
                    }
                    NormalFormAtom::Predicate(p) => {
                        if p.bindings()?.is_subset(&seen_variables) {
                            collected.push(NormalFormAtom::Predicate(p.clone()));
                        } else {
                            pending.push(NormalFormAtom::Predicate(p.clone()));
                        }
                    }
                    NormalFormAtom::Unification(u) => {
                        if u.bindings_in_expr()?.is_subset(&seen_variables) {
                            collected.push(NormalFormAtom::Unification(u.clone()));
                        } else {
                            pending.push(NormalFormAtom::Unification(u.clone()));
                        }
                    }
                }
            }
        }

        if !pending.is_empty() {
            for atom in pending {
                match atom {
                    NormalFormAtom::Rule(_) | NormalFormAtom::Relation(_) => unreachable!(),
                    NormalFormAtom::NegatedRule(r) => {
                        if r.args.iter().any(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRule(r.clone()));
                        } else {
                            bail!(UnsafeNegation(r.span));
                        }
                    }
                    NormalFormAtom::NegatedRelation(v) => {
                        if v.args.iter().any(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRelation(v.clone()));
                        } else {
                            bail!(UnsafeNegation(v.span));
                        }
                    }
                    NormalFormAtom::Predicate(p) => {
                        bail!(UnboundVariable(p.span()))
                    }
                    NormalFormAtom::Unification(u) => {
                        bail!(UnboundVariable(u.span))
                    }
                    NormalFormAtom::HnswSearch(s) => {
                        bail!(UnboundVariable(s.span))
                    }
                    NormalFormAtom::FtsSearch(s) => {
                        bail!(UnboundVariable(s.span))
                    }
                    NormalFormAtom::LshSearch(s) => {
                        bail!(UnboundVariable(s.span))
                    }
                }
            }
        }

        Ok(NormalFormInlineRule {
            head: self.head,
            aggr: self.aggr,
            body: collected,
        })
    }
}
