use std::collections::BTreeSet;
use std::mem;

use miette::{bail, Diagnostic, Result};
use thiserror::Error;

use crate::data::program::{NormalFormAtom, NormalFormRule};
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

impl NormalFormRule {
    pub(crate) fn convert_to_well_ordered_rule(self) -> Result<Self> {
        let mut seen_variables = BTreeSet::default();
        let mut round_1_collected = vec![];
        let mut pending = vec![];

        for atom in self.body {
            match atom {
                NormalFormAtom::Unification(u) => {
                    if u.is_const() {
                        seen_variables.insert(u.binding.clone());
                        round_1_collected.push(NormalFormAtom::Unification(u));
                    } else {
                        let unif_vars = u.bindings_in_expr();
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
                NormalFormAtom::Relation(mut v) => {
                    for arg in &mut v.args {
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
            }
        }

        let mut collected = vec![];
        seen_variables.clear();
        let mut last_pending = vec![];
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
            }
            for atom in last_pending.iter() {
                match atom {
                    NormalFormAtom::Rule(_)
                    | NormalFormAtom::Relation(_) => unreachable!(),
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
                    NormalFormAtom::Predicate(p) => {
                        if p.bindings().is_subset(&seen_variables) {
                            collected.push(NormalFormAtom::Predicate(p.clone()));
                        } else {
                            pending.push(NormalFormAtom::Predicate(p.clone()));
                        }
                    }
                    NormalFormAtom::Unification(u) => {
                        if u.bindings_in_expr().is_subset(&seen_variables) {
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
                    NormalFormAtom::Rule(_)
                    | NormalFormAtom::Relation(_) => unreachable!(),
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
                        bail!(UnsafeNegation(p.span()))
                    }
                    NormalFormAtom::Unification(u) => {
                        bail!(UnsafeNegation(u.span))
                    }
                }
            }
        }

        Ok(NormalFormRule {
            head: self.head,
            aggr: self.aggr,
            body: collected,
        })
    }
}
