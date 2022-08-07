use std::collections::BTreeSet;
use std::mem;

use anyhow::{bail, Result};

use crate::data::program::{NormalFormAtom, NormalFormRule};
use crate::data::symb::Symbol;

impl NormalFormRule {
    pub(crate) fn convert_to_well_ordered_rule(self) -> Result<Self> {
        let mut seen_variables = BTreeSet::default();
        let mut round_1_collected = vec![];
        let mut pending = vec![];
        let mut symb_count = 0;
        let mut process_ignored_symbol = |symb: &mut Symbol| {
            if symb.is_ignored_var() {
                symb_count += 1;
                let mut new_symb = Symbol::from(&format!("_{}", symb_count) as &str);
                mem::swap(&mut new_symb, symb);
            }
        };

        for atom in self.body {
            match atom {
                NormalFormAtom::Unification(mut u) => {
                    process_ignored_symbol(&mut u.binding);
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
                NormalFormAtom::AttrTriple(mut t) => {
                    process_ignored_symbol(&mut t.value);
                    process_ignored_symbol(&mut t.entity);
                    seen_variables.insert(t.value.clone());
                    seen_variables.insert(t.entity.clone());
                    round_1_collected.push(NormalFormAtom::AttrTriple(t));
                }
                NormalFormAtom::Rule(mut r) => {
                    for arg in &mut r.args {
                        process_ignored_symbol(arg);
                        seen_variables.insert(arg.clone());
                    }
                    round_1_collected.push(NormalFormAtom::Rule(r))
                }
                NormalFormAtom::NegatedAttrTriple(mut t) => {
                    process_ignored_symbol(&mut t.value);
                    process_ignored_symbol(&mut t.entity);
                    pending.push(NormalFormAtom::NegatedAttrTriple(t))
                }
                NormalFormAtom::NegatedRule(mut r) => {
                    for arg in &mut r.args {
                        process_ignored_symbol(arg);
                    }
                    pending.push(NormalFormAtom::NegatedRule(r))
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
                NormalFormAtom::AttrTriple(t) => {
                    seen_variables.insert(t.value.clone());
                    seen_variables.insert(t.entity.clone());
                    collected.push(NormalFormAtom::AttrTriple(t))
                }
                NormalFormAtom::Rule(r) => {
                    seen_variables.extend(r.args.iter().cloned());
                    collected.push(NormalFormAtom::Rule(r))
                }
                NormalFormAtom::NegatedAttrTriple(_)
                | NormalFormAtom::NegatedRule(_)
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
                    NormalFormAtom::AttrTriple(_) | NormalFormAtom::Rule(_) => unreachable!(),
                    NormalFormAtom::NegatedAttrTriple(t) => {
                        if seen_variables.contains(&t.value) && seen_variables.contains(&t.entity) {
                            collected.push(NormalFormAtom::NegatedAttrTriple(t.clone()));
                        } else {
                            pending.push(NormalFormAtom::NegatedAttrTriple(t.clone()));
                        }
                    }
                    NormalFormAtom::NegatedRule(r) => {
                        if r.args.iter().all(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRule(r.clone()));
                        } else {
                            pending.push(NormalFormAtom::NegatedRule(r.clone()));
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
                    NormalFormAtom::AttrTriple(_) | NormalFormAtom::Rule(_) => unreachable!(),
                    NormalFormAtom::NegatedAttrTriple(t) => {
                        if seen_variables.contains(&t.value) || seen_variables.contains(&t.entity) {
                            collected.push(NormalFormAtom::NegatedAttrTriple(t.clone()));
                        } else {
                            bail!("found unsafe triple in rule: {:?}", t)
                        }
                    }
                    NormalFormAtom::NegatedRule(r) => {
                        if r.args.iter().any(|a| seen_variables.contains(a)) {
                            collected.push(NormalFormAtom::NegatedRule(r.clone()));
                        } else {
                            bail!("found unsafe rule application in rule: {:?}", r);
                        }
                    }
                    NormalFormAtom::Predicate(p) => {
                        bail!("found unsafe predicate in rule: {:?}", p)
                    }
                    NormalFormAtom::Unification(u) => {
                        bail!("found unsafe unification in rule: {:?}", u)
                    }
                }
            }
        }

        Ok(NormalFormRule {
            head: self.head,
            aggr: self.aggr,
            body: collected,
            vld: self.vld,
        })
    }
}

// fn reorder_rule_body_for_negations(clauses: Vec<Atom>) -> Result<Vec<Atom>> {
//     let (negations, others): (Vec<_>, _) = clauses.into_iter().partition(|a| a.is_negation());
//     let mut seen_bindings = BTreeSet::new();
//     for a in &others {
//         a.collect_bindings(&mut seen_bindings);
//     }
//     let mut negations_with_meta = negations
//         .into_iter()
//         .map(|p| {
//             let p = p.into_negated().unwrap();
//             let mut bindings = Default::default();
//             p.collect_bindings(&mut bindings);
//             let valid_bindings: BTreeSet<_> =
//                 bindings.intersection(&seen_bindings).cloned().collect();
//             (Some(p), valid_bindings)
//         })
//         .collect_vec();
//     let mut ret = vec![];
//     seen_bindings.clear();
//     for a in others {
//         a.collect_bindings(&mut seen_bindings);
//         ret.push(a);
//         for (negated, pred_bindings) in negations_with_meta.iter_mut() {
//             if negated.is_none() {
//                 continue;
//             }
//             if seen_bindings.is_superset(pred_bindings) {
//                 let negated = negated.take().unwrap();
//                 ret.push(Atom::Negation(Box::new(negated)));
//             }
//         }
//     }
//     Ok(ret)
// }
//
// fn reorder_rule_body_for_predicates(clauses: Vec<Atom>) -> Result<Vec<Atom>> {
//     let (predicates, others): (Vec<_>, _) = clauses.into_iter().partition(|a| a.is_predicate());
//     let mut predicates_with_meta = predicates
//         .into_iter()
//         .map(|p| {
//             let p = p.into_predicate().unwrap();
//             let bindings = p.bindings();
//             (Some(p), bindings)
//         })
//         .collect_vec();
//     let mut seen_bindings = BTreeSet::new();
//     let mut ret = vec![];
//     for a in others {
//         a.collect_bindings(&mut seen_bindings);
//         ret.push(a);
//         for (pred, pred_bindings) in predicates_with_meta.iter_mut() {
//             if pred.is_none() {
//                 continue;
//             }
//             if seen_bindings.is_superset(pred_bindings) {
//                 let pred = pred.take().unwrap();
//                 ret.push(Atom::Predicate(pred));
//             }
//         }
//     }
//     for (p, bindings) in predicates_with_meta {
//         ensure!(
//                 p.is_none(),
//                 "unsafe bindings {:?} found in predicate {:?}",
//                 bindings
//                     .difference(&seen_bindings)
//                     .cloned()
//                     .collect::<BTreeSet<_>>(),
//                 p.unwrap()
//             );
//     }
//     Ok(ret)
// }
