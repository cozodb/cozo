use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use itertools::Itertools;

use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::query::compile::{
    Atom, BindingHeadTerm, DatalogProgram, Rule, RuleApplyAtom, RuleSet, Term,
};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct AdornedHead {
    name: Keyword,
    adornment: Vec<bool>,
}

type AdornedDatalogProgram = BTreeMap<AdornedHead, RuleSet>;

pub(crate) fn magic_sets_rewrite(prog: &DatalogProgram) -> DatalogProgram {
    let own_rules: BTreeSet<_> = prog.keys().collect();
    let adorned = adorn_program(prog, &own_rules);
    adorned_to_magic(&adorned)
}

fn adorn_atom(
    atom: &Atom,
    adorned_prog: &AdornedDatalogProgram,
    pending: &mut Vec<AdornedHead>,
    seen_bindings: &mut BTreeSet<Keyword>,
    own_rules: &BTreeSet<&Keyword>,
) -> Atom {
    match atom {
        Atom::AttrTriple(a) => {
            if let Term::Var(ref kw) = a.entity {
                seen_bindings.insert(kw.clone());
            }
            if let Term::Var(ref kw) = a.value {
                seen_bindings.insert(kw.clone());
            }
            Atom::AttrTriple(a.clone())
        }
        Atom::Predicate(p) => {
            // predicate cannot introduce new bindings
            Atom::Predicate(p.clone())
        }
        Atom::Rule(rule) => {
            if own_rules.contains(&rule.name) {
                // first mark adorned rules
                // then
                let mut adornment = Vec::with_capacity(rule.args.len());
                for term in rule.args.iter() {
                    if let Term::Var(kw) = term {
                        let var_is_free = seen_bindings.insert(kw.clone());
                        adornment.push(!var_is_free);
                    } else {
                        adornment.push(false);
                    }
                }
                let adorned_head = AdornedHead {
                    name: rule.name.clone(),
                    adornment: adornment.clone(),
                };
                let mut ret_rule = rule.clone();
                ret_rule.adornment = Some(adornment.clone());
                let ret_rule_head = AdornedHead {
                    name: rule.name.clone(),
                    adornment: adornment.clone(),
                };
                if !adorned_prog.contains_key(&ret_rule_head) {
                    pending.push(adorned_head);
                }
                Atom::Rule(ret_rule)
            } else {
                Atom::Rule(rule.clone())
            }
        }
        n @ Atom::Negation(_) => n.clone(),
        Atom::Conjunction(_) | Atom::Disjunction(_) => unreachable!(),
    }
}

fn adorn_rule(
    rule: &Rule,
    adorned_prog: &AdornedDatalogProgram,
    pending: &mut Vec<AdornedHead>,
    own_rules: &BTreeSet<&Keyword>,
) -> Rule {
    let mut seen_bindings = BTreeSet::new();

    let mut ret_body = Vec::with_capacity(rule.body.len());

    for atom in &rule.body {
        let new_atom = adorn_atom(atom, adorned_prog, pending, &mut seen_bindings, own_rules);
        ret_body.push(new_atom);
    }
    Rule {
        head: rule.head.clone(),
        body: ret_body,
        vld: rule.vld,
    }
}

fn adorn_program(input: &DatalogProgram, own_rules: &BTreeSet<&Keyword>) -> AdornedDatalogProgram {
    // prerequisites: the input is already in disjunctive normal form,
    let mut adorned_program: AdornedDatalogProgram = Default::default();
    let mut pending_adornment = vec![];

    let entry_ruleset = input.get(&PROG_ENTRY as &Keyword).unwrap();
    let mut adorned_rules = Vec::with_capacity(entry_ruleset.rules.len());
    for rule in &entry_ruleset.rules {
        let adorned_rule = adorn_rule(rule, &adorned_program, &mut pending_adornment, own_rules);
        adorned_rules.push(adorned_rule);
    }
    adorned_program.insert(
        AdornedHead {
            name: PROG_ENTRY.clone(),
            adornment: vec![],
        },
        RuleSet {
            rules: adorned_rules,
            arity: entry_ruleset.arity,
        },
    );

    while let Some(head) = pending_adornment.pop() {
        if adorned_program.contains_key(&head) {
            continue;
        }

        let original_rule_set = input.get(&head.name).unwrap();
        let mut adorned_rules = Vec::with_capacity(original_rule_set.rules.len());
        for rule in &original_rule_set.rules {
            let adorned_rule =
                adorn_rule(rule, &adorned_program, &mut pending_adornment, own_rules);
            adorned_rules.push(adorned_rule);
        }

        adorned_program.insert(
            head,
            RuleSet {
                rules: adorned_rules,
                arity: original_rule_set.arity,
            },
        );
    }

    adorned_program
}

fn make_adorned_kw(name: &Keyword, prefix: &str, adornment: &[bool]) -> Keyword {
    let mut rule_name = format!("[{}]{}", name.0, prefix,);
    for bound in adornment {
        rule_name.push(if *bound { 'b' } else { 'f' })
    }
    Keyword::from(&rule_name as &str)
}

fn make_magic_rule_head(name: &Keyword, adornment: &[bool]) -> Keyword {
    make_adorned_kw(name, "", adornment)
}

fn make_magic_input_rule_head(name: &Keyword, adornment: &[bool]) -> Keyword {
    make_adorned_kw(name, "I", adornment)
}

fn make_magic_sup_rule_head(
    name: &Keyword,
    rule_idx: usize,
    pos: usize,
    adornment: &[bool],
) -> Keyword {
    let mut rule_name = format!("[{}.{}]S{}", name.0, rule_idx, pos);
    for bound in adornment {
        rule_name.push(if *bound { 'b' } else { 'f' })
    }
    Keyword::from(&rule_name as &str)
}

fn make_magic_sup_rule_app(
    name: &Keyword,
    rule_idx: usize,
    pos: usize,
    args: &[Keyword],
    adornment: &[bool],
) -> Atom {
    let rule_name = make_magic_sup_rule_head(name, rule_idx, pos, adornment);
    Atom::Rule(RuleApplyAtom {
        name: rule_name,
        args: args.iter().map(|kw| Term::Var(kw.clone())).collect_vec(),
        adornment: None,
    })
}

fn make_magic_input_rule_app(name: &Keyword, adornment: &[bool], args: &[BindingHeadTerm]) -> Atom {
    let rule_name = make_magic_input_rule_head(name, adornment);
    Atom::Rule(RuleApplyAtom {
        name: rule_name,
        args: args
            .iter()
            .zip(adornment.iter())
            .filter_map(|(ht, is_bound)| {
                if *is_bound {
                    Some(Term::Var(ht.name.clone()))
                } else {
                    None
                }
            })
            .collect_vec(),
        adornment: None,
    })
}

// fn make_magic_sup_rule_app()

fn adorned_to_magic(input: &AdornedDatalogProgram) -> DatalogProgram {
    let mut ret_prog: DatalogProgram = Default::default();

    for (rule_head, rule_set) in input {
        for (rule_idx, rule) in rule_set.rules.iter().enumerate() {
            let mut rule_is_bound = false;
            for is_bound in &rule_head.adornment {
                if *is_bound {
                    rule_is_bound = true;
                    break;
                }
            }
            if rule_is_bound {
                let sup_rule_head =
                    make_magic_sup_rule_head(&rule_head.name, rule_idx, 0, &rule_head.adornment);
                let args = rule
                    .head
                    .iter()
                    .zip(rule_head.adornment.iter())
                    .filter_map(|(arg, is_bound)| {
                        if *is_bound {
                            Some(BindingHeadTerm {
                                name: arg.name.clone(),
                                aggr: arg.aggr.clone(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                let entry = ret_prog.entry(sup_rule_head).or_insert_with(|| RuleSet {
                    rules: vec![],
                    arity: args.len(),
                });
                let body = vec![make_magic_input_rule_app(
                    &rule_head.name,
                    &rule_head.adornment,
                    &args,
                )];
                debug_assert_eq!(entry.arity, args.len());
                entry.rules.push(Rule {
                    head: args,
                    body,
                    vld: rule.vld,
                })
            }

            let sup_0_bindings = rule
                .head
                .iter()
                .zip(rule_head.adornment.iter())
                .filter_map(|(head_term, is_bound)| {
                    if *is_bound {
                        Some(head_term.name.clone())
                    } else {
                        None
                    }
                })
                .collect_vec();

            let mut collected_atoms = vec![];

            if rule_is_bound {
                collected_atoms.push(make_magic_sup_rule_app(
                    &rule_head.name,
                    rule_idx,
                    0,
                    &sup_0_bindings,
                    &rule_head.adornment,
                ));
            }
            let mut seen_bindings: BTreeSet<_> = sup_0_bindings.iter().cloned().collect();
            for (atom_idx, atom) in rule.body.iter().enumerate() {
                let atom_idx = atom_idx + 1;
                match atom {
                    Atom::AttrTriple(a) => {
                        if let Term::Var(ref kw) = a.entity {
                            seen_bindings.insert(kw.clone());
                        }
                        if let Term::Var(ref kw) = a.value {
                            seen_bindings.insert(kw.clone());
                        }
                        collected_atoms.push(Atom::AttrTriple(a.clone()))
                    }
                    Atom::Predicate(p) => collected_atoms.push(Atom::Predicate(p.clone())),
                    n @ Atom::Negation(_) => collected_atoms.push(n.clone()),
                    Atom::Conjunction(_) | Atom::Disjunction(_) => unreachable!(),
                    Atom::Rule(r) => {
                        if let Some(r_adornment) = r.adornment.as_ref() {
                            if r_adornment.iter().all(|bound| !*bound) {
                                let head = make_magic_rule_head(&r.name, r_adornment);
                                collected_atoms.push(Atom::Rule(RuleApplyAtom {
                                    name: head.clone(),
                                    args: r.args.clone(),
                                    adornment: None,
                                }));
                            } else {
                                let sup_head = make_magic_sup_rule_head(
                                    &rule_head.name,
                                    rule_idx,
                                    atom_idx,
                                    &rule_head.adornment,
                                );
                                // todo: order it such that seen bindings has the applied rule as prefix
                                // see m7 in notes
                                let args = seen_bindings.iter().collect_vec();
                                let entry =
                                    ret_prog.entry(sup_head.clone()).or_insert_with(|| RuleSet {
                                        rules: vec![],
                                        arity: args.len(),
                                    });
                                let mut sup_rule_atoms = vec![];
                                mem::swap(&mut sup_rule_atoms, &mut collected_atoms);
                                let head_args = args
                                    .iter()
                                    .map(|kw| BindingHeadTerm {
                                        name: (*kw).clone(),
                                        aggr: Default::default(),
                                    })
                                    .collect_vec();
                                debug_assert_eq!(entry.arity, head_args.len());
                                entry.rules.push(Rule {
                                    head: head_args,
                                    body: sup_rule_atoms,
                                    vld: rule.vld,
                                });
                                let sup_app_rule_atom = RuleApplyAtom {
                                    name: sup_head,
                                    args: args
                                        .iter()
                                        .map(|kw| Term::Var((*kw).clone()))
                                        .collect_vec(),
                                    adornment: None,
                                };
                                let sup_app_rule = Atom::Rule(sup_app_rule_atom);
                                collected_atoms.push(sup_app_rule.clone());

                                let head = make_magic_rule_head(&r.name, r_adornment);
                                collected_atoms.push(Atom::Rule(RuleApplyAtom {
                                    name: head.clone(),
                                    args: r.args.clone(),
                                    adornment: None,
                                }));

                                let ihead = make_magic_input_rule_head(&r.name, &r_adornment);
                                let mut arity = 0;
                                for is_bound in r_adornment {
                                    if *is_bound {
                                        arity += 1;
                                    }
                                }
                                let ientry = ret_prog.entry(ihead).or_insert_with(|| RuleSet {
                                    rules: vec![],
                                    arity,
                                });
                                let ientry_args = r
                                    .args
                                    .iter()
                                    .zip(r_adornment.iter())
                                    .filter_map(|(kw, is_bound)| {
                                        if *is_bound {
                                            Some(BindingHeadTerm {
                                                name: kw.get_var().cloned().unwrap(),
                                                aggr: Default::default(),
                                            })
                                        } else {
                                            None
                                        }
                                    })
                                    .collect_vec();
                                debug_assert_eq!(ientry.arity, ientry_args.len());
                                ientry.rules.push(Rule {
                                    head: ientry_args,
                                    body: vec![sup_app_rule],
                                    vld: rule.vld,
                                });
                            }
                        } else {
                            collected_atoms.push(Atom::Rule(r.clone()))
                        }
                    }
                }
            }
            let new_rule_name = if rule_head.name.is_prog_entry() {
                rule_head.name.clone()
            } else {
                make_magic_rule_head(&rule_head.name, &rule_head.adornment)
            };
            let ruleset = ret_prog.entry(new_rule_name).or_insert_with(|| RuleSet {
                rules: vec![],
                arity: rule_set.arity,
            });
            debug_assert_eq!(ruleset.arity, rule.head.len());
            ruleset.rules.push(Rule {
                head: rule.head.clone(),
                body: collected_atoms,
                vld: rule.vld,
            })
        }
    }

    ret_prog
}
