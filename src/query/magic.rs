use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::data::program::{
    MagicAtom, MagicAttrTripleAtom, MagicKeyword, MagicProgram, MagicRule, MagicRuleApplyAtom,
    NormalFormAtom, NormalFormProgram, NormalFormRule, StratifiedMagicProgram,
    StratifiedNormalFormProgram,
};
use crate::query::compile::{
    Atom, BindingHeadTerm, DatalogProgram, Rule, RuleApplyAtom, RuleSet, Term,
};

impl StratifiedNormalFormProgram {
    pub(crate) fn magic_sets_rewrite(self) -> StratifiedMagicProgram {
        let mut upstream_rules = BTreeSet::from([PROG_ENTRY.clone()]);
        let mut collected = vec![];
        for prog in self.0 {
            let adorned = prog.adorn(&upstream_rules);
            collected.push(adorned.magic_rewrite());
            upstream_rules.extend(prog.get_downstream_rules());
        }
        StratifiedMagicProgram(collected)
    }
}

impl MagicProgram {
    fn magic_rewrite(self) -> MagicProgram {
        let mut ret_prog = MagicProgram {
            prog: Default::default(),
        };
        for (rule_head, rules) in self.prog {
            // at this point, rule_head must be Muggle or Magic, the remaining options are impossible
            let rule_name = rule_head.as_keyword();
            let adornment = rule_head.magic_adornment();

            // can only be true if rule is magic and args are not all free
            let rule_has_bound_args = rule_head.has_bound_adornment();

            for (rule_idx, rule) in rules.into_iter().enumerate() {
                let mut sup_idx = 0;
                let mut make_sup_kw = || {
                    let ret = MagicKeyword::Sup {
                        inner: rule_name.clone(),
                        adornment: adornment.into(),
                        rule_idx: rule_idx as u16,
                        sup_idx,
                    };
                    sup_idx += 1;
                    ret
                };
                let mut collected_atoms = vec![];
                let mut seen_bindings: BTreeSet<Keyword> = Default::default();

                // SIP from input rule if rule has any bound args
                if rule_has_bound_args {
                    let sup_kw = make_sup_kw();

                    let sup_args = rule
                        .head
                        .iter()
                        .zip(adornment.iter())
                        .filter_map(
                            |(arg, is_bound)| if *is_bound { Some(arg.clone()) } else { None },
                        )
                        .collect_vec();
                    let sup_aggr = vec![None; sup_args.len()];
                    let sup_body = vec![MagicAtom::Rule(MagicRuleApplyAtom {
                        name: MagicKeyword::Input {
                            inner: rule_name.clone(),
                            adornment: adornment.into(),
                        },
                        args: sup_args.clone(),
                    })];

                    ret_prog.prog.insert(
                        sup_kw.clone(),
                        vec![MagicRule {
                            head: sup_args.clone(),
                            aggr: sup_aggr,
                            body: sup_body,
                            vld: rule.vld,
                        }],
                    );

                    seen_bindings.extend(sup_args.iter().cloned());

                    collected_atoms.push(MagicAtom::Rule(MagicRuleApplyAtom {
                        name: sup_kw,
                        args: sup_args,
                    }))
                }
                for atom in rule.body {
                    match atom {
                        a @ (MagicAtom::Predicate(_)
                        | MagicAtom::NegatedAttrTriple(_)
                        | MagicAtom::NegatedRule(_)) => {
                            collected_atoms.push(a);
                        }
                        MagicAtom::AttrTriple(t) => {
                            seen_bindings.insert(t.entity.clone());
                            seen_bindings.insert(t.value.clone());
                            collected_atoms.push(MagicAtom::AttrTriple(t));
                        }
                        MagicAtom::Unification(u) => {
                            seen_bindings.insert(u.binding.clone());
                            collected_atoms.push(MagicAtom::Unification(u));
                        }
                        MagicAtom::Rule(r_app) => {
                            if r_app.name.has_bound_adornment() {
                                // we are guaranteed to have a magic rule application
                                let sup_kw = make_sup_kw();
                                let args = seen_bindings.iter().cloned().collect_vec();
                                let sup_rule_entry =
                                    ret_prog.prog.entry(sup_kw.clone()).or_default();
                                let mut sup_rule_atoms = vec![];
                                mem::swap(&mut sup_rule_atoms, &mut collected_atoms);

                                // add the sup rule to the program, this clears all collected atoms
                                sup_rule_entry.push(MagicRule {
                                    head: args.clone(),
                                    aggr: vec![None; args.len()],
                                    body: sup_rule_atoms,
                                    vld: rule.vld,
                                });

                                // add the sup rule application to the collected atoms
                                let sup_rule_app = MagicAtom::Rule(MagicRuleApplyAtom {
                                    name: sup_kw.clone(),
                                    args,
                                });
                                collected_atoms.push(sup_rule_app.clone());

                                // finally add to the input rule application
                                let inp_kw = MagicKeyword::Input {
                                    inner: r_app.name.as_keyword().clone(),
                                    adornment: r_app.name.magic_adornment().into(),
                                };
                                let inp_entry = ret_prog.prog.entry(inp_kw.clone()).or_default();
                                let inp_args = r_app
                                    .args
                                    .iter()
                                    .zip(r_app.name.magic_adornment())
                                    .filter_map(
                                        |(kw, is_bound)| {
                                            if *is_bound {
                                                Some(kw.clone())
                                            } else {
                                                None
                                            }
                                        },
                                    )
                                    .collect_vec();
                                let inp_aggr = vec![None; inp_args.len()];
                                inp_entry.push(MagicRule {
                                    head: inp_args,
                                    aggr: inp_aggr,
                                    body: vec![sup_rule_app],
                                    vld: rule.vld,
                                })
                            }
                            seen_bindings.extend(r_app.args.iter().cloned());
                            collected_atoms.push(MagicAtom::Rule(r_app));
                        }
                    }
                }

                let entry = ret_prog.prog.entry(rule_head.clone()).or_default();
                entry.push(MagicRule {
                    head: rule.head,
                    aggr: rule.aggr,
                    body: collected_atoms,
                    vld: rule.vld,
                });
            }
        }
        ret_prog
    }
}

impl NormalFormProgram {
    fn get_downstream_rules(&self) -> BTreeSet<Keyword> {
        let own_rules: BTreeSet<_> = self.prog.keys().collect();
        let mut downstream_rules: BTreeSet<Keyword> = Default::default();
        for rules in self.prog.values() {
            for rule in rules {
                for atom in rule.body.iter() {
                    match atom {
                        NormalFormAtom::Rule(r_app) | NormalFormAtom::NegatedRule(r_app) => {
                            if !own_rules.contains(&r_app.name) {
                                downstream_rules.insert(r_app.name.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        downstream_rules
    }
    fn adorn(&self, upstream_rules: &BTreeSet<Keyword>) -> MagicProgram {
        let rules_to_rewrite: BTreeSet<_> = self
            .prog
            .keys()
            .filter(|k| !upstream_rules.contains(k))
            .cloned()
            .collect();

        let mut pending_adornment = vec![];
        let mut adorned_prog = MagicProgram {
            prog: Default::default(),
        };

        for (rule_name, rules) in &self.prog {
            if rules_to_rewrite.contains(rule_name) {
                // processing starts with the sets of rules NOT subject to rewrite
                continue;
            }
            let mut adorned_rules = Vec::with_capacity(rules.len());
            for rule in rules {
                let adorned_rule = rule.adorn(
                    &mut pending_adornment,
                    &rules_to_rewrite,
                    Default::default(),
                );
                adorned_rules.push(adorned_rule);
            }
            adorned_prog.prog.insert(
                MagicKeyword::Muggle {
                    inner: rule_name.clone(),
                },
                adorned_rules,
            );
        }

        while let Some(head) = pending_adornment.pop() {
            if adorned_prog.prog.contains_key(&head) {
                continue;
            }
            let original_rules = self.prog.get(head.as_keyword()).unwrap();
            let adornment = head.magic_adornment();
            let mut adorned_rules = Vec::with_capacity(original_rules.len());
            for rule in original_rules {
                let seen_bindings = rule
                    .head
                    .iter()
                    .zip(adornment.iter())
                    .filter_map(|(kw, bound)| if *bound { Some(kw.clone()) } else { None })
                    .collect();
                let adorned_rule =
                    rule.adorn(&mut pending_adornment, &rules_to_rewrite, seen_bindings);
                adorned_rules.push(adorned_rule);
            }
            adorned_prog.prog.insert(head, adorned_rules);
        }
        adorned_prog
    }
}

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

impl NormalFormAtom {
    fn adorn(
        &self,
        pending: &mut Vec<MagicKeyword>,
        seen_bindings: &mut BTreeSet<Keyword>,
        rules_to_rewrite: &BTreeSet<Keyword>,
    ) -> MagicAtom {
        match self {
            NormalFormAtom::AttrTriple(a) => {
                let t = MagicAttrTripleAtom {
                    attr: a.attr.clone(),
                    entity: a.entity.clone(),
                    value: a.value.clone(),
                };
                if !seen_bindings.contains(&a.entity) {
                    seen_bindings.insert(a.entity.clone());
                }
                if !seen_bindings.contains(&a.value) {
                    seen_bindings.insert(a.value.clone());
                }
                MagicAtom::AttrTriple(t)
            }
            NormalFormAtom::Predicate(p) => {
                // predicate cannot introduce new bindings
                MagicAtom::Predicate(p.clone())
            }
            NormalFormAtom::Rule(rule) => {
                if rules_to_rewrite.contains(&rule.name) {
                    // first mark adorned rules
                    // then
                    let mut adornment = SmallVec::new();
                    for arg in rule.args.iter() {
                        adornment.push(seen_bindings.insert(arg.clone()));
                    }
                    let name = MagicKeyword::Magic {
                        inner: rule.name.clone(),
                        adornment,
                    };

                    pending.push(name.clone());

                    MagicAtom::Rule(MagicRuleApplyAtom {
                        name,
                        args: rule.args.clone(),
                    })
                } else {
                    MagicAtom::Rule(MagicRuleApplyAtom {
                        name: MagicKeyword::Muggle {
                            inner: rule.name.clone(),
                        },
                        args: rule.args.clone(),
                    })
                }
            }
            NormalFormAtom::NegatedAttrTriple(na) => {
                MagicAtom::NegatedAttrTriple(MagicAttrTripleAtom {
                    attr: na.attr.clone(),
                    entity: na.entity.clone(),
                    value: na.value.clone(),
                })
            }
            NormalFormAtom::NegatedRule(nr) => MagicAtom::NegatedRule(MagicRuleApplyAtom {
                name: MagicKeyword::Muggle {
                    inner: nr.name.clone(),
                },
                args: nr.args.clone(),
            }),
            NormalFormAtom::Unification(u) => {
                seen_bindings.insert(u.binding.clone());
                MagicAtom::Unification(u.clone())
            }
        }
    }
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

impl NormalFormRule {
    fn adorn(
        &self,
        pending: &mut Vec<MagicKeyword>,
        rules_to_rewrite: &BTreeSet<Keyword>,
        mut seen_bindings: BTreeSet<Keyword>,
    ) -> MagicRule {
        let mut ret_body = Vec::with_capacity(self.body.len());

        for atom in &self.body {
            let new_atom = atom.adorn(pending, &mut seen_bindings, rules_to_rewrite);
            ret_body.push(new_atom);
        }
        MagicRule {
            head: self.head.clone(),
            aggr: self.aggr.clone(),
            body: ret_body,
            vld: self.vld,
        }
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
                // muggle rules are always unbound
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
                // bound rules have initial entry
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
