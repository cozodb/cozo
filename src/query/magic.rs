use std::collections::BTreeSet;
use std::mem;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::data::program::{
    MagicAtom, MagicAttrTripleAtom, MagicKeyword, MagicProgram, MagicRule, MagicRuleApplyAtom,
    NormalFormAtom, NormalFormProgram, NormalFormRule, StratifiedMagicProgram,
    StratifiedNormalFormProgram,
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
                            dbg!(&r_app);
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
                                inp_entry.push(dbg!(MagicRule {
                                    head: inp_args,
                                    aggr: inp_aggr,
                                    body: vec![sup_rule_app],
                                    vld: rule.vld,
                                }))
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
                        adornment.push(!seen_bindings.insert(arg.clone()));
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
