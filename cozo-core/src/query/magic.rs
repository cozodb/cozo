/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeSet;
use std::mem;

use itertools::Itertools;
use miette::{bail, ensure, Result};
use smallvec::SmallVec;
use smartstring::SmartString;

use crate::data::program::{
    FixedRuleArg, MagicAtom, MagicFixedRuleApply, MagicFixedRuleRuleArg, MagicInlineRule,
    MagicProgram, MagicRelationApplyAtom, MagicRuleApplyAtom, MagicRulesOrFixed, MagicSymbol,
    NormalFormAtom, NormalFormInlineRule, NormalFormProgram, NormalFormRulesOrFixed,
    StratifiedMagicProgram, StratifiedNormalFormProgram,
};
use crate::data::relation::{ColType, NullableColType};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::parse::SourceSpan;
use crate::query::logical::NamedFieldNotFound;
use crate::query::ra::InvalidTimeTravelScanning;
use crate::runtime::transact::SessionTx;

impl NormalFormProgram {
    pub(crate) fn exempt_aggr_rules_for_magic_sets(&self, exempt_rules: &mut BTreeSet<Symbol>) {
        for (name, rule_set) in self.prog.iter() {
            if self.disable_magic_rewrite {
                exempt_rules.insert(name.clone());
                continue;
            }
            match rule_set {
                NormalFormRulesOrFixed::Rules { rules: rule_set } => {
                    'outer: for rule in rule_set.iter() {
                        for aggr in rule.aggr.iter() {
                            if aggr.is_some() {
                                exempt_rules.insert(name.clone());
                                continue 'outer;
                            }
                        }
                    }
                }
                NormalFormRulesOrFixed::Fixed { fixed: _ } => {}
            }
        }
    }
}

impl StratifiedNormalFormProgram {
    pub(crate) fn magic_sets_rewrite(self, tx: &SessionTx<'_>) -> Result<StratifiedMagicProgram> {
        let mut exempt_rules = BTreeSet::from([Symbol::new(PROG_ENTRY, SourceSpan(0, 0))]);
        let mut collected = vec![];
        for prog in self.0 {
            prog.exempt_aggr_rules_for_magic_sets(&mut exempt_rules);
            let down_stream_rules = prog.get_downstream_rules();
            let adorned = prog.adorn(&exempt_rules, tx)?;
            collected.push(adorned.magic_rewrite());
            exempt_rules.extend(down_stream_rules);
        }
        Ok(StratifiedMagicProgram(collected))
    }
}

impl MagicProgram {
    fn magic_rewrite(self) -> MagicProgram {
        let mut ret_prog = MagicProgram {
            prog: Default::default(),
        };
        for (rule_head, ruleset) in self.prog {
            match ruleset {
                MagicRulesOrFixed::Rules { rules: ruleset } => {
                    magic_rewrite_ruleset(rule_head, ruleset, &mut ret_prog);
                }
                MagicRulesOrFixed::Fixed { fixed } => {
                    ret_prog
                        .prog
                        .insert(rule_head, MagicRulesOrFixed::Fixed { fixed });
                }
            }
        }
        ret_prog
    }
}

fn magic_rewrite_ruleset(
    rule_head: MagicSymbol,
    ruleset: Vec<MagicInlineRule>,
    ret_prog: &mut MagicProgram,
) {
    // at this point, rule_head must be Muggle or Magic, the remaining options are impossible
    let rule_name = rule_head.as_plain_symbol();
    let adornment = rule_head.magic_adornment();

    // can only be true if rule is magic and args are not all free
    let rule_has_bound_args = rule_head.has_bound_adornment();

    for (rule_idx, rule) in ruleset.into_iter().enumerate() {
        let mut sup_idx = 0;
        let mut make_sup_kw = || {
            let ret = MagicSymbol::Sup {
                inner: rule_name.clone(),
                adornment: adornment.into(),
                rule_idx: rule_idx as u16,
                sup_idx,
            };
            sup_idx += 1;
            ret
        };
        let mut collected_atoms = vec![];
        let mut seen_bindings: BTreeSet<Symbol> = Default::default();

        // SIP from input rule if rule has any bound args
        if rule_has_bound_args {
            let sup_kw = make_sup_kw();

            let sup_args = rule
                .head
                .iter()
                .zip(adornment.iter())
                .filter_map(
                    |(arg, is_bound)| {
                        if *is_bound {
                            Some(arg.clone())
                        } else {
                            None
                        }
                    },
                )
                .collect_vec();
            let sup_aggr = vec![None; sup_args.len()];
            let sup_body = vec![MagicAtom::Rule(MagicRuleApplyAtom {
                name: MagicSymbol::Input {
                    inner: rule_name.clone(),
                    adornment: adornment.into(),
                },
                args: sup_args.clone(),
                span: rule_head.symbol().span,
            })];

            ret_prog.prog.insert(
                sup_kw.clone(),
                MagicRulesOrFixed::Rules {
                    rules: vec![MagicInlineRule {
                        head: sup_args.clone(),
                        aggr: sup_aggr,
                        body: sup_body,
                    }],
                },
            );

            seen_bindings.extend(sup_args.iter().cloned());

            collected_atoms.push(MagicAtom::Rule(MagicRuleApplyAtom {
                name: sup_kw,
                args: sup_args,
                span: rule_head.symbol().span,
            }))
        }
        for atom in rule.body {
            match atom {
                a @ (MagicAtom::Predicate(_)
                | MagicAtom::NegatedRule(_)
                | MagicAtom::NegatedRelation(_)) => {
                    collected_atoms.push(a);
                }
                MagicAtom::Relation(v) => {
                    seen_bindings.extend(v.args.iter().cloned());
                    collected_atoms.push(MagicAtom::Relation(v));
                }
                MagicAtom::Unification(u) => {
                    seen_bindings.insert(u.binding.clone());
                    collected_atoms.push(MagicAtom::Unification(u));
                }
                MagicAtom::HnswSearch(s) => {
                    seen_bindings.extend(s.all_bindings().cloned());
                    collected_atoms.push(MagicAtom::HnswSearch(s));
                }
                MagicAtom::FtsSearch(s) => {
                    seen_bindings.extend(s.all_bindings().cloned());
                    collected_atoms.push(MagicAtom::FtsSearch(s));
                }
                MagicAtom::LshSearch(s) => {
                    seen_bindings.extend(s.all_bindings().cloned());
                    collected_atoms.push(MagicAtom::LshSearch(s));
                }
                MagicAtom::Rule(r_app) => {
                    if r_app.name.has_bound_adornment() {
                        // we are guaranteed to have a magic rule application
                        let sup_kw = make_sup_kw();
                        let args = seen_bindings.iter().cloned().collect_vec();
                        let sup_rule_entry = ret_prog
                            .prog
                            .entry(sup_kw.clone())
                            .or_default()
                            .mut_rules()
                            .unwrap();
                        let mut sup_rule_atoms = vec![];
                        mem::swap(&mut sup_rule_atoms, &mut collected_atoms);

                        // add the sup rule to the program, this clears all collected atoms
                        sup_rule_entry.push(MagicInlineRule {
                            head: args.clone(),
                            aggr: vec![None; args.len()],
                            body: sup_rule_atoms,
                        });

                        // add the sup rule application to the collected atoms
                        let sup_rule_app = MagicAtom::Rule(MagicRuleApplyAtom {
                            name: sup_kw.clone(),
                            args,
                            span: rule_head.symbol().span,
                        });
                        collected_atoms.push(sup_rule_app.clone());

                        // finally add to the input rule application
                        let inp_kw = MagicSymbol::Input {
                            inner: r_app.name.as_plain_symbol().clone(),
                            adornment: r_app.name.magic_adornment().into(),
                        };
                        let inp_entry = ret_prog
                            .prog
                            .entry(inp_kw.clone())
                            .or_default()
                            .mut_rules()
                            .unwrap();
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
                        inp_entry.push(MagicInlineRule {
                            head: inp_args,
                            aggr: inp_aggr,
                            body: vec![sup_rule_app],
                        });
                    }
                    seen_bindings.extend(r_app.args.iter().cloned());
                    collected_atoms.push(MagicAtom::Rule(r_app));
                }
            }
        }

        let entry = ret_prog
            .prog
            .entry(rule_head.clone())
            .or_default()
            .mut_rules()
            .unwrap();
        entry.push(MagicInlineRule {
            head: rule.head,
            aggr: rule.aggr,
            body: collected_atoms,
        });
    }
}

impl NormalFormProgram {
    fn get_downstream_rules(&self) -> BTreeSet<Symbol> {
        let own_rules: BTreeSet<_> = self.prog.keys().collect();
        let mut downstream_rules: BTreeSet<Symbol> = Default::default();
        for rules in self.prog.values() {
            match rules {
                NormalFormRulesOrFixed::Rules { rules } => {
                    for rule in rules {
                        for atom in rule.body.iter() {
                            match atom {
                                NormalFormAtom::Rule(r_app)
                                | NormalFormAtom::NegatedRule(r_app) => {
                                    if !own_rules.contains(&r_app.name) {
                                        downstream_rules.insert(r_app.name.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                NormalFormRulesOrFixed::Fixed { fixed } => {
                    for rel in fixed.rule_args.iter() {
                        if let FixedRuleArg::InMem { name, .. } = rel {
                            downstream_rules.insert(name.clone());
                        }
                    }
                }
            }
        }
        downstream_rules
    }
    fn adorn(self, upstream_rules: &BTreeSet<Symbol>, tx: &SessionTx<'_>) -> Result<MagicProgram> {
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
            match rules {
                NormalFormRulesOrFixed::Fixed { fixed } => {
                    adorned_prog.prog.insert(
                        MagicSymbol::Muggle {
                            inner: rule_name.clone(),
                        },
                        MagicRulesOrFixed::Fixed {
                            fixed: MagicFixedRuleApply {
                                span: fixed.span,
                                fixed_handle: fixed.fixed_handle.clone(),
                                fixed_impl: fixed.fixed_impl.clone(),
                                rule_args: fixed
                                    .rule_args
                                    .iter()
                                    .map(|r| -> Result<MagicFixedRuleRuleArg> {
                                        Ok(match r {
                                            FixedRuleArg::InMem {
                                                name,
                                                bindings,
                                                span,
                                            } => MagicFixedRuleRuleArg::InMem {
                                                name: MagicSymbol::Muggle {
                                                    inner: name.clone(),
                                                },
                                                bindings: bindings.clone(),
                                                span: *span,
                                            },
                                            FixedRuleArg::Stored {
                                                name,
                                                bindings,
                                                span,
                                                valid_at,
                                            } => {
                                                if valid_at.is_some() {
                                                    let relation = tx.get_relation(name, false)?;
                                                    let last_col_type = &relation
                                                        .metadata
                                                        .keys
                                                        .last()
                                                        .unwrap()
                                                        .typing;
                                                    if *last_col_type
                                                        != (NullableColType {
                                                            coltype: ColType::Validity,
                                                            nullable: false,
                                                        })
                                                    {
                                                        bail!(InvalidTimeTravelScanning(
                                                            name.to_string(),
                                                            *span
                                                        ));
                                                    }
                                                }

                                                MagicFixedRuleRuleArg::Stored {
                                                    name: name.clone(),
                                                    bindings: bindings.clone(),
                                                    valid_at: *valid_at,
                                                    span: *span,
                                                }
                                            }
                                            FixedRuleArg::NamedStored {
                                                name,
                                                bindings,
                                                valid_at,
                                                span,
                                            } => {
                                                let relation = tx.get_relation(name, false)?;
                                                if valid_at.is_some() {
                                                    let last_col_type = &relation
                                                        .metadata
                                                        .keys
                                                        .last()
                                                        .unwrap()
                                                        .typing;
                                                    if *last_col_type
                                                        != (NullableColType {
                                                            coltype: ColType::Validity,
                                                            nullable: false,
                                                        })
                                                    {
                                                        bail!(InvalidTimeTravelScanning(
                                                            name.to_string(),
                                                            *span
                                                        ));
                                                    }
                                                }
                                                let fields: BTreeSet<_> = relation
                                                    .metadata
                                                    .keys
                                                    .iter()
                                                    .chain(relation.metadata.non_keys.iter())
                                                    .map(|col| &col.name)
                                                    .collect();
                                                for k in bindings.keys() {
                                                    ensure!(
                                                        fields.contains(&k),
                                                        NamedFieldNotFound(
                                                            name.to_string(),
                                                            k.to_string(),
                                                            *span
                                                        )
                                                    );
                                                }
                                                let new_bindings = relation
                                                    .metadata
                                                    .keys
                                                    .iter()
                                                    .chain(relation.metadata.non_keys.iter())
                                                    .enumerate()
                                                    .map(|(i, col)| match bindings.get(&col.name) {
                                                        None => Symbol::new(
                                                            SmartString::from(format!("{i}")),
                                                            Default::default(),
                                                        ),
                                                        Some(k) => k.clone(),
                                                    })
                                                    .collect_vec();
                                                MagicFixedRuleRuleArg::Stored {
                                                    name: name.clone(),
                                                    bindings: new_bindings,
                                                    valid_at: *valid_at,
                                                    span: *span,
                                                }
                                            }
                                        })
                                    })
                                    .try_collect()?,
                                options: fixed.options.clone(),
                                arity: fixed.arity,
                            },
                        },
                    );
                }
                NormalFormRulesOrFixed::Rules { rules } => {
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
                        MagicSymbol::Muggle {
                            inner: rule_name.clone(),
                        },
                        MagicRulesOrFixed::Rules {
                            rules: adorned_rules,
                        },
                    );
                }
            }
        }

        while let Some(head) = pending_adornment.pop() {
            if adorned_prog.prog.contains_key(&head) {
                continue;
            }
            let original_rules = self
                .prog
                .get(head.as_plain_symbol())
                .unwrap()
                .rules()
                .unwrap();
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
            adorned_prog.prog.insert(
                head,
                MagicRulesOrFixed::Rules {
                    rules: adorned_rules,
                },
            );
        }
        Ok(adorned_prog)
    }
}

impl NormalFormAtom {
    fn adorn(
        &self,
        pending: &mut Vec<MagicSymbol>,
        seen_bindings: &mut BTreeSet<Symbol>,
        rules_to_rewrite: &BTreeSet<Symbol>,
    ) -> MagicAtom {
        match self {
            NormalFormAtom::Relation(v) => {
                let v = MagicRelationApplyAtom {
                    name: v.name.clone(),
                    args: v.args.clone(),
                    valid_at: v.valid_at,
                    span: v.span,
                };
                for arg in v.args.iter() {
                    if !seen_bindings.contains(arg) {
                        seen_bindings.insert(arg.clone());
                    }
                }
                MagicAtom::Relation(v)
            }
            NormalFormAtom::HnswSearch(s) => {
                for arg in s.all_bindings() {
                    if !seen_bindings.contains(arg) {
                        seen_bindings.insert(arg.clone());
                    }
                }
                MagicAtom::HnswSearch(s.clone())
            }
            NormalFormAtom::FtsSearch(s) => {
                for arg in s.all_bindings() {
                    if !seen_bindings.contains(arg) {
                        seen_bindings.insert(arg.clone());
                    }
                }
                MagicAtom::FtsSearch(s.clone())
            }
            NormalFormAtom::LshSearch(s) => {
                for arg in s.all_bindings() {
                    if !seen_bindings.contains(arg) {
                        seen_bindings.insert(arg.clone());
                    }
                }
                MagicAtom::LshSearch(s.clone())
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
                    let name = MagicSymbol::Magic {
                        inner: rule.name.clone(),
                        adornment,
                    };

                    pending.push(name.clone());

                    MagicAtom::Rule(MagicRuleApplyAtom {
                        name,
                        args: rule.args.clone(),
                        span: rule.span,
                    })
                } else {
                    MagicAtom::Rule(MagicRuleApplyAtom {
                        name: MagicSymbol::Muggle {
                            inner: rule.name.clone(),
                        },
                        args: rule.args.clone(),
                        span: rule.span,
                    })
                }
            }
            NormalFormAtom::NegatedRule(nr) => MagicAtom::NegatedRule(MagicRuleApplyAtom {
                name: MagicSymbol::Muggle {
                    inner: nr.name.clone(),
                },
                args: nr.args.clone(),
                span: nr.span,
            }),
            NormalFormAtom::NegatedRelation(nv) => {
                MagicAtom::NegatedRelation(MagicRelationApplyAtom {
                    name: nv.name.clone(),
                    args: nv.args.clone(),
                    valid_at: nv.valid_at,
                    span: nv.span,
                })
            }
            NormalFormAtom::Unification(u) => {
                seen_bindings.insert(u.binding.clone());
                MagicAtom::Unification(u.clone())
            }
        }
    }
}

impl NormalFormInlineRule {
    fn adorn(
        &self,
        pending: &mut Vec<MagicSymbol>,
        rules_to_rewrite: &BTreeSet<Symbol>,
        mut seen_bindings: BTreeSet<Symbol>,
    ) -> MagicInlineRule {
        let mut ret_body = Vec::with_capacity(self.body.len());

        for atom in &self.body {
            let new_atom = atom.adorn(pending, &mut seen_bindings, rules_to_rewrite);
            ret_body.push(new_atom);
        }
        MagicInlineRule {
            head: self.head.clone(),
            aggr: self.aggr.clone(),
            body: ret_body,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::DbInstance;
    use serde_json::json;

    #[test]
    fn strange_case() {
        let db = DbInstance::default();

        let query = r#"
            x[A] := A = 1
            y[A, A] := A = 1
            y[A, B] := A = 0, B = 1, x[B]

            ?[C] := y[A, _], y[C, A]

            :disable_magic_rewrite true
        "#;

        let res = db.run_default(query).unwrap().into_json();
        assert_eq!(res["rows"], json!([[0], [1]]));
    }
}
