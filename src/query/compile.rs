use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, ensure, Result};
use itertools::Itertools;

use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::program::{MagicAtom, MagicRule, MagicSymbol, StratifiedMagicProgram};
use crate::data::symb::Symbol;
use crate::parse::query::ConstRules;
use crate::query::relation::Relation;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

pub(crate) type CompiledProgram = BTreeMap<MagicSymbol, CompiledRuleSet>;

#[derive(Debug)]
pub(crate) struct CompiledRuleSet {
    pub(crate) rules: Vec<CompiledRule>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum AggrKind {
    None,
    Normal,
    Meet,
}

impl CompiledRuleSet {
    pub(crate) fn aggr_kind(&self) -> AggrKind {
        let mut is_aggr = false;
        for rule in &self.rules {
            for aggr in &rule.aggr {
                if aggr.is_some() {
                    is_aggr = true;
                    break;
                }
            }
        }
        if !is_aggr {
            return AggrKind::None;
        }
        for aggr in self.rules[0].aggr.iter() {
            if let Some(aggr) = aggr {
                if !aggr.is_meet {
                    return AggrKind::Normal;
                }
            }
        }
        return AggrKind::Meet;
    }
}

#[derive(Debug)]
pub(crate) struct CompiledRule {
    pub(crate) aggr: Vec<Option<Aggregation>>,
    pub(crate) relation: Relation,
    pub(crate) contained_rules: BTreeSet<MagicSymbol>,
}

impl SessionTx {
    pub(crate) fn stratified_magic_compile(
        &mut self,
        prog: &StratifiedMagicProgram,
        const_rules: &ConstRules,
    ) -> Result<(Vec<CompiledProgram>, BTreeMap<MagicSymbol, TempStore>)> {
        let mut stores: BTreeMap<MagicSymbol, TempStore> = Default::default();

        for (name, data) in const_rules {
            let store = self.new_rule_store(name.clone(), data[0].0.len());
            for tuple in data {
                store.put(tuple, 0)?;
            }
            stores.insert(name.clone(), store);
        }

        for stratum in prog.0.iter() {
            for (name, ruleset) in &stratum.prog {
                ensure!(
                    !const_rules.contains_key(name),
                    "name clash between rule and const rule: {:?}",
                    name
                );
                stores.insert(
                    name.clone(),
                    self.new_rule_store(name.clone(), ruleset.rules[0].head.len()),
                );
            }
        }

        let compiled: Vec<_> = prog
            .0
            .iter()
            .rev()
            .map(|cur_prog| -> Result<CompiledProgram> {
                cur_prog
                    .prog
                    .iter()
                    .map(|(k, body)| -> Result<(MagicSymbol, CompiledRuleSet)> {
                        let mut collected = Vec::with_capacity(body.rules.len());
                        for (rule_idx, rule) in body.rules.iter().enumerate() {
                            let header = &rule.head;
                            let mut relation =
                                self.compile_magic_rule_body(&rule, k, rule_idx, &stores, &header)?;
                            relation.fill_predicate_binding_indices();
                            collected.push(CompiledRule {
                                aggr: rule.aggr.clone(),
                                relation,
                                contained_rules: rule.contained_rules(),
                            })
                        }
                        ensure!(
                            collected.iter().map(|r| &r.aggr).all_equal(),
                            "rule heads must contain identical aggregations: {:?}",
                            collected
                        );
                        Ok((k.clone(), CompiledRuleSet { rules: collected }))
                    })
                    .try_collect()
            })
            .try_collect()?;
        Ok((compiled, stores))
    }
    pub(crate) fn compile_magic_rule_body(
        &mut self,
        rule: &MagicRule,
        rule_name: &MagicSymbol,
        rule_idx: usize,
        stores: &BTreeMap<MagicSymbol, TempStore>,
        ret_vars: &[Symbol],
    ) -> Result<Relation> {
        let mut ret = Relation::unit();
        let mut seen_variables = BTreeSet::new();
        let mut serial_id = 0;
        let mut gen_symb = || {
            let ret = Symbol::from(&format!("**{}", serial_id) as &str);
            serial_id += 1;
            ret
        };
        for atom in &rule.body {
            match atom {
                MagicAtom::AttrTriple(t) => {
                    let mut join_left_keys = vec![];
                    let mut join_right_keys = vec![];
                    let e_kw = if seen_variables.contains(&t.entity) {
                        let kw = gen_symb();
                        join_left_keys.push(t.entity.clone());
                        join_right_keys.push(kw.clone());
                        kw
                    } else {
                        seen_variables.insert(t.entity.clone());
                        t.entity.clone()
                    };
                    let v_kw = if seen_variables.contains(&t.value) {
                        let kw = gen_symb();
                        join_left_keys.push(t.value.clone());
                        join_right_keys.push(kw.clone());
                        kw
                    } else {
                        seen_variables.insert(t.value.clone());
                        t.value.clone()
                    };
                    let right = Relation::triple(t.attr.clone(), rule.vld, e_kw, v_kw);
                    if ret.is_unit() {
                        ret = right
                    } else {
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = ret.join(right, join_left_keys, join_right_keys);
                    }
                }
                MagicAtom::Rule(rule_app) => {
                    let store = stores
                        .get(&rule_app.name)
                        .ok_or_else(|| anyhow!("undefined rule '{:?}' encountered", rule_app.name))?
                        .clone();
                    ensure!(
                        store.arity == rule_app.args.len(),
                        "arity mismatch in rule application {:?}, expect {}, found {}",
                        rule_app.name,
                        store.arity,
                        rule_app.args.len()
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb();
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                        }
                    }

                    let right = Relation::derived(right_vars, store);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.join(right, prev_joiner_vars, right_joiner_vars);
                }
                MagicAtom::NegatedAttrTriple(a_triple) => {
                    let mut join_left_keys = vec![];
                    let mut join_right_keys = vec![];
                    let e_kw = {
                        if seen_variables.contains(&a_triple.entity) {
                            let kw = gen_symb();
                            join_left_keys.push(a_triple.entity.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            a_triple.entity.clone()
                        }
                    };
                    let v_kw = {
                        if seen_variables.contains(&a_triple.value) {
                            let kw = gen_symb();
                            join_left_keys.push(a_triple.value.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            a_triple.value.clone()
                        }
                    };
                    ensure!(
                        !join_right_keys.is_empty(),
                        "unsafe negation: {} and {} are unbound",
                        e_kw,
                        v_kw
                    );
                    let right = Relation::triple(a_triple.attr.clone(), rule.vld, e_kw, v_kw);
                    if ret.is_unit() {
                        ret = right;
                    } else {
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = ret.neg_join(right, join_left_keys, join_right_keys);
                    }
                }
                MagicAtom::NegatedRule(rule_app) => {
                    let store = stores
                        .get(&rule_app.name)
                        .ok_or_else(|| {
                            anyhow!("undefined rule encountered: '{:?}'", rule_app.name)
                        })?
                        .clone();
                    ensure!(
                        store.arity == rule_app.args.len(),
                        "arity mismatch for {:?}, expect {}, got {}",
                        rule_app.name,
                        store.arity,
                        rule_app.args.len()
                    );

                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb();
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            right_vars.push(var.clone());
                        }
                    }

                    let right = Relation::derived(right_vars, store);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.neg_join(right, prev_joiner_vars, right_joiner_vars);
                }
                MagicAtom::Predicate(p) => {
                    ret = ret.filter(p.clone());
                }
                MagicAtom::Unification(u) => {
                    if seen_variables.contains(&u.binding) {
                        ret = ret.filter(Expr::build_equate(vec![
                            Expr::Binding(u.binding.clone(), None),
                            u.expr.clone(),
                        ]));
                    } else {
                        seen_variables.insert(u.binding.clone());
                        ret = ret.unify(u.binding.clone(), u.expr.clone());
                    }
                }
            }
        }

        let ret_vars_set = ret_vars.iter().cloned().collect();
        ret.eliminate_temp_vars(&ret_vars_set)?;
        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            ret = ret.cartesian_join(Relation::unit());
            ret.eliminate_temp_vars(&ret_vars_set)?;
        }

        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        ensure!(
            cur_ret_set == ret_vars_set,
            "unbound variables in rule head for {:?}.{}: variables required {:?}, of which only {:?} are bound.\n{:#?}\n{:#?}",
            rule_name,
            rule_idx,
            ret_vars_set,
            cur_ret_set,
            rule,
            ret
        );
        let cur_ret_bindings = ret.bindings_after_eliminate();
        if ret_vars != cur_ret_bindings {
            ret = ret.reorder(ret_vars.to_vec());
        }

        Ok(ret)
    }
}
