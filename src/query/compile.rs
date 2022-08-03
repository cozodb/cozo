use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, ensure, Result};

use crate::data::expr::Expr;
use crate::data::keyword::Keyword;
use crate::data::program::{MagicAtom, MagicKeyword, MagicRule};
use crate::query::relation::Relation;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn compile_magic_rule_body(
        &mut self,
        rule: &MagicRule,
        rule_name: &MagicKeyword,
        rule_idx: usize,
        stores: &BTreeMap<MagicKeyword, TempStore>,
        ret_vars: &[Keyword],
    ) -> Result<Relation> {
        let mut ret = Relation::unit();
        let mut seen_variables = BTreeSet::new();
        let mut serial_id = 0;
        let mut gen_kw = || {
            let ret = Keyword::from(&format!("**{}", serial_id) as &str);
            serial_id += 1;
            ret
        };
        for atom in &rule.body {
            match atom {
                MagicAtom::AttrTriple(t) => {
                    let mut join_left_keys = vec![];
                    let mut join_right_keys = vec![];
                    let e_kw = if seen_variables.contains(&t.entity) {
                        let kw = gen_kw();
                        join_left_keys.push(t.entity.clone());
                        join_right_keys.push(kw.clone());
                        kw
                    } else {
                        seen_variables.insert(t.entity.clone());
                        t.entity.clone()
                    };
                    let v_kw = if seen_variables.contains(&t.value) {
                        let kw = gen_kw();
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
                        .ok_or_else(|| anyhow!("undefined rule {:?} encountered", rule_app.name))?
                        .clone();
                    ensure!(
                        store.key_size == rule_app.args.len(),
                        "arity mismatch in rule application {:?}, expect {}, found {}",
                        rule_app.name,
                        store.key_size,
                        rule_app.args.len()
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_kw();
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
                            let kw = gen_kw();
                            join_left_keys.push(a_triple.entity.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            seen_variables.insert(a_triple.entity.clone());
                            a_triple.entity.clone()
                        }
                    };
                    let v_kw = {
                        if seen_variables.contains(&a_triple.value) {
                            let kw = gen_kw();
                            join_left_keys.push(a_triple.value.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            seen_variables.insert(a_triple.value.clone());
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
                        .ok_or_else(|| anyhow!("undefined rule encountered: {:?}", rule_app.name))?
                        .clone();
                    ensure!(
                        store.key_size == rule_app.args.len(),
                        "arity mismatch for {:?}, expect {}, got {}",
                        rule_app.name,
                        store.key_size,
                        rule_app.args.len()
                    );

                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_kw();
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
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
