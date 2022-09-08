use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use miette::{ensure, Context, Diagnostic, Result};
use thiserror::Error;

use crate::algo::AlgoNotFoundError;
use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::program::{
    ConstRule, ConstRules, MagicAlgoApply, MagicAtom, MagicRule, MagicRulesOrAlgo, MagicSymbol,
    StratifiedMagicProgram,
};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::query::relation::RelAlgebra;
use crate::query::reorder::UnsafeNegation;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) type CompiledProgram = BTreeMap<MagicSymbol, CompiledRuleSet>;

#[derive(Debug)]
pub(crate) enum CompiledRuleSet {
    Rules(Vec<CompiledRule>),
    Algo(MagicAlgoApply),
}

unsafe impl Send for CompiledRuleSet {}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum AggrKind {
    None,
    Normal,
    Meet,
}

impl CompiledRuleSet {
    pub(crate) fn aggr_kind(&self) -> AggrKind {
        match self {
            CompiledRuleSet::Rules(rules) => {
                let mut is_aggr = false;
                for rule in rules {
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
                for aggr in rules[0].aggr.iter() {
                    if let Some((aggr, _args)) = aggr {
                        if !aggr.is_meet {
                            return AggrKind::Normal;
                        }
                    }
                }
                AggrKind::Meet
            }
            CompiledRuleSet::Algo(_) => AggrKind::None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompiledRule {
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) relation: RelAlgebra,
    pub(crate) contained_rules: BTreeSet<MagicSymbol>,
}

#[derive(Debug, Error, Diagnostic)]
#[error("Requested rule {0} not found")]
#[diagnostic(code(eval::rule_not_found))]
struct RuleNotFound(String, #[label] SourceSpan);

#[derive(Debug, Error, Diagnostic)]
#[error("Arity mismatch for rule application {0}")]
#[diagnostic(code(eval::rule_arity_mismatch))]
#[diagnostic(help("Required arity: {1}, number of arguments given: {2}"))]
struct ArityMismatch(String, usize, usize, #[label] SourceSpan);

impl SessionTx {
    pub(crate) fn stratified_magic_compile(
        &mut self,
        prog: &StratifiedMagicProgram,
        const_rules: &ConstRules,
    ) -> Result<(Vec<CompiledProgram>, BTreeMap<MagicSymbol, DerivedRelStore>)> {
        let mut stores: BTreeMap<MagicSymbol, DerivedRelStore> = Default::default();

        for (name, ConstRule { data, .. }) in const_rules {
            let store = self.new_rule_store(name.clone(), data[0].0.len());
            for tuple in data {
                store.put(tuple.clone(), 0);
            }
            stores.insert(name.clone(), store);
        }

        for stratum in prog.0.iter() {
            for (name, ruleset) in &stratum.prog {
                stores.insert(
                    name.clone(),
                    self.new_rule_store(
                        name.clone(),
                        ruleset.arity().ok_or_else(|| {
                            AlgoNotFoundError(name.symbol().to_string(), name.symbol().span)
                        })?,
                    ),
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
                        match body {
                            MagicRulesOrAlgo::Rules { rules: body } => {
                                let mut collected = Vec::with_capacity(body.len());
                                for  rule in body.iter() {
                                    let header = &rule.head;
                                    let mut relation =
                                        self.compile_magic_rule_body(rule, k, &stores, header)?;
                                    relation.fill_normal_binding_indices().with_context(|| {
                                        format!(
                                            "error encountered when filling binding indices for {:#?}",
                                            relation
                                        )
                                    })?;
                                    collected.push(CompiledRule {
                                        aggr: rule.aggr.clone(),
                                        relation,
                                        contained_rules: rule.contained_rules()
                                    })
                                }
                                Ok((k.clone(), CompiledRuleSet::Rules(collected)))
                            }

                            MagicRulesOrAlgo::Algo { algo: algo_apply } => {
                                Ok((k.clone(), CompiledRuleSet::Algo(algo_apply.clone())))
                            }
                        }
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
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        ret_vars: &[Symbol],
    ) -> Result<RelAlgebra> {
        let mut ret = RelAlgebra::unit(rule_name.symbol().span);
        let mut seen_variables = BTreeSet::new();
        let mut serial_id = 0;
        let mut gen_symb = |span| {
            let ret = Symbol::new(&format!("**{}", serial_id) as &str, span);
            serial_id += 1;
            ret
        };
        for atom in &rule.body {
            match atom {
                MagicAtom::AttrTriple(t) => {
                    let mut join_left_keys = vec![];
                    let mut join_right_keys = vec![];
                    let e_kw = if seen_variables.contains(&t.entity) {
                        let kw = gen_symb(t.entity.span);
                        join_left_keys.push(t.entity.clone());
                        join_right_keys.push(kw.clone());
                        kw
                    } else {
                        seen_variables.insert(t.entity.clone());
                        t.entity.clone()
                    };
                    let v_kw = if seen_variables.contains(&t.value) {
                        let kw = gen_symb(t.value.span);
                        join_left_keys.push(t.value.clone());
                        join_right_keys.push(kw.clone());
                        kw
                    } else {
                        seen_variables.insert(t.value.clone());
                        t.value.clone()
                    };
                    let right = RelAlgebra::triple(t.attr.clone(), rule.vld, e_kw, v_kw, t.span);
                    if ret.is_unit() {
                        ret = right
                    } else {
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = ret.join(right, join_left_keys, join_right_keys, t.span);
                    }
                }
                MagicAtom::Rule(rule_app) => {
                    let store = stores
                        .get(&rule_app.name)
                        .ok_or_else(|| {
                            RuleNotFound(
                                rule_app.name.symbol().to_string(),
                                rule_app.name.symbol().span,
                            )
                        })?
                        .clone();

                    ensure!(
                        store.arity == rule_app.args.len(),
                        ArityMismatch(
                            rule_app.name.symbol().to_string(),
                            store.arity,
                            rule_app.args.len(),
                            rule_app.span
                        )
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                        }
                    }

                    let right = RelAlgebra::derived(right_vars, store, rule_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.join(right, prev_joiner_vars, right_joiner_vars, rule_app.span);
                }
                MagicAtom::Relation(rel_app) => {
                    let store = self.get_relation(&rel_app.name)?;
                    ensure!(
                        store.arity == rel_app.args.len(),
                        ArityMismatch(
                            rel_app.name.to_string(),
                            store.arity,
                            rel_app.args.len(),
                            rel_app.span
                        )
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rel_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                        }
                    }

                    let right = RelAlgebra::relation(right_vars, store, rel_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.join(right, prev_joiner_vars, right_joiner_vars, rel_app.span);
                }
                MagicAtom::NegatedAttrTriple(a_triple) => {
                    let mut join_left_keys = vec![];
                    let mut join_right_keys = vec![];
                    let e_kw = {
                        if seen_variables.contains(&a_triple.entity) {
                            let kw = gen_symb(a_triple.entity.span);
                            join_left_keys.push(a_triple.entity.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            a_triple.entity.clone()
                        }
                    };
                    let v_kw = {
                        if seen_variables.contains(&a_triple.value) {
                            let kw = gen_symb(a_triple.value.span);
                            join_left_keys.push(a_triple.value.clone());
                            join_right_keys.push(kw.clone());
                            kw
                        } else {
                            a_triple.value.clone()
                        }
                    };
                    ensure!(!join_right_keys.is_empty(), UnsafeNegation(a_triple.span));
                    let right = RelAlgebra::triple(
                        a_triple.attr.clone(),
                        rule.vld,
                        e_kw,
                        v_kw,
                        a_triple.span,
                    );
                    if ret.is_unit() {
                        ret = right;
                    } else {
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = ret.neg_join(right, join_left_keys, join_right_keys, a_triple.span);
                    }
                }
                MagicAtom::NegatedRule(rule_app) => {
                    let store = stores
                        .get(&rule_app.name)
                        .ok_or_else(|| {
                            RuleNotFound(
                                rule_app.name.symbol().to_string(),
                                rule_app.name.symbol().span,
                            )
                        })?
                        .clone();
                    ensure!(
                        store.arity == rule_app.args.len(),
                        ArityMismatch(
                            rule_app.name.symbol().to_string(),
                            store.arity,
                            rule_app.args.len(),
                            rule_app.span
                        )
                    );

                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            right_vars.push(var.clone());
                        }
                    }

                    let right = RelAlgebra::derived(right_vars, store, rule_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.neg_join(right, prev_joiner_vars, right_joiner_vars, rule_app.span);
                }
                MagicAtom::NegatedRelation(relation_app) => {
                    let store = self.get_relation(&relation_app.name)?;
                    ensure!(
                        store.arity == relation_app.args.len(),
                        ArityMismatch(
                            relation_app.name.to_string(),
                            store.arity,
                            relation_app.args.len(),
                            relation_app.span
                        )
                    );

                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &relation_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            right_vars.push(var.clone());
                        }
                    }

                    let right = RelAlgebra::relation(right_vars, store, relation_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.neg_join(
                        right,
                        prev_joiner_vars,
                        right_joiner_vars,
                        relation_app.span,
                    );
                }
                MagicAtom::Predicate(p) => {
                    if let Some(fs) = ret.get_filters() {
                        fs.extend(p.to_conjunction());
                    } else {
                        ret = ret.filter(p.clone());
                    }
                }
                MagicAtom::Unification(u) => {
                    if seen_variables.contains(&u.binding) {
                        let expr = if u.one_many_unif {
                            Expr::build_is_in(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        } else {
                            Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        };
                        if let Some(fs) = ret.get_filters() {
                            fs.push(expr);
                        } else {
                            ret = ret.filter(expr);
                        }
                    } else {
                        seen_variables.insert(u.binding.clone());
                        ret = ret.unify(u.binding.clone(), u.expr.clone(), u.one_many_unif, u.span);
                    }
                }
            }
        }

        let ret_vars_set = ret_vars.iter().cloned().collect();
        ret.eliminate_temp_vars(&ret_vars_set)?;
        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            let ret_span = ret.span();
            ret = ret.cartesian_join(RelAlgebra::unit(ret_span), ret_span);
            ret.eliminate_temp_vars(&ret_vars_set)?;
        }

        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        #[derive(Debug, Error, Diagnostic)]
        #[error("Symbol '{0}' in rule head is unbound")]
        #[diagnostic(code(eval::unbound_symb_in_head))]
        #[diagnostic(help(
            "Note that symbols occurring only in negated positions are not considered bound"
        ))]
        struct UnboundSymbolInRuleHead(String, #[label] SourceSpan);

        ensure!(cur_ret_set == ret_vars_set, {
            let unbound = ret_vars_set.difference(&cur_ret_set).next().unwrap();
            UnboundSymbolInRuleHead(unbound.to_string(), unbound.span)
        });
        let cur_ret_bindings = ret.bindings_after_eliminate();
        if ret_vars != cur_ret_bindings {
            ret = ret.reorder(ret_vars.to_vec());
        }

        Ok(ret)
    }
}
