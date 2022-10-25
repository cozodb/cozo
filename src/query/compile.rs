use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use miette::{bail, ensure, Context, Diagnostic, Result};
use thiserror::Error;

use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::program::{
    MagicAlgoApply, MagicAtom, MagicInlineRule, MagicRulesOrAlgo, MagicSymbol,
    StratifiedMagicProgram,
};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::query::relation::RelAlgebra;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::relation::{AccessLevel, InsufficientAccessLevel};
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
                for (aggr, _args) in rules[0].aggr.iter().flatten() {
                    if !aggr.is_meet {
                        return AggrKind::Normal;
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
    ) -> Result<(Vec<CompiledProgram>, BTreeMap<MagicSymbol, InMemRelation>)> {
        let mut stores: BTreeMap<MagicSymbol, InMemRelation> = Default::default();

        for stratum in prog.0.iter() {
            for (name, ruleset) in &stratum.prog {
                stores.insert(
                    name.clone(),
                    self.new_rule_store(name.clone(), ruleset.arity()?),
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
                                for rule in body.iter() {
                                    let header = &rule.head;
                                    let mut relation =
                                        self.compile_magic_rule_body(rule, k, &stores, header)?;
                                    relation.fill_binding_indices().with_context(|| {
                                        format!(
                                            "error encountered when filling binding indices for {:#?}",
                                            relation
                                        )
                                    })?;
                                    collected.push(CompiledRule {
                                        aggr: rule.aggr.clone(),
                                        relation,
                                        contained_rules: rule.contained_rules(),
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
        rule: &MagicInlineRule,
        rule_name: &MagicSymbol,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
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
                    let store = self.get_relation(&rel_app.name, false)?;
                    if store.access_level < AccessLevel::ReadOnly {
                        bail!(InsufficientAccessLevel(
                            store.name.to_string(),
                            "reading rows".to_string(),
                            store.access_level
                        ));
                    }
                    ensure!(
                        store.arity() == rel_app.args.len(),
                        ArityMismatch(
                            rel_app.name.to_string(),
                            store.arity(),
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
                    let store = self.get_relation(&relation_app.name, false)?;
                    ensure!(
                        store.arity() == relation_app.args.len(),
                        ArityMismatch(
                            relation_app.name.to_string(),
                            store.arity(),
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
                    ret = ret.filter(p.clone());
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
                        ret = ret.filter(expr);
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
