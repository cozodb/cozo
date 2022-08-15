use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use serde_json::{json, Map};

use crate::data::aggr::get_aggr;
use crate::data::attr::Attribute;
use crate::data::expr::{get_op, Expr, OP_LIST};
use crate::data::id::{EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::program::{
    InputAtom, InputAttrTripleAtom, InputProgram, InputRule, InputRuleApplyAtom, InputTerm,
    InputViewApplyAtom, MagicSymbol, Unification,
};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::query::pull::PullSpecs;
use crate::runtime::transact::SessionTx;
use crate::runtime::view::{ViewRelId, ViewRelKind, ViewRelMetadata};
use crate::utils::swap_result_option;

pub(crate) type OutSpec = (Vec<(usize, Option<PullSpecs>)>, Option<Vec<String>>);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum SortDir {
    Asc,
    Dsc,
}

impl TryFrom<&'_ JsonValue> for SortDir {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> std::result::Result<Self, Self::Error> {
        match value {
            JsonValue::String(s) => Ok(match s as &str {
                "asc" => SortDir::Asc,
                "desc" => SortDir::Dsc,
                _ => bail!(
                    "unexpected value {} for sort direction specification",
                    value
                ),
            }),
            _ => bail!(
                "unexpected value {} for sort direction specification",
                value
            ),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ViewOp {
    Create,
    Rederive,
    Put,
    Retract,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct QueryOutOptions {
    pub(crate) out_spec: Option<OutSpec>,
    pub(crate) vld: Validity,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) sorters: Vec<(Symbol, SortDir)>,
    pub(crate) as_view: Option<(ViewRelMetadata, ViewOp)>,
}

impl QueryOutOptions {
    pub(crate) fn num_to_take(&self) -> Option<usize> {
        match (self.limit, self.offset) {
            (None, _) => None,
            (Some(i), None) => Some(i),
            (Some(i), Some(j)) => Some(i + j),
        }
    }
}

pub(crate) type ConstRules = BTreeMap<MagicSymbol, Vec<Tuple>>;

impl SessionTx {
    pub(crate) fn parse_query(
        &mut self,
        payload: &JsonValue,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<(InputProgram, QueryOutOptions, ConstRules)> {
        let vld = match payload.get("since") {
            None => Validity::current(),
            Some(v) => Validity::try_from(v)?,
        };
        let q = payload
            .get("q")
            .ok_or_else(|| anyhow!("expect field 'q' in query {}", payload))?;
        let rules_payload = q
            .as_array()
            .ok_or_else(|| anyhow!("expect field 'q' to be an array in query {}", payload))?;
        ensure!(!rules_payload.is_empty(), "no rules in {}", payload);
        let input_prog = if rules_payload.first().unwrap().is_array() {
            let q = json!([{"rule": "?", "args": rules_payload}]);
            self.parse_input_rule_sets(&q, vld, &params_pool)?
        } else {
            self.parse_input_rule_sets(q, vld, &params_pool)?
        };
        let entry_bindings = &input_prog
            .prog
            .get(&PROG_ENTRY)
            .ok_or_else(|| anyhow!("program has no entry point"))?
            .first()
            .unwrap()
            .head;
        let out_spec = payload
            .get("out")
            .map(|spec| self.parse_query_out_spec(spec, entry_bindings));
        let out_spec = swap_result_option(out_spec)?;
        let limit = swap_result_option(payload.get("limit").map(|v| {
            v.as_u64()
                .map(|v| v as usize)
                .ok_or_else(|| anyhow!("'limit' must be a positive number"))
        }))?;
        let offset = swap_result_option(payload.get("offset").map(|v| {
            v.as_u64()
                .map(|v| v as usize)
                .ok_or_else(|| anyhow!("'offset' must be a positive number"))
        }))?;
        let const_rules = if let Some(rules) = payload.get("const_rules") {
            rules
                .as_object()
                .ok_or_else(|| anyhow!("const rules is expected to be an object"))?
                .iter()
                .map(|(k, v)| -> Result<(MagicSymbol, Vec<Tuple>)> {
                    let data: Vec<Tuple> = v
                        .as_array()
                        .ok_or_else(|| anyhow!("rules spec is expected to be an array"))?
                        .iter()
                        .map(|v| -> Result<Tuple> {
                            let tuple = v
                                .as_array()
                                .ok_or_else(|| {
                                    anyhow!("data in rule is expected to be an array, got {}", v)
                                })?
                                .iter()
                                .map(|v| Self::parse_const_expr(v, &params_pool))
                                .try_collect()?;
                            Ok(Tuple(tuple))
                        })
                        .try_collect()?;
                    let name = Symbol::from(k as &str);
                    ensure!(
                        !name.is_reserved(),
                        "reserved name for const rule: {}",
                        name
                    );
                    ensure!(!data.is_empty(), "const rule is empty: {}", name);
                    ensure!(
                        data.iter().map(|t| t.0.len()).all_equal(),
                        "const rule have varying length data: {}",
                        name
                    );
                    Ok((MagicSymbol::Muggle { inner: name }, data))
                })
                .try_collect()?
        } else {
            BTreeMap::default()
        };
        let mut sorters: Vec<_> = payload
            .get("sort")
            .unwrap_or(&json!([]))
            .as_array()
            .ok_or_else(|| anyhow!("'sort' is expected to be an array"))?
            .iter()
            .map(|sorter| -> Result<(Symbol, SortDir)> {
                let sorter = sorter
                    .as_object()
                    .ok_or_else(|| anyhow!("'sort' must be an array of objects"))?;
                ensure!(
                    sorter.len() == 1,
                    "'sort' spec must be an object of a single pair"
                );
                let (k, v) = sorter.iter().next().unwrap();

                let k = Symbol::from(k as &str);
                let d = SortDir::try_from(v)?;
                Ok((k, d))
            })
            .try_collect()?;
        if !sorters.is_empty() {
            let entry = input_prog
                .prog
                .get(&PROG_ENTRY)
                .ok_or_else(|| anyhow!("program entry point not found"))?;
            ensure!(
                entry.iter().map(|e| &e.head).all_equal(),
                "program entry point must have equal bindings"
            );
            let entry_head = &entry[0].head;
            if sorters
                .iter()
                .map(|(k, _v)| k)
                .eq(entry_head.iter().take(sorters.len()))
                && sorters.iter().all(|(_k, v)| *v == SortDir::Asc)
            {
                sorters = vec![];
            }

            if !sorters.is_empty() {
                let head_symbols: BTreeSet<_> = entry_head.iter().collect();
                for (k, _) in sorters.iter() {
                    if !head_symbols.contains(k) {
                        bail!("sorted argument {} not found in program entry head", k);
                    }
                }
            }
        }
        let as_view = match payload.get("view") {
            None => None,
            Some(view_payload) => Some({
                if out_spec.is_some() {
                    bail!("cannot use out spec with 'view'");
                }

                let opts = view_payload
                    .as_object()
                    .ok_or_else(|| anyhow!("view options must be an object"))?;
                let (op, name) = if let Some(name) = opts.get("create") {
                    (ViewOp::Create, name)
                } else if let Some(name) = opts.get("rederive") {
                    (ViewOp::Rederive, name)
                } else if let Some(name) = opts.get("put") {
                    (ViewOp::Put, name)
                } else if let Some(name) = opts.get("retract") {
                    (ViewOp::Retract, name)
                } else {
                    bail!("cannot parse view options: {}", view_payload);
                };
                let name = name.as_str().ok_or_else(|| anyhow!("view name must be a string"))?;
                let name = Symbol::from(name);
                ensure!(!name.is_reserved(), "view name {} is reserved", name);
                let entry = input_prog
                    .prog
                    .get(&PROG_ENTRY)
                    .ok_or_else(|| anyhow!("program entry point not found"))?;

                (
                    ViewRelMetadata {
                        name,
                        id: ViewRelId::SYSTEM,
                        arity: entry[0].head.len(),
                        kind: ViewRelKind::Manual,
                    },
                    op,
                )
            }),
        };
        Ok((
            input_prog,
            QueryOutOptions {
                out_spec,
                vld,
                limit,
                offset,
                sorters,
                as_view,
            },
            const_rules,
        ))
    }
    fn parse_query_out_spec(
        &mut self,
        payload: &JsonValue,
        entry_bindings: &[Symbol],
    ) -> Result<OutSpec> {
        match payload {
            JsonValue::Object(out_spec_map) => {
                let out_spec = out_spec_map.values().cloned().collect_vec();
                let pull_specs = self.parse_pull_specs_for_query_spec(&out_spec, entry_bindings)?;
                let map_keys = out_spec_map.keys().cloned().collect_vec();
                Ok((pull_specs, Some(map_keys)))
            }
            JsonValue::Array(out_spec) => {
                let pull_specs = self.parse_pull_specs_for_query_spec(out_spec, entry_bindings)?;
                Ok((pull_specs, None))
            }
            v => bail!("out spec should be an array, found {}", v),
        }
    }

    pub(crate) fn parse_pull_specs_for_query_spec(
        &mut self,
        out_spec: &Vec<JsonValue>,
        entry_bindings: &[Symbol],
    ) -> Result<Vec<(usize, Option<PullSpecs>)>> {
        let entry_bindings: BTreeMap<_, _> = entry_bindings
            .iter()
            .enumerate()
            .map(|(i, h)| (h, i))
            .collect();
        out_spec
            .iter()
            .map(|spec| -> Result<(usize, Option<PullSpecs>)> {
                match spec {
                    JsonValue::String(s) => {
                        let symb = Symbol::from(s as &str);
                        let idx = *entry_bindings
                            .get(&symb)
                            .ok_or_else(|| anyhow!("binding {} not found", symb))?;
                        Ok((idx, None))
                    }
                    JsonValue::Object(m) => {
                        let symb = m
                            .get("pull")
                            .ok_or_else(|| anyhow!("expect field 'pull' in {:?}", m))?
                            .as_str()
                            .ok_or_else(|| anyhow!("expect 'pull' to be a binding in {:?}", m))?;
                        let symb = Symbol::from(symb);
                        let idx = *entry_bindings
                            .get(&symb)
                            .ok_or_else(|| anyhow!("binding {} not found", symb))?;
                        let spec = m
                            .get("spec")
                            .ok_or_else(|| anyhow!("expect field 'spec' in {:?}", m))?;
                        let specs = self.parse_pull(spec, 0)?;
                        Ok((idx, Some(specs)))
                    }
                    v => bail!("expect binding or map, got {:?}", v),
                }
            })
            .try_collect()
    }

    pub(crate) fn parse_input_rule_sets(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<InputProgram> {
        let rules = payload
            .as_array()
            .ok_or_else(|| anyhow!("expect array for rules, got {}", payload))?
            .iter()
            .map(|o| self.parse_input_rule_definition(o, default_vld, params_pool));
        let mut collected: BTreeMap<Symbol, Vec<InputRule>> = BTreeMap::new();
        for res in rules {
            let (name, rule) = res?;
            match collected.entry(name) {
                Entry::Vacant(e) => {
                    e.insert(vec![rule]);
                }
                Entry::Occupied(mut e) => {
                    e.get_mut().push(rule);
                }
            }
        }
        let ret: BTreeMap<Symbol, Vec<InputRule>> = collected
            .into_iter()
            .map(|(name, rules)| -> Result<(Symbol, Vec<InputRule>)> {
                let mut arities = rules.iter().map(|r| r.head.len());
                let arity = arities.next().unwrap();
                for other in arities {
                    if other != arity {
                        bail!("arity mismatch for rules under the name of {}", name);
                    }
                }
                Ok((name, rules))
            })
            .try_collect()?;

        match ret.get(&PROG_ENTRY as &Symbol) {
            None => bail!("no entry defined for datalog program"),
            Some(ruleset) => {
                if !ruleset.iter().map(|r| &r.head).all_equal() {
                    bail!("all heads for the entry query must be identical");
                } else {
                    Ok(InputProgram { prog: ret })
                }
            }
        }
    }
    fn parse_input_predicate_atom(
        payload: &Map<String, JsonValue>,
        param_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<InputAtom> {
        let mut pred = Self::parse_apply_expr(payload, param_pool)?;
        if let Expr::Apply(op, _) = &pred {
            ensure!(
                op.is_predicate,
                "non-predicate expression in predicate position: {}",
                op.name
            );
        }
        pred.partial_eval(param_pool)?;
        Ok(InputAtom::Predicate(pred))
    }
    fn parse_unification(
        payload: &Map<String, JsonValue>,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<InputAtom> {
        let binding = payload
            .get("unify")
            .ok_or_else(|| anyhow!("expect expression to have field 'unify'"))?
            .as_str()
            .ok_or_else(|| anyhow!("expect field 'unify' to be a symbol"))?;
        let binding = Symbol::from(binding);
        ensure!(
            binding.is_query_var(),
            "binding for unification {} is reserved",
            binding
        );
        let expr = payload
            .get("expr")
            .ok_or_else(|| anyhow!("expect unify map to have field 'expr'"))?;
        let mut expr = Self::parse_expr_arg(expr, params_pool)?;
        expr.partial_eval(params_pool)?;
        let one_many_unif = match payload.get("multi") {
            None => false,
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow!("unification 'multi' field must be a boolean"))?,
        };
        Ok(InputAtom::Unification(Unification {
            binding,
            expr,
            one_many_unif,
        }))
    }
    fn parse_apply_expr(
        payload: &Map<String, JsonValue>,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<Expr> {
        if let Some(name) = payload.get("param") {
            let name = name
                .as_str()
                .ok_or_else(|| anyhow!("input var cannot be specified as {}", name))?;
            ensure!(
                name.starts_with("$") && name.len() > 1,
                "wrong input var format: {}",
                name
            );
            return Ok(Expr::Param(Symbol::from(name)));
        }

        let name = payload
            .get("op")
            .ok_or_else(|| anyhow!("expect expression to have key 'pred'"))?
            .as_str()
            .ok_or_else(|| anyhow!("expect key 'pred' to be a string referring to a predicate"))?;

        let op = get_op(name).ok_or_else(|| anyhow!("unknown operator {}", name))?;

        let args: Box<[Expr]> = payload
            .get("args")
            .ok_or_else(|| anyhow!("expect key 'args' in expression"))?
            .as_array()
            .ok_or_else(|| anyhow!("expect key 'args' to be an array"))?
            .iter()
            .map(|v| Self::parse_expr_arg(v, params_pool))
            .try_collect()?;

        if op.vararg {
            ensure!(
                args.len() >= op.min_arity,
                "arity mismatch for vararg op {}: expect minimum of {}, got {}",
                op.name,
                op.min_arity,
                args.len()
            );
        } else if args.len() != op.min_arity {
            ensure!(
                args.len() == op.min_arity,
                "arity mismatch for op {}: expect {}, got {}",
                op.name,
                op.min_arity,
                args.len()
            );
        }

        Ok(Expr::Apply(op, args))
    }
    fn parse_const_expr(
        payload: &JsonValue,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<DataValue> {
        let res = Self::parse_expr_arg(payload, params_pool)?;
        Ok(match res {
            Expr::Const(v) => v,
            v => bail!("cannot convert {:?} to constant", v),
        })
    }
    fn parse_expr_arg(
        payload: &JsonValue,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<Expr> {
        match payload {
            JsonValue::String(s) => {
                let symb = Symbol::from(s as &str);
                if symb.is_reserved() {
                    Ok(Expr::Binding(symb, None))
                } else {
                    Ok(Expr::Const(DataValue::String(s.into())))
                }
            }
            JsonValue::Object(map) => {
                if let Some(v) = map.get("const") {
                    Ok(Expr::Const(v.into()))
                } else if map.contains_key("op") || map.contains_key("param") {
                    let mut ret = Self::parse_apply_expr(map, params_pool)?;
                    ret.partial_eval(params_pool)?;
                    Ok(ret)
                } else {
                    bail!("expression object must contain either 'const' or 'pred' key");
                }
            }
            JsonValue::Array(l) => {
                let l: Vec<_> = l
                    .iter()
                    .map(|v| Self::parse_expr_arg(v, params_pool))
                    .try_collect()?;
                Ok(Expr::Apply(&OP_LIST, l.into()))
            }
            v => Ok(Expr::Const(v.into())),
        }
    }
    fn parse_input_rule_atom(
        &mut self,
        payload: &Map<String, JsonValue>,
        vld: Validity,
    ) -> Result<InputAtom> {
        let rule_name = payload
            .get("rule")
            .ok_or_else(|| anyhow!("expect key 'rule' in rule atom"))?
            .as_str()
            .ok_or_else(|| anyhow!("expect value for key 'rule' to be a string"))?
            .into();
        let args = payload
            .get("args")
            .ok_or_else(|| anyhow!("expect key 'args' in rule atom"))?
            .as_array()
            .ok_or_else(|| anyhow!("expect value for key 'args' to be an array"))?
            .iter()
            .map(|value_rep| -> Result<InputTerm<DataValue>> {
                if let Some(s) = value_rep.as_str() {
                    let var = Symbol::from(s);
                    if s.starts_with(['?', '_']) {
                        return Ok(InputTerm::Var(var));
                    } else {
                        ensure!(
                            !var.is_reserved(),
                            "{} is a reserved string value and must be quoted",
                            s
                        )
                    }
                }
                if let Some(o) = value_rep.as_object() {
                    return if let Some(c) = o.get("const") {
                        Ok(InputTerm::Const(c.into()))
                    } else {
                        let eid = self.parse_eid_from_map(o, vld)?;
                        Ok(InputTerm::Const(eid.to_value()))
                    };
                }
                Ok(InputTerm::Const(value_rep.into()))
            })
            .try_collect()?;
        Ok(InputAtom::Rule(InputRuleApplyAtom {
            name: rule_name,
            args,
        }))
    }
    fn parse_input_view_atom(
        &mut self,
        payload: &Map<String, JsonValue>,
        vld: Validity,
    ) -> Result<InputAtom> {
        let rule_name = payload
            .get("view")
            .ok_or_else(|| anyhow!("expect key 'view' in rule atom"))?
            .as_str()
            .ok_or_else(|| anyhow!("expect value for key 'view' to be a string"))?
            .into();
        let args = payload
            .get("args")
            .ok_or_else(|| anyhow!("expect key 'args' in rule atom"))?
            .as_array()
            .ok_or_else(|| anyhow!("expect value for key 'args' to be an array"))?
            .iter()
            .map(|value_rep| -> Result<InputTerm<DataValue>> {
                if let Some(s) = value_rep.as_str() {
                    let var = Symbol::from(s);
                    if s.starts_with(['?', '_']) {
                        return Ok(InputTerm::Var(var));
                    } else {
                        ensure!(
                            !var.is_reserved(),
                            "{} is a reserved string value and must be quoted",
                            s
                        )
                    }
                }
                if let Some(o) = value_rep.as_object() {
                    return if let Some(c) = o.get("const") {
                        Ok(InputTerm::Const(c.into()))
                    } else {
                        let eid = self.parse_eid_from_map(o, vld)?;
                        Ok(InputTerm::Const(eid.to_value()))
                    };
                }
                Ok(InputTerm::Const(value_rep.into()))
            })
            .try_collect()?;
        Ok(InputAtom::View(InputViewApplyAtom {
            name: rule_name,
            args,
        }))
    }
    fn parse_input_rule_definition(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<(Symbol, InputRule)> {
        let rule_name = payload
            .get("rule")
            .ok_or_else(|| anyhow!("expect key 'rule' in rule definition"))?;
        let rule_name = Symbol::try_from(rule_name)?;
        if !rule_name.is_prog_entry() {
            rule_name.validate_not_reserved()?;
        }
        let vld = payload
            .get("at")
            .map(Validity::try_from)
            .unwrap_or(Ok(default_vld))?;
        let args = payload
            .get("args")
            .ok_or_else(|| anyhow!("expect key 'args' in rule definition"))?
            .as_array()
            .ok_or_else(|| anyhow!("expect value for key 'args' to be an array"))?;
        let mut args = args.iter();
        let rule_head_payload = args
            .next()
            .ok_or_else(|| anyhow!("expect value for key 'args' to be a non-empty array"))?;
        let rule_head_vec = rule_head_payload
            .as_array()
            .ok_or_else(|| anyhow!("expect rule head to be an array, got {}", rule_head_payload))?;
        let mut rule_head = vec![];
        let mut rule_aggr = vec![];
        for head_item in rule_head_vec {
            if let Some(s) = head_item.as_str() {
                let symbol = Symbol::from(s);
                symbol.validate_query_var()?;
                rule_head.push(symbol);
                rule_aggr.push(None);
            } else if let Some(m) = head_item.as_object() {
                let s = m
                    .get("symb")
                    .ok_or_else(|| anyhow!("expect field 'symb' in rule head map"))?
                    .as_str()
                    .ok_or_else(|| {
                        anyhow!("expect field 'symb' in rule head map to be a symbol")
                    })?;
                let symbol = Symbol::from(s);
                symbol.validate_query_var()?;

                let aggr = m
                    .get("aggr")
                    .ok_or_else(|| anyhow!("expect field 'aggr' in rule head map"))?
                    .as_str()
                    .ok_or_else(|| {
                        anyhow!("expect field 'aggr' in rule head map to be a symbol")
                    })?;
                let aggr = get_aggr(aggr)
                    .ok_or_else(|| anyhow!("aggregation '{}' not found", aggr))?
                    .clone();
                let aggr_args: Vec<DataValue> = match m.get("args") {
                    None => vec![],
                    Some(aggr_args) => aggr_args
                        .as_array()
                        .ok_or_else(|| anyhow!("aggregation args must be an array"))?
                        .iter()
                        .map(|v| Self::parse_const_expr(v, params_pool))
                        .try_collect()?,
                };

                rule_head.push(symbol);
                rule_aggr.push(Some((aggr, aggr_args)));
            } else {
                bail!("cannot parse {} as rule head", head_item);
            }
        }
        let rule_body: Vec<InputAtom> = args
            .map(|el| self.parse_input_atom(el, default_vld, params_pool))
            .try_collect()?;

        ensure!(
            rule_head.len() == rule_head.iter().collect::<BTreeSet<_>>().len(),
            "duplicate variables in rule head: {:?}",
            rule_head
        );

        Ok((
            rule_name,
            InputRule {
                head: rule_head,
                aggr: rule_aggr,
                body: rule_body,
                vld,
            },
        ))
    }
    fn parse_input_atom(
        &mut self,
        payload: &JsonValue,
        vld: Validity,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<InputAtom> {
        match payload {
            JsonValue::Array(arr) => match arr as &[JsonValue] {
                [entity_rep, attr_rep, value_rep] => {
                    self.parse_input_triple_atom(entity_rep, attr_rep, value_rep, vld)
                }
                _ => unimplemented!(),
            },
            JsonValue::Object(map) => {
                if map.contains_key("rule") {
                    self.parse_input_rule_atom(map, vld)
                } else if map.contains_key("view") {
                    self.parse_input_view_atom(map, vld)
                } else if map.contains_key("op") {
                    Self::parse_input_predicate_atom(map, params_pool)
                } else if map.contains_key("unify") {
                    Self::parse_unification(map, params_pool)
                } else if map.contains_key("conj")
                    || map.contains_key("disj")
                    || map.contains_key("not_exists")
                {
                    ensure!(
                        map.len() == 1,
                        "arity mismatch for atom definition {:?}: expect only one key",
                        map
                    );
                    self.parse_input_logical_atom(map, vld, params_pool)
                } else {
                    bail!("unexpected atom definition {:?}", map);
                }
            }
            v => bail!("expected atom definition {:?}", v),
        }
    }
    fn parse_input_logical_atom(
        &mut self,
        map: &Map<String, JsonValue>,
        vld: Validity,
        params_pool: &BTreeMap<Symbol, DataValue>,
    ) -> Result<InputAtom> {
        let (k, v) = map.iter().next().unwrap();
        Ok(match k as &str {
            "not_exists" => {
                let arg = self.parse_input_atom(v, vld, params_pool)?;
                InputAtom::Negation(Box::new(arg))
            }
            n @ ("conj" | "disj") => {
                let args = v
                    .as_array()
                    .ok_or_else(|| anyhow!("expect array argument for atom {}", n))?
                    .iter()
                    .map(|a| self.parse_input_atom(a, vld, params_pool))
                    .try_collect()?;
                if k == "conj" {
                    InputAtom::Conjunction(args)
                } else {
                    InputAtom::Disjunction(args)
                }
            }
            _ => unreachable!(),
        })
    }
    fn parse_input_triple_atom(
        &mut self,
        entity_rep: &JsonValue,
        attr_rep: &JsonValue,
        value_rep: &JsonValue,
        vld: Validity,
    ) -> Result<InputAtom> {
        let entity = self.parse_input_triple_atom_entity(entity_rep, vld)?;
        let attr = self.parse_triple_atom_attr(attr_rep)?;
        let value = self.parse_input_triple_clause_value(value_rep, &attr, vld)?;
        Ok(InputAtom::AttrTriple(InputAttrTripleAtom {
            attr,
            entity,
            value,
        }))
    }
    fn parse_eid_from_map(
        &mut self,
        m: &Map<String, JsonValue>,
        vld: Validity,
    ) -> Result<EntityId> {
        ensure!(
            m.len() == 1,
            "expect map to contain exactly one pair, got {:?}",
            m
        );
        let (k, v) = m.iter().next().unwrap();
        let symb = Symbol::from(k as &str);
        let attr = self
            .attr_by_name(&symb)?
            .ok_or_else(|| anyhow!("attribute {} not found", symb))?;
        ensure!(
            attr.indexing.is_unique_index(),
            "pull inside query must use unique index, of which {} is not",
            attr.name
        );
        let value = attr.val_type.coerce_value(v.into())?;
        let eid = self
            .eid_by_unique_av(&attr, &value, vld)?
            .unwrap_or(EntityId(0));
        Ok(eid)
    }
    fn parse_value_from_map(
        &mut self,
        m: &Map<String, JsonValue>,
        attr: &Attribute,
    ) -> Result<DataValue> {
        ensure!(
            m.len() == 1,
            "expect map to contain exactly one pair, got {:?}",
            m
        );
        let (k, v) = m.iter().next().unwrap();
        ensure!(k == "const", "expect key 'const', got {:?}", m);
        let value = attr.val_type.coerce_value(v.into())?;
        Ok(value)
    }
    fn parse_input_triple_clause_value(
        &mut self,
        value_rep: &JsonValue,
        attr: &Attribute,
        vld: Validity,
    ) -> Result<InputTerm<DataValue>> {
        if let Some(s) = value_rep.as_str() {
            let var = Symbol::from(s);
            if s.starts_with(['?', '_']) {
                return Ok(InputTerm::Var(var));
            } else {
                ensure!(!var.is_reserved(), "reserved string {} must be quoted", s);
            }
        }
        if let Some(o) = value_rep.as_object() {
            return if attr.val_type.is_ref_type() {
                let eid = self.parse_eid_from_map(o, vld)?;
                Ok(InputTerm::Const(DataValue::from(eid.0 as i64)))
            } else {
                Ok(InputTerm::Const(self.parse_value_from_map(o, attr)?))
            };
        }
        Ok(InputTerm::Const(
            attr.val_type.coerce_value(value_rep.into())?,
        ))
    }
    fn parse_input_triple_atom_entity(
        &mut self,
        entity_rep: &JsonValue,
        vld: Validity,
    ) -> Result<InputTerm<EntityId>> {
        if let Some(s) = entity_rep.as_str() {
            let var = Symbol::from(s);
            if s.starts_with(['?', '_']) {
                return Ok(InputTerm::Var(var));
            } else {
                ensure!(!var.is_reserved(), "reserved string {} must be quoted", s);
            }
        }
        if let Some(u) = entity_rep.as_u64() {
            return Ok(InputTerm::Const(EntityId(u)));
        }
        if let Some(o) = entity_rep.as_object() {
            let eid = self.parse_eid_from_map(o, vld)?;
            return Ok(InputTerm::Const(eid));
        }
        bail!("cannot parse {} as entity", entity_rep);
    }
    fn parse_triple_atom_attr(&mut self, attr_rep: &JsonValue) -> Result<Attribute> {
        match attr_rep {
            JsonValue::String(s) => {
                let kw = Symbol::from(s as &str);
                let attr = self
                    .attr_by_name(&kw)?
                    .ok_or_else(|| anyhow!("attribute {} not found", kw))?;
                Ok(attr)
            }
            v => bail!("expect attribute name for triple atom, got {}", v),
        }
    }
}
