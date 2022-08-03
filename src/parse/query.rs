use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use serde_json::{json, Map};

use crate::data::attr::Attribute;
use crate::data::expr::{get_op, Expr};
use crate::data::id::{EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::data::program::{
    InputAtom, InputAttrTripleAtom, InputProgram, InputRule, InputRuleApplyAtom, InputTerm,
    Unification,
};
use crate::data::value::DataValue;
use crate::query::pull::PullSpecs;
use crate::runtime::transact::SessionTx;
use crate::utils::swap_result_option;

pub(crate) type OutSpec = (Vec<(usize, Option<PullSpecs>)>, Option<Vec<String>>);

impl SessionTx {
    pub(crate) fn parse_query(
        &mut self,
        payload: &JsonValue,
    ) -> Result<(InputProgram, Option<OutSpec>, Validity)> {
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
            self.parse_input_rule_sets(&q, vld)?
        } else {
            self.parse_input_rule_sets(q, vld)?
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
        Ok((input_prog, out_spec, vld))
    }
    fn parse_query_out_spec(
        &mut self,
        payload: &JsonValue,
        entry_bindings: &[Keyword],
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
        entry_bindings: &[Keyword],
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
                        let kw = Keyword::from(s as &str);
                        let idx = *entry_bindings
                            .get(&kw)
                            .ok_or_else(|| anyhow!("binding {} not found", kw))?;
                        Ok((idx, None))
                    }
                    JsonValue::Object(m) => {
                        let kw = m
                            .get("pull")
                            .ok_or_else(|| anyhow!("expect field 'pull' in {:?}", m))?
                            .as_str()
                            .ok_or_else(|| anyhow!("expect 'pull' to be a binding in {:?}", m))?;
                        let kw = Keyword::from(kw);
                        let idx = *entry_bindings
                            .get(&kw)
                            .ok_or_else(|| anyhow!("binding {} not found", kw))?;
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
    ) -> Result<InputProgram> {
        let rules = payload
            .as_array()
            .ok_or_else(|| anyhow!("expect array for rules, got {}", payload))?
            .iter()
            .map(|o| self.parse_input_rule_definition(o, default_vld));
        let mut collected: BTreeMap<Keyword, Vec<InputRule>> = BTreeMap::new();
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
        let ret: BTreeMap<Keyword, Vec<InputRule>> = collected
            .into_iter()
            .map(|(name, rules)| -> Result<(Keyword, Vec<InputRule>)> {
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

        match ret.get(&PROG_ENTRY as &Keyword) {
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
    fn parse_input_predicate_atom(payload: &Map<String, JsonValue>) -> Result<InputAtom> {
        let mut pred = Self::parse_expr(payload)?;
        if let Expr::Apply(op, _) = &pred {
            ensure!(
                op.is_predicate,
                "non-predicate expression in predicate position: {}",
                op.name
            );
        }
        pred.partial_eval()?;
        Ok(InputAtom::Predicate(pred))
    }
    fn parse_unification(payload: &Map<String, JsonValue>) -> Result<InputAtom> {
        let binding = payload
            .get("unify")
            .ok_or_else(|| anyhow!("expect expression to have field 'unify'"))?
            .as_str()
            .ok_or_else(|| anyhow!("expect field 'unify' to be a keyword"))?;
        let binding = Keyword::from(binding);
        ensure!(
            binding.is_query_var(),
            "binding for unification {} is reserved",
            binding
        );
        let expr = payload
            .get("expr")
            .ok_or_else(|| anyhow!("expect unify map to have field 'expr'"))?;
        let expr = Self::parse_expr_arg(expr)?;
        Ok(InputAtom::Unification(Unification { binding, expr }))
    }
    fn parse_expr(payload: &Map<String, JsonValue>) -> Result<Expr> {
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
            .map(Self::parse_expr_arg)
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
    fn parse_expr_arg(payload: &JsonValue) -> Result<Expr> {
        match payload {
            JsonValue::String(s) => {
                let kw = Keyword::from(s as &str);
                if kw.is_reserved() {
                    Ok(Expr::Binding(kw, None))
                } else {
                    Ok(Expr::Const(DataValue::String(s.into())))
                }
            }
            JsonValue::Object(map) => {
                if let Some(v) = map.get("const") {
                    Ok(Expr::Const(v.into()))
                } else if map.contains_key("op") {
                    Self::parse_expr(map)
                } else {
                    bail!("expression object must contain either 'const' or 'pred' key");
                }
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
                    let var = Keyword::from(s);
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
    fn parse_input_rule_definition(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
    ) -> Result<(Keyword, InputRule)> {
        let rule_name = payload
            .get("rule")
            .ok_or_else(|| anyhow!("expect key 'rule' in rule definition"))?;
        let rule_name = Keyword::try_from(rule_name)?;
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
                rule_head.push(Keyword::from(s));
                rule_aggr.push(None);
            } else {
                todo!()
            }
        }
        let rule_body: Vec<InputAtom> = args
            .map(|el| self.parse_input_atom(el, default_vld))
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
    fn parse_input_atom(&mut self, payload: &JsonValue, vld: Validity) -> Result<InputAtom> {
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
                } else if map.contains_key("op") {
                    Self::parse_input_predicate_atom(map)
                } else if map.contains_key("unify") {
                    Self::parse_unification(map)
                } else if map.contains_key("conj")
                    || map.contains_key("disj")
                    || map.contains_key("not_exists")
                {
                    ensure!(
                        map.len() == 1,
                        "arity mismatch for atom definition {:?}: expect only one key",
                        map
                    );
                    self.parse_input_logical_atom(map, vld)
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
    ) -> Result<InputAtom> {
        let (k, v) = map.iter().next().unwrap();
        Ok(match k as &str {
            "not_exists" => {
                let arg = self.parse_input_atom(v, vld)?;
                InputAtom::Negation(Box::new(arg))
            }
            n @ ("conj" | "disj") => {
                let args = v
                    .as_array()
                    .ok_or_else(|| anyhow!("expect array argument for atom {}", n))?
                    .iter()
                    .map(|a| self.parse_input_atom(a, vld))
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
        let kw = Keyword::from(k as &str);
        let attr = self
            .attr_by_kw(&kw)?
            .ok_or_else(|| anyhow!("attribute {} not found", kw))?;
        ensure!(
            attr.indexing.is_unique_index(),
            "pull inside query must use unique index, of which {} is not",
            attr.keyword
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
            let var = Keyword::from(s);
            if s.starts_with(['?', '_']) {
                return Ok(InputTerm::Var(var));
            } else {
                ensure!(!var.is_reserved(), "reserved string {} must be quoted", s);
            }
        }
        if let Some(o) = value_rep.as_object() {
            return if attr.val_type.is_ref_type() {
                let eid = self.parse_eid_from_map(o, vld)?;
                Ok(InputTerm::Const(DataValue::Int(eid.0 as i64)))
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
            let var = Keyword::from(s);
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
        todo!()
    }
    fn parse_triple_atom_attr(&mut self, attr_rep: &JsonValue) -> Result<Attribute> {
        match attr_rep {
            JsonValue::String(s) => {
                let kw = Keyword::from(s as &str);
                let attr = self
                    .attr_by_kw(&kw)?
                    .ok_or_else(|| anyhow!("attribute {} not found", kw))?;
                Ok(attr)
            }
            v => bail!("expect attribute keyword for triple atom, got {}", v),
        }
    }
}
