use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use itertools::Itertools;
use serde_json::Map;

use crate::data::attr::Attribute;
use crate::data::expr::{get_op, Expr};
use crate::data::json::JsonValue;
use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::data::value::DataValue;
use crate::parse::triple::TxError;
use crate::query::compile::{
    Atom, AttrTripleAtom, BindingHeadTerm, DatalogProgram, LogicalAtom, QueryCompilationError,
    Rule, RuleApplyAtom, RuleSet, Term,
};
use crate::runtime::transact::SessionTx;
use crate::{EntityId, Validity};

impl SessionTx {
    pub fn parse_rule_sets(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
    ) -> Result<DatalogProgram> {
        let rules = payload
            .as_array()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(payload.clone(), "expected array".to_string())
            })?
            .iter()
            .map(|o| self.parse_rule_definition(o, default_vld));
        let mut collected: BTreeMap<Keyword, Vec<Rule>> = BTreeMap::new();
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
        let ret: DatalogProgram = collected
            .into_iter()
            .map(|(name, rules)| -> Result<(Keyword, RuleSet)> {
                let mut arities = rules.iter().map(|r| r.head.len());
                let arity = arities.next().unwrap();
                for other in arities {
                    if other != arity {
                        return Err(QueryCompilationError::ArityMismatch(name).into());
                    }
                }
                Ok((name, RuleSet { rules, arity }))
            })
            .try_collect()?;

        match ret.get(&PROG_ENTRY) {
            None => Err(QueryCompilationError::NoEntryToProgram.into()),
            Some(ruleset) => {
                if !ruleset
                    .rules
                    .iter()
                    .map(|r| r.head.iter().map(|b| &b.name).collect_vec())
                    .all_equal()
                {
                    Err(QueryCompilationError::EntryHeadsNotIdentical.into())
                } else {
                    Ok(ret)
                }
            }
        }
    }
    fn parse_predicate_atom(payload: &Map<String, JsonValue>) -> Result<Atom> {
        let mut pred = Self::parse_expr(payload)?;
        if let Expr::Apply(op, _) = &pred {
            if !op.is_predicate {
                return Err(QueryCompilationError::NotAPredicate(op.name).into());
            }
        }
        pred.partial_eval()?;
        Ok(Atom::Predicate(pred))
    }
    fn parse_expr(payload: &Map<String, JsonValue>) -> Result<Expr> {
        let name = payload
            .get("pred")
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'pred'".to_string(),
                )
            })?
            .as_str()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'pred' to be the name of a predicate".to_string(),
                )
            })?;

        let op =
            get_op(name).ok_or_else(|| QueryCompilationError::UnknownOperator(name.to_string()))?;

        let args: Box<[Expr]> = payload
            .get("args")
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'args'".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'args' to be an array".to_string(),
                )
            })?
            .iter()
            .map(Self::parse_expr_arg)
            .try_collect()?;

        if op.vararg {
            if args.len() < op.min_arity {
                return Err(QueryCompilationError::PredicateArityMismatch(
                    op.name,
                    op.min_arity,
                    args.len(),
                )
                .into());
            }
        } else if args.len() != op.min_arity {
            return Err(QueryCompilationError::PredicateArityMismatch(
                op.name,
                op.min_arity,
                args.len(),
            )
            .into());
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
                } else if map.contains_key("pred") {
                    Self::parse_expr(map)
                } else {
                    Err(QueryCompilationError::UnexpectedForm(
                        JsonValue::Object(map.clone()),
                        "must contain either 'const' or 'pred' key".to_string(),
                    )
                    .into())
                }
            }
            v => Ok(Expr::Const(v.into())),
        }
    }
    fn parse_rule_atom(&mut self, payload: &Map<String, JsonValue>, vld: Validity) -> Result<Atom> {
        let rule_name = payload
            .get("rule")
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'rule'".to_string(),
                )
            })?
            .as_str()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'rule' to be string".to_string(),
                )
            })?
            .into();
        let args = payload
            .get("args")
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'args'".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'args' to be an array".to_string(),
                )
            })?
            .iter()
            .map(|value_rep| -> Result<Term<DataValue>> {
                if let Some(s) = value_rep.as_str() {
                    let var = Keyword::from(s);
                    if s.starts_with(['?', '_']) {
                        return Ok(Term::Var(var));
                    } else if var.is_reserved() {
                        return Err(QueryCompilationError::UnexpectedForm(
                            value_rep.clone(),
                            "reserved string values must be quoted".to_string(),
                        )
                        .into());
                    }
                }
                if let Some(o) = value_rep.as_object() {
                    return if let Some(c) = o.get("const") {
                        Ok(Term::Const(c.into()))
                    } else {
                        let eid = self.parse_eid_from_map(o, vld)?;
                        Ok(Term::Const(DataValue::EnId(eid)))
                    };
                }
                Ok(Term::Const(value_rep.into()))
            })
            .try_collect()?;
        Ok(Atom::Rule(RuleApplyAtom {
            name: rule_name,
            args,
        }))
    }
    fn parse_rule_definition(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
    ) -> Result<(Keyword, Rule)> {
        let rule_name = payload.get("rule").ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(
                payload.clone(),
                "expected key 'rule'".to_string(),
            )
        })?;
        let rule_name = Keyword::try_from(rule_name)?;
        let vld = payload
            .get("at")
            .map(Validity::try_from)
            .unwrap_or(Ok(default_vld))?;
        let args = payload
            .get("args")
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    payload.clone(),
                    "expected key 'args'".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                QueryCompilationError::UnexpectedForm(
                    payload.clone(),
                    "expected key 'args' to be an array".to_string(),
                )
            })?;
        let mut args = args.iter();
        let rule_head = args.next().ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(
                payload.clone(),
                "expected key 'args' to be an array containing at least one element".to_string(),
            )
        })?;
        let rule_head = rule_head.as_array().ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(
                rule_head.clone(),
                "expect rule head to be an array".to_string(),
            )
        })?;
        let rule_head: Vec<_> = rule_head
            .iter()
            .map(|el| -> Result<BindingHeadTerm> {
                if let Some(s) = el.as_str() {
                    Ok(BindingHeadTerm {
                        name: Keyword::from(s),
                        aggr: Default::default(),
                    })
                } else {
                    todo!()
                }
            })
            .try_collect()?;
        let rule_body: Vec<_> = args
            .map(|el| self.parse_atom(el, default_vld))
            .try_collect()?;

        let rule_body = Self::reorder_rule_body_for_predicates(rule_body)?;

        if rule_head.len()
            != rule_head
                .iter()
                .map(|h| &h.name)
                .collect::<BTreeSet<_>>()
                .len()
        {
            return Err(QueryCompilationError::DuplicateVariables(
                rule_head.into_iter().map(|h| h.name).collect_vec(),
            )
            .into());
        }

        Ok((
            rule_name,
            Rule {
                head: rule_head,
                body: rule_body,
                vld,
            },
        ))
    }

    fn reorder_rule_body_for_predicates(clauses: Vec<Atom>) -> Result<Vec<Atom>> {
        let (predicates, others): (Vec<_>, _) = clauses.into_iter().partition(|a| a.is_predicate());
        let mut predicates_with_meta = predicates
            .into_iter()
            .map(|p| {
                let p = p.into_predicate().unwrap();
                let bindings = p.bindings();
                (Some(p), bindings)
            })
            .collect_vec();
        let mut seen_bindings = BTreeSet::new();
        let mut ret = vec![];
        for a in others {
            a.collect_bindings(&mut seen_bindings);
            ret.push(a);
            for (pred, pred_bindings) in predicates_with_meta.iter_mut() {
                if pred.is_none() {
                    continue;
                }
                if seen_bindings.is_superset(pred_bindings) {
                    let pred = pred.take().unwrap();
                    ret.push(Atom::Predicate(pred));
                }
            }
        }
        for (p, bindings) in predicates_with_meta {
            if let Some(p) = p {
                let diff = bindings.difference(&seen_bindings).cloned().collect();
                return Err(QueryCompilationError::UnsafeBindingInPredicate(p, diff).into());
            }
        }
        Ok(ret)
    }

    fn parse_atom(&mut self, payload: &JsonValue, vld: Validity) -> Result<Atom> {
        match payload {
            JsonValue::Array(arr) => match arr as &[JsonValue] {
                [entity_rep, attr_rep, value_rep] => {
                    self.parse_triple_atom(entity_rep, attr_rep, value_rep, vld)
                }
                _ => unimplemented!(),
            },
            JsonValue::Object(map) => {
                if map.contains_key("rule") {
                    self.parse_rule_atom(map, vld)
                } else if map.contains_key("pred") {
                    Self::parse_predicate_atom(map)
                } else if map.contains_key("conj")
                    || map.contains_key("disj")
                    || map.contains_key("not_exists")
                {
                    if map.len() != 1 {
                        return Err(QueryCompilationError::UnexpectedForm(
                            JsonValue::Object(map.clone()),
                            "too many keys".to_string(),
                        )
                        .into());
                    }
                    self.parse_logical_atom(map, vld)
                } else {
                    Err(QueryCompilationError::UnexpectedForm(
                        JsonValue::Object(map.clone()),
                        "unknown format".to_string(),
                    )
                    .into())
                }
            }
            v => Err(QueryCompilationError::UnexpectedForm(
                v.clone(),
                "unknown format".to_string(),
            )
            .into()),
        }
    }
    fn parse_logical_atom(&mut self, map: &Map<String, JsonValue>, vld: Validity) -> Result<Atom> {
        let (k, v) = map.iter().next().unwrap();
        Ok(match k as &str {
            "not_exists" => {
                let arg = self.parse_atom(v, vld)?;
                Atom::Logical(LogicalAtom::Negation(Box::new(arg)))
            }
            "conj" | "disj" => {
                let args = v
                    .as_array()
                    .ok_or_else(|| {
                        QueryCompilationError::UnexpectedForm(v.clone(), "expect array".to_string())
                    })?
                    .iter()
                    .map(|a| self.parse_atom(a, vld))
                    .try_collect()?;
                if k == "conj" {
                    Atom::Logical(LogicalAtom::Conjunction(args))
                } else {
                    Atom::Logical(LogicalAtom::Disjunction(args))
                }
            }
            _ => unreachable!(),
        })
    }
    fn parse_triple_atom(
        &mut self,
        entity_rep: &JsonValue,
        attr_rep: &JsonValue,
        value_rep: &JsonValue,
        vld: Validity,
    ) -> Result<Atom> {
        let entity = self.parse_triple_atom_entity(entity_rep, vld)?;
        let attr = self.parse_triple_atom_attr(attr_rep)?;
        let value = self.parse_triple_clause_value(value_rep, &attr, vld)?;
        Ok(Atom::AttrTriple(AttrTripleAtom {
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
        if m.len() != 1 {
            return Err(QueryCompilationError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "expect object with exactly one field".to_string(),
            )
            .into());
        }
        let (k, v) = m.iter().next().unwrap();
        let kw = Keyword::from(k as &str);
        let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
        if !attr.indexing.is_unique_index() {
            return Err(QueryCompilationError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "attribute is not a unique index".to_string(),
            )
            .into());
        }
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
        if m.len() != 1 {
            return Err(QueryCompilationError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "expect object with exactly one field".to_string(),
            )
            .into());
        }
        let (k, v) = m.iter().next().unwrap();
        if k != "const" {
            return Err(QueryCompilationError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "expect object with exactly one field named 'const'".to_string(),
            )
            .into());
        }
        let value = attr.val_type.coerce_value(v.into())?;
        Ok(value)
    }
    fn parse_triple_clause_value(
        &mut self,
        value_rep: &JsonValue,
        attr: &Attribute,
        vld: Validity,
    ) -> Result<Term<DataValue>> {
        if let Some(s) = value_rep.as_str() {
            let var = Keyword::from(s);
            if s.starts_with(['?', '_']) {
                return Ok(Term::Var(var));
            } else if var.is_reserved() {
                return Err(QueryCompilationError::UnexpectedForm(
                    value_rep.clone(),
                    "reserved string values must be quoted".to_string(),
                )
                .into());
            }
        }
        if let Some(o) = value_rep.as_object() {
            return if attr.val_type.is_ref_type() {
                let eid = self.parse_eid_from_map(o, vld)?;
                Ok(Term::Const(DataValue::EnId(eid)))
            } else {
                Ok(Term::Const(self.parse_value_from_map(o, attr)?))
            };
        }
        Ok(Term::Const(attr.val_type.coerce_value(value_rep.into())?))
    }
    fn parse_triple_atom_entity(
        &mut self,
        entity_rep: &JsonValue,
        vld: Validity,
    ) -> Result<Term<EntityId>> {
        if let Some(s) = entity_rep.as_str() {
            let var = Keyword::from(s);
            if s.starts_with(['?', '_']) {
                return Ok(Term::Var(var));
            } else if var.is_reserved() {
                return Err(QueryCompilationError::UnexpectedForm(
                    entity_rep.clone(),
                    "reserved string values must be quoted".to_string(),
                )
                .into());
            }
        }
        if let Some(u) = entity_rep.as_u64() {
            return Ok(Term::Const(EntityId(u)));
        }
        if let Some(o) = entity_rep.as_object() {
            let eid = self.parse_eid_from_map(o, vld)?;
            return Ok(Term::Const(eid));
        }
        todo!()
    }
    fn parse_triple_atom_attr(&mut self, attr_rep: &JsonValue) -> Result<Attribute> {
        match attr_rep {
            JsonValue::String(s) => {
                let kw = Keyword::from(s as &str);
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
                Ok(attr)
            }
            v => Err(QueryCompilationError::UnexpectedForm(
                v.clone(),
                "expect attribute keyword".to_string(),
            )
            .into()),
        }
    }
}
