use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use anyhow::Result;
use itertools::Itertools;
use serde_json::Map;

use crate::{EntityId, Validity};
use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::preprocess::triple::TxError;
use crate::query::compile::{
    Atom, AttrTripleAtom, BindingHeadTerm, DatalogProgram, QueryCompilationError, Rule,
    RuleApplyAtom, RuleSet, Term,
};
use crate::runtime::transact::SessionTx;

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
        collected
            .into_iter()
            .map(|(name, rules)| -> Result<(Keyword, RuleSet)> {
                let mut arities = rules.iter().map(|r| r.head.len());
                let arity = arities.next().unwrap();
                for other in arities {
                    if other != arity {
                        return Err(QueryCompilationError::ArityMismatch(name).into());
                    }
                }
                Ok((
                    name,
                    RuleSet {
                        rules: rules,
                        arity,
                    },
                ))
            })
            .try_collect()
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
        let rule_head = rule_head
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

        Ok((
            rule_name,
            Rule {
                head: rule_head,
                body: rule_body,
                vld,
            },
        ))
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
                // rule application, or built-in predicates,
                // or disjunction/negation (convert to disjunctive normal forms)
                if map.contains_key("rule") {
                    self.parse_rule_atom(map, vld)
                } else if map.contains_key("pred") {
                    dbg!(map);
                    todo!()
                } else {
                    todo!()
                }
            }
            _ => unimplemented!(),
        }
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
