use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Sub;

use anyhow::Result;
use itertools::Itertools;
use serde_json::Map;

use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::tuple::TupleIter;
use crate::data::value::DataValue;
use crate::preprocess::triple::TxError;
use crate::runtime::transact::SessionTx;
use crate::transact::query::{
    InlineFixedRelation, InnerJoin, Joiner, Relation, ReorderRelation, StoredDerivedRelation,
    TripleRelation,
};
use crate::transact::throwaway::ThrowawayArea;
use crate::{EntityId, Validity};

/// example ruleset in python and javascript
/// ```python
/// [
///     R.ancestor(["?a", "?b"],
///         T.parent("?a", "?b")),
///     R.ancestor(["?a", "?b"],
///         T.parent("?a", "?c"),
///         R.ancestor("?c", "?b")),
///     Q(["?a"],
///         R.ancestor("?a", {"name": "Anne"}))
/// ]
///
/// [
///     Q.at("1990-01-01")(["?old_than_anne"],
///         T.age({"name": "Anne"}, "?anne_age"),
///         T.age("?older_than_anne", "?age"),
///         Gt("?age", "?anne_age"))
/// ]
/// ```
/// we also have `F.is_married(["anne", "brutus"], ["constantine", "delphi"])` for ad-hoc fact rules
#[derive(Debug, thiserror::Error)]
pub enum QueryProcError {
    #[error("error parsing query clause {0}: {1}")]
    UnexpectedForm(JsonValue, String),
    #[error("arity mismatch for rule {0}: all definitions must have the same arity")]
    ArityMismatch(Keyword),
    #[error("encountered undefined rule {0}")]
    UndefinedRule(Keyword),
    #[error("safety: unbound variables {0:?}")]
    UnsafeUnboundVars(BTreeSet<Keyword>),
    #[error("program logic error: {0}")]
    LogicError(String),
    #[error("entry not found: expect a rule named '?'")]
    EntryNotFound,
}

#[derive(Clone, Debug)]
pub(crate) enum Term<T> {
    Var(Keyword),
    Const(T),
}

impl<T> Term<T> {
    pub(crate) fn get_var(&self) -> Option<&Keyword> {
        match self {
            Self::Var(k) => Some(k),
            Self::Const(_) => None,
        }
    }
    pub(crate) fn get_const(&self) -> Option<&T> {
        match self {
            Self::Const(v) => Some(v),
            Self::Var(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AttrTripleAtom {
    pub(crate) attr: Attribute,
    pub(crate) entity: Term<EntityId>,
    pub(crate) value: Term<DataValue>,
}

#[derive(Clone, Debug)]
pub struct RuleApplyAtom {
    pub(crate) name: Keyword,
    pub(crate) args: Vec<Term<DataValue>>,
}

#[derive(Clone, Debug)]
pub struct PredicateAtom {
    pub(crate) left: Term<DataValue>,
    pub(crate) right: Term<DataValue>,
}

#[derive(Clone, Debug)]
pub(crate) enum Expr {
    Const(Term<DataValue>),
}

#[derive(Clone, Debug)]
pub enum Atom {
    AttrTriple(AttrTripleAtom),
    Rule(RuleApplyAtom),
    Predicate(PredicateAtom),
}

#[derive(Clone, Debug)]
pub struct RuleSet {
    pub(crate) sets: Vec<Rule>,
    pub(crate) arity: usize,
}

impl RuleSet {
    fn contained_rules(&self) -> BTreeSet<Keyword> {
        let mut collected = BTreeSet::new();
        for rule in &self.sets {
            for clause in &rule.body {
                if let Atom::Rule(rule) = clause {
                    collected.insert(rule.name.clone());
                }
            }
        }
        collected
    }
}

pub(crate) type DatalogProgram = BTreeMap<Keyword, RuleSet>;

#[derive(Clone, Debug, Default)]
pub enum Aggregation {
    #[default]
    None,
}

#[derive(Clone, Debug)]
pub(crate) struct Rule {
    pub(crate) head: Vec<(Keyword, Aggregation)>,
    pub(crate) body: Vec<Atom>,
    pub(crate) vld: Validity,
}

impl SessionTx {
    pub fn semi_naive_evaluate(&mut self, prog: &DatalogProgram) -> Result<ThrowawayArea> {
        let stores = prog
            .iter()
            .map(|(k, s)| (k.clone(), (self.new_throwaway(), s.arity)))
            .collect::<BTreeMap<_, _>>();
        let ret_area = stores
            .get(&Keyword::from("?"))
            .ok_or(QueryProcError::EntryNotFound)?
            .0
            .clone();
        let compiled: BTreeMap<_, _> = prog
            .iter()
            .map(
                |(k, body)| -> Result<(Keyword, Vec<(Vec<(Keyword, Aggregation)>, Relation)>)> {
                    let mut collected = Vec::with_capacity(body.sets.len());
                    for rule in &body.sets {
                        let header = rule.head.iter().map(|(k, v)| k).cloned().collect_vec();
                        let relation =
                            self.compile_rule_body(&rule.body, rule.vld, &stores, &header)?;
                        collected.push((rule.head.clone(), relation));
                    }
                    Ok((k.clone(), collected))
                },
            )
            .try_collect()?;

        // dbg!(&compiled);

        for epoch in 1u32.. {
            eprintln!("epoch {}", epoch);
            let mut new_derived = false;
            let snapshot = self.throwaway.make_snapshot();
            if epoch == 1 {
                let epoch_encoded = epoch.to_be_bytes();
                for (k, rules) in compiled.iter() {
                    let (store, _arity) = stores.get(k).unwrap();
                    let use_delta = BTreeSet::default();
                    for (rule_n, (_head, relation)) in rules.iter().enumerate() {
                        for item_res in relation.iter(self, epoch, &use_delta, &snapshot) {
                            let item = item_res?;
                            eprintln!("item for {}.{}: {:?} at {}", k, rule_n, item, epoch);
                            store.put(&item, &epoch_encoded)?;
                            new_derived = true;
                        }
                    }
                }
            } else {
                let epoch_encoded = epoch.to_be_bytes();
                for (k, rules) in compiled.iter() {
                    let (store, _arity) = stores.get(k).unwrap();
                    for (rule_n, (_head, relation)) in rules.iter().enumerate() {
                        for (delta_store, _) in stores.values() {
                            let use_delta = BTreeSet::from([delta_store.id]);
                            for item_res in relation.iter(self, epoch, &use_delta, &snapshot) {
                                // todo: if the relation does not depend on the delta, skip
                                let item = item_res?;
                                // improvement: the clauses can actually be evaluated in parallel
                                if store.put_if_absent(&item, &epoch_encoded)? {
                                    eprintln!("item for {}.{}: {:?} at {}", k, rule_n, item, epoch);
                                    new_derived = true;
                                } else {
                                    eprintln!("item for {}.{}: {:?} at {}, rederived", k, rule_n, item, epoch);
                                }
                            }
                        }
                    }
                }
            }
            if !new_derived {
                break;
            }
        }
        Ok(ret_area)
    }
    pub fn parse_rule_sets(
        &mut self,
        payload: &JsonValue,
        default_vld: Validity,
    ) -> Result<DatalogProgram> {
        let rules = payload
            .as_array()
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(payload.clone(), "expected array".to_string())
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
                        return Err(QueryProcError::ArityMismatch(name).into());
                    }
                }
                Ok((name, RuleSet { sets: rules, arity }))
            })
            .try_collect()
    }
    fn parse_rule_atom(&mut self, payload: &Map<String, JsonValue>, vld: Validity) -> Result<Atom> {
        let rule_name = payload
            .get("rule")
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'rule'".to_string(),
                )
            })?
            .as_str()
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'rule' to be string".to_string(),
                )
            })?
            .into();
        let args = payload
            .get("args")
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(
                    JsonValue::Object(payload.clone()),
                    "expect key 'args'".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(
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
                        return Err(QueryProcError::UnexpectedForm(
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
            QueryProcError::UnexpectedForm(payload.clone(), "expected key 'rule'".to_string())
        })?;
        let rule_name = Keyword::try_from(rule_name)?;
        let vld = payload
            .get("at")
            .map(Validity::try_from)
            .unwrap_or(Ok(default_vld))?;
        let args = payload
            .get("args")
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(payload.clone(), "expected key 'args'".to_string())
            })?
            .as_array()
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(
                    payload.clone(),
                    "expected key 'args' to be an array".to_string(),
                )
            })?;
        let mut args = args.iter();
        let rule_head = args.next().ok_or_else(|| {
            QueryProcError::UnexpectedForm(
                payload.clone(),
                "expected key 'args' to be an array containing at least one element".to_string(),
            )
        })?;
        let rule_head = rule_head.as_array().ok_or_else(|| {
            QueryProcError::UnexpectedForm(
                rule_head.clone(),
                "expect rule head to be an array".to_string(),
            )
        })?;
        let rule_head = rule_head
            .iter()
            .map(|el| -> Result<(Keyword, Aggregation)> {
                if let Some(s) = el.as_str() {
                    Ok((Keyword::from(s), Default::default()))
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
    fn compile_rule_body(
        &mut self,
        clauses: &[Atom],
        vld: Validity,
        stores: &BTreeMap<Keyword, (ThrowawayArea, usize)>,
        ret_vars: &[Keyword],
    ) -> Result<Relation> {
        let mut ret = Relation::unit();
        let mut seen_variables = BTreeSet::new();
        let mut id_serial = 0;
        let mut next_ignored_kw = || -> Keyword {
            let s = format!("*{}", id_serial);
            let kw = Keyword::from(&s as &str);
            id_serial += 1;
            kw
        };
        for clause in clauses {
            match clause {
                Atom::AttrTriple(a_triple) => match (&a_triple.entity, &a_triple.value) {
                    (Term::Const(eid), Term::Var(v_kw)) => {
                        let temp_join_key_left = next_ignored_kw();
                        let temp_join_key_right = next_ignored_kw();
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![temp_join_key_left.clone()],
                            data: vec![vec![DataValue::EnId(*eid)]],
                            to_eliminate: Default::default(),
                        });
                        if ret.is_unit() {
                            ret = const_rel;
                        } else {
                            ret = Relation::Join(Box::new(InnerJoin {
                                left: ret,
                                right: const_rel,
                                joiner: Joiner {
                                    left_keys: vec![],
                                    right_keys: vec![],
                                },
                                to_eliminate: Default::default(),
                            }));
                        }

                        let mut join_left_keys = vec![temp_join_key_left];
                        let mut join_right_keys = vec![temp_join_key_right.clone()];

                        let v_kw = {
                            if seen_variables.contains(v_kw) {
                                let ret = next_ignored_kw();
                                // to_eliminate.insert(ret.clone());
                                join_left_keys.push(v_kw.clone());
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(v_kw.clone());
                                v_kw.clone()
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr.clone(),
                            vld,
                            bindings: [temp_join_key_right, v_kw],
                        });
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right,
                            joiner: Joiner {
                                left_keys: join_left_keys,
                                right_keys: join_right_keys,
                            },
                            to_eliminate: Default::default(),
                        }));
                    }
                    (Term::Var(e_kw), Term::Const(val)) => {
                        let temp_join_key_left = next_ignored_kw();
                        let temp_join_key_right = next_ignored_kw();
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![temp_join_key_left.clone()],
                            data: vec![vec![val.clone()]],
                            to_eliminate: Default::default(),
                        });
                        if ret.is_unit() {
                            ret = const_rel;
                        } else {
                            ret = Relation::Join(Box::new(InnerJoin {
                                left: ret,
                                right: const_rel,
                                joiner: Joiner {
                                    left_keys: vec![],
                                    right_keys: vec![],
                                },
                                to_eliminate: Default::default(),
                            }));
                        }

                        let mut join_left_keys = vec![temp_join_key_left];
                        let mut join_right_keys = vec![temp_join_key_right.clone()];

                        let e_kw = {
                            if seen_variables.contains(&e_kw) {
                                let ret = next_ignored_kw();
                                join_left_keys.push(e_kw.clone());
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(e_kw.clone());
                                e_kw.clone()
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr.clone(),
                            vld,
                            bindings: [e_kw, temp_join_key_right],
                        });
                        debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right,
                            joiner: Joiner {
                                left_keys: join_left_keys,
                                right_keys: join_right_keys,
                            },
                            to_eliminate: Default::default(),
                        }));
                    }
                    (Term::Var(e_kw), Term::Var(v_kw)) => {
                        let mut join_left_keys = vec![];
                        let mut join_right_keys = vec![];
                        if e_kw == v_kw {
                            unimplemented!();
                        }
                        let e_kw = {
                            if seen_variables.contains(&e_kw) {
                                let ret = next_ignored_kw();
                                join_left_keys.push(e_kw.clone());
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(e_kw.clone());
                                e_kw.clone()
                            }
                        };
                        let v_kw = {
                            if seen_variables.contains(v_kw) {
                                let ret = next_ignored_kw();
                                join_left_keys.push(v_kw.clone());
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(v_kw.clone());
                                v_kw.clone()
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr.clone(),
                            vld,
                            bindings: [e_kw, v_kw],
                        });
                        if ret.is_unit() {
                            ret = right;
                        } else {
                            debug_assert_eq!(join_left_keys.len(), join_right_keys.len());
                            ret = Relation::Join(Box::new(InnerJoin {
                                left: ret,
                                right,
                                joiner: Joiner {
                                    left_keys: join_left_keys,
                                    right_keys: join_right_keys,
                                },
                                to_eliminate: Default::default(),
                            }));
                        }
                    }
                    (Term::Const(eid), Term::Const(val)) => {
                        let (left_var_1, left_var_2) = (next_ignored_kw(), next_ignored_kw());
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![left_var_1.clone(), left_var_2.clone()],
                            data: vec![vec![DataValue::EnId(*eid), val.clone()]],
                            to_eliminate: Default::default(),
                        });
                        if ret.is_unit() {
                            ret = const_rel;
                        } else {
                            ret = Relation::Join(Box::new(InnerJoin {
                                left: ret,
                                right: const_rel,
                                joiner: Joiner {
                                    left_keys: vec![],
                                    right_keys: vec![],
                                },
                                to_eliminate: Default::default(),
                            }));
                        }
                        let (right_var_1, right_var_2) = (next_ignored_kw(), next_ignored_kw());

                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr.clone(),
                            vld,
                            bindings: [right_var_1.clone(), right_var_2.clone()],
                        });
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right,
                            joiner: Joiner {
                                left_keys: vec![left_var_1.clone(), left_var_2.clone()],
                                right_keys: vec![right_var_1.clone(), right_var_2.clone()],
                            },
                            to_eliminate: Default::default(),
                        }));
                    }
                },
                Atom::Rule(rule_app) => {
                    let (store, arity) = stores
                        .get(&rule_app.name)
                        .ok_or_else(|| QueryProcError::UndefinedRule(rule_app.name.clone()))?
                        .clone();
                    if arity != rule_app.args.len() {
                        return Err(QueryProcError::ArityMismatch(rule_app.name.clone()).into());
                    }

                    let mut prev_joiner_vars = vec![];
                    let mut temp_left_bindings = vec![];
                    let mut temp_left_joiner_vals = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for term in &rule_app.args {
                        match term {
                            Term::Var(var) => {
                                if seen_variables.contains(var) {
                                    prev_joiner_vars.push(var.clone());
                                    let rk = next_ignored_kw();
                                    right_vars.push(rk.clone());
                                    right_joiner_vars.push(rk);
                                } else {
                                    seen_variables.insert(var.clone());
                                    right_vars.push(var.clone());
                                }
                            }
                            Term::Const(constant) => {
                                temp_left_joiner_vals.push(constant.clone());
                                let left_kw = next_ignored_kw();
                                prev_joiner_vars.push(left_kw.clone());
                                temp_left_bindings.push(left_kw);
                                let right_kw = next_ignored_kw();
                                right_joiner_vars.push(right_kw.clone());
                                right_vars.push(right_kw);
                            }
                        }
                    }

                    if !temp_left_joiner_vals.is_empty() {
                        let const_joiner = Relation::Fixed(InlineFixedRelation {
                            bindings: temp_left_bindings,
                            data: vec![temp_left_joiner_vals],
                            to_eliminate: Default::default(),
                        });
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right: const_joiner,
                            joiner: Joiner {
                                left_keys: vec![],
                                right_keys: vec![],
                            },
                            to_eliminate: Default::default(),
                        }))
                    }

                    let right = Relation::Derived(StoredDerivedRelation {
                        bindings: right_vars,
                        storage: store,
                    });
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = Relation::Join(Box::new(InnerJoin {
                        left: ret,
                        right,
                        joiner: Joiner {
                            left_keys: prev_joiner_vars,
                            right_keys: right_joiner_vars,
                        },
                        to_eliminate: Default::default(),
                    }))
                }
                Atom::Predicate(_) => {
                    todo!()
                }
            }
        }

        let ret_vars_set = ret_vars.iter().cloned().collect();

        ret.eliminate_temp_vars(&ret_vars_set)?;
        let cur_ret_set: BTreeSet<_> = ret.bindings().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            ret = Relation::Join(Box::new(InnerJoin {
                left: ret,
                right: Relation::unit(),
                joiner: Joiner {
                    left_keys: vec![],
                    right_keys: vec![],
                },
                to_eliminate: Default::default(),
            }));
            ret.eliminate_temp_vars(&ret_vars_set)?;
        }

        let cur_ret_set: BTreeSet<_> = ret.bindings().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            let diff = cur_ret_set.sub(&cur_ret_set);
            return Err(QueryProcError::UnsafeUnboundVars(diff).into());
        }
        let cur_ret_bindings = ret.bindings();
        if ret_vars != cur_ret_bindings {
            ret = Relation::Reorder(ReorderRelation {
                relation: Box::new(ret),
                new_order: ret_vars.to_vec(),
            })
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
            return Err(QueryProcError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "expect object with exactly one field".to_string(),
            )
            .into());
        }
        let (k, v) = m.iter().next().unwrap();
        let kw = Keyword::from(k as &str);
        let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
        if !attr.indexing.is_unique_index() {
            return Err(QueryProcError::UnexpectedForm(
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
            return Err(QueryProcError::UnexpectedForm(
                JsonValue::Object(m.clone()),
                "expect object with exactly one field".to_string(),
            )
            .into());
        }
        let (k, v) = m.iter().next().unwrap();
        if k != "const" {
            return Err(QueryProcError::UnexpectedForm(
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
                return Err(QueryProcError::UnexpectedForm(
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
                return Err(QueryProcError::UnexpectedForm(
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
            v => Err(QueryProcError::UnexpectedForm(
                v.clone(),
                "expect attribute keyword".to_string(),
            )
            .into()),
        }
    }
}
