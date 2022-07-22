use std::collections::BTreeSet;

use anyhow::Result;
use itertools::Itertools;
use serde_json::Map;

use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::preprocess::triple::TxError;
use crate::runtime::transact::SessionTx;
use crate::transact::query::{InlineFixedRelation, InnerJoin, Joiner, Relation, TripleRelation};
use crate::transact::throwaway::ThrowawayArea;
use crate::{EntityId, Validity};

/// example ruleset in python and javascript
/// ```python
/// [
///     R.ancestor(["?a", "?b],
///         T.parent("?a", "?b")),
///     R.ancestor(["?a", "?b"],
///         T.parent("?a", "?c"),
///         R.ancestor("?c", "?b")),
///     Q(["?a"],
///         R.ancestor("?a", {"name": "Anne"}))
/// ]
///
/// [
///     Q(["?old_than_anne"],
///         T.age({"name": "Anne"}, "?anne_age"),
///         T.age("?older_than_anne", "?age"),
///         Gt("?age", "?anne_age")).at("1990-01-01")
/// ]
/// ```

#[derive(Debug, thiserror::Error)]
pub enum QueryProcError {
    #[error("error parsing query clause {0}: {1}")]
    UnexpectedForm(JsonValue, String),
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
    pub(crate) rule: Keyword,
    pub(crate) args: Vec<Term<DataValue>>,
}

#[derive(Clone, Debug)]
pub struct UnificationAtom {
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
    Unification(UnificationAtom),
}

#[derive(Clone, Debug)]
pub struct RuleSet {
    pub(crate) name: Keyword,
    pub(crate) storage: Option<ThrowawayArea>,
    pub(crate) sets: Vec<Rule>,
    pub(crate) arity: usize,
}

#[derive(Clone, Debug, Default)]
pub enum Aggregation {
    #[default]
    None,
}

#[derive(Clone, Debug)]
pub(crate) struct Rule {
    pub(crate) head: Vec<(Keyword, Aggregation)>,
    pub(crate) body: Vec<Atom>,
}

impl SessionTx {
    pub fn parse_rule_sets(&mut self, payload: &JsonValue) -> Result<Vec<RuleSet>> {
        todo!()
    }
    pub fn parse_rule_body(&mut self, payload: &JsonValue, vld: Validity) -> Result<Vec<Atom>> {
        payload
            .as_array()
            .ok_or_else(|| {
                QueryProcError::UnexpectedForm(payload.clone(), "expect array".to_string())
            })?
            .iter()
            .map(|el| self.parse_atom(el, vld))
            .try_collect()
    }
    pub fn compile_rule_body(&mut self, clauses: Vec<Atom>, vld: Validity) -> Result<Relation> {
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
                Atom::AttrTriple(a_triple) => match (a_triple.entity, a_triple.value) {
                    (Term::Const(eid), Term::Var(v_kw)) => {
                        let temp_join_key_left = next_ignored_kw();
                        let temp_join_key_right = next_ignored_kw();
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![temp_join_key_left.clone()],
                            data: vec![vec![DataValue::EnId(eid)]],
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
                            if seen_variables.contains(&v_kw) {
                                let ret = next_ignored_kw();
                                // to_eliminate.insert(ret.clone());
                                join_left_keys.push(v_kw);
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(v_kw.clone());
                                v_kw
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr,
                            vld,
                            bindings: [temp_join_key_right, v_kw],
                        });
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
                            data: vec![vec![val]],
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
                                join_left_keys.push(e_kw);
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(e_kw.clone());
                                e_kw
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr,
                            vld,
                            bindings: [e_kw, temp_join_key_right],
                        });
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
                                join_left_keys.push(e_kw);
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(e_kw.clone());
                                e_kw
                            }
                        };
                        let v_kw = {
                            if seen_variables.contains(&v_kw) {
                                let ret = next_ignored_kw();
                                join_left_keys.push(v_kw);
                                join_right_keys.push(ret.clone());
                                ret
                            } else {
                                seen_variables.insert(v_kw.clone());
                                v_kw
                            }
                        };
                        let right = Relation::Triple(TripleRelation {
                            attr: a_triple.attr,
                            vld,
                            bindings: [e_kw, v_kw],
                        });
                        if ret.is_unit() {
                            ret = right;
                        } else {
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
                            data: vec![vec![DataValue::EnId(eid), val]],
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
                            attr: a_triple.attr,
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
                    todo!()
                }
                Atom::Unification(_) => {
                    todo!()
                }
            }
        }

        ret.eliminate_temp_vars()?;
        if ret.bindings().iter().any(|b| b.is_ignored_binding()) {
            ret = Relation::Join(Box::new(InnerJoin {
                left: ret,
                right: Relation::unit(),
                joiner: Joiner {
                    left_keys: vec![],
                    right_keys: vec![],
                },
                to_eliminate: Default::default(),
            }));
            ret.eliminate_temp_vars()?;
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
                todo!()
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
