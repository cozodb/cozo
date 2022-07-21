use std::collections::BTreeSet;

use anyhow::Result;
use itertools::Itertools;

use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::preprocess::triple::TxError;
use crate::runtime::transact::SessionTx;
use crate::transact::query::{
    InlineFixedRelation, InnerJoin, Joiner, ProjectedRelation, Relation, TripleRelation,
};
use crate::{EntityId, Validity};

#[derive(Debug, thiserror::Error)]
pub enum QueryClauseError {
    #[error("error parsing query clause {0}: {1}")]
    UnexpectedForm(JsonValue, String),
}

#[derive(Clone, Debug)]
pub(crate) enum MaybeVariable<T> {
    Variable(Keyword),
    Const(T),
}

impl<T> MaybeVariable<T> {
    pub(crate) fn get_var(&self) -> Option<&Keyword> {
        match self {
            Self::Variable(k) => Some(k),
            Self::Const(_) => None,
        }
    }
    pub(crate) fn get_const(&self) -> Option<&T> {
        match self {
            Self::Const(v) => Some(v),
            Self::Variable(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AttrTripleClause {
    pub(crate) attr: Attribute,
    pub(crate) entity: MaybeVariable<EntityId>,
    pub(crate) value: MaybeVariable<DataValue>,
}

#[derive(Clone, Debug)]
pub enum Clause {
    AttrTriple(AttrTripleClause),
}

impl SessionTx {
    pub fn parse_clauses(&mut self, payload: &JsonValue, vld: Validity) -> Result<Vec<Clause>> {
        payload
            .as_array()
            .ok_or_else(|| {
                QueryClauseError::UnexpectedForm(payload.clone(), "expect array".to_string())
            })?
            .iter()
            .map(|el| self.parse_clause(el, vld))
            .try_collect()
    }
    pub fn compile_clauses(&mut self, clauses: Vec<Clause>, vld: Validity) -> Result<Relation> {
        let mut ret = Relation::unit();
        let mut seen_variables = BTreeSet::new();
        for clause in clauses {
            match clause {
                Clause::AttrTriple(a_triple) => match (a_triple.entity, a_triple.value) {
                    (MaybeVariable::Const(eid), MaybeVariable::Variable(v_kw)) => {
                        let mut to_eliminate = BTreeSet::new();

                        let temp_join_key_left = Keyword::rand();
                        let temp_join_key_right = Keyword::rand();
                        to_eliminate.insert(temp_join_key_left.clone());
                        to_eliminate.insert(temp_join_key_right.clone());
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![temp_join_key_left.clone()],
                            data: vec![vec![DataValue::EnId(eid)]],
                        });
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right: const_rel,
                            joiner: Joiner {
                                left_keys: vec![],
                                right_keys: vec![],
                            },
                        }));

                        let mut join_left_keys = vec![temp_join_key_left];
                        let mut join_right_keys = vec![temp_join_key_right.clone()];

                        let v_kw = {
                            if seen_variables.contains(&v_kw) {
                                let ret = Keyword::rand();
                                to_eliminate.insert(ret.clone());
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
                        }));
                        ret = Relation::Project(Box::new(ProjectedRelation {
                            relation: ret,
                            eliminate: to_eliminate,
                        }))
                    }
                    (MaybeVariable::Variable(e_kw), MaybeVariable::Const(val)) => {
                        let mut to_eliminate = BTreeSet::new();

                        let temp_join_key_left = Keyword::rand();
                        let temp_join_key_right = Keyword::rand();
                        to_eliminate.insert(temp_join_key_left.clone());
                        to_eliminate.insert(temp_join_key_right.clone());
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![temp_join_key_left.clone()],
                            data: vec![vec![val]],
                        });
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right: const_rel,
                            joiner: Joiner {
                                left_keys: vec![],
                                right_keys: vec![],
                            },
                        }));

                        let mut join_left_keys = vec![temp_join_key_left];
                        let mut join_right_keys = vec![temp_join_key_right.clone()];

                        let e_kw = {
                            if seen_variables.contains(&e_kw) {
                                let ret = Keyword::rand();
                                to_eliminate.insert(ret.clone());
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
                        }));
                        ret = Relation::Project(Box::new(ProjectedRelation {
                            relation: ret,
                            eliminate: to_eliminate,
                        }))
                    }
                    (MaybeVariable::Variable(e_kw), MaybeVariable::Variable(v_kw)) => {
                        let mut to_eliminate = BTreeSet::new();
                        let mut join_left_keys = vec![];
                        let mut join_right_keys = vec![];
                        if e_kw == v_kw {
                            unimplemented!();
                        }
                        let e_kw = {
                            if seen_variables.contains(&e_kw) {
                                let ret = Keyword::rand();
                                to_eliminate.insert(ret.clone());
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
                                let ret = Keyword::rand();
                                to_eliminate.insert(ret.clone());
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
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right,
                            joiner: Joiner {
                                left_keys: join_left_keys,
                                right_keys: join_right_keys,
                            },
                        }));
                        if !to_eliminate.is_empty() {
                            ret = Relation::Project(Box::new(ProjectedRelation {
                                relation: ret,
                                eliminate: to_eliminate,
                            }))
                        }
                    }
                    (MaybeVariable::Const(eid), MaybeVariable::Const(val)) => {
                        let (left_var_1, left_var_2) = (Keyword::rand(), Keyword::rand());
                        let const_rel = Relation::Fixed(InlineFixedRelation {
                            bindings: vec![left_var_1.clone(), left_var_2.clone()],
                            data: vec![vec![DataValue::EnId(eid), val]],
                        });
                        ret = Relation::Join(Box::new(InnerJoin {
                            left: ret,
                            right: const_rel,
                            joiner: Joiner {
                                left_keys: vec![],
                                right_keys: vec![],
                            },
                        }));
                        let (right_var_1, right_var_2) = (Keyword::rand(), Keyword::rand());

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
                        }));
                        ret = Relation::Project(Box::new(ProjectedRelation {
                            relation: ret,
                            eliminate: BTreeSet::from([
                                left_var_1,
                                left_var_2,
                                right_var_1,
                                right_var_2,
                            ]),
                        }))
                    }
                },
            }
        }
        let eliminate: BTreeSet<Keyword> = seen_variables
            .into_iter()
            .filter(|kw| !kw.is_query_binding())
            .collect();
        if !eliminate.is_empty() {
            ret = Relation::Project(Box::new(ProjectedRelation {
                relation: ret,
                eliminate,
            }))
        }

        Ok(ret)
    }
    fn parse_clause(&mut self, payload: &JsonValue, vld: Validity) -> Result<Clause> {
        match payload {
            JsonValue::Array(arr) => match arr as &[JsonValue] {
                [entity_rep, attr_rep, value_rep] => {
                    self.parse_triple_clause(entity_rep, attr_rep, value_rep, vld)
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }
    fn parse_triple_clause(
        &mut self,
        entity_rep: &JsonValue,
        attr_rep: &JsonValue,
        value_rep: &JsonValue,
        vld: Validity,
    ) -> Result<Clause> {
        let entity = self.parse_triple_clause_entity(entity_rep)?;
        let attr = self.parse_triple_clause_attr(attr_rep)?;
        let value = self.parse_triple_clause_value(value_rep, &attr, vld)?;
        Ok(Clause::AttrTriple(AttrTripleClause {
            attr,
            entity,
            value,
        }))
    }
    fn parse_triple_clause_value(
        &mut self,
        value_rep: &JsonValue,
        attr: &Attribute,
        vld: Validity,
    ) -> Result<MaybeVariable<DataValue>> {
        if let Some(s) = value_rep.as_str() {
            if s.starts_with(['?', '_']) {
                return Ok(MaybeVariable::Variable(Keyword::from(s)));
            }
        }
        if let Some(o) = value_rep.as_object() {
            if attr.val_type.is_ref_type() {
                unimplemented!()
            }
        }
        Ok(MaybeVariable::Const(
            attr.val_type.coerce_value(value_rep.into())?,
        ))
    }
    fn parse_triple_clause_entity(
        &mut self,
        entity_rep: &JsonValue,
    ) -> Result<MaybeVariable<EntityId>> {
        if let Some(s) = entity_rep.as_str() {
            if s.starts_with(['?', '_']) {
                return Ok(MaybeVariable::Variable(Keyword::from(s)));
            }
        }
        if let Some(u) = entity_rep.as_u64() {
            return Ok(MaybeVariable::Const(EntityId(u)));
        }
        todo!()
    }
    fn parse_triple_clause_attr(&mut self, attr_rep: &JsonValue) -> Result<Attribute> {
        match attr_rep {
            JsonValue::String(s) => {
                let kw = Keyword::from(s as &str);
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
                Ok(attr)
            }
            v => Err(QueryClauseError::UnexpectedForm(
                v.clone(),
                "expect attribute keyword".to_string(),
            )
            .into()),
        }
    }
}
