use anyhow::Result;
use itertools::Itertools;

use crate::data::attr::Attribute;
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::preprocess::triple::TxError;
use crate::runtime::transact::SessionTx;
use crate::{EntityId, Validity};

#[derive(Debug, thiserror::Error)]
pub enum QueryClauseError {
    #[error("error parsing query clause {0}: {1}")]
    UnexpectedForm(JsonValue, String),
}

#[derive(Clone, Debug)]
pub(crate) enum MaybeVariable<T> {
    Ignore,
    Variable(Keyword),
    Const(T),
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
            if s.starts_with('?') {
                return Ok(MaybeVariable::Variable(Keyword::from(s)));
            } else if s.starts_with('_') {
                return Ok(MaybeVariable::Ignore);
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
            if s.starts_with('?') {
                return Ok(MaybeVariable::Variable(Keyword::from(s)));
            } else if s.starts_with('_') {
                return Ok(MaybeVariable::Ignore);
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
