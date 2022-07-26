use serde_json::json;
pub(crate) use serde_json::Value as JsonValue;

use crate::data::attr::{Attribute, AttributeCardinality, AttributeIndex, AttributeTyping};
use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::keyword::{Keyword, KeywordError};
use crate::data::value::DataValue;

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error("cannot convert JSON value {0} to {1}")]
    Conversion(JsonValue, String),
    #[error("missing field '{1}' in value {0}")]
    MissingField(JsonValue, String),
}

impl From<JsonValue> for DataValue {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => DataValue::Null,
            JsonValue::Bool(b) => DataValue::Bool(b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => DataValue::Int(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::Float(f.into()),
                    None => DataValue::String(n.to_string().into()),
                },
            },
            JsonValue::String(s) => DataValue::String(s.into()),
            JsonValue::Array(arr) => DataValue::List(arr.iter().map(DataValue::from).collect()),
            JsonValue::Object(d) => DataValue::List(
                d.into_iter()
                    .map(|(k, v)| {
                        DataValue::List([DataValue::String(k.into()), DataValue::from(v)].into())
                    })
                    .collect(),
            ),
        }
    }
}

impl<'a> From<&'a JsonValue> for DataValue {
    fn from(v: &'a JsonValue) -> Self {
        match v {
            JsonValue::Null => DataValue::Null,
            JsonValue::Bool(b) => DataValue::Bool(*b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => DataValue::Int(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::Float(f.into()),
                    None => DataValue::String(n.to_string().into()),
                },
            },
            JsonValue::String(s) => DataValue::String(s.into()),
            JsonValue::Array(arr) => DataValue::List(arr.iter().map(DataValue::from).collect()),
            JsonValue::Object(d) => DataValue::List(
                d.into_iter()
                    .map(|(k, v)| {
                        DataValue::List([DataValue::String(k.into()), DataValue::from(v)].into())
                    })
                    .collect(),
            ),
        }
    }
}

impl From<DataValue> for JsonValue {
    fn from(v: DataValue) -> Self {
        match v {
            DataValue::Null => JsonValue::Null,
            DataValue::Bool(b) => JsonValue::Bool(b),
            DataValue::Int(i) => JsonValue::Number(i.into()),
            DataValue::Float(f) => json!(f.0),
            DataValue::String(t) => JsonValue::String(t.into()),
            DataValue::Uuid(uuid) => JsonValue::String(uuid.to_string()),
            DataValue::Bytes(bytes) => JsonValue::String(base64::encode(bytes)),
            DataValue::List(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            DataValue::DescVal(v) => JsonValue::from(*v.0),
            DataValue::Bottom => JsonValue::Null,
            DataValue::EnId(i) => JsonValue::Number(i.0.into()),
            DataValue::Keyword(t) => JsonValue::String(t.to_string()),
            DataValue::Timestamp(i) => JsonValue::Number(i.into()),
        }
    }
}

impl TryFrom<&'_ JsonValue> for Keyword {
    type Error = anyhow::Error;
    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let s = value
            .as_str()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "Keyword".to_string()))?;
        Ok(Keyword::from(s))
    }
}

impl TryFrom<&'_ JsonValue> for Attribute {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let map = value
            .as_object()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "Attribute".to_string()))?;
        let id = match map.get("id") {
            None => AttrId(0),
            Some(v) => AttrId::try_from(v)?,
        };
        let keyword = map
            .get("keyword")
            .ok_or_else(|| JsonError::MissingField(value.clone(), "keyword".to_string()))?;
        let keyword = Keyword::try_from(keyword)?;
        if keyword.is_reserved() {
            return Err(KeywordError::ReservedKeyword(keyword).into());
        }
        let cardinality = map
            .get("cardinality")
            .ok_or_else(|| JsonError::MissingField(value.clone(), "cardinality".to_string()))?
            .as_str()
            .ok_or_else(|| {
                JsonError::Conversion(value.clone(), "AttributeCardinality".to_string())
            })?;
        let cardinality = AttributeCardinality::try_from(cardinality)?;
        let val_type = map
            .get("type")
            .ok_or_else(|| JsonError::MissingField(value.clone(), "type".to_string()))?
            .as_str()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "AttributeTyping".to_string()))?;
        let val_type = AttributeTyping::try_from(val_type)?;

        let indexing = match map.get("index") {
            None => AttributeIndex::None,
            Some(JsonValue::Bool(true)) => AttributeIndex::Indexed,
            Some(JsonValue::Bool(false)) => AttributeIndex::None,
            Some(v) => AttributeIndex::try_from(v.as_str().ok_or_else(|| {
                JsonError::Conversion(value.clone(), "AttributeIndexing".to_string())
            })?)?,
        };

        let with_history = match map.get("history") {
            None => true,
            Some(v) => v.as_bool().ok_or_else(|| {
                JsonError::Conversion(value.clone(), "AttributeWithHistory".to_string())
            })?,
        };

        Ok(Attribute {
            id,
            keyword,
            cardinality,
            val_type,
            indexing,
            with_history,
        })
    }
}

impl From<Attribute> for JsonValue {
    fn from(attr: Attribute) -> Self {
        json!({
            "id": attr.id.0,
            "keyword": attr.keyword.to_string(),
            "cardinality": attr.cardinality.to_string(),
            "type": attr.val_type.to_string(),
            "index": attr.indexing.to_string(),
            "history": attr.with_history
        })
    }
}

impl From<AttrId> for JsonValue {
    fn from(id: AttrId) -> Self {
        JsonValue::Number(id.0.into())
    }
}

impl TryFrom<&'_ JsonValue> for AttrId {
    type Error = JsonError;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "AttrId".to_string()))?;
        Ok(AttrId(v))
    }
}

impl From<EntityId> for JsonValue {
    fn from(id: EntityId) -> Self {
        JsonValue::Number(id.0.into())
    }
}

impl TryFrom<&'_ JsonValue> for EntityId {
    type Error = JsonError;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "EntityId".to_string()))?;
        Ok(EntityId(v))
    }
}

impl From<TxId> for JsonValue {
    fn from(id: TxId) -> Self {
        JsonValue::Number(id.0.into())
    }
}

impl TryFrom<&'_ JsonValue> for TxId {
    type Error = JsonError;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| JsonError::Conversion(value.clone(), "TxId".to_string()))?;
        Ok(TxId(v))
    }
}
