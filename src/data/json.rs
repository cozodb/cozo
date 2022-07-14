use crate::data::attr::{Attribute, AttributeCardinality, AttributeIndex, AttributeTyping};
use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::keyword::{Keyword, KeywordError};
use crate::data::value::Value;
use serde_json::json;
pub(crate) use serde_json::Value as JsonValue;

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error("cannot convert JSON value {0} to {1}")]
    Conversion(JsonValue, String),
    #[error("missing field '{1}' in value {0}")]
    MissingField(JsonValue, String),
}

impl<'a> From<&'a JsonValue> for Value<'a> {
    fn from(v: &'a JsonValue) -> Self {
        match v {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Bool(*b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => match n.as_f64() {
                    Some(f) => Value::Float(f.into()),
                    None => Value::String(n.to_string().into()),
                },
            },
            JsonValue::String(s) => Value::String(s.into()),
            JsonValue::Array(arr) => Value::Tuple(arr.into_iter().map(Value::from).collect()),
            JsonValue::Object(d) => Value::Tuple(
                d.into_iter()
                    .map(|(k, v)| Value::Tuple([Value::String(k.into()), Value::from(v)].into()))
                    .collect(),
            ),
        }
    }
}
impl From<Value<'_>> for JsonValue {
    fn from(v: Value<'_>) -> Self {
        match v {
            Value::Null => JsonValue::Null,
            Value::Bool(b) => JsonValue::Bool(b),
            Value::Int(i) => JsonValue::Number(i.into()),
            Value::Float(f) => json!(f.0),
            Value::String(t) => JsonValue::String(t.into_owned()),
            Value::Uuid(uuid) => JsonValue::String(uuid.to_string()),
            Value::Bytes(bytes) => JsonValue::String(base64::encode(bytes)),
            Value::Tuple(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            Value::DescVal(v) => JsonValue::from(*v.0),
            Value::Bottom => JsonValue::Null,
            Value::EnId(i) => JsonValue::Number(i.0.into()),
            Value::Keyword(t) => JsonValue::String(t.to_string()),
            Value::Timestamp(i) => JsonValue::Number(i.into()),
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
            return Err(KeywordError::ReservedKeyword(keyword.clone()).into());
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
            Some(serde_json::Value::Bool(true)) => AttributeIndex::Indexed,
            Some(serde_json::Value::Bool(false)) => AttributeIndex::None,
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
