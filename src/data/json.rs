use anyhow::{anyhow, ensure};
use serde_json::json;
pub(crate) use serde_json::Value as JsonValue;

use crate::data::attr::{Attribute, AttributeCardinality, AttributeIndex, AttributeTyping};
use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::symb::Symbol;
use crate::data::value::{DataValue, Number};

impl From<JsonValue> for DataValue {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => DataValue::Null,
            JsonValue::Bool(b) => DataValue::Bool(b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => DataValue::from(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::from(f),
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
                Some(i) => DataValue::from(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::from(f),
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
            DataValue::Number(Number::Int(i)) => JsonValue::Number(i.into()),
            DataValue::Number(Number::Float(f)) => json!(f),
            DataValue::String(t) => JsonValue::String(t.into()),
            DataValue::Uuid(uuid) => JsonValue::String(uuid.to_string()),
            DataValue::Bytes(bytes) => JsonValue::String(base64::encode(bytes)),
            DataValue::List(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            DataValue::DescVal(v) => JsonValue::from(*v.0),
            DataValue::Bottom => JsonValue::Null,
            DataValue::Timestamp(i) => JsonValue::Number(i.into()),
            DataValue::Guard => JsonValue::Null,
            DataValue::Set(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            DataValue::Regex(r) => {
                json!(r.0.as_str())
            }
            // DataValue::Map(m) => {
            //     JsonValue::Array(m.into_iter().map(|(k, v)| json!([k, v])).collect())
            // }
        }
    }
}

impl TryFrom<&'_ JsonValue> for Symbol {
    type Error = anyhow::Error;
    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let s = value
            .as_str()
            .ok_or_else(|| anyhow!("failed to convert {} to a symbol", value))?;
        Ok(Symbol::from(s))
    }
}

impl TryFrom<&'_ JsonValue> for Attribute {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let map = value
            .as_object()
            .ok_or_else(|| anyhow!("expect object in attribute definition, got {}", value))?;
        let id = match map.get("id") {
            None => AttrId(0),
            Some(v) => AttrId::try_from(v)?,
        };
        let name = map
            .get("name")
            .ok_or_else(|| anyhow!("expect field 'name' in attribute definition, got {}", value))?;
        let symb = Symbol::try_from(name)?;
        ensure!(!symb.is_reserved(), "cannot use reserved symbol {}", symb);
        let cardinality = map
            .get("cardinality")
            .ok_or_else(|| anyhow!("expect field 'cardinality' in {}", value))?
            .as_str()
            .ok_or_else(|| anyhow!("expect field 'cardinality' to be a string, got {}", value))?;
        let cardinality = AttributeCardinality::try_from(cardinality)?;
        let val_type = map
            .get("type")
            .ok_or_else(|| anyhow!("expect field 'type' in {}", value))?
            .as_str()
            .ok_or_else(|| anyhow!("expect field 'type' in {} to be a string", value))?;
        let val_type = AttributeTyping::try_from(val_type)?;

        let indexing = match map.get("index") {
            None => AttributeIndex::None,
            Some(JsonValue::Bool(true)) => AttributeIndex::Indexed,
            Some(JsonValue::Bool(false)) => AttributeIndex::None,
            Some(v) => AttributeIndex::try_from(
                v.as_str()
                    .ok_or_else(|| anyhow!("cannot convert {} to attribute indexing", v))?,
            )?,
        };

        let with_history = match map.get("history") {
            None => false,
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow!("cannot convert {} to attribute with history flag", v))?,
        };

        Ok(Attribute {
            id,
            name: symb,
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
            "name": attr.name.to_string(),
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
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| anyhow!("cannot convert {} to attr id", value))?;
        Ok(AttrId(v))
    }
}

impl From<EntityId> for JsonValue {
    fn from(id: EntityId) -> Self {
        JsonValue::Number(id.0.into())
    }
}

impl TryFrom<&'_ JsonValue> for EntityId {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| anyhow!("cannot convert {} to entity id", value))?;
        Ok(EntityId(v))
    }
}

impl From<TxId> for JsonValue {
    fn from(id: TxId) -> Self {
        JsonValue::Number(id.0.into())
    }
}

impl TryFrom<&'_ JsonValue> for TxId {
    type Error = anyhow::Error;

    fn try_from(value: &'_ JsonValue) -> Result<Self, Self::Error> {
        let v = value
            .as_u64()
            .ok_or_else(|| anyhow!("cannot convert {} to tx id", value))?;
        Ok(TxId(v))
    }
}
