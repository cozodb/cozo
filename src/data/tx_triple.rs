use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::Value;
use crate::runtime::transact::SessionTx;
use anyhow::Result;
use serde_json::Map;

pub(crate) struct Triple<'a> {
    id: EntityId,
    attr: AttrId,
    value: Value<'a>,
}

pub struct Quintuple<'a> {
    triple: Triple<'a>,
    op: StoreOp,
    validity: Validity,
}

#[derive(Debug, thiserror::Error)]
pub enum TxError {
    #[error("Error decoding {0}: {1}")]
    Decoding(serde_json::Value, String),
    #[error("triple length error")]
    TripleLength,
    #[error("attribute not found: {0}")]
    AttrNotFound(Keyword),
}

impl SessionTx {
    /// Requests are like these
    /// ```json
    /// {"tx": [...], "comment": "a comment", "since": timestamp}
    /// ```
    /// each line in `tx` is `{"put: ...}`, `{"retract": ...}`, `{"erase": ...}` or `{"ensure": ...}`
    /// these can also have a `from` field, overriding the timestamp
    /// the dots can be triples
    /// ```json
    /// [12345, ":x/y", 12345]
    /// ```
    /// triples with tempid
    /// ```json
    /// ["tempid1", ":x/y", 12345]
    /// ```
    /// objects format
    /// ```json
    /// {
    ///     "_id": 12345,
    ///     "_tempid": "xyzwf",
    ///     "ns/fieldname": 111
    /// }
    /// ```
    /// nesting is allowed for values of type `ref` and `component`
    pub fn parse_tx_requests<'a>(
        &mut self,
        req: &'a serde_json::Value,
    ) -> Result<(Vec<Quintuple<'a>>, String)> {
        let map = req
            .as_object()
            .ok_or_else(|| TxError::Decoding(req.clone(), "expected object".to_string()))?;
        let items = map
            .get("tx")
            .ok_or_else(|| TxError::Decoding(req.clone(), "expected field 'tx'".to_string()))?
            .as_array()
            .ok_or_else(|| {
                TxError::Decoding(
                    req.clone(),
                    "expected field 'tx' to be an array".to_string(),
                )
            })?;
        let default_since = match map.get("since") {
            None => Validity::current(),
            Some(v) => v.try_into()?,
        };
        let comment = match map.get("comment") {
            None => "".to_string(),
            Some(v) => v.to_string(),
        };
        let mut collected = Vec::with_capacity(items.len());
        for item in items {
            collected.push(self.parse_tx_request_item(item, default_since)?)
        }
        Ok((collected, comment))
    }
    fn parse_tx_request_item<'a>(
        &mut self,
        item: &'a serde_json::Value,
        default_since: Validity,
    ) -> Result<Quintuple<'a>> {
        if let Some(arr) = item.as_array() {
            return self.parse_tx_request_arr(arr, default_since);
        }

        if let Some(obj) = item.as_object() {
            return self.parse_tx_request_obj(obj, default_since);
        }

        Err(TxError::Decoding(item.clone(), "expected object or array".to_string()).into())
    }
    fn parse_tx_request_arr<'a>(
        &mut self,
        item: &'a [serde_json::Value],
        default_since: Validity,
    ) -> Result<Quintuple<'a>> {
        match item {
            [eid, attr_kw, val] => {
                let kw: Keyword = attr_kw.try_into()?;
                let attr = self.attr_by_kw(&kw)?.ok_or(TxError::AttrNotFound(kw))?;
                todo!()
            }
            vs => Err(TxError::TripleLength.into()),
        }
    }
    fn parse_tx_request_obj<'a>(
        &mut self,
        item: &'a Map<String, serde_json::Value>,
        default_since: Validity,
    ) -> Result<Quintuple<'a>> {
        todo!()
    }
}
