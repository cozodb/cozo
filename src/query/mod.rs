use anyhow::Result;
use serde_json::json;

use crate::data::json::JsonValue;
use crate::query::compile::QueryCompilationError;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;
use crate::Validity;

pub(crate) mod compile;
pub(crate) mod eval;
pub(crate) mod pull;
pub(crate) mod relation;

impl SessionTx {
    pub fn run_query(&mut self, payload: &JsonValue) -> Result<TempStore> {
        let vld = match payload.get("since") {
            None => Validity::current(),
            Some(v) => Validity::try_from(v)?,
        };
        let q = payload.get("q").ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(payload.clone(), "expect key 'q'".to_string())
        })?;
        let rules_payload = q.as_array().ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(q.clone(), "expect array".to_string())
        })?;
        if rules_payload.is_empty() {
            return Err(QueryCompilationError::UnexpectedForm(
                payload.clone(),
                "empty rules".to_string(),
            )
            .into());
        }
        let prog = if rules_payload.first().unwrap().is_array() {
            let q = json!([{"rule": "?", "args": rules_payload}]);
            self.parse_rule_sets(&q, vld)?
        } else {
            self.parse_rule_sets(q, vld)?
        };
        let res = self.semi_naive_evaluate(&prog)?;
        Ok(res)
    }
}
