use miette::{Diagnostic, ensure, Result};
use thiserror::Error;

use cozorocks::CfHandle::Snd;

use crate::data::program::RelationOp;
use crate::data::tuple::Tuple;
use crate::runtime::relation::RelationHandle;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn execute_relation<'a>(
        &'a mut self,
        res_iter: impl Iterator<Item=Result<Tuple>> + 'a,
        op: RelationOp,
        meta: &RelationHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut to_clear = None;
        if op == RelationOp::ReDerive {
            if let Ok(c) = self.destroy_relation(&meta.name) {
                to_clear = Some(c);
            }
        }
        let relation_store = if op == RelationOp::ReDerive || op == RelationOp::Create {
            self.create_relation(meta.clone())?
        } else {
            let found = self.get_relation(&meta.name)?;

            #[derive(Debug, Error, Diagnostic)]
            #[error("Attempting to write into relation {0} of arity {1} with data of arity {2}")]
            #[diagnostic(code(eval::relation_arity_mismatch))]
            struct RelationArityMismatch(String, usize, usize);

            ensure!(
                found.arity() == meta.arity(),
                RelationArityMismatch(found.name.to_string(), found.arity(), meta.arity())
            );
            found
        };
        if op == RelationOp::Retract {
            for data in res_iter {
                let data = data?;
                let encoded = data.encode_as_key(relation_store.id);
                self.tx.del(&encoded, Snd)?;
            }
        } else {
            for data in res_iter {
                let data = data?;
                let encoded = data.encode_as_key(relation_store.id);
                self.tx.put(&encoded, &[], Snd)?;
            }
        }
        Ok(to_clear)
    }
}
