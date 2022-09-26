use itertools::Itertools;
use miette::{Diagnostic, Result};
use smartstring::SmartString;
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::program::{ConstRule, MagicSymbol, RelationOp};
use crate::data::relation::{ColumnDef, NullableColType};
use crate::data::symb::Symbol;
use crate::data::tuple::{EncodedTuple, Tuple};
use crate::data::value::DataValue;
use crate::parse::parse_script;
use crate::runtime::relation::InputRelationHandle;
use crate::runtime::transact::SessionTx;
use crate::Db;

#[derive(Debug, Error, Diagnostic)]
#[error("attempting to write into relation {0} of arity {1} with data of arity {2}")]
#[diagnostic(code(eval::relation_arity_mismatch))]
struct RelationArityMismatch(String, usize, usize);

impl SessionTx {
    pub(crate) fn execute_relation<'a>(
        &'a mut self,
        db: &Db,
        res_iter: impl Iterator<Item = Result<Tuple>> + 'a,
        op: RelationOp,
        meta: &InputRelationHandle,
        headers: &[Symbol],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut to_clear = vec![];
        let mut overwrite_old_triggers = None;
        if op == RelationOp::Overwrite {
            if let Ok(old_handle) = self.get_relation(&meta.name) {
                if old_handle.has_triggers() {
                    overwrite_old_triggers = Some((old_handle.put_triggers, old_handle.del_triggers))
                }
                for trigger in &old_handle.overwrite_triggers {
                    let program =
                        parse_script(trigger, &Default::default())?.get_single_program()?;

                    let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                        if err.source_code().is_some() {
                            err
                        } else {
                            err.with_source_code(trigger.to_string())
                        }
                    })?;
                    to_clear.extend(cleanups);
                }
            }
            if let Ok(c) = self.destroy_relation(&meta.name) {
                to_clear.push(c);
            }
        }
        let mut relation_store = if op == RelationOp::Overwrite || op == RelationOp::Create {
            self.create_relation(meta.clone())?
        } else {
            self.get_relation(&meta.name)?
        };
        if let Some((old_put, old_retract)) = overwrite_old_triggers {
            relation_store.put_triggers = old_put;
            relation_store.del_triggers = old_retract;
        }
        let InputRelationHandle {
            metadata,
            key_bindings,
            dep_bindings,
            span,
            ..
        } = meta;
        if op == RelationOp::Del {
            let key_extractors = make_extractors(
                &relation_store.metadata.keys,
                &metadata.keys,
                key_bindings,
                headers,
            )?;

            let has_triggers = !relation_store.del_triggers.is_empty();
            let mut new_tuples = vec![];
            let mut old_tuples = vec![];

            for tuple in res_iter {
                let tuple = tuple?;
                let extracted = Tuple(
                    key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?,
                );
                let key = relation_store.adhoc_encode_key(&extracted, *span)?;
                if has_triggers {
                    if let Some(existing) = self.tx.get(&key, false)? {
                        let mut tup = extracted.clone();
                        if !existing.is_empty() {
                            let v_tup = EncodedTuple(&existing);
                            if v_tup.arity() > 0 {
                                tup.0.extend(v_tup.decode().0);
                            }
                        }
                        old_tuples.push(tup);
                    }
                    new_tuples.push(extracted.clone());
                }
                self.tx.del(&key)?;
            }

            if has_triggers && !new_tuples.is_empty() {
                for trigger in &relation_store.del_triggers {
                    let mut program =
                        parse_script(trigger, &Default::default())?.get_single_program()?;

                    let mut bindings = relation_store
                        .metadata
                        .keys
                        .iter()
                        .map(|k| Symbol::new(k.name.clone(), Default::default()))
                        .collect_vec();

                    program.const_rules.insert(
                        MagicSymbol::Muggle {
                            inner: Symbol::new(SmartString::from("_new"), Default::default()),
                        },
                        ConstRule {
                            bindings: bindings.clone(),
                            data: new_tuples.clone(),
                            span: Default::default(),
                        },
                    );

                    let v_bindings = relation_store
                        .metadata
                        .non_keys
                        .iter()
                        .map(|k| Symbol::new(k.name.clone(), Default::default()));
                    bindings.extend(v_bindings);

                    program.const_rules.insert(
                        MagicSymbol::Muggle {
                            inner: Symbol::new(SmartString::from("_old"), Default::default()),
                        },
                        ConstRule {
                            bindings,
                            data: old_tuples.clone(),
                            span: Default::default(),
                        },
                    );

                    let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                        if err.source_code().is_some() {
                            err
                        } else {
                            err.with_source_code(trigger.to_string())
                        }
                    })?;
                    to_clear.extend(cleanups);
                }
            }
        } else {
            let mut key_extractors = make_extractors(
                &relation_store.metadata.keys,
                &metadata.keys,
                key_bindings,
                headers,
            )?;

            let has_triggers = !relation_store.put_triggers.is_empty();
            let mut new_tuples = vec![];
            let mut old_tuples = vec![];

            let val_extractors = make_extractors(
                &relation_store.metadata.non_keys,
                &metadata.non_keys,
                dep_bindings,
                headers,
            )?;
            key_extractors.extend(val_extractors);

            for tuple in res_iter {
                let tuple = tuple?;

                let extracted = Tuple(
                    key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?,
                );

                let key = relation_store.adhoc_encode_key(&extracted, *span)?;
                let val = relation_store.adhoc_encode_val(&extracted, *span)?;

                if has_triggers {
                    if let Some(existing) = self.tx.get(&key, false)? {
                        let mut tup = extracted.clone();
                        if !existing.is_empty() {
                            let v_tup = EncodedTuple(&existing);
                            if v_tup.arity() > 0 {
                                tup.0.extend(v_tup.decode().0);
                            }
                        }
                        old_tuples.push(tup);
                    }

                    new_tuples.push(extracted.clone());
                }

                self.tx.put(&key, &val)?;
            }

            if has_triggers && !new_tuples.is_empty() {
                for trigger in &relation_store.put_triggers {
                    let mut program =
                        parse_script(trigger, &Default::default())?.get_single_program()?;

                    let mut bindings = relation_store
                        .metadata
                        .keys
                        .iter()
                        .map(|k| Symbol::new(k.name.clone(), Default::default()))
                        .collect_vec();
                    let v_bindings = relation_store
                        .metadata
                        .non_keys
                        .iter()
                        .map(|k| Symbol::new(k.name.clone(), Default::default()));
                    bindings.extend(v_bindings);

                    program.const_rules.insert(
                        MagicSymbol::Muggle {
                            inner: Symbol::new(SmartString::from("_new"), Default::default()),
                        },
                        ConstRule {
                            bindings: bindings.clone(),
                            data: new_tuples.clone(),
                            span: Default::default(),
                        },
                    );

                    program.const_rules.insert(
                        MagicSymbol::Muggle {
                            inner: Symbol::new(SmartString::from("_old"), Default::default()),
                        },
                        ConstRule {
                            bindings,
                            data: old_tuples.clone(),
                            span: Default::default(),
                        },
                    );

                    let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                        if err.source_code().is_some() {
                            err
                        } else {
                            err.with_source_code(trigger.to_string())
                        }
                    })?;
                    to_clear.extend(cleanups);
                }
            }
        }

        Ok(to_clear)
    }
}

enum DataExtractor {
    DefaultExtractor(Expr, NullableColType),
    IndexExtractor(usize, NullableColType),
}

impl DataExtractor {
    fn extract_data(&self, tuple: &Tuple) -> Result<DataValue> {
        Ok(match self {
            DataExtractor::DefaultExtractor(expr, typ) => {
                typ.coerce(expr.clone().eval_to_const()?)?
            }
            DataExtractor::IndexExtractor(i, typ) => typ.coerce(tuple.0[*i].clone())?,
        })
    }
}

fn make_extractors(
    stored: &[ColumnDef],
    input: &[ColumnDef],
    bindings: &[Symbol],
    tuple_headers: &[Symbol],
) -> Result<Vec<DataExtractor>> {
    stored
        .iter()
        .map(|s| make_extractor(s, input, bindings, tuple_headers))
        .try_collect()
}

fn make_extractor(
    stored: &ColumnDef,
    input: &[ColumnDef],
    bindings: &[Symbol],
    tuple_headers: &[Symbol],
) -> Result<DataExtractor> {
    for (inp_col, inp_binding) in input.iter().zip(bindings.iter()) {
        if inp_col.name == stored.name {
            for (idx, tuple_head) in tuple_headers.iter().enumerate() {
                if tuple_head == inp_binding {
                    return Ok(DataExtractor::IndexExtractor(idx, stored.typing.clone()));
                }
            }
        }
    }
    if let Some(expr) = &stored.default_gen {
        Ok(DataExtractor::DefaultExtractor(
            expr.clone(),
            stored.typing.clone(),
        ))
    } else {
        #[derive(Debug, Error, Diagnostic)]
        #[error("cannot make extractor for column {0}")]
        #[diagnostic(code(eval::unable_to_make_extractor))]
        struct UnableToMakeExtractor(String);
        Err(UnableToMakeExtractor(stored.name.to_string()).into())
    }
}
