use crate::db::engine::Session;
use crate::db::table::TableInfo;
use crate::error::CozoError::LogicError;
use crate::error::{CozoError, Result};
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::tuple::Tuple;
use crate::relation::value::Value;
use pest::iterators::Pair;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;

/// # key layouts
///
/// * Node
///   * `[table_id, keys]`
/// * Edge
///   * `[table_id, fst_table_id, fst_keys, is_forward, snd_keys, other_keys]` twice, the backward has no data
/// * Associate
///   * Same as the main one
///
/// # Logic of the operations
///
/// * Insert/Upsert
///   * Just add the stuff in
/// * No update or delete: only possible through relational query
/// * Delete
///   * Only need the keys
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum MutationKind {
    Upsert,
    Insert,
}

impl<'a> Session<'a> {
    pub fn run_mutation(
        &mut self,
        pair: Pair<Rule>,
        params: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let mut pairs = pair.into_inner();
        let kind = match pairs.next().unwrap().as_rule() {
            Rule::upsert => MutationKind::Upsert,
            Rule::insert => MutationKind::Insert,
            _ => unreachable!(),
        };
        let (evaluated, expr) = self.partial_eval(
            Value::from_pair(pairs.next().unwrap())?,
            params,
            &Default::default(),
        )?;
        if !evaluated {
            return Err(LogicError(
                "Mutation encountered unevaluated expression".to_string(),
            ));
        }
        let expr = match expr {
            Value::List(v) => v,
            _ => {
                return Err(LogicError(
                    "Mutation requires iterator of values".to_string(),
                ))
            }
        };
        let mut default_kind = None;
        // let mut filters: Option<()> = None;
        for p in pairs {
            match p.as_rule() {
                Rule::name_in_def => default_kind = Some(build_name_in_def(p, true)?),
                Rule::mutation_filter => todo!(), // filters = Some(()), // TODO
                _ => unreachable!(),
            }
        }
        // println!("{:?}", kind);
        // println!("{:?}", expr);
        // println!("{:?}", default_kind);
        // println!("{:?}", filters);

        let mut mutation_manager = MutationManager::new(self, default_kind);
        // Coercion

        for item in expr {
            let val_map = match item {
                Value::Dict(d) => d,
                _ => return Err(LogicError("Must be structs".to_string())),
            };
            mutation_manager.process_insert(kind == MutationKind::Insert, val_map)?;
        }

        Ok(())
    }
}

struct MutationManager<'a, 'b> {
    sess: &'a Session<'b>,
    cache: RefCell<BTreeMap<String, Rc<TableInfo>>>,
    default_tbl: Option<String>,
}

impl<'a, 'b> MutationManager<'a, 'b> {
    fn new(sess: &'a Session<'b>, default_tbl: Option<String>) -> Self {
        Self {
            sess,
            cache: RefCell::new(BTreeMap::new()),
            default_tbl,
        }
    }
    fn get_table_info(&self, tbl_name: Cow<str>) -> Result<Rc<TableInfo>> {
        if !self.cache.borrow().contains_key(tbl_name.as_ref()) {
            let coercer = self.sess.get_table_info(&tbl_name)?;
            self.cache
                .borrow_mut()
                .insert(tbl_name.as_ref().to_string(), Rc::new(coercer));
        }
        let cache = self.cache.borrow();
        let info = cache
            .get(tbl_name.as_ref())
            .ok_or_else(|| CozoError::LogicError("Cannot resolve table".to_string()))?;

        Ok(info.clone())
    }
    fn process_insert(
        &mut self,
        error_on_existing: bool,
        mut val_map: BTreeMap<Cow<str>, Value>,
    ) -> Result<()> {
        let tbl_name = match val_map.get("_type") {
            Some(Value::Text(t)) => t.clone(),
            Some(_) => return Err(LogicError("Table kind must be text".to_string())),
            None => match &self.default_tbl {
                Some(v) => v.clone().into(),
                None => return Err(LogicError("Cannot determine table kind".to_string())),
            },
        };
        let table_info = self.get_table_info(tbl_name)?;

        let mut key_tuple;

        match table_info.kind {
            DataKind::Node => {
                key_tuple = Tuple::with_prefix(table_info.table_id.id as u32);
                for (k, v) in &table_info.key_typing {
                    let raw = val_map.remove(k.as_str()).unwrap_or(Value::Null);
                    let processed = v.coerce(raw)?;
                    key_tuple.push_value(&processed);
                }

                let mut val_tuple = Tuple::with_data_prefix(DataKind::Data);
                for (k, v) in &table_info.val_typing {
                    let raw = val_map.remove(k.as_str()).unwrap_or(Value::Null);
                    let processed = v.coerce(raw)?;
                    val_tuple.push_value(&processed);
                }
                if error_on_existing
                    && self
                        .sess
                        .key_exists(&key_tuple, table_info.table_id.in_root)?
                {
                    return Err(CozoError::KeyConflict(key_tuple));
                }
                self.sess.define_raw_key(
                    &key_tuple,
                    Some(&val_tuple),
                    table_info.table_id.in_root,
                )?;
            }
            DataKind::Edge => {
                key_tuple = Tuple::with_prefix(table_info.table_id.id as u32);
                key_tuple.push_int(table_info.src_table_id.id);

                let mut ikey_tuple = Tuple::with_prefix(table_info.table_id.id as u32);
                ikey_tuple.push_int(table_info.dst_table_id.id);

                let mut val_tuple = Tuple::with_data_prefix(DataKind::Data);

                let src = val_map.remove("_src").unwrap_or(Value::Null);
                let src_key_list = match src {
                    Value::List(v) => v,
                    v => vec![v],
                };

                if src_key_list.len() != table_info.src_key_typing.len() {
                    return Err(CozoError::LogicError("Error in _src key".to_string()));
                }

                let mut src_keys = Vec::with_capacity(src_key_list.len());

                for (t, v) in table_info
                    .src_key_typing
                    .iter()
                    .zip(src_key_list.into_iter())
                {
                    let v = t.1.coerce(v)?;
                    key_tuple.push_value(&v);
                    src_keys.push(v);
                }

                key_tuple.push_bool(true);

                let dst = val_map.remove("_dst").unwrap_or(Value::Null);
                let dst_key_list = match dst {
                    Value::List(v) => v,
                    v => vec![v],
                };

                if dst_key_list.len() != table_info.dst_key_typing.len() {
                    return Err(CozoError::LogicError("Error in _dst key".to_string()));
                }

                for (t, v) in table_info
                    .dst_key_typing
                    .iter()
                    .zip(dst_key_list.into_iter())
                {
                    let v = t.1.coerce(v)?;
                    key_tuple.push_value(&v);
                    ikey_tuple.push_value(&v);
                }

                ikey_tuple.push_bool(false);

                for v in src_keys {
                    ikey_tuple.push_value(&v);
                }

                for (k, v) in &table_info.key_typing {
                    let raw = val_map.remove(k.as_str()).unwrap_or(Value::Null);
                    let processed = v.coerce(raw)?;
                    key_tuple.push_value(&processed);
                }
                for (k, v) in &table_info.val_typing {
                    let raw = val_map.remove(k.as_str()).unwrap_or(Value::Null);
                    let processed = v.coerce(raw)?;
                    val_tuple.push_value(&processed);
                }
                if error_on_existing
                    && self
                        .sess
                        .key_exists(&key_tuple, table_info.table_id.in_root)?
                {
                    return Err(CozoError::KeyConflict(key_tuple));
                }
                self.sess.define_raw_key(
                    &key_tuple,
                    Some(&val_tuple),
                    table_info.table_id.in_root,
                )?;
                self.sess.define_raw_key(
                    &ikey_tuple,
                    Some(&key_tuple),
                    table_info.table_id.in_root,
                )?;
            }
            _ => unreachable!(),
        }

        let existing_keys: HashSet<_> = val_map.iter().map(|(k, _)| k.to_string()).collect();

        for assoc in &table_info.associates {
            if assoc.data_keys.is_subset(&existing_keys) {
                let mut val_tuple = Tuple::with_data_prefix(DataKind::Data);
                for (k, v) in &assoc.val_typing {
                    let raw = val_map.remove(k.as_str()).unwrap_or(Value::Null);
                    let processed = v.coerce(raw)?;
                    val_tuple.push_value(&processed);
                }
                key_tuple.overwrite_prefix(assoc.table_id.id as u32);
                self.sess
                    .define_raw_key(&key_tuple, Some(&val_tuple), assoc.table_id.in_root)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::engine::Engine;
    use crate::parser::{Parser, Rule};
    use crate::relation::tuple::Tuple;
    use crate::relation::value::Value;
    use pest::Parser as PestParser;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Instant;

    #[test]
    fn test_mutation() {
        let db_path = "_test_db_mutation";
        let engine = Engine::new(db_path.to_string(), true).unwrap();

        {
            let mut sess = engine.session().unwrap();
            let s = r#"
                create node "Person" {
                    *id: Int,
                    name: Text,
                    email: ?Text,
                    habits: ?[?Text]
                }

                create edge (Person)-[Friend]->(Person) {
                    relation: ?Text
                }

                create assoc WorkInfo : Person {
                    work_id: Int
                }

                create assoc RelationshipData: Person {
                    status: Text
                }
            "#;
            for p in Parser::parse(Rule::file, s).unwrap() {
                if p.as_rule() == Rule::EOI {
                    break;
                }
                sess.run_definition(p).unwrap();
            }
            sess.commit().unwrap();
        }

        {
            let mut sess = engine.session().unwrap();
            println!("{:#?}", sess.resolve("Person"));
            let s = r#"
                insert [
                    {id: 1, name: "Jack", work_id: 4},
                    {id: 2, name: "Joe", habits: ["Balls"], _type: "Person"},
                    {_type: "Friend", _src: 1, _dst: 2, relation: "mate"}
                ] as Person;
            "#;
            let p = Parser::parse(Rule::file, s).unwrap().next().unwrap();
            assert!(sess.run_mutation(p.clone(), &Default::default()).is_ok());
            sess.commit().unwrap();
            assert!(sess.run_mutation(p.clone(), &Default::default()).is_err());
            sess.rollback().unwrap();
            let it = sess.txn.iterator(true, &sess.perm_cf);
            it.to_first();
            while let Some((k, v)) = unsafe { it.pair() } {
                println!("K: {:?}, V: {:?}", Tuple::new(k), Tuple::new(v));
                it.next();
            }
        }

        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }

    #[test]
    fn test_big_mutation() {
        let db_path = "_test_big_mutation";
        let engine = Engine::new(db_path.to_string(), true).unwrap();

        {
            let mut sess = engine.session().unwrap();
            let s = fs::read_to_string("test_data/hr.cozo").unwrap();
            let start = Instant::now();

            for p in Parser::parse(Rule::file, &s).unwrap() {
                if p.as_rule() == Rule::EOI {
                    break;
                }
                sess.run_definition(p).unwrap();
            }
            sess.commit().unwrap();
            let duration = start.elapsed();
            println!("Time elapsed {:?}", duration);
        }

        {
            let mut sess = engine.session().unwrap();
            let data = fs::read_to_string("test_data/hr.json").unwrap();
            let value = Value::parse_str(&data).unwrap();
            assert!(value.is_evaluated());
            let s = "insert $data;";
            let p = Parser::parse(Rule::file, &s).unwrap().next().unwrap();
            let params = BTreeMap::from([("$data".into(), value)]);

            let start = Instant::now();

            assert!(sess.run_mutation(p.clone(), &params).is_ok());
            sess.commit().unwrap();
            assert!(sess.run_mutation(p.clone(), &params).is_err());
            sess.rollback().unwrap();
            let duration = start.elapsed();

            let it = sess.txn.iterator(true, &sess.perm_cf);
            it.to_first();
            while let Some((k, v)) = unsafe { it.pair() } {
                println!("K: {:?}, V: {:?}", Tuple::new(k), Tuple::new(v));
                it.next();
            }
            println!("Time elapsed {:?}", duration);
        }

        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}
