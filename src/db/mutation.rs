use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use pest::iterators::Pair;
use crate::db::engine::Session;
use crate::db::eval::Environment;
use crate::error::CozoError::LogicError;
use crate::error::{CozoError, Result};
use crate::parser::Rule;
use crate::parser::text_identifier::build_name_in_def;
use crate::relation::data::DataKind;
use crate::relation::typing::Typing;
use crate::relation::value::Value;

/// # key layouts
///
/// * Node
///   * `[table_id, keys]`
/// * Edge
///   * `[table_id, fst_table_id, fst_keys, is_forward, snd_keys, other_keys]` twice, the backward has no data
/// * Associate
///   * Same as the main one

impl<'a, 't> Session<'a, 't> {
    pub fn run_mutation(&mut self, pair: Pair<Rule>) -> Result<()> {
        let mut pairs = pair.into_inner();
        let kind = pairs.next().unwrap();
        let (evaluated, expr) = self.partial_eval(Value::from_pair(pairs.next().unwrap())?)?;
        if !evaluated {
            return Err(LogicError("Mutation encountered unevaluated expression".to_string()));
        }
        let expr = match expr {
            Value::List(v) => v,
            _ => return Err(LogicError("Mutation requires iterator of values".to_string()))
        };
        let mut default_kind = None;
        let mut filters: Option<()> = None;
        for p in pairs {
            match p.as_rule() {
                Rule::name_in_def => default_kind = Some(build_name_in_def(p, true)?),
                Rule::mutation_filter => filters = Some(()), // TODO
                _ => unreachable!()
            }
        }
        println!("{:?}", kind);
        println!("{:?}", expr);
        println!("{:?}", default_kind);
        println!("{:?}", filters);

        let mut mutation_manager = MutationManager::new(self, default_kind);
        // Coercion

        for item in expr {
            let val_map = match item {
                Value::Dict(d) => d,
                _ => return Err(LogicError("Must be structs".to_string()))
            };
            mutation_manager.add(val_map)?;
        }

        Ok(())
    }
}

struct MutationManager<'a, 'b, 't> {
    sess: &'a Session<'b, 't>,
    cache: BTreeMap<String, KVCoercer>,
    default_tbl: Option<String>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
struct KVCoercer {
    table_id: i64,
    in_root: bool,
    data_keys: HashSet<String>,
    key_typing: Vec<(String, Typing)>,
    val_typing: Vec<(String, Typing)>,
    src_key_typing: Vec<Typing>,
    dst_key_typing: Vec<Typing>,
    assocs: Vec<KVCoercer>
}

impl<'a, 'b, 't> MutationManager<'a, 'b, 't> {
    fn new(sess: &'a Session<'b, 't>, default_tbl: Option<String>) -> Self {
        Self { sess, cache: BTreeMap::new(), default_tbl }
    }
    fn get_table_info(&mut self, tbl_name: Cow<str>) -> Result<&KVCoercer> {
        if !self.cache.contains_key(tbl_name.as_ref()) {
            let coercer = match self.sess.resolve(&tbl_name)? {
                None => return Err(CozoError::UndefinedType(tbl_name.to_string())),
                Some(tpl) => {
                    let mut main_coercer = match tpl.data_kind()? {
                        DataKind::Node => {
                            let key_extractor = Typing::try_from(tpl.get_text(2)
                                .ok_or_else(|| CozoError::BadDataFormat(tpl.data.as_ref().to_vec()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let val_extractor = Typing::try_from(tpl.get_text(3)
                                .ok_or_else(|| CozoError::BadDataFormat(tpl.data.as_ref().to_vec()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let in_root = tpl.get_bool(0).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;
                            let table_id = tpl.get_int(1).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;

                            KVCoercer {
                                table_id,
                                in_root,
                                data_keys: val_extractor.iter().map(|(k, _)| k.clone()).collect(),
                                key_typing: key_extractor,
                                val_typing: val_extractor,
                                src_key_typing: vec![],
                                dst_key_typing: vec![],
                                assocs: vec![]
                            }
                        }
                        DataKind::Edge => {
                            let other_key_extractor = Typing::try_from(tpl.get_text(6)
                                .ok_or_else(|| CozoError::LogicError("Key extraction failed".to_string()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let val_extractor = Typing::try_from(tpl.get_text(7)
                                .ok_or_else(|| CozoError::LogicError("Val extraction failed".to_string()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let src_in_root = tpl.get_bool(2)
                                .ok_or_else(|| CozoError::LogicError("Src in root extraction failed".to_string()))?;
                            let src_id = tpl.get_int(3)
                                .ok_or_else(|| CozoError::LogicError("Src id extraction failed".to_string()))?;
                            let dst_in_root = tpl.get_bool(4)
                                .ok_or_else(|| CozoError::LogicError("Dst in root extraction failed".to_string()))?;
                            let dst_id = tpl.get_int(5)
                                .ok_or_else(|| CozoError::LogicError("Dst id extraction failed".to_string()))?;
                            let src = self.sess.get_table_info(src_id, src_in_root)?
                                .ok_or_else(|| CozoError::LogicError("Getting src failed".to_string()))?;
                            let src_key = Typing::try_from(src.get_text(2)
                                .ok_or_else(|| CozoError::BadDataFormat(tpl.data.as_ref().to_vec()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let src_key_typing = src_key.into_iter().map(|(_, v)| v).collect();

                            let dst = self.sess.get_table_info(dst_id, dst_in_root)?
                                .ok_or_else(|| CozoError::LogicError("Getting dst failed".to_string()))?;
                            let dst_key = Typing::try_from(dst.get_text(2)
                                .ok_or_else(|| CozoError::BadDataFormat(tpl.data.as_ref().to_vec()))?.as_ref())?
                                .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                            let dst_key_typing = dst_key.into_iter().map(|(_, v)| v).collect();

                            let in_root = tpl.get_bool(0).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;
                            let table_id = tpl.get_int(1).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;

                            KVCoercer {
                                table_id,
                                in_root,
                                data_keys: val_extractor.iter().map(|(k, _)| k.clone()).collect(),
                                key_typing: other_key_extractor,
                                val_typing: val_extractor,
                                src_key_typing,
                                dst_key_typing,
                                assocs: vec![]
                            }
                        }
                        _ => return Err(LogicError("Cannot insert into non-tables".to_string()))
                    };
                    let related = self.sess.resolve_related_tables(&tbl_name)?;
                    for (_n, d) in related {
                        let t = d.get_text(4)
                            .ok_or_else(|| CozoError::LogicError("Unable to extract typing from assoc".to_string()))?;
                        let t = Typing::try_from(t.as_ref())?
                            .extract_named_tuple().ok_or_else(|| CozoError::LogicError("Corrupt data".to_string()))?;
                        let in_root = d.get_bool(0).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;
                        let table_id = d.get_int(1).ok_or_else(|| CozoError::LogicError("Cannot extract in root".to_string()))?;

                        let coercer = KVCoercer {
                            table_id,
                            in_root,
                            data_keys: t.iter().map(|(k, _)| k.clone()).collect(),
                            key_typing: vec![],
                            val_typing: t,
                            src_key_typing: vec![],
                            dst_key_typing: vec![],
                            assocs: vec![]
                        };

                        main_coercer.assocs.push(coercer);
                    }
                    main_coercer
                }
            };
            println!("Inserting info {} {:#?}", tbl_name, coercer);
            self.cache.insert(tbl_name.as_ref().to_string(), coercer);
        }
        let info = self.cache.get(tbl_name.as_ref())
            .ok_or_else(|| CozoError::LogicError("Cannot resolve table".to_string()))?;

        Ok(info)
    }
    fn add(&mut self, val_map: BTreeMap<Cow<str>, Value>) -> Result<()> {
        let tbl_name = match val_map.get("_type") {
            Some(Value::Text(t)) => t.clone(),
            Some(_) => return Err(LogicError("Table kind must be text".to_string())),
            None => match &self.default_tbl {
                Some(v) => v.clone().into(),
                None => return Err(LogicError("Cannot determine table kind".to_string()))
            }
        };
        let table_info = self.get_table_info(tbl_name)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use pest::Parser as PestParser;
    use crate::db::engine::Engine;
    use crate::db::eval::Environment;
    use crate::parser::{Parser, Rule};

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
                    {id: 1, name: "Jack"},
                    {id: 2, name: "Joe", habits: ["Balls"], _type: "Person"},
                    {_type: "Friend", _src: 1, _dst: 2, relation: "mate"}
                ] as Person;
            "#;
            let p = Parser::parse(Rule::file, s).unwrap().next().unwrap();
            sess.run_mutation(p).unwrap();
        }

        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}