use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use pest::iterators::Pair;
use cozorocks::SlicePtr;
use crate::db::engine::Session;
use crate::db::eval::Environment;
use crate::error::CozoError::LogicError;
use crate::error::{CozoError, Result};
use crate::parser::Rule;
use crate::parser::text_identifier::build_name_in_def;
use crate::relation::data::DataKind;
use crate::relation::tuple::{OwnTuple, Tuple};
use crate::relation::value::Value;

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
    cache: BTreeMap<String, ()>,
    categorized: BTreeMap<String, BTreeSet<OwnTuple>>,
    default_tbl: Option<String>,
}

impl<'a, 'b, 't> MutationManager<'a, 'b, 't> {
    fn new(sess: &'a Session<'b, 't>, default_tbl: Option<String>) -> Self {
        Self { sess, cache: BTreeMap::new(), categorized: BTreeMap::new(), default_tbl }
    }
    fn add(&mut self, val_map: BTreeMap<Cow<str>, Value>) -> Result<()> {
        let tbl_name = match val_map.get("_type") {
            Some(Value::Text(t)) => t as &str,
            Some(_) => return Err(LogicError("Table kind must be text".to_string())),
            None => match &self.default_tbl {
                Some(v) => v as &str,
                None => return Err(LogicError("Cannot determine table kind".to_string()))
            }
        };
        match self.cache.get(tbl_name) {
            None => {
                match self.sess.resolve(tbl_name)? {
                    None => return Err(CozoError::UndefinedType(tbl_name.to_string())),
                    Some(tpl) => {
                        match tpl.data_kind()? {
                            DataKind::Node => {
                                println!("Found node {:?}", tpl);
                            }
                            DataKind::Edge => {
                                println!("Found edge {:?}", tpl);
                            }
                            _ => return Err(LogicError("Cannot insert into non-tables".to_string()))
                        }

                    }
                }
                // self.cache.insert(tbl_name.to_string(), ());
            }
            Some(_t) => {}
        }
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
                insert [{id: 1, name: "Jack"}, {id: 2, name: "Joe", habits: ["Balls"]}] as Person;
            "#;
            let p = Parser::parse(Rule::file, s).unwrap().next().unwrap();
            sess.run_mutation(p).unwrap();
        }

        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}