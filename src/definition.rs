use std::collections::BTreeMap;
use pest::iterators::{Pair, Pairs};
use crate::ast::parse_string;
use crate::env::{Env, LayeredEnv, StructuredEnvItem};
use crate::error::Result;
use crate::error::CozoError::*;
use crate::eval::Evaluator;
use crate::storage::Storage;
use crate::typing::{Col, Columns, Edge, Index, Node, StorageStatus, Structured, TableId, Typing};
use crate::typing::StorageStatus::{Planned, Stored};
use crate::value::{ByteArrayBuilder, ByteArrayParser, Value};
use crate::parser::{Parser, Rule};
use pest::Parser as PestParser;
// use rocksdb::IteratorMode;

fn parse_ident(pair: Pair<Rule>) -> String {
    pair.as_str().to_string()
}

fn build_name_in_def(pair: Pair<Rule>, forbid_underscore: bool) -> Result<String> {
    let inner = pair.into_inner().next().unwrap();
    let name = match inner.as_rule() {
        Rule::ident => parse_ident(inner),
        Rule::raw_string | Rule::s_quoted_string | Rule::quoted_string => parse_string(inner)?,
        _ => unreachable!()
    };
    if forbid_underscore && name.starts_with('_') {
        Err(ReservedIdent)
    } else {
        Ok(name)
    }
}

fn parse_col_name(pair: Pair<Rule>) -> Result<(String, bool)> {
    let mut pairs = pair.into_inner();
    let mut is_key = false;
    let mut nxt_pair = pairs.next().unwrap();
    if nxt_pair.as_rule() == Rule::key_marker {
        is_key = true;
        nxt_pair = pairs.next().unwrap();
    }

    Ok((build_name_in_def(nxt_pair, true)?, is_key))
}


impl StructuredEnvItem {
    pub fn build_edge_def(&mut self, pair: Pair<Rule>, global: bool) -> Result<String> {
        let mut inner = pair.into_inner();
        let src_name = build_name_in_def(inner.next().unwrap(), true)?;
        let src = self.resolve(&src_name).ok_or(UndefinedType)?;
        let src_id = if let Structured::Node(n) = src {
            n.id.clone()
        } else {
            return Err(WrongType);
        };
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let dst_name = build_name_in_def(inner.next().unwrap(), true)?;
        let dst = self.resolve(&dst_name).ok_or(UndefinedType)?;
        let dst_id = if let Structured::Node(n) = dst {
            n.id.clone()
        } else {
            return Err(WrongType);
        };
        if global && (!src_id.global || !dst_id.global) {
            return Err(IncompatibleEdge);
        }
        let (keys, cols) = if let Some(p) = inner.next() {
            self.build_col_defs(p)?
        } else {
            (vec![], vec![])
        };
        let table_id = TableId { name: name.clone(), global };
        let edge = Edge {
            status: Planned,
            src: src_id,
            dst: dst_id,
            id: table_id.clone(),
            keys,
            cols,
        };
        if self.define_new(name.clone(), Structured::Edge(edge)) {
            if let Some(Structured::Node(src)) = self.resolve_mut(&src_name) {
                src.out_e.push(table_id.clone());
            } else {
                unreachable!()
            }

            if let Some(Structured::Node(dst)) = self.resolve_mut(&dst_name) {
                dst.in_e.push(table_id.clone());
            } else {
                unreachable!()
            }
            Ok(name.to_string())
        } else {
            Err(NameConflict)
        }
    }
    pub fn build_node_def(&mut self, pair: Pair<Rule>, global: bool) -> Result<String> {
        let mut inner = pair.into_inner();
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let (keys, cols) = self.build_col_defs(inner.next().unwrap())?;
        let table_id = TableId { name: name.clone(), global };
        let node = Node {
            status: Planned,
            id: table_id,
            keys,
            cols,
            out_e: vec![],
            in_e: vec![],
            attached: vec![],
        };
        if self.define_new(name.clone(), Structured::Node(node)) {
            Ok(name.to_string())
        } else {
            Err(NameConflict)
        }
    }

    fn build_col_list(&mut self, pair: Pair<Rule>) -> Result<Vec<String>> {
        let mut ret = vec![];
        for p in pair.into_inner() {
            ret.push(build_name_in_def(p, true)?);
        }
        Ok(ret)
    }
    fn build_columns_def(&mut self, pair: Pair<Rule>, global: bool) -> Result<String> {
        let mut inner = pair.into_inner();
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let node_name = build_name_in_def(inner.next().unwrap(), true)?;
        let node = self.resolve(&node_name).ok_or(UndefinedType)?;
        let node_id = if let Structured::Node(n) = node {
            n.id.clone()
        } else if let Structured::Edge(n) = node {
            n.id.clone()
        } else {
            return Err(WrongType);
        };
        let (keys, cols) = self.build_col_defs(inner.next().unwrap())?;
        if !keys.is_empty() {
            return Err(UnexpectedIndexColumns);
        }
        let table_id = TableId { name: name.clone(), global };
        if table_id.global && !node_id.global {
            return Err(IncompatibleEdge);
        }

        if self.define_new(name.clone(), Structured::Columns(Columns {
            status: Planned,
            id: table_id,
            attached: node_id,
            cols,
        })) {
            Ok(name)
        } else {
            Err(NameConflict)
        }
    }

    fn build_index_def(&mut self, pair: Pair<Rule>, global: bool) -> Result<String> {
        let mut inner = pair.into_inner();
        let mut name = build_name_in_def(inner.next().unwrap(), true)?;
        let node_name;
        let nxt = inner.next().unwrap();

        let col_list = match nxt.as_rule() {
            Rule::col_list => {
                node_name = name;
                name = "_".to_string() + &node_name;
                let cols = self.build_col_list(nxt)?;
                name.push('_');
                for col in &cols {
                    name.push('_');
                    name += col;
                }
                cols
            }
            _ => {
                node_name = build_name_in_def(nxt, true)?;
                self.build_col_list(inner.next().unwrap())?
            }
        };

        let node = self.resolve(&node_name).ok_or(UndefinedType)?;
        let node_id = if let Structured::Node(n) = node {
            n.id.clone()
        } else {
            return Err(WrongType);
        };
        let table_id = TableId { name: name.clone(), global };

        if table_id.global && !node_id.global {
            return Err(IncompatibleEdge);
        }

        // TODO: make sure cols make sense

        if self.define_new(name.clone(), Structured::Index(Index {
            status: Planned,
            id: table_id,
            attached: node_id,
            cols: col_list,
        })) {
            Ok(name)
        } else {
            Err(NameConflict)
        }
    }

    pub fn build_type_from_str(&self, src: &str) -> Result<Typing> {
        let ast = Parser::parse(Rule::typing, src)?.next().unwrap();
        self.build_type(ast)
    }

    fn build_type(&self, pair: Pair<Rule>) -> Result<Typing> {
        let mut pairs = pair.into_inner();
        let mut inner = pairs.next().unwrap();
        let nullable = if Rule::nullable_marker == inner.as_rule() {
            inner = pairs.next().unwrap();
            true
        } else {
            false
        };
        let t = match inner.as_rule() {
            Rule::simple_type => {
                let name = parse_ident(inner.into_inner().next().unwrap());
                if let Some(Structured::Typing(t)) = self.resolve(&name) {
                    t.clone()
                } else {
                    return Err(UndefinedType);
                }
            }
            Rule::list_type => {
                let inner_t = self.build_type(inner.into_inner().next().unwrap())?;
                Typing::HList(Box::new(inner_t))
            }
            // Rule::tuple_type => {},
            _ => unreachable!()
        };
        Ok(if nullable {
            Typing::Nullable(Box::new(t))
        } else {
            t
        })
    }

    fn build_default_value(&self, _pair: Pair<Rule>) -> Result<Value<'static>> {
        // TODO: _pair is an expression, parse it and evaluate it to a constant value
        Ok(Value::Null)
    }

    fn build_col_entry(&self, pair: Pair<Rule>) -> Result<(Col, bool)> {
        let mut pairs = pair.into_inner();
        let (name, is_key) = parse_col_name(pairs.next().unwrap())?;
        let typ = self.build_type(pairs.next().unwrap())?;
        let default = if let Some(p) = pairs.next() {
            // TODO: check value is suitable for the type
            self.build_default_value(p)?
        } else {
            Value::Null
        };
        Ok((Col {
            name,
            typ,
            default,
        }, is_key))
    }

    fn build_col_defs(&self, pair: Pair<Rule>) -> Result<(Vec<Col>, Vec<Col>)> {
        let mut keys = vec![];
        let mut cols = vec![];
        for pair in pair.into_inner() {
            let (col, is_key) = self.build_col_entry(pair)?;
            if is_key {
                keys.push(col)
            } else {
                cols.push(col)
            }
        }

        Ok((keys, cols))
    }
}

#[repr(u8)]
pub enum TableKind {
    Node = 1,
    Edge = 2,
    Columns = 3,
    Index = 4,
}

impl Storage {
    fn all_metadata(&self, env: &StructuredEnvItem) -> Result<Vec<Structured>> {
        todo!()
        // let it = self.db.as_ref().ok_or(DatabaseClosed)?.full_iterator(IteratorMode::Start);
        //
        // let mut ret = vec![];
        // for (k, v) in it {
        //     let mut key_parser = ByteArrayParser::new(&k);
        //     let table_name = key_parser.parse_value().unwrap().get_string().unwrap();
        //
        //     let mut data_parser = ByteArrayParser::new(&v);
        //     let table_kind = data_parser.parse_value().unwrap();
        //     let table_id = TableId { name: table_name, global: true };
        //     match table_kind {
        //         Value::UInt(i) if i == TableKind::Node as u64 => {
        //             let keys: Vec<_> = data_parser.parse_value().unwrap().get_list().unwrap()
        //                 .into_iter().map(|v| {
        //                 let mut vs = v.get_list().unwrap().into_iter();
        //                 let name = vs.next().unwrap().get_string().unwrap();
        //                 let typ = vs.next().unwrap().get_string().unwrap();
        //                 let typ = env.build_type_from_str(&typ).unwrap();
        //                 let default = vs.next().unwrap().into_owned();
        //                 Col {
        //                     name,
        //                     typ,
        //                     default,
        //                 }
        //             }).collect();
        //             let cols: Vec<_> = data_parser.parse_value().unwrap().get_list().unwrap()
        //                 .into_iter().map(|v| {
        //                 let mut vs = v.get_list().unwrap().into_iter();
        //                 let name = vs.next().unwrap().get_string().unwrap();
        //                 let typ = vs.next().unwrap().get_string().unwrap();
        //                 let typ = env.build_type_from_str(&typ).unwrap();
        //                 let default = vs.next().unwrap().into_owned();
        //                 Col {
        //                     name,
        //                     typ,
        //                     default,
        //                 }
        //             }).collect();
        //             let node = Node {
        //                 status: StorageStatus::Stored,
        //                 id: table_id,
        //                 keys,
        //                 cols,
        //                 out_e: vec![], // TODO fix these
        //                 in_e: vec![],
        //                 attached: vec![],
        //             };
        //             ret.push(Structured::Node(node));
        //         }
        //         Value::UInt(i) if i == TableKind::Edge as u64 => {
        //             let src_name = data_parser.parse_value().unwrap().get_string().unwrap();
        //             let dst_name = data_parser.parse_value().unwrap().get_string().unwrap();
        //             let src_id = TableId { name: src_name, global: true };
        //             let dst_id = TableId { name: dst_name, global: true };
        //             let keys: Vec<_> = data_parser.parse_value().unwrap().get_list().unwrap()
        //                 .into_iter().map(|v| {
        //                 let mut vs = v.get_list().unwrap().into_iter();
        //                 let name = vs.next().unwrap().get_string().unwrap();
        //                 let typ = vs.next().unwrap().get_string().unwrap();
        //                 let typ = env.build_type_from_str(&typ).unwrap();
        //                 let default = vs.next().unwrap().into_owned();
        //                 Col {
        //                     name,
        //                     typ,
        //                     default,
        //                 }
        //             }).collect();
        //             let cols: Vec<_> = data_parser.parse_value().unwrap().get_list().unwrap()
        //                 .into_iter().map(|v| {
        //                 let mut vs = v.get_list().unwrap().into_iter();
        //                 let name = vs.next().unwrap().get_string().unwrap();
        //                 let typ = vs.next().unwrap().get_string().unwrap();
        //                 let typ = env.build_type_from_str(&typ).unwrap();
        //                 let default = vs.next().unwrap().into_owned();
        //                 Col {
        //                     name,
        //                     typ,
        //                     default,
        //                 }
        //             }).collect();
        //             let edge = Edge {
        //                 status: StorageStatus::Stored,
        //                 src: src_id,
        //                 dst: dst_id,
        //                 id: table_id,
        //                 keys,
        //                 cols,
        //             };
        //             ret.push(Structured::Edge(edge));
        //         }
        //         Value::UInt(i) if i == TableKind::Columns as u64 => {
        //             todo!()
        //         }
        //         Value::UInt(i) if i == TableKind::Index as u64 => {
        //             todo!()
        //         }
        //         _ => unreachable!()
        //     }
        // }
        // Ok(ret)
    }

    fn persist_node(&mut self, node: &mut Node) -> Result<()> {
        let mut key_writer = ByteArrayBuilder::with_capacity(8);
        key_writer.build_value(&Value::RefString(&node.id.name));
        let mut val_writer = ByteArrayBuilder::with_capacity(128);
        val_writer.build_value(&Value::UInt(TableKind::Node as u64));
        val_writer.build_value(&Value::List(Box::new(node.keys.iter().map(|k| {
            Value::List(Box::new(vec![
                Value::RefString(&k.name),
                Value::OwnString(Box::new(format!("{}", k.typ))),
                k.default.clone(),
            ]))
        }).collect())));
        val_writer.build_value(&Value::List(Box::new(node.cols.iter().map(|k| {
            Value::List(Box::new(vec![
                Value::RefString(&k.name),
                Value::OwnString(Box::new(format!("{}", k.typ))),
                k.default.clone(),
            ]))
        }).collect())));

        self.put_global(&key_writer.get(), &val_writer.get())?;
        node.status = Stored;
        Ok(())
    }

    fn persist_edge(&mut self, edge: &mut Edge) -> Result<()> {
        let mut key_writer = ByteArrayBuilder::with_capacity(8);
        key_writer.build_value(&Value::RefString(&edge.id.name));

        let mut val_writer = ByteArrayBuilder::with_capacity(128);
        val_writer.build_value(&Value::UInt(TableKind::Edge as u64));
        val_writer.build_value(&Value::RefString(&edge.src.name));
        val_writer.build_value(&Value::RefString(&edge.dst.name));
        val_writer.build_value(&Value::List(Box::new(edge.keys.iter().map(|k| {
            Value::List(Box::new(vec![
                Value::RefString(&k.name),
                Value::OwnString(Box::new(format!("{}", k.typ))),
                k.default.clone(),
            ]))
        }).collect())));
        val_writer.build_value(&Value::List(Box::new(edge.cols.iter().map(|k| {
            Value::List(Box::new(vec![
                Value::RefString(&k.name),
                Value::OwnString(Box::new(format!("{}", k.typ))),
                k.default.clone(),
            ]))
        }).collect())));

        self.put_global(&key_writer.get(), &val_writer.get())?;
        edge.status = Stored;
        Ok(())
    }
}

impl Evaluator {
    pub fn restore_metadata(&mut self) -> Result<()> {
        let mds = self.storage.all_metadata(self.s_envs.root())?;
        for md in &mds {
            match md {
                v @ Structured::Node(n) => {
                    // TODO: check if they are the same if one already exists
                    self.s_envs.root_define(n.id.name.clone(), v.clone());
                }
                v @ Structured::Edge(e) => {
                    self.s_envs.root_define(e.id.name.clone(), v.clone());
                }
                Structured::Columns(_) => {}
                Structured::Index(_) => {}
                Structured::Typing(_) => unreachable!()
            }
        }
        Ok(())
    }

    fn persist_change(&mut self, tname: &str, global: bool) -> Result<()> {
        let tbl = self.s_envs.resolve_mut(tname).unwrap();
        self.storage.create_table(tname, global)?;
        if global {
            match tbl {
                Structured::Node(n) => self.storage.persist_node(n),
                Structured::Edge(e) => self.storage.persist_edge(e),
                Structured::Columns(_) => unimplemented!(),
                Structured::Index(_) => unimplemented!(),
                Structured::Typing(_) => panic!(),
            }
        } else {
            Ok(())
        }
    }


    pub fn build_table(&mut self, pairs: Pairs<Rule>) -> Result<()> {
        let mut new_tables = vec![];
        for pair in pairs {
            match pair.as_rule() {
                r @ (Rule::global_def | Rule::local_def) => {
                    let inner = pair.into_inner().next().unwrap();
                    let global = r == Rule::global_def;
                    let env_to_build = if global {
                        self.s_envs.root_mut()
                    } else {
                        self.s_envs.cur_mut()
                    };
                    new_tables.push((global, match inner.as_rule() {
                        Rule::node_def => {
                            env_to_build.build_node_def(inner, global)?
                        }
                        Rule::edge_def => {
                            env_to_build.build_edge_def(inner, global)?
                        }
                        Rule::columns_def => {
                            env_to_build.build_columns_def(inner, global)?
                        }
                        Rule::index_def => {
                            env_to_build.build_index_def(inner, global)?
                        }
                        _ => todo!()
                    }));
                }
                Rule::EOI => {}
                _ => unreachable!()
            }
        }
        for (global, tname) in &new_tables {
            self.persist_change(tname, *global).unwrap(); // TODO proper error handling
        }
        Ok(())
    }

    pub fn insert_data(&mut self, _pairs: Pairs<Rule>, _bindings: BTreeMap<&str, Value>) -> Result<()> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use pest::Parser as PestParser;
    use crate::parser::Parser;

    #[test]
    fn definitions() {
        let s = r#"
            create node "Person" {
                *id: Int,
                name: String,
                email: ?String,
                habits: ?[?String]
            }

            create edge (Person)-[Friend]->(Person) {
                relation: ?String
            }
        "#;
        let parsed = Parser::parse(Rule::file, s).unwrap();
        let mut eval = Evaluator::new("_path_for_rocksdb_storagex".to_string()).unwrap();
        eval.build_table(parsed).unwrap();
        eval.restore_metadata().unwrap();
        eval.storage.delete().unwrap();
        println!("{:#?}", eval.s_envs.resolve("Person"));
        println!("{:#?}", eval.s_envs.resolve("Friend"));
    }
}