use pest::iterators::{Pair, Pairs};
use crate::ast::parse_string;
use crate::env::{Env, StructuredEnvItem};
use crate::error::Result;
use crate::error::CozoError::*;
use crate::eval::Evaluator;
use crate::parser::{Rule};
use crate::storage::Storage;
use crate::typing::{Col, Columns, Edge, Index, Node, Structured, TableId, Typing};
use crate::typing::Persistence::{Global, Local};
use crate::typing::StorageStatus::{Planned, Stored};
use crate::value::{ByteArrayBuilder, Value};

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
    pub fn build_edge_def(&mut self, pair: Pair<Rule>, table_id: TableId) -> Result<String> {
        let mut inner = pair.into_inner();
        let src_name = build_name_in_def(inner.next().unwrap(), true)?;
        let src = self.resolve(&src_name).ok_or(UndefinedType)?;
        let src_id = if let Structured::Node(n) = src {
            n.id
        } else {
            return Err(WrongType);
        };
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let dst_name = build_name_in_def(inner.next().unwrap(), true)?;
        let dst = self.resolve(&dst_name).ok_or(UndefinedType)?;
        let dst_id = if let Structured::Node(n) = dst {
            n.id
        } else {
            return Err(WrongType);
        };
        if table_id.0 == Global && (src_id.0 == Local || dst_id.0 == Local) {
            return Err(IncompatibleEdge);
        }
        let (keys, cols) = if let Some(p) = inner.next() {
            self.build_col_defs(p)?
        } else {
            (vec![], vec![])
        };
        let edge = Edge {
            status: Planned,
            src: src_id,
            dst: dst_id,
            id: table_id,
            keys,
            cols,
        };
        if self.define_new(name.to_string(), Structured::Edge(edge)) {
            if let Some(Structured::Node(src)) = self.resolve_mut(&src_name) {
                src.out_e.push(table_id);
            } else {
                unreachable!()
            }

            if let Some(Structured::Node(dst)) = self.resolve_mut(&dst_name) {
                dst.in_e.push(table_id);
            } else {
                unreachable!()
            }
            Ok(name.to_string())
        } else {
            Err(NameConflict)
        }
    }
    pub fn build_node_def(&mut self, pair: Pair<Rule>, table_id: TableId) -> Result<String> {
        let mut inner = pair.into_inner();
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let (keys, cols) = self.build_col_defs(inner.next().unwrap())?;
        let node = Node {
            status: Planned,
            id: table_id,
            keys,
            cols,
            out_e: vec![],
            in_e: vec![],
            attached: vec![],
        };
        if self.define_new(name.to_string(), Structured::Node(node)) {
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
    fn build_columns_def(&mut self, pair: Pair<Rule>, table_id: TableId) -> Result<String> {
        let mut inner = pair.into_inner();
        let name = build_name_in_def(inner.next().unwrap(), true)?;
        let node_name = build_name_in_def(inner.next().unwrap(), true)?;
        let node = self.resolve(&node_name).ok_or(UndefinedType)?;
        let node_id = if let Structured::Node(n) = node {
            n.id
        } else if let Structured::Edge(n) = node {
            n.id
        } else {
            return Err(WrongType);
        };
        let (keys, cols) = self.build_col_defs(inner.next().unwrap())?;
        if !keys.is_empty() {
            return Err(UnexpectedIndexColumns);
        }
        if table_id.0 == Global && node_id.0 == Local {
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

    fn build_index_def(&mut self, pair: Pair<Rule>, table_id: TableId) -> Result<String> {
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
            n.id
        } else {
            return Err(WrongType);
        };
        if table_id.0 == Global && node_id.0 == Local {
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

impl Storage {
    fn persist_node(&mut self, node: &mut Node) -> Result<()> {
        let mut key_writer = ByteArrayBuilder::with_capacity(8);
        key_writer.build_varint(0);
        key_writer.build_value(&Value::UInt(node.id.1 as u64));
        // println!("{:#?}", node);
        // println!("{:?}", key_writer.get());
        node.status = Stored;
        Ok(())
    }

    fn persist_edge(&mut self, edge: &mut Edge) -> Result<()> {
        edge.status = Stored;
        Ok(())
    }
}

impl Evaluator {
    pub fn restore_metadata(&mut self) {}

    fn persist_change(&mut self, tname: &str) -> Result<()> {
        let tbl = self.s_envs.resolve_mut(tname).unwrap();
        match tbl {
            Structured::Node(n) => self.storage.persist_node(n),
            Structured::Edge(e) => self.storage.persist_edge(e),
            Structured::Columns(_) => unimplemented!(),
            Structured::Index(_) => unimplemented!(),
            Structured::Typing(_) => panic!(),
        }
    }


    pub fn build_table(&mut self, pairs: Pairs<Rule>) -> Result<()> {
        let mut new_tables = vec![];
        for pair in pairs {
            match pair.as_rule() {
                r @ (Rule::global_def | Rule::local_def) => {
                    let inner = pair.into_inner().next().unwrap();
                    let is_local = r == Rule::local_def;
                    let next_id = self.s_envs.get_next_table_id(is_local);
                    let env_to_build = if is_local {
                        self.s_envs.root_mut()
                    } else {
                        self.s_envs.cur_mut()
                    };

                    new_tables.push(match inner.as_rule() {
                        Rule::node_def => {
                            env_to_build.build_node_def(inner, next_id)?
                        }
                        Rule::edge_def => {
                            env_to_build.build_edge_def(inner, next_id)?
                        }
                        Rule::columns_def => {
                            env_to_build.build_columns_def(inner, next_id)?
                        }
                        Rule::index_def => {
                            env_to_build.build_index_def(inner, next_id)?
                        }
                        _ => todo!()
                    });
                }
                Rule::EOI => {}
                _ => unreachable!()
            }
        }
        for tname in &new_tables {
            self.persist_change(tname).unwrap(); // TODO proper error handling
        }
        Ok(())
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
            local node "Person" {
                *id: Int,
                name: String,
                email: ?String,
                habits: ?[?String]
            }

            local edge (Person)-[Friend]->(Person) {
                relation: ?String
            }
        "#;
        let parsed = Parser::parse(Rule::file, s).unwrap();
        let mut eval = Evaluator::new("_path_for_rocksdb_storagex".to_string()).unwrap();
        eval.build_table(parsed).unwrap();
        // println!("{:#?}", eval.s_envs.resolve("Person"));
        // println!("{:#?}", eval.s_envs.resolve("Friend"));
    }
}