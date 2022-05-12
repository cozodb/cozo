use crate::db::engine::Session;
use crate::db::table::TableInfo;
use crate::error::{CozoError, Result};
use crate::parser::text_identifier::{build_name_in_def, parse_string};
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::value;
use crate::relation::value::{StaticValue, Value};
use pest::iterators::Pair;
use std::collections::BTreeMap;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FromEl {
    Simple(Box<SimpleFromEl>),
    Chain(Vec<EdgeOrNodeEl>),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SimpleFromEl {
    pub table: String,
    pub binding: String,
    pub info: TableInfo,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum EdgeOrNodeKind {
    FwdEdge,
    BwdEdge,
    Node,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct EdgeOrNodeEl {
    pub table: String,
    pub binding: Option<String>,
    pub info: TableInfo,
    pub kind: EdgeOrNodeKind,
    pub left_outer_marker: bool,
    pub right_outer_marker: bool,
}

impl<'a> Session<'a> {
    pub fn parse_from_pattern(&self, pair: Pair<Rule>) -> Result<Vec<FromEl>> {
        let res: Result<Vec<_>> = pair
            .into_inner()
            .map(|p| match p.as_rule() {
                Rule::simple_from_pattern => self.parse_simple_from_pattern(p),
                Rule::node_edge_pattern => self.parse_node_edge_pattern(p),
                _ => unreachable!(),
            })
            .collect();
        res
    }

    fn parse_simple_from_pattern(&self, pair: Pair<Rule>) -> Result<FromEl> {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str();
        if name.starts_with('_') {
            return Err(CozoError::LogicError(
                "Pattern binding cannot start with underscore".to_string(),
            ));
        }
        let table_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let table_info = self.get_table_info(&table_name)?;
        let ret = FromEl::Simple(Box::new(SimpleFromEl {
            binding: name.to_string(),
            table: table_name,
            info: table_info,
        }));
        Ok(ret)
    }

    fn parse_node_edge_pattern(&self, pair: Pair<Rule>) -> Result<FromEl> {
        let res: Result<Vec<_>> = pair
            .into_inner()
            .map(|p| match p.as_rule() {
                Rule::node_pattern => self.parse_node_pattern(p),
                Rule::edge_pattern => {
                    let right_join;
                    let mut pairs = p.into_inner();
                    let mut nxt = pairs.next().unwrap();
                    if nxt.as_rule() == Rule::outer_join_marker {
                        right_join = true;
                        nxt = pairs.next().unwrap();
                    } else {
                        right_join = false;
                    }
                    let mut edge = match nxt.as_rule() {
                        Rule::fwd_edge_pattern => self.parse_edge_pattern(nxt, true)?,
                        Rule::bwd_edge_pattern => self.parse_edge_pattern(nxt, false)?,
                        _ => unreachable!(),
                    };
                    edge.left_outer_marker = pairs.next().is_some();
                    edge.right_outer_marker = right_join;
                    Ok(edge)
                }
                _ => unreachable!(),
            })
            .collect();
        let res = res?;
        let connects = res.windows(2).all(|v| {
            let left = &v[0];
            let right = &v[1];
            match (&left.kind, &right.kind) {
                (EdgeOrNodeKind::FwdEdge, EdgeOrNodeKind::Node) => {
                    left.info.dst_table_id == right.info.table_id
                }
                (EdgeOrNodeKind::BwdEdge, EdgeOrNodeKind::Node) => {
                    left.info.src_table_id == right.info.table_id
                }
                (EdgeOrNodeKind::Node, EdgeOrNodeKind::FwdEdge) => {
                    left.info.table_id == right.info.src_table_id
                }
                (EdgeOrNodeKind::Node, EdgeOrNodeKind::BwdEdge) => {
                    left.info.table_id == right.info.dst_table_id
                }
                _ => unreachable!(),
            }
        });
        if !connects {
            return Err(CozoError::LogicError("Chain does not connect".to_string()));
        }
        if res.is_empty() {
            return Err(CozoError::LogicError("Empty chain not allowed".to_string()));
        }
        Ok(FromEl::Chain(res))
    }

    fn parse_node_pattern(&self, pair: Pair<Rule>) -> Result<EdgeOrNodeEl> {
        let (table, binding, info) = self.parse_node_or_edge(pair)?;
        if info.kind != DataKind::Node {
            return Err(CozoError::LogicError(format!("{} is not a node", table)));
        }
        Ok(EdgeOrNodeEl {
            table,
            binding,
            info,
            kind: EdgeOrNodeKind::Node,
            left_outer_marker: false,
            right_outer_marker: false,
        })
    }

    fn parse_edge_pattern(&self, pair: Pair<Rule>, is_fwd: bool) -> Result<EdgeOrNodeEl> {
        let (table, binding, info) = self.parse_node_or_edge(pair)?;
        if info.kind != DataKind::Edge {
            return Err(CozoError::LogicError(format!("{} is not an edge", table)));
        }
        Ok(EdgeOrNodeEl {
            table,
            binding,
            info,
            kind: if is_fwd {
                EdgeOrNodeKind::FwdEdge
            } else {
                EdgeOrNodeKind::BwdEdge
            },
            left_outer_marker: false,
            right_outer_marker: false,
        })
    }

    fn parse_node_or_edge(&self, pair: Pair<Rule>) -> Result<(String, Option<String>, TableInfo)> {
        let name;

        let mut pairs = pair.into_inner();
        let mut cur_pair = pairs.next().unwrap();
        if cur_pair.as_rule() == Rule::ident {
            name = Some(cur_pair.as_str());
            cur_pair = pairs.next().unwrap();
        } else {
            name = None;
        }
        let table_name = build_name_in_def(cur_pair, true)?;
        let table_info = self.get_table_info(&table_name)?;
        // println!("{:?}, {}, {:?}", name, table_name, table_info);

        Ok((table_name, name.map(|v| v.to_string()), table_info))
    }

    pub fn parse_where_pattern(&self, pair: Pair<Rule>) -> Result<Value> {
        let conditions = pair
            .into_inner()
            .map(Value::from_pair)
            .collect::<Result<Vec<_>>>()?;
        Ok(Value::Apply(value::OP_AND.into(), conditions).to_static())
    }

    pub fn parse_select_pattern(&self, pair: Pair<Rule>) -> Result<Selection> {
        let mut pairs = pair.into_inner();
        let mut nxt = pairs.next().unwrap();
        let scoped = match nxt.as_rule() {
            Rule::scoped_dict => {
                let mut pp = nxt.into_inner();
                let name = pp.next().unwrap().as_str();
                nxt = pp.next().unwrap();
                Some(name.to_string())
            }
            _ => None,
        };

        let mut keys = vec![];
        let mut merged = vec![];
        let mut collected_vals = BTreeMap::new();

        for p in nxt.into_inner() {
            match p.as_rule() {
                Rule::grouped_pair => {
                    let mut pp = p.into_inner();
                    let id = parse_string(pp.next().unwrap())?;
                    let val = Value::from_pair(pp.next().unwrap())?;
                    keys.push((id, val.to_static()));
                }
                Rule::dict_pair => {
                    let mut inner = p.into_inner();
                    let name = parse_string(inner.next().unwrap())?;
                    let val_pair = inner.next().unwrap();
                    let val = Value::from_pair(val_pair)?;
                    collected_vals.insert(name.into(), val);
                }
                Rule::spreading => {
                    let el = p.into_inner().next().unwrap();
                    let to_concat = Value::from_pair(el)?;
                    if !matches!(
                        to_concat,
                        Value::Dict(_)
                            | Value::Variable(_)
                            | Value::IdxAccess(_, _)
                            | Value::FieldAccess(_, _)
                            | Value::Apply(_, _)
                    ) {
                        return Err(CozoError::LogicError("Cannot spread".to_string()));
                    }
                    if !collected_vals.is_empty() {
                        merged.push(Value::Dict(collected_vals));
                        collected_vals = BTreeMap::new();
                    }
                    merged.push(to_concat);
                }
                Rule::scoped_accessor => {
                    let name = parse_string(p.into_inner().next().unwrap())?;
                    let val =
                        Value::FieldAccess(name.clone().into(), Value::Variable("_".into()).into());
                    collected_vals.insert(name.into(), val);
                }
                _ => unreachable!(),
            }
        }

        let vals = if merged.is_empty() {
            collected_vals.into_iter().map(|(k, v)| (k.to_string(), v.to_static())).collect::<Vec<_>>()
        } else {
            // construct it with help of partial eval
            todo!()
            // if !collected_vals.is_empty() {
            //     merged.push(Value::Dict(collected_vals));
            // }
            // Value::Apply(value::METHOD_MERGE.into(), merged).to_static()
        };

        let mut ordering = vec![];
        let mut limit = None;
        let mut offset = None;

        for p in pairs {
            match p.as_rule() {
                Rule::order_pattern => {
                    for p in p.into_inner() {
                        ordering.push((
                            p.as_rule() == Rule::order_asc,
                            parse_string(p.into_inner().next().unwrap())?,
                        ))
                    }
                }
                Rule::offset_pattern => {
                    for p in p.into_inner() {
                        match p.as_rule() {
                            Rule::limit_clause => {
                                limit = Some(
                                    p.into_inner()
                                        .next()
                                        .unwrap()
                                        .as_str()
                                        .replace('_', "")
                                        .parse::<i64>()?,
                                );
                            }
                            Rule::offset_clause => {
                                offset = Some(
                                    p.into_inner()
                                        .next()
                                        .unwrap()
                                        .as_str()
                                        .replace('_', "")
                                        .parse::<i64>()?,
                                );
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(Selection {
            scoped,
            keys,
            vals,
            ordering,
            limit,
            offset,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Selection {
    pub scoped: Option<String>,
    pub keys: Vec<(String, StaticValue)>,
    pub vals: Vec<(String, StaticValue)>,
    pub ordering: Vec<(bool, String)>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::fs;
    // use super::*;
    use crate::db::engine::Engine;
    use crate::parser::{Parser, Rule};
    use pest::Parser as PestParser;

    #[test]
    fn parse_patterns() {
        let db_path = "_test_db_query";
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

                create node Z {
                    *id: Text
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

            let s = "from a:Friend, (b:Person)-[:Friend]->(c:Z), x:Person";
            let parsed = Parser::parse(Rule::from_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(parsed.as_rule(), Rule::from_pattern);
            assert!(sess.parse_from_pattern(parsed).is_err());

            let s = "from a:Friend, (b:Person)-[:Friend]->?(c:Person), x:Person";
            let parsed = Parser::parse(Rule::from_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(parsed.as_rule(), Rule::from_pattern);
            let from_pattern = sess.parse_from_pattern(parsed).unwrap();
            println!("{:#?}", from_pattern);

            let s = "where b.id > c.id || x.name.is_null(), a.id == 5, x.name == 'Joe', x.name.len() == 3";
            let parsed = Parser::parse(Rule::where_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            let where_result = sess.parse_where_pattern(parsed).unwrap();
            println!("{:#?}", where_result);

            let s = "select {*id: a.id, b: a.b, c: a.c} ordered [e, +c, -b] limit 1 offset 2";
            let parsed = Parser::parse(Rule::select_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            let select_result = sess.parse_select_pattern(parsed).unwrap();
            println!("{:#?}", select_result);
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}
