// struct Filter;
//
// enum QueryPlan {
//     Union {
//         args: Vec<QueryPlan>
//     },
//     Intersection {
//         args: Vec<QueryPlan>
//     },
//     Difference {
//         left: Box<QueryPlan>,
//         right: Box<QueryPlan>,
//     },
//     Selection {
//         arg: Box<QueryPlan>,
//         filter: (),
//     },
//     Projection {
//         arg: Box<QueryPlan>,
//         keys: (),
//         fields: (),
//     },
//     Product {
//         args: Vec<QueryPlan>
//     },
//     Join {
//         args: Vec<QueryPlan>
//     },
//     LeftJoin {
//         left: Box<QueryPlan>,
//         right: Box<QueryPlan>
//     },
//     BaseRelation {
//         relation: ()
//     },
// }


use pest::iterators::Pair;
use crate::db::engine::Session;
use crate::db::table::TableInfo;
use crate::error::CozoError::LogicError;
use crate::parser::Rule;
use crate::error::{CozoError, Result};
use crate::parser::text_identifier::build_name_in_def;
use crate::relation::data::DataKind;

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

#[derive(Debug, Eq, PartialEq, Clone)]
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
}

impl<'a> Session<'a> {
    pub fn parse_from_pattern(&self, pair: Pair<Rule>) -> Result<Vec<FromEl>> {
        let res: Result<Vec<_>> = pair.into_inner().map(|p| {
            match p.as_rule() {
                Rule::simple_from_pattern => self.parse_simple_from_pattern(p),
                Rule::node_edge_pattern => self.parse_node_edge_pattern(p),
                _ => unreachable!()
            }
        }).collect();
        res
    }

    fn parse_simple_from_pattern(&self, pair: Pair<Rule>) -> Result<FromEl> {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str();
        if name.starts_with('_') {
            return Err(LogicError("Pattern binding cannot start with underscore".to_string()));
        }
        let table_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let table_info = self.get_table_info(&table_name)?;
        let ret = FromEl::Simple(Box::new(SimpleFromEl { binding: name.to_string(), table: table_name, info: table_info }));
        Ok(ret)
    }

    fn parse_node_edge_pattern(&self, pair: Pair<Rule>) -> Result<FromEl> {
        let res: Result<Vec<_>> = pair.into_inner().map(|p| {
            match p.as_rule() {
                Rule::node_pattern => self.parse_node_pattern(p),
                Rule::fwd_edge_pattern => self.parse_edge_pattern(p, true),
                Rule::bwd_edge_pattern => self.parse_edge_pattern(p, false),
                _ => unreachable!()
            }
        }).collect();
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
                _ => unreachable!()
            }
        });
        if !connects {
            return Err(CozoError::LogicError("Chain does not connect".to_string()));
        }
        Ok(FromEl::Chain(res))
    }

    fn parse_node_pattern(&self, pair: Pair<Rule>) -> Result<EdgeOrNodeEl> {
        let (table, binding, info) = self.parse_node_or_edge(pair)?;
        if info.kind != DataKind::Node {
            return Err(LogicError(format!("{} is not a node", table)));
        }
        Ok(EdgeOrNodeEl {
            table,
            binding,
            info,
            kind: EdgeOrNodeKind::Node,
        })
    }

    fn parse_edge_pattern(&self, pair: Pair<Rule>, is_fwd: bool) -> Result<EdgeOrNodeEl> {
        let (table, binding, info) = self.parse_node_or_edge(pair)?;
        if info.kind != DataKind::Edge {
            return Err(LogicError(format!("{} is not an edge", table)));
        }
        Ok(EdgeOrNodeEl {
            table,
            binding,
            info,
            kind: if is_fwd { EdgeOrNodeKind::FwdEdge } else { EdgeOrNodeKind::BwdEdge },
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
}

#[cfg(test)]
mod tests {
    use std::fs;
    // use super::*;
    use crate::parser::{Parser, Rule};
    use pest::Parser as PestParser;
    use crate::db::engine::Engine;

    #[test]
    fn parse_patterns() {
        let db_path = "_test_db_plan";
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


            let s = "from a:Friend, (b:Person)-[:Friend]->(c:Person), x:Person";
            let parsed = Parser::parse(Rule::from_pattern, s).unwrap().next().unwrap();
            assert_eq!(parsed.as_rule(), Rule::from_pattern);
            sess.parse_from_pattern(parsed).unwrap();

            let s = "from a:Friend, (b:Person)-[:Friend]->(c:Z), x:Person";
            let parsed = Parser::parse(Rule::from_pattern, s).unwrap().next().unwrap();
            assert_eq!(parsed.as_rule(), Rule::from_pattern);
            assert!(sess.parse_from_pattern(parsed).is_err());
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}