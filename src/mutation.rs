use std::collections::BTreeMap;
use pest::iterators::Pair;
use crate::ast::{build_expr, Expr, ExprVisitor};
use crate::definition::build_name_in_def;
use crate::env::Env;
use crate::error::CozoError::{UndefinedTable, ValueRequired};
use crate::eval::Evaluator;
use crate::storage::{RocksStorage};
use crate::error::Result;
use crate::parser::{Parser, Rule};
use crate::typing::Structured;
use crate::value::Value;

impl Evaluator<RocksStorage> {
    pub fn eval_mutation(&mut self, pair: Pair<Rule>) -> Result<()> {
        let mut pairs = pair.into_inner();
        let op = pairs.next().unwrap().as_rule();
        let expr = pairs.next().unwrap();
        let main_target;
        // let filters;
        match pairs.next() {
            None => {
                main_target = None;
                // filters = None;
            }
            Some(v) => {
                match v.as_rule() {
                    Rule::name_in_def => {
                        let resolved = self.env.resolve(&build_name_in_def(v, true)?)
                            .ok_or(UndefinedTable)?;
                        main_target = Some(resolved);
                    }
                    Rule::mutation_filter => {
                        main_target = None;
                        todo!()
                    }
                    _ => unreachable!()
                }
            }
        }
        let expr = build_expr(expr)?;
        let expr = self.visit_expr(&expr)?;
        let val = match expr {
            Expr::Const(v) => v,
            _ => return Err(ValueRequired)
        };
        println!("{:#?}", val);
        let coerced_values = self.coerce_table_values(&val, main_target);
        Ok(())
    }

    fn coerce_table_values(&self, values: &Value, table: Option<&Structured>) -> BTreeMap<&Structured, Vec<Value>> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;
    use crate::ast::{Expr, ExprVisitor, parse_expr_from_str};
    use crate::eval::{BareEvaluator, EvaluatorWithStorage};
    use pest::Parser as PestParser;
    use cozo_rocks::DBImpl;
    use crate::env::Env;
    use crate::typing::Structured;

    #[test]
    fn data() -> Result<()> {
        let ddl = fs::read_to_string("test_data/hr.cozo")?;
        let parsed = Parser::parse(Rule::file, &ddl).unwrap();
        let mut eval = EvaluatorWithStorage::new("_path_hr".to_string()).unwrap();
        eval.build_table(parsed).unwrap();
        eval.restore_metadata().unwrap();
        println!("{:?}", eval.storage.db.all_cf_names());

        let insertion = "insert $data;";
        let mut insert_stmt = Parser::parse(Rule::mutation, insertion).unwrap();

        let data = fs::read_to_string("test_data/hr.json")?;
        let parsed = parse_expr_from_str(&data)?;
        let mut ev = BareEvaluator::default();
        let evaluated = ev.visit_expr(&parsed)?;
        let bound_value = match evaluated {
            Expr::Const(v) => v,
            _ => unreachable!()
        };
        eval.env.push();
        eval.env.define("$data".to_string(), Structured::Value(bound_value.to_owned()));
        eval.eval_mutation(insert_stmt.next().unwrap()).unwrap();
        // println!("{:#?}", evaluated);
        eval.env.pop();
        Ok(())
    }
}