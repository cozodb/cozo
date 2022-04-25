use pest::iterators::Pair;
use crate::db::engine::Session;
use crate::parser::Rule;
use crate::relation::tuple::Tuple;

impl<'a> Session<'a> {
    pub fn parse_table_def(&self, pair: Pair<Rule>) -> Tuple<Vec<u8>> {
        todo!()
    }
}