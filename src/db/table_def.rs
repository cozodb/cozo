use pest::iterators::Pair;
use crate::db::engine::Session;
use crate::parser::Rule;
use crate::relation::tuple::Tuple;

impl<'a> Session<'a> {
    pub fn parse_table_def(&self, pair: Pair<Rule>) -> Tuple<Vec<u8>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;
    use pest::Parser as PestParser;

    #[test]
    fn node() {
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
        let mut parsed = Parser::parse(Rule::file, s).unwrap();
        let first_t = parsed.next().unwrap().into_inner().next().unwrap();
        let second_t = parsed.next().unwrap().into_inner().next().unwrap();
        println!("{:#?}", first_t);
    }
}