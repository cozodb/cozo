use std::result;
use crate::parser::{Pair, Pairs};

#[derive(thiserror::Error, Debug)]
pub enum AlgebraParseError {
    #[error("{0} cannot be chained")]
    Unchainable(String)
}

type Result<T> = result::Result<T, AlgebraParseError>;

trait RelationalAlgebra {
    fn name(&self) -> &str;
}

const NAME_VALUES: &str = "Values";

struct RaFromValues;

impl RaFromValues {
    fn build(prev: Option<()>, args: Pairs) -> Result<Self> {
        if prev!= None {
            return Err(AlgebraParseError::Unchainable(NAME_VALUES.to_string()))
        }
        dbg!(args);
        todo!()
    }
}

impl RelationalAlgebra for RaFromValues {
    fn name(&self) -> &str {
        NAME_VALUES
    }
}

const NAME_INSERT: &str = "Insert";


fn build_ra_expr(pair: Pair) {
    let built: Option<()> = None;
    for pair in pair.into_inner() {
        let mut pairs = pair.into_inner();
        match pairs.next().unwrap().as_str() {
            NAME_INSERT => todo!(),
            NAME_VALUES => {
                let _res = RaFromValues::build(built, pairs);
            },
            _ => unimplemented!()
        }
    }
    todo!()
}

#[cfg(test)]
mod tests {
    use crate::parser::{CozoParser, Rule};
    use pest::Parser;
    use crate::algebra::parser::build_ra_expr;

    #[test]
    fn parse_ra() {
        let s = r#"
         Values([{x: 1}])
        .Insert(f:Friend)
        "#;
        build_ra_expr(CozoParser::parse(Rule::ra_expr_all, s).unwrap().into_iter().next().unwrap());
        let s = r#"
         From(f:Person-[:HasJob]->j:Job,
              f.id == 101, j.id > 10)
        .Select(f: {*id: f.id})
        "#;
        build_ra_expr(CozoParser::parse(Rule::ra_expr_all, s).unwrap().into_iter().next().unwrap());
    }
}