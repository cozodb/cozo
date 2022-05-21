use crate::data::eval::EvalError;
use crate::data::expr::Expr;
use crate::data::parser::ExprParseError;
use crate::data::value::StaticValue;
use crate::parser::{Pair, Pairs, Rule};
use std::result;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub(crate) enum AlgebraParseError {
    #[error("{0} cannot be chained")]
    Unchainable(String),

    #[error("wrong argument count for {0}")]
    WrongArgumentCount(String),

    #[error("wrong argument type for {0}: {0}")]
    WrongArgumentType(String, String),

    #[error(transparent)]
    ExprParse(#[from] ExprParseError),

    #[error(transparent)]
    EvalError(#[from] EvalError),
}

type Result<T> = result::Result<T, AlgebraParseError>;

trait RelationalAlgebra {
    fn name(&self) -> &str;
}

const NAME_RA_FROM_VALUES: &str = "Values";

#[derive(Clone, Debug)]
struct RaFromValues {
    values: StaticValue,
}

impl RaFromValues {
    fn build(prev: Option<Arc<dyn RelationalAlgebra>>, mut args: Pairs) -> Result<Self> {
        if !matches!(prev, None) {
            return Err(AlgebraParseError::Unchainable(
                NAME_RA_FROM_VALUES.to_string(),
            ));
        }
        let data = args.next().unwrap().into_inner().next().unwrap();
        if data.as_rule() != Rule::expr {
            return Err(AlgebraParseError::WrongArgumentType(
                NAME_RA_FROM_VALUES.to_string(),
                format!("{:?}", data.as_rule()),
            ));
        }
        if args.next() != None {
            return Err(AlgebraParseError::WrongArgumentCount(
                NAME_RA_FROM_VALUES.to_string(),
            ));
        }
        let data = Expr::try_from(data)?;
        let data = data.row_eval(&())?.to_static();

        Ok(Self { values: data })
    }
}

impl RelationalAlgebra for RaFromValues {
    fn name(&self) -> &str {
        NAME_RA_FROM_VALUES
    }
}

const NAME_INSERT: &str = "Insert";

fn build_ra_expr(pair: Pair) -> Result<Arc<dyn RelationalAlgebra>> {
    let mut built: Option<Arc<dyn RelationalAlgebra>> = None;
    for pair in pair.into_inner() {
        let mut pairs = pair.into_inner();
        match pairs.next().unwrap().as_str() {
            NAME_INSERT => todo!(),
            NAME_RA_FROM_VALUES => {
                built = Some(Arc::new(RaFromValues::build(built, pairs)?));
            }
            _ => unimplemented!(),
        }
    }
    Ok(built.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CozoParser, Rule};
    use pest::Parser;

    #[test]
    fn parse_ra() -> Result<()> {
        let s = r#"
         Values([{x: 1}])
        //.Insert(f:Friend)
        "#;
        build_ra_expr(
            CozoParser::parse(Rule::ra_expr_all, s)
                .unwrap()
                .into_iter()
                .next()
                .unwrap(),
        )?;

        // let s = r#"
        //  From(f:Person-[:HasJob]->j:Job,
        //       f.id == 101, j.id > 10)
        // .Select(f: {*id: f.id})
        // "#;
        // build_ra_expr(CozoParser::parse(Rule::ra_expr_all, s).unwrap().into_iter().next().unwrap());
        Ok(())
    }
}
