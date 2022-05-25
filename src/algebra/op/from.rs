use crate::algebra::op::RelationalAlgebra;
use crate::context::TempDbContext;
use crate::parser::Pairs;
use anyhow::Result;
use std::sync::Arc;
use crate::algebra::parser::AlgebraParseError;

pub(crate) const NAME_FROM: &str = "From";

pub(crate) fn build_from_clause<'a>(
    ctx: &'a TempDbContext<'a>,
    prev: Option<Arc<dyn RelationalAlgebra + 'a>>,
    mut args: Pairs,
) -> Result<Arc<dyn RelationalAlgebra + 'a>> {
    if !matches!(prev, None) {
        return Err(
            AlgebraParseError::Unchainable(NAME_FROM.to_string()).into(),
        );
    }

    todo!()
}
