use crate::algebra::parser::RaBox;
use crate::context::TempDbContext;

pub(crate) const NAME_UPDATE: &str = "Update";


pub(crate) struct UpdateOp<'a> {
    source: RaBox<'a>,
    ctx: &'a TempDbContext<'a>,
    binding: String,
}