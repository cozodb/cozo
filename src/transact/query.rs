use crate::data::keyword::Keyword;
use crate::transact::pull::PullSpec;

pub(crate) struct QuerySpec {
    find: Vec<(Keyword, PullSpec)>,
    rules: (),
    input: (),
    order: (),
    limit: Option<usize>,
    offset: Option<usize>,
}
