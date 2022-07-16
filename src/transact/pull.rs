use crate::data::attr::{Attribute, AttributeCardinality};
use crate::data::keyword::Keyword;

pub(crate) type PullSpecs = Vec<PullSpec>;

pub(crate) enum PullSpec {
    None,
    PullAll,
    Recurse(Keyword),
    Attr(AttrPullSpec),
}

pub(crate) struct AttrPullSpec {
    pub(crate) attr: Attribute,
    pub(crate) reverse: bool,
    pub(crate) name: Keyword,
    pub(crate) cardinality: AttributeCardinality,
    pub(crate) take: Option<usize>,
    pub(crate) nested: PullSpecs,
}

pub(crate) struct RecursePullSpec {
    pub(crate) parent: Keyword,
    pub(crate) max_depth: Option<usize>,
}
