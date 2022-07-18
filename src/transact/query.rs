use crate::data::attr::Attribute;
use crate::data::keyword::Keyword;
use crate::data::value::DataValue;
use crate::transact::pull::PullSpec;
use crate::Validity;

pub(crate) struct QuerySpec {
    find: Vec<(Keyword, PullSpec)>,
    rules: (),
    input: (),
    order: (),
    limit: Option<usize>,
    offset: Option<usize>,
}

pub(crate) enum Relation {
    Attr(Attribute, Validity),
    FullAttr(Attribute),
    Derived(DerivedRelation),
}

pub(crate) struct DerivedRelation {
    name: Keyword,
    arity: usize,
}

impl Relation {
    pub(crate) fn arity(&self) -> usize {
        match self {
            Relation::Attr(_, _) => 3,
            Relation::FullAttr(_) => 5,
            Relation::Derived(r) => r.arity,
        }
    }
}

pub(crate) enum RelationSlot {
    Var(Keyword),
    Const(DataValue),
}

pub(crate) struct BoundRelation {
    relation: Relation,
    slots: Vec<RelationSlot>,
}
