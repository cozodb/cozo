pub(crate) mod op;
pub(crate) mod parser;

use crate::data::expr::StaticExpr;
use crate::data::tuple_set::TupleSet;

pub(crate) trait Algebra {
    fn get_iterator(&self) -> TupleSource;
    fn get_filter(&self) -> Filter;
}

type AlgebraItem = Box<dyn Algebra>;
type AlgebraPair = (Box<dyn Algebra>, Box<dyn Algebra>);

#[derive(Clone, Debug)]
pub(crate) struct Filter {
    pub(crate) filter: Option<StaticExpr>,
    pub(crate) skip: Option<usize>,
    pub(crate) take: Option<usize>,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            filter: None,
            skip: None,
            take: None,
        }
    }
}

type TupleSource = Box<dyn Iterator<Item = TupleSet>>;

// pub(crate) struct Source(TupleSource);

pub(crate) struct Edge(Filter);

pub(crate) struct Node(Filter);

pub(crate) struct InnerJoin(Filter, AlgebraPair);

pub(crate) struct LeftJoin(Filter, AlgebraPair);

pub(crate) struct Cartesian(Filter, AlgebraPair);

pub(crate) struct LeftCartesian(Filter, AlgebraPair);

pub(crate) struct Intersection(Filter, AlgebraPair);

pub(crate) struct Difference(Filter, AlgebraPair);

pub(crate) struct Selection(Filter, AlgebraItem);

pub(crate) struct Sort(Filter, AlgebraItem);
// Group
// Window
// Materialize
// Walk
// WalkRepeat
// Values

pub(crate) struct Select {
    algebra: AlgebraItem,
}

pub(crate) struct Update {
    algebra: AlgebraItem,
}

pub(crate) struct Insert {
    algebra: AlgebraItem,
    upsert: bool,
}

pub(crate) struct Delete {
    algebra: AlgebraItem,
}
