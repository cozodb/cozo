use crate::algebra::op::{
    build_from_clause, AssocOp, CartesianJoin, Insertion, LimitOp, NestedLoopLeft,
    RelationFromValues, RelationalAlgebra, SelectOp, TableScan, TaggedInsertion, WhereFilter,
    NAME_FROM, NAME_INSERTION, NAME_RELATION_FROM_VALUES, NAME_SELECT, NAME_SKIP,
    NAME_TAGGED_INSERTION, NAME_TAGGED_UPSERT, NAME_TAKE, NAME_UPSERT, NAME_WHERE,
};
use crate::context::TempDbContext;
use crate::data::tuple::OwnTuple;
use crate::data::tuple_set::{BindingMap, TableId, TupleSet};
use crate::data::value::StaticValue;
use crate::ddl::reify::TableInfo;
use crate::parser::{Pair, Rule};
use anyhow::Result;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};

#[derive(thiserror::Error, Debug)]
pub(crate) enum AlgebraParseError {
    #[error("{0} cannot be chained")]
    Unchainable(String),

    #[error("wrong argument type for {0}({1}): {2}")]
    WrongArgumentType(String, usize, String),

    #[error("Table not found {0}")]
    TableNotFound(String),

    #[error("Wrong table kind {0:?}")]
    WrongTableKind(TableId),

    #[error("Table id not found {0:?}")]
    TableIdNotFound(TableId),

    #[error("Not enough arguments for {0}")]
    NotEnoughArguments(String),

    #[error("Value error {0:?}")]
    ValueError(StaticValue),

    #[error("Parse error {0}")]
    Parse(String),

    #[error("Data key conflict {0:?}")]
    KeyConflict(OwnTuple),

    #[error("No association between {0} and {1}")]
    NoAssociation(String, String),

    #[error("Duplicate binding {0}")]
    DuplicateBinding(String),
}

pub(crate) fn assert_rule(pair: &Pair, rule: Rule, name: &str, u: usize) -> Result<()> {
    if pair.as_rule() == rule {
        Ok(())
    } else {
        Err(AlgebraParseError::WrongArgumentType(
            name.to_string(),
            u,
            format!("{:?}", pair.as_rule()),
        )
        .into())
    }
}

// this looks stupid but is the easiest way to get downcasting
pub(crate) enum RaBox<'a> {
    Insertion(Box<Insertion<'a>>),
    TaggedInsertion(Box<TaggedInsertion<'a>>),
    FromValues(Box<RelationFromValues>),
    TableScan(Box<TableScan<'a>>),
    WhereFilter(Box<WhereFilter<'a>>),
    SelectOp(Box<SelectOp<'a>>),
    AssocOp(Box<AssocOp<'a>>),
    LimitOp(Box<LimitOp<'a>>),
    Cartesian(Box<CartesianJoin<'a>>),
    NestedLoopLeft(Box<NestedLoopLeft<'a>>),
}

impl<'a> RaBox<'a> {
    pub(crate) fn sources(&self) -> Vec<&RaBox> {
        match self {
            RaBox::Insertion(inner) => vec![&inner.source],
            RaBox::TaggedInsertion(_inner) => vec![],
            RaBox::FromValues(_inner) => vec![],
            RaBox::TableScan(_inner) => vec![],
            RaBox::WhereFilter(inner) => vec![&inner.source],
            RaBox::SelectOp(inner) => vec![&inner.source],
            RaBox::AssocOp(inner) => vec![&inner.source],
            RaBox::LimitOp(inner) => vec![&inner.source],
            RaBox::Cartesian(inner) => vec![&inner.left, &inner.right],
            RaBox::NestedLoopLeft(inner) => vec![&inner.left],
        }
    }
}

impl<'a> Debug for RaBox<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.name())?;
        for sub in self.sources() {
            write!(f, " {:?}", sub)?;
        }
        write!(f, ")")
    }
}

impl<'b> RelationalAlgebra for RaBox<'b> {
    fn name(&self) -> &str {
        match self {
            RaBox::Insertion(inner) => inner.name(),
            RaBox::TaggedInsertion(inner) => inner.name(),
            RaBox::FromValues(inner) => inner.name(),
            RaBox::TableScan(inner) => inner.name(),
            RaBox::WhereFilter(inner) => inner.name(),
            RaBox::SelectOp(inner) => inner.name(),
            RaBox::AssocOp(inner) => inner.name(),
            RaBox::LimitOp(inner) => inner.name(),
            RaBox::Cartesian(inner) => inner.name(),
            RaBox::NestedLoopLeft(inner) => inner.name(),
        }
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        match self {
            RaBox::Insertion(inner) => inner.bindings(),
            RaBox::TaggedInsertion(inner) => inner.bindings(),
            RaBox::FromValues(inner) => inner.bindings(),
            RaBox::TableScan(inner) => inner.bindings(),
            RaBox::WhereFilter(inner) => inner.bindings(),
            RaBox::SelectOp(inner) => inner.bindings(),
            RaBox::AssocOp(inner) => inner.bindings(),
            RaBox::LimitOp(inner) => inner.bindings(),
            RaBox::Cartesian(inner) => inner.bindings(),
            RaBox::NestedLoopLeft(inner) => inner.bindings(),
        }
    }

    fn binding_map(&self) -> Result<BindingMap> {
        match self {
            RaBox::Insertion(inner) => inner.binding_map(),
            RaBox::TaggedInsertion(inner) => inner.binding_map(),
            RaBox::FromValues(inner) => inner.binding_map(),
            RaBox::TableScan(inner) => inner.binding_map(),
            RaBox::WhereFilter(inner) => inner.binding_map(),
            RaBox::SelectOp(inner) => inner.binding_map(),
            RaBox::AssocOp(inner) => inner.binding_map(),
            RaBox::LimitOp(inner) => inner.binding_map(),
            RaBox::Cartesian(inner) => inner.binding_map(),
            RaBox::NestedLoopLeft(inner) => inner.binding_map(),
        }
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        match self {
            RaBox::Insertion(inner) => inner.iter(),
            RaBox::TaggedInsertion(inner) => inner.iter(),
            RaBox::FromValues(inner) => inner.iter(),
            RaBox::TableScan(inner) => inner.iter(),
            RaBox::WhereFilter(inner) => inner.iter(),
            RaBox::SelectOp(inner) => inner.iter(),
            RaBox::AssocOp(inner) => inner.iter(),
            RaBox::LimitOp(inner) => inner.iter(),
            RaBox::Cartesian(inner) => inner.iter(),
            RaBox::NestedLoopLeft(inner) => inner.iter(),
        }
    }

    fn identity(&self) -> Option<TableInfo> {
        match self {
            RaBox::Insertion(inner) => inner.identity(),
            RaBox::TaggedInsertion(inner) => inner.identity(),
            RaBox::FromValues(inner) => inner.identity(),
            RaBox::TableScan(inner) => inner.identity(),
            RaBox::WhereFilter(inner) => inner.identity(),
            RaBox::SelectOp(inner) => inner.identity(),
            RaBox::AssocOp(inner) => inner.identity(),
            RaBox::LimitOp(inner) => inner.identity(),
            RaBox::Cartesian(inner) => inner.identity(),
            RaBox::NestedLoopLeft(inner) => inner.identity(),
        }
    }
}

pub(crate) fn build_relational_expr<'a>(ctx: &'a TempDbContext, pair: Pair) -> Result<RaBox<'a>> {
    let mut built: Option<RaBox> = None;
    for pair in pair.into_inner() {
        let mut pairs = pair.into_inner();
        match pairs.next().unwrap().as_str() {
            NAME_INSERTION => {
                built = Some(RaBox::Insertion(Box::new(Insertion::build(
                    ctx, built, pairs, false,
                )?)))
            }
            NAME_UPSERT => {
                built = Some(RaBox::Insertion(Box::new(Insertion::build(
                    ctx, built, pairs, true,
                )?)))
            }
            NAME_TAGGED_INSERTION => {
                built = Some(RaBox::TaggedInsertion(Box::new(TaggedInsertion::build(
                    ctx, built, pairs, false,
                )?)))
            }
            NAME_TAGGED_UPSERT => {
                built = Some(RaBox::TaggedInsertion(Box::new(TaggedInsertion::build(
                    ctx, built, pairs, true,
                )?)))
            }
            NAME_RELATION_FROM_VALUES => {
                built = Some(RaBox::FromValues(Box::new(RelationFromValues::build(
                    ctx, built, pairs,
                )?)));
            }
            NAME_FROM => {
                built = Some(build_from_clause(ctx, built, pairs)?);
            }
            NAME_WHERE => {
                built = Some(RaBox::WhereFilter(Box::new(WhereFilter::build(
                    ctx, built, pairs,
                )?)))
            }
            NAME_SELECT => {
                built = Some(RaBox::SelectOp(Box::new(SelectOp::build(
                    ctx, built, pairs,
                )?)))
            }
            NAME_TAKE => {
                built = Some(RaBox::LimitOp(Box::new(LimitOp::build(
                    ctx, built, pairs, NAME_TAKE,
                )?)))
            }
            NAME_SKIP => {
                built = Some(RaBox::LimitOp(Box::new(LimitOp::build(
                    ctx, built, pairs, NAME_SKIP,
                )?)))
            }
            _ => unimplemented!(),
        }
    }
    Ok(built.unwrap())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::data::tuple::Tuple;
    use crate::parser::{CozoParser, Rule};
    use crate::runtime::options::default_read_options;
    use crate::runtime::session::tests::create_test_db;
    use anyhow::Result;
    use pest::Parser;
    use std::collections::BTreeMap;
    use std::time::Instant;

    const HR_DATA: &str = include_str!("../../test_data/hr.json");

    #[test]
    fn parse_ra() -> Result<()> {
        let (db, mut sess) = create_test_db("_test_parser.db");
        let start = Instant::now();
        {
            let ctx = sess.temp_ctx(true);
            let s = r#"
                           Values(v: [id, name], [[100, 'confidential'], [101, 'top secret']])
                          .Upsert(Department, d: {...v})
                          "#;
            let ra = build_relational_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            dbg!(&ra);
            dbg!(ra.get_values()?);
            ctx.txn.commit().unwrap();
        }
        {
            let ctx = sess.temp_ctx(true);
            let s = format!("UpsertTagged({})", HR_DATA);
            let ra = build_relational_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, &s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            // for t in ra.iter().unwrap() {
            //     dbg!(t.unwrap());
            // }
            dbg!(&ra);
            dbg!(ra.get_values()?);

            ctx.txn.commit().unwrap();
        }
        let duration_insert = start.elapsed();
        let start = Instant::now();
        {
            let ctx = sess.temp_ctx(true);
            let s = r#"
             From(e:Employee, hj:HasJob, j:Job)
            .Where(e.id >= 122, e.id < 130, e.id == hj._src_id, hj._dst_id == j.id)
            .Select({...e, title: j.title, salary: hj.salary})
            .Skip(1)
            .Take(1)
            "#;
            let ra = build_relational_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            dbg!(&ra);
            dbg!(ra.get_values()?);
        }
        let duration_scan = start.elapsed();
        let start = Instant::now();
        {
            let ctx = sess.temp_ctx(true);
            let s = r#"
             From(e:Employee-[hj:HasJob]->?j:Job)
            .Where(e.id >= 122, e.id < 130)
            .Select({...e, title: j.title, salary: hj.salary})
            "#;
            let ra = build_relational_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            dbg!(&ra);
            dbg!(ra.get_values()?);
        }
        let duration_join = start.elapsed();
        let start = Instant::now();
        {
            let ctx = sess.temp_ctx(true);
            let s = r#"
             From(j:Job<-[hj:HasJob]-?e:Employee)
            .Where(e.id >= 122, e.id < 130)
            .Select({...e, title: j.title, salary: hj.salary})
            "#;
            let ra = build_relational_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            dbg!(&ra);
            dbg!(ra.get_values()?);
        }
        let duration_join_back = start.elapsed();
        let start = Instant::now();
        let mut r_opts = default_read_options();
        r_opts.set_total_order_seek(true);
        r_opts.set_prefix_same_as_start(false);
        let it = sess.main.iterator(&r_opts);
        it.to_first();
        let mut n: BTreeMap<u32, usize> = BTreeMap::new();
        while it.is_valid() {
            let (k, v) = it.pair().unwrap();
            let k = Tuple::new(k);
            let v = Tuple::new(v);
            if v.get_prefix() == 0 {
                *n.entry(k.get_prefix()).or_default() += 1;
            }
            it.next();
        }
        let duration_list = start.elapsed();
        dbg!(
            duration_insert,
            duration_scan,
            duration_join,
            duration_join_back,
            duration_list,
            n
        );
        Ok(())
    }
}
