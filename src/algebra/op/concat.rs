use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::{Expr};
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::data::value::Value;
use crate::ddl::reify::TableInfo;
use crate::parser::Pairs;
use anyhow::Result;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_CONCAT: &str = "Concat";
pub(crate) const NAME_UNION: &str = "Union";
pub(crate) const NAME_INTERSECT: &str = "Intersect";
pub(crate) const NAME_DIFF: &str = "Diff";
pub(crate) const NAME_SYM_DIFF: &str = "SymDiff";

pub(crate) struct ConcatOp<'a> {
    pub(crate) sources: Vec<RaBox<'a>>,
}

impl<'a> ConcatOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_CONCAT.to_string());
        let mut sources = vec![];
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        sources.push(source);
        for arg in args {
            let source = build_relational_expr(ctx, arg)?;
            sources.push(source)
        }
        Ok(Self { sources })
    }
    fn value_entries(&self, binding_map: &BindingMap) -> Result<Vec<Expr>> {
        let dft = BTreeMap::new();
        let own_binding_map = self.binding_map()?;
        let mut ret = vec![];
        for (k, vs) in own_binding_map.inner_map {
            let sub_map = binding_map.inner_map.get(&k).unwrap_or(&dft);
            for (sk, _) in vs {
                match sub_map.get(&sk) {
                    None => ret.push(Expr::Const(Value::Null)),
                    Some(idx) => ret.push(Expr::TupleSetIdx(*idx)),
                }
            }
        }

        Ok(ret)
    }
}

impl<'b> RelationalAlgebra for ConcatOp<'b> {
    fn name(&self) -> &str {
        NAME_CONCAT
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        let mut ret = BTreeSet::new();
        for el in &self.sources {
            ret.extend(el.bindings()?)
        }
        Ok(ret)
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let mut ret: BTreeMap<String, BTreeMap<String, TupleSetIdx>> = BTreeMap::new();
        for el in self.sources.iter() {
            let el = el.binding_map()?.inner_map;
            for (k, vs) in el {
                let tgt = ret.entry(k).or_default();
                for (sk, _) in vs {
                    if let Entry::Vacant(e) = tgt.entry(sk) {
                        e.insert(TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: 0,
                        });
                    }
                }
            }
        }

        let mut idx: usize = 0;
        for vs in ret.values_mut() {
            for v in vs.values_mut() {
                v.col_idx = idx;
                idx += 1;
            }
        }

        Ok(BindingMap {
            inner_map: ret,
            key_size: 0,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let mut sources: Vec<Box<dyn Iterator<Item = Result<TupleSet>>>> = vec![];
        for source in &self.sources {
            let source_map = source.binding_map()?;
            let val_extractors = self.value_entries(&source_map)?;

            let iter = source.iter()?.map(move |tset| -> Result<TupleSet> {
                let tset = tset?;
                let mut tuple = OwnTuple::with_data_prefix(DataKind::Data);
                for extractor in &val_extractors {
                    let value = extractor.row_eval(&tset)?;
                    tuple.push_value(&value);
                }
                let ret = TupleSet {
                    keys: vec![],
                    vals: vec![tuple.into()],
                };
                Ok(ret)
            });
            sources.push(Box::new(iter));
        }

        Ok(Box::new(ConcatIterator {
            sources: sources.into_iter(),
            current_source: Box::new([].into_iter()),
        }))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}

pub(crate) struct ConcatIterator<
    'a,
    T: Iterator<Item = Box<dyn Iterator<Item = Result<TupleSet>> + 'a>>,
> {
    sources: T,
    current_source: Box<dyn Iterator<Item = Result<TupleSet>> + 'a>,
}

impl<'a, T: Iterator<Item = Box<dyn Iterator<Item = Result<TupleSet>> + 'a>>> Iterator
    for ConcatIterator<'a, T>
{
    type Item = Result<TupleSet>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_source.next() {
                Some(res) => return Some(res),
                None => match self.sources.next() {
                    None => return None,
                    Some(source) => {
                        self.current_source = source;
                        continue;
                    }
                },
            }
        }
    }
}
