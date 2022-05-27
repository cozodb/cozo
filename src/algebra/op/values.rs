use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{assert_rule, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::data::value::{StaticValue, Value};
use crate::ddl::reify::TableInfo;
use crate::parser::{Pairs, Rule};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_RELATION_FROM_VALUES: &str = "Values";

#[derive(Clone, Debug)]
pub(crate) struct RelationFromValues {
    binding_map: BindingMap,
    values: Vec<Vec<StaticValue>>,
}

impl RelationFromValues {
    pub(crate) fn build<'a>(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        if !matches!(prev, None) {
            return Err(
                AlgebraParseError::Unchainable(NAME_RELATION_FROM_VALUES.to_string()).into(),
            );
        }
        let not_enough_args =
            || AlgebraParseError::NotEnoughArguments(NAME_RELATION_FROM_VALUES.to_string());
        let schema = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .ok_or_else(not_enough_args)?;
        assert_rule(&schema, Rule::scoped_list, NAME_RELATION_FROM_VALUES, 0)?;
        let mut schema_pairs = schema.into_inner();
        let binding = schema_pairs.next().ok_or_else(not_enough_args)?.as_str();
        let binding_map = schema_pairs
            .enumerate()
            .map(|(i, v)| {
                (
                    v.as_str().to_string(),
                    TupleSetIdx {
                        is_key: false,
                        t_set: 0,
                        col_idx: i,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let n_fields = binding_map.len();
        let binding_map = BindingMap {
            inner_map: BTreeMap::from([(binding.to_string(), binding_map)]),
            key_size: 1,
            val_size: 1,
        };
        let data = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .ok_or_else(not_enough_args)?;
        assert_rule(&data, Rule::expr, NAME_RELATION_FROM_VALUES, 1)?;
        let data = Expr::try_from(data)?.interpret_eval(ctx)?.into_static();
        let data = data.into_vec().map_err(AlgebraParseError::ValueError)?;
        let values = data
            .into_iter()
            .map(|v| -> Result<Vec<Value>> {
                match v.into_vec() {
                    Ok(v) => {
                        if v.len() == n_fields {
                            Ok(v)
                        } else {
                            Err(AlgebraParseError::ValueError(Value::List(v)).into())
                        }
                    }
                    Err(v) => Err(AlgebraParseError::ValueError(v).into()),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            binding_map,
            values,
        })
    }
}

impl RelationalAlgebra for RelationFromValues {
    fn name(&self) -> &str {
        NAME_RELATION_FROM_VALUES
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .binding_map
            .inner_map
            .iter()
            .map(|(k, v)| k.to_string())
            .collect())
    }

    fn binding_map(&self) -> Result<BindingMap> {
        Ok(self.binding_map.clone())
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let it = self.values.iter().map(|vs| {
            let mut tuple = OwnTuple::with_data_prefix(DataKind::Data);
            for v in vs {
                tuple.push_value(v);
            }
            let mut tset = TupleSet::default();
            tset.push_val(tuple.into());
            Ok(tset)
        });
        Ok(Box::new(it))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
