use crate::algebra::op::RelationalAlgebra;
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::{Expr};
use crate::data::parser::{parse_keyed_dict, parse_scoped_dict};
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{
    BindingMap, BindingMapEvalContext, TupleSet, TupleSetEvalContext, TupleSetIdx,
};
use crate::data::value::Value;
use crate::ddl::reify::TableInfo;
use crate::parser::{Pairs, Rule};
use crate::runtime::options::default_write_options;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_SELECT: &str = "Select";

pub(crate) struct SelectOp<'a> {
    ctx: &'a TempDbContext<'a>,
    pub(crate) source: RaBox<'a>,
    binding: String,
    extract_map: Expr,
}

impl<'a> SelectOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_SELECT.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let pair = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();
        let (binding, keys, extract_map) = match pair.as_rule() {
            Rule::keyed_dict => {
                let (keys, vals) = parse_keyed_dict(pair)?;
                ("_".to_string(), keys, vals)
            }
            _ => {
                assert_rule(&pair, Rule::scoped_dict, NAME_SELECT, 2)?;
                parse_scoped_dict(pair)?
            }
        };

        if !keys.is_empty() {
            return Err(
                AlgebraParseError::Parse("Cannot have keyed map in Select".to_string()).into(),
            );
        }

        Ok(Self {
            ctx,
            source,
            binding,
            extract_map,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SelectOpError {
    #[error("Selection needs a dict, got {0:?}")]
    NeedsDict(Expr),
}

impl<'b> RelationalAlgebra for SelectOp<'b> {
    fn name(&self) -> &str {
        NAME_SELECT
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.to_string()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let extract_map = match self.extract_map.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d
                .keys()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.to_string(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            Expr::Const(Value::Dict(d)) => d
                .keys()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.to_string(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            ex => return Err(SelectOpError::NeedsDict(ex).into()),
        };
        Ok(BindingMap {
            inner_map: BTreeMap::from([(self.binding.clone(), extract_map)]),
            key_size: 1,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let extraction_vec = match self
            .extract_map
            .clone()
            .partial_eval(&binding_ctx)?
        {
            Expr::Dict(d) => d.values().cloned().collect::<Vec<_>>(),
            Expr::Const(Value::Dict(d)) => d
                .values()
                .map(|v| Expr::Const(v.clone()))
                .collect::<Vec<_>>(),
            ex => return Err(SelectOpError::NeedsDict(ex).into()),
        };

        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let w_opts = default_write_options();

        let iter = self.source.iter()?.map(move |tset| -> Result<TupleSet> {
            let tset = tset?;
            let eval_ctx = TupleSetEvalContext {
                tuple_set: &tset,
                txn: &txn,
                temp_db: &temp_db,
                write_options: &w_opts,
            };
            let mut tuple = OwnTuple::with_data_prefix(DataKind::Data);
            for expr in &extraction_vec {
                let value = expr.row_eval(&eval_ctx)?;
                tuple.push_value(&value);
            }
            let mut out = TupleSet::default();
            out.vals.push(tuple.into());
            Ok(out)
        });
        Ok(Box::new(iter))
    }

    fn identity(&self) -> Option<TableInfo> {
        todo!()
    }
}
