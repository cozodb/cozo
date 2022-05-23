use crate::context::TempDbContext;
use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::{Expr, StaticExpr};
use crate::data::parser::{parse_scoped_dict, ExprParseError};
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{
    BindingMap, BindingMapEvalContext, TableId, TupleSet, TupleSetError, TupleSetIdx,
};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use crate::ddl::reify::{
    AssocInfo, DdlContext, DdlReifyError, EdgeInfo, IndexInfo, NodeInfo, TableInfo,
};
use crate::parser::text_identifier::{build_name_in_def, TextParseError};
use crate::parser::{CozoParser, Pair, Pairs, Rule};
use crate::runtime::options::default_write_options;
use crate::runtime::session::Definable;
use cozorocks::BridgeError;
use pest::error::Error;
use pest::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::result;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub(crate) enum AlgebraParseError {
    #[error("{0} cannot be chained")]
    Unchainable(String),

    #[error("wrong argument count for {0}")]
    WrongArgumentCount(String),

    #[error("wrong argument type for {0}({1}): {2}")]
    WrongArgumentType(String, usize, String),

    #[error(transparent)]
    ExprParse(#[from] ExprParseError),

    #[error(transparent)]
    EvalError(#[from] EvalError),

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

    #[error(transparent)]
    TextParse(#[from] TextParseError),

    #[error(transparent)]
    Reify(#[from] DdlReifyError),

    #[error(transparent)]
    TupleSet(#[from] TupleSetError),

    #[error(transparent)]
    Bridge(#[from] BridgeError),
}

impl From<pest::error::Error<Rule>> for AlgebraParseError {
    fn from(err: Error<Rule>) -> Self {
        AlgebraParseError::Parse(format!("{:?}", err))
    }
}

type Result<T> = result::Result<T, AlgebraParseError>;

pub(crate) trait InterpretContext: PartialEvalContext {
    fn resolve_definable(&self, name: &str) -> Option<Definable>;
    fn resolve_table(&self, name: &str) -> Option<TableId>;
    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo>;
    fn get_node_info(&self, table_id: TableId) -> Result<NodeInfo>;
    fn get_edge_info(&self, table_id: TableId) -> Result<EdgeInfo>;
    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>>;
    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)>;
    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>>;
}

impl<'a> InterpretContext for TempDbContext<'a> {
    fn resolve_definable(&self, _name: &str) -> Option<Definable> {
        todo!()
    }

    fn resolve_table(&self, name: &str) -> Option<TableId> {
        self.table_id_by_name(name).ok()
    }

    fn get_table_info(&self, table_id: TableId) -> Result<TableInfo> {
        self.table_by_id(table_id)
            .map_err(|_| AlgebraParseError::TableIdNotFound(table_id))
    }

    fn get_node_info(&self, table_id: TableId) -> Result<NodeInfo> {
        match self.get_table_info(table_id)? {
            TableInfo::Node(n) => Ok(n),
            _ => Err(AlgebraParseError::WrongTableKind(table_id)),
        }
    }

    fn get_edge_info(&self, table_id: TableId) -> Result<EdgeInfo> {
        match self.get_table_info(table_id)? {
            TableInfo::Edge(n) => Ok(n),
            _ => Err(AlgebraParseError::WrongTableKind(table_id)),
        }
    }

    fn get_table_assocs(&self, table_id: TableId) -> Result<Vec<AssocInfo>> {
        let res = self.assocs_by_main_id(table_id)?;
        Ok(res)
    }

    fn get_node_edges(&self, table_id: TableId) -> Result<(Vec<EdgeInfo>, Vec<EdgeInfo>)> {
        todo!()
    }

    fn get_table_indices(&self, table_id: TableId) -> Result<Vec<IndexInfo>> {
        todo!()
    }
}

pub(crate) trait RelationalAlgebra {
    fn name(&self) -> &str;
    fn binding_map(&self) -> Result<BindingMap>;
    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>>;
}

const NAME_RA_FROM_VALUES: &str = "Values";

#[derive(Clone, Debug)]
struct RaFromValues {
    binding_map: BindingMap,
    values: Vec<Vec<StaticValue>>,
}

fn assert_rule(pair: &Pair, rule: Rule, name: &str, u: usize) -> Result<()> {
    if pair.as_rule() == rule {
        Ok(())
    } else {
        Err(AlgebraParseError::WrongArgumentType(
            name.to_string(),
            u,
            format!("{:?}", pair.as_rule()),
        ))
    }
}

impl RaFromValues {
    fn build<'a>(
        ctx: &'a TempDbContext<'a>,
        prev: Option<Arc<dyn RelationalAlgebra + 'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        if !matches!(prev, None) {
            return Err(AlgebraParseError::Unchainable(
                NAME_RA_FROM_VALUES.to_string(),
            ));
        }
        let not_enough_args =
            || AlgebraParseError::NotEnoughArguments(NAME_RA_FROM_VALUES.to_string());
        let schema = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .ok_or_else(not_enough_args)?;
        assert_rule(&schema, Rule::scoped_list, NAME_RA_FROM_VALUES, 0)?;
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
        let binding_map = BTreeMap::from([(binding.to_string(), binding_map)]);
        let data = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .ok_or_else(not_enough_args)?;
        assert_rule(&data, Rule::expr, NAME_RA_FROM_VALUES, 1)?;
        let data = Expr::try_from(data)?.interpret_eval(ctx)?.to_static();
        let data = data.into_vec().map_err(AlgebraParseError::ValueError)?;
        let values = data
            .into_iter()
            .map(|v| match v.into_vec() {
                Ok(v) => {
                    if v.len() == n_fields {
                        Ok(v)
                    } else {
                        Err(AlgebraParseError::ValueError(Value::List(v)))
                    }
                }
                Err(v) => Err(AlgebraParseError::ValueError(v)),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            binding_map: binding_map,
            values,
        })
    }
}

impl RelationalAlgebra for RaFromValues {
    fn name(&self) -> &str {
        NAME_RA_FROM_VALUES
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
}

const NAME_RA_INSERT: &str = "Insert";

struct RaInsert<'a> {
    ctx: &'a TempDbContext<'a>,
    source: Arc<dyn RelationalAlgebra + 'a>,
    binding: String,
    target_info: TableInfo,
    assoc_infos: Vec<AssocInfo>,
    extract_map: StaticExpr,
}

// problem: binding map must survive optimization. now it doesn't
impl<'a> RaInsert<'a> {
    fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<Arc<dyn RelationalAlgebra + 'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_RA_INSERT.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_ra_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let table_name = args.next().ok_or_else(not_enough_args)?;
        let (table_name, assoc_names) = parse_table_with_assocs(table_name)?;
        let pair = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();
        assert_rule(&pair, Rule::scoped_dict, NAME_RA_INSERT, 2)?;
        let (binding, keys, extract_map) = parse_scoped_dict(pair)?;
        if !keys.is_empty() {
            return Err(AlgebraParseError::Parse(
                "Cannot have keyed map in Insert".to_string(),
            ));
        }
        let extract_map = extract_map.to_static();

        let target_id = ctx
            .resolve_table(&table_name)
            .ok_or_else(|| AlgebraParseError::TableNotFound(table_name.to_string()))?;
        let target_info = ctx.get_table_info(target_id)?;
        let assoc_infos = ctx
            .get_table_assocs(target_id)?
            .into_iter()
            .filter(|v| assoc_names.contains(&v.name))
            .collect::<Vec<_>>();
        Ok(Self {
            ctx,
            binding,
            source,
            target_info,
            assoc_infos,
            extract_map,
        })
    }

    fn build_binding_map_inner(&self) -> Result<BTreeMap<String, TupleSetIdx>> {
        let mut binding_map_inner = BTreeMap::new();
        match &self.target_info {
            TableInfo::Node(n) => {
                for (i, k) in n.keys.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: true,
                            t_set: 0,
                            col_idx: i,
                        },
                    );
                }
                for (i, k) in n.vals.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    );
                }
            }
            TableInfo::Edge(e) => {
                let src = self.ctx.get_node_info(e.src_id)?;
                let dst = self.ctx.get_node_info(e.dst_id)?;
                for (i, k) in src.keys.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: true,
                            t_set: 0,
                            col_idx: i + 1,
                        },
                    );
                }
                for (i, k) in dst.keys.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: true,
                            t_set: 0,
                            col_idx: i + 2 + src.keys.len(),
                        },
                    );
                }
                for (i, k) in e.keys.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: true,
                            t_set: 0,
                            col_idx: i + 2 + src.keys.len() + dst.keys.len(),
                        },
                    );
                }
                for (i, k) in e.vals.iter().enumerate() {
                    binding_map_inner.insert(
                        k.name.clone(),
                        TupleSetIdx {
                            is_key: false,
                            t_set: 0,
                            col_idx: i,
                        },
                    );
                }
            }
            _ => unreachable!(),
        }
        for (iset, info) in self.assoc_infos.iter().enumerate() {
            for (i, k) in info.vals.iter().enumerate() {
                binding_map_inner.insert(
                    k.name.clone(),
                    TupleSetIdx {
                        is_key: false,
                        t_set: iset + 1,
                        col_idx: i,
                    },
                );
            }
        }
        Ok(binding_map_inner)
    }
}

impl<'a> RelationalAlgebra for RaInsert<'a> {
    fn name(&self) -> &str {
        NAME_RA_INSERT
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let inner = self.build_binding_map_inner()?;
        Ok(BTreeMap::from([(self.binding.clone(), inner)]))
    }

    fn iter<'b>(&'b self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'b>> {
        let source_map = self.source.binding_map()?;
        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let extract_map = match self.extract_map.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d,
            v => return Err(AlgebraParseError::Parse(format!("{:?}", v))),
        };

        // let build_extractor = |col: &ColSchema| {
        //     let extractor = extract_map
        //         .get(&col.name)
        //         .cloned()
        //         .unwrap_or(Expr::Const(Value::Null))
        //         .to_static();
        //     let typing = col.typing.clone();
        //     (extractor, typing)
        // };

        let (key_builder, val_builder, inv_key_builder) = match &self.target_info {
            TableInfo::Node(n) => {
                let key_builder = n
                    .keys
                    .iter()
                    .map(|v| v.make_extractor(&extract_map))
                    .collect::<Vec<_>>();
                let val_builder = n
                    .vals
                    .iter()
                    .map(|v| v.make_extractor(&extract_map))
                    .collect::<Vec<_>>();
                (key_builder, val_builder, None)
            }
            TableInfo::Edge(e) => {
                let src = self.ctx.get_node_info(e.src_id)?;
                let dst = self.ctx.get_node_info(e.dst_id)?;
                let src_key_part = [(Expr::Const(Value::Int(e.src_id.id as i64)), Typing::Any)];
                let dst_key_part = [(Expr::Const(Value::Int(e.dst_id.id as i64)), Typing::Any)];
                let fwd_edge_part = [(Expr::Const(Value::Bool(true)), Typing::Any)];
                let bwd_edge_part = [(Expr::Const(Value::Bool(true)), Typing::Any)];
                let key_builder = src_key_part
                    .into_iter()
                    .chain(src.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .chain(fwd_edge_part.into_iter())
                    .chain(dst.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .chain(e.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .collect::<Vec<_>>();
                let inv_key_builder = dst_key_part
                    .into_iter()
                    .chain(dst.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .chain(bwd_edge_part.into_iter())
                    .chain(src.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .chain(e.keys.iter().map(|v| v.make_extractor(&extract_map)))
                    .collect::<Vec<_>>();
                let val_builder = e
                    .vals
                    .iter()
                    .map(|v| v.make_extractor(&extract_map))
                    .collect::<Vec<_>>();
                (key_builder, val_builder, Some(inv_key_builder))
            }
            _ => unreachable!(),
        };
        let assoc_val_builders = self
            .assoc_infos
            .iter()
            .map(|info| {
                (
                    info.tid,
                    info.vals
                        .iter()
                        .map(|v| v.make_extractor(&extract_map))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let target_key = self.target_info.table_id();

        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let write_opts = default_write_options();

        Ok(Box::new(self.source.iter()?.map(
            move |tset| -> Result<TupleSet> {
                let tset = tset?;
                let mut key = tset.eval_to_tuple(target_key.id, &key_builder)?;
                let val = tset.eval_to_tuple(DataKind::Data as u32, &val_builder)?;
                if target_key.in_root {
                    txn.put(&key, &val)?;
                } else {
                    temp_db.put(&write_opts, &key, &val)?;
                }
                if let Some(builder) = &inv_key_builder {
                    let inv_key = tset.eval_to_tuple(target_key.id, builder)?;
                    if target_key.in_root {
                        txn.put(&inv_key, &key)?;
                    } else {
                        temp_db.put(&write_opts, &inv_key, &key)?;
                    }
                }
                let assoc_vals = assoc_val_builders
                    .iter()
                    .map(|(tid, builder)| -> Result<OwnTuple> {
                        let ret = tset.eval_to_tuple(DataKind::Data as u32, builder)?;
                        key.overwrite_prefix(tid.id);
                        if tid.in_root {
                            txn.put(&key, &ret)?;
                        } else {
                            temp_db.put(&write_opts, &key, &ret)?;
                        }
                        Ok(ret)
                    })
                    .collect::<Result<Vec<_>>>()?;

                key.overwrite_prefix(target_key.id);

                let mut ret = TupleSet::default();
                ret.push_key(key.into());
                ret.push_val(val.into());
                for av in assoc_vals {
                    ret.push_val(av.into())
                }
                Ok(ret)
            },
        )))
    }
}

pub(crate) fn build_ra_expr<'a>(
    ctx: &'a TempDbContext,
    pair: Pair,
) -> Result<Arc<dyn RelationalAlgebra + 'a>> {
    let mut built: Option<Arc<dyn RelationalAlgebra>> = None;
    for pair in pair.into_inner() {
        let mut pairs = pair.into_inner();
        match pairs.next().unwrap().as_str() {
            NAME_RA_INSERT => built = Some(Arc::new(RaInsert::build(ctx, built, pairs)?)),
            NAME_RA_FROM_VALUES => {
                built = Some(Arc::new(RaFromValues::build(ctx, built, pairs)?));
            }
            _ => unimplemented!(),
        }
    }
    Ok(built.unwrap())
}

fn parse_table_with_assocs(pair: Pair) -> Result<(String, BTreeSet<String>)> {
    let pair = CozoParser::parse(Rule::table_with_assocs_all, pair.as_str())?
        .next()
        .unwrap();
    let mut pairs = pair.into_inner();
    let name = build_name_in_def(pairs.next().unwrap(), true)?;
    let assoc_names = pairs
        .map(|v| build_name_in_def(v, true))
        .collect::<result::Result<BTreeSet<_>, TextParseError>>()?;
    Ok((name, assoc_names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::tuple::Tuple;
    use crate::parser::{CozoParser, Rule};
    use crate::runtime::options::default_read_options;
    use crate::runtime::session::tests::create_test_db;
    use pest::Parser;

    #[test]
    fn parse_ra() -> Result<()> {
        let (db, mut sess) = create_test_db("_test_parser.db");
        {
            let ctx = sess.temp_ctx(true);
            let s = r#"
                           Values(v: [id, name], [[100, 'confidential'], [101, 'top secret']])
                          .Insert(Department, d: {...v})
                          "#;
            let ra = build_ra_expr(
                &ctx,
                CozoParser::parse(Rule::ra_expr_all, s)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            )?;
            for t in ra.iter().unwrap() {
                t.unwrap();
            }
            ctx.txn.commit().unwrap();
        }
        let mut r_opts = default_read_options();
        r_opts.set_total_order_seek(true);
        let it = sess.main.iterator(&r_opts);
        it.to_first();
        while it.is_valid() {
            let (k, v) = it.pair().unwrap();
            let k = Tuple::new(k);
            let v = Tuple::new(v);
            if k.get_prefix() != 0 {
                dbg!((k, v));
            }
            it.next();
        }
        Ok(())
    }
}
