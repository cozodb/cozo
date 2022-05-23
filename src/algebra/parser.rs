use crate::context::TempDbContext;
use crate::data::eval::{EvalError, PartialEvalContext};
use crate::data::expr::{Expr, StaticExpr};
use crate::data::parser::{parse_scoped_dict, ExprParseError};
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{BindingMap, BindingMapEvalContext, TableId, TupleSet, TupleSetIdx};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use crate::ddl::reify::{
    AssocInfo, DdlContext, DdlReifyError, EdgeInfo, IndexInfo, NodeInfo, TableInfo,
};
use crate::parser::text_identifier::{build_name_in_def, TextParseError};
use crate::parser::{CozoParser, Pair, Pairs, Rule};
use crate::runtime::session::Definable;
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
    fn binding_map(&self) -> BindingMap;
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = TupleSet> + 'a>;
}

const NAME_RA_FROM_VALUES: &str = "Values";

#[derive(Clone, Debug)]
struct RaFromValues {
    binding: BindingMap,
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
        ctx: &'a impl InterpretContext,
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
        dbg!(&binding_map);
        dbg!(&values);
        Ok(Self {
            binding: binding_map,
            values,
        })
    }
}

impl RelationalAlgebra for RaFromValues {
    fn name(&self) -> &str {
        NAME_RA_FROM_VALUES
    }

    fn binding_map(&self) -> BindingMap {
        self.binding.clone()
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = TupleSet> + 'a> {
        let it = self.values.iter().map(|vs| {
            let mut tuple = OwnTuple::with_data_prefix(DataKind::Data);
            for v in vs {
                tuple.push_value(v);
            }
            let mut tset = TupleSet::default();
            tset.push_val(tuple.into());
            tset
        });
        Box::new(it)
    }
}

const NAME_RA_INSERT: &str = "Insert";

struct RaInsert<'a, C: InterpretContext + 'a> {
    ctx: &'a C,
    source: Arc<dyn RelationalAlgebra + 'a>,
    target_info: TableInfo,
    assoc_infos: Vec<AssocInfo>,
    extract_map: StaticExpr,
    // key_builder: Vec<(StaticExpr, Typing)>,
    // inv_key_builder: Option<Vec<(StaticExpr, Typing)>>,
    // val_builder: Vec<(StaticExpr, Typing)>,
    // assoc_val_builders: BTreeMap<TableId, Vec<(StaticExpr, Typing)>>,
    binding_map: BindingMap,
}

// problem: binding map must survive optimization. now it doesn't
impl<'a, C: InterpretContext + 'a> RaInsert<'a, C> {
    fn build(
        ctx: &'a C,
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
        // let source_map = source.binding_map();
        // let binding_ctx = BindingMapEvalContext {
        //     map: source_map,
        //     parent: ctx,
        // };
        // let extract_map = match vals.partial_eval(&binding_ctx)? {
        //     Expr::Dict(d) => d,
        //     v => return Err(AlgebraParseError::Parse(format!("{:?}", v))),
        // };
        //
        // let keys = keys
        //     .into_iter()
        //     .map(|(k, v)| -> Result<(String, Expr)> {
        //         let v = v.partial_eval(&binding_ctx)?;
        //         Ok((k, v))
        //     })
        //     .collect::<Result<BTreeMap<_, _>>>()?;

        // let (key_builder, val_builder, inv_key_builder) = match target_info {
        //     TableInfo::Node(n) => {
        //         let key_builder = n
        //             .keys
        //             .iter()
        //             .map(|col| {
        //                 let extractor = extract_map
        //                     .get(&col.name)
        //                     .cloned()
        //                     .unwrap_or(Expr::Const(Value::Null))
        //                     .to_static();
        //                 let typing = col.typing.clone();
        //                 (extractor, typing)
        //             })
        //             .collect::<Vec<_>>();
        //         let val_builder = n
        //             .vals
        //             .iter()
        //             .map(|col| {
        //                 let extractor = extract_map
        //                     .get(&col.name)
        //                     .cloned()
        //                     .unwrap_or(Expr::Const(Value::Null))
        //                     .to_static();
        //                 let typing = col.typing.clone();
        //                 (extractor, typing)
        //             })
        //             .collect::<Vec<_>>();
        //         (key_builder, val_builder, None)
        //     }
        //     TableInfo::Edge(e) => {
        //         todo!()
        //     }
        //     _ => return Err(AlgebraParseError::WrongTableKind(table_name.to_string())),
        // };
        // let assoc_infos = assoc_names
        //     .iter()
        //     .map(|name| -> Result<TableInfo> {
        //         let table_id = ctx
        //             .resolve_table(&table_name)
        //             .ok_or_else(|| AlgebraParseError::TableNotFound(table_name.to_string()))?;
        //         ctx.get_table_info(table_id)
        //     })
        //     .collect::<Result<Vec<_>>>()?;
        let assoc_infos = ctx
            .get_table_assocs(target_id)?
            .into_iter()
            .filter(|v| assoc_names.contains(&v.name))
            .collect::<Vec<_>>();
        let binding_map_inner = Self::build_binding_map_inner(ctx, &target_info, &assoc_infos)?;
        let binding_map = BTreeMap::from([(binding, binding_map_inner)]);
        dbg!(&target_info);
        dbg!(&assoc_infos);
        dbg!(&extract_map);
        dbg!(&binding_map);
        Ok(Self {
            ctx,
            source,
            target_info,
            assoc_infos,
            extract_map,
            binding_map,
        })
    }

    fn build_binding_map_inner(
        ctx: &impl InterpretContext,
        target_info: &TableInfo,
        assoc_infos: &Vec<AssocInfo>,
    ) -> Result<BTreeMap<String, TupleSetIdx>> {
        let mut binding_map_inner = BTreeMap::new();
        match &target_info {
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
                let src = ctx.get_node_info(e.src_id)?;
                let dst = ctx.get_node_info(e.dst_id)?;
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
        for (iset, info) in assoc_infos.iter().enumerate() {
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

impl<'a, C: InterpretContext + 'a> RelationalAlgebra for RaInsert<'a, C> {
    fn name(&self) -> &str {
        NAME_RA_INSERT
    }

    fn binding_map(&self) -> BindingMap {
        self.binding_map.clone()
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = TupleSet> + 'b> {
        self.source.iter()
    }
}

pub(crate) fn build_ra_expr<'a>(
    ctx: &'a impl InterpretContext,
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
    use crate::parser::{CozoParser, Rule};
    use crate::runtime::session::tests::create_test_db;
    use pest::Parser;

    #[test]
    fn parse_ra() -> Result<()> {
        let (db, mut sess) = create_test_db("_test_parser.db");
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
        for t in ra.iter() {
            dbg!(t);
        }

        // let s = r#"
        //  From(f:Person-[:HasJob]->j:Job,
        //       f.id == 101, j.id > 10)
        // .Select(f: {*id: f.id})
        // "#;
        // build_ra_expr(CozoParser::parse(Rule::ra_expr_all, s).unwrap().into_iter().next().unwrap());
        Ok(())
    }
}
