use crate::algebra::op::{
    build_binding_map_from_info, parse_chain_names_single,
    InterpretContext, KeyBuilderSet, MutationError, RelationalAlgebra,
};
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::parser::parse_scoped_dict;
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{BindingMap, BindingMapEvalContext, TupleSet, TupleSetEvalContext};
use crate::data::typing::Typing;
use crate::data::value::Value;
use crate::ddl::reify::{AssocInfo, DdlContext, TableInfo};
use crate::parser::{Pairs, Rule};
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::Result;
use cozorocks::PinnableSlicePtr;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_UPDATE: &str = "Update";

pub(crate) struct UpdateOp<'a> {
    ctx: &'a TempDbContext<'a>,
    pub(crate) source: RaBox<'a>,
    binding: String,
    target_info: TableInfo,
    assoc_infos: Vec<AssocInfo>,
    extract_map: Expr,
    update_main: bool,
}

impl<'a> UpdateOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_UPDATE.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };
        let mut pair = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();

        let mut assocs = vec![];
        let mut main = vec![];
        let update_main;
        match pair.as_rule() {
            Rule::chain => {
                let chain_el_names = parse_chain_names_single(pair)?;
                pair = args
                    .next()
                    .ok_or_else(not_enough_args)?
                    .into_inner()
                    .next()
                    .unwrap();
                for name in chain_el_names {
                    let tid = ctx
                        .resolve_table(&name)
                        .ok_or(AlgebraParseError::TableNotFound(name))?;
                    match ctx.table_by_id(tid)? {
                        TableInfo::Assoc(info) => assocs.push(info),
                        info @ (TableInfo::Node(_) | TableInfo::Edge(_)) => main.push(info),
                        _ => return Err(AlgebraParseError::WrongTableKind(tid).into()),
                    }
                }
                update_main = match main.len() {
                    0 => {
                        let main_id = assocs.get(0).unwrap().src_id;
                        let main_info = ctx.table_by_id(main_id)?;
                        main.push(main_info);
                        false
                    }
                    1 => {
                        assocs = ctx.assocs_by_main_id(main.get(0).unwrap().table_id())?;
                        true
                    }
                    _ => return Err(MutationError::WrongSpecification.into()),
                };
            }
            _ => match source.identity() {
                None => return Err(not_enough_args().into()),
                Some(table_info) => {
                    main.push(table_info);
                    update_main = true;
                }
            },
        }

        let main = main.pop().unwrap();
        let main_id = main.table_id();

        for assoc in &assocs {
            if assoc.src_id != main_id {
                return Err(AlgebraParseError::NoAssociation(
                    assoc.name.clone(),
                    main.table_name().to_string(),
                )
                .into());
            }
        }

        assert_rule(&pair, Rule::scoped_dict, NAME_UPDATE, 2)?;
        let (binding, keys, extract_map) = parse_scoped_dict(pair)?;
        if !keys.is_empty() {
            return Err(
                AlgebraParseError::Parse("Cannot have keyed map in Insert".to_string()).into(),
            );
        }

        Ok(Self {
            ctx,
            binding,
            source,
            target_info: main,
            assoc_infos: assocs,
            extract_map,
            update_main,
        })
    }
}

impl<'a> RelationalAlgebra for UpdateOp<'a> {
    fn name(&self) -> &str {
        NAME_UPDATE
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let inner =
            build_binding_map_from_info(self.ctx, &self.target_info, &self.assoc_infos, true)?;
        Ok(BindingMap {
            inner_map: BTreeMap::from([(self.binding.clone(), inner)]),
            key_size: 1,
            val_size: self.assoc_infos.len() + if self.update_main { 1 } else { 0 },
        })
    }

    fn iter<'b>(&'b self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'b>> {
        let source_map = self.source.binding_map()?;
        if source_map.inner_map.len() != 1 {
            return Err(MutationError::WrongSpecification.into());
        }

        let binding_ctx = BindingMapEvalContext {
            map: &source_map,
            parent: self.ctx,
        };
        let mut extract_map = match self.extract_map.clone().partial_eval(&binding_ctx)? {
            Expr::Dict(d) => d,
            v => return Err(AlgebraParseError::Parse(format!("{:?}", v)).into()),
        };

        for (k, v) in source_map.inner_map.values().next().unwrap() {
            if !extract_map.contains_key(k) {
                extract_map.insert(k.to_string(), Expr::TupleSetIdx(*v));
            }
        }

        let (key_builder, val_builder, _) =
            make_update_key_builders(self.ctx, &self.target_info, &extract_map)?;
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

        let r_opts = default_read_options();
        let mut temp_slice = PinnableSlicePtr::default();
        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let w_opts = default_write_options();

        Ok(Box::new(self.source.iter()?.map(
            move |tset| -> Result<TupleSet> {
                let eval_ctx = TupleSetEvalContext {
                    tuple_set: &tset?,
                    txn: &txn,
                    temp_db: &temp_db,
                    write_options: &w_opts,
                };
                let mut key = eval_ctx.eval_to_tuple(target_key.id, &key_builder)?;

                let existing = if target_key.in_root {
                    eval_ctx.txn.get(&r_opts, &key, &mut temp_slice)?
                } else {
                    eval_ctx.temp_db.get(&r_opts, &key, &mut temp_slice)?
                };
                if !existing {
                    return Err(AlgebraParseError::ValueNotFound(key.to_owned()).into());
                }

                let mut ret = TupleSet::default();

                if self.update_main {
                    let val = eval_ctx.eval_to_tuple(DataKind::Data as u32, &val_builder)?;

                    if target_key.in_root {
                        eval_ctx.txn.put(&key, &val)?;
                    } else {
                        eval_ctx.temp_db.put(eval_ctx.write_options, &key, &val)?;
                    }

                    ret.push_val(val.into());
                }

                let assoc_vals = assoc_val_builders
                    .iter()
                    .map(|(tid, builder)| -> Result<OwnTuple> {
                        let ret = eval_ctx.eval_to_tuple(DataKind::Data as u32, builder)?;
                        key.overwrite_prefix(tid.id);
                        if tid.in_root {
                            eval_ctx.txn.put(&key, &ret)?;
                        } else {
                            eval_ctx.temp_db.put(eval_ctx.write_options, &key, &ret)?;
                        }
                        Ok(ret)
                    })
                    .collect::<Result<Vec<_>>>()?;

                key.overwrite_prefix(target_key.id);

                ret.push_key(key.into());
                for av in assoc_vals {
                    ret.push_val(av.into())
                }
                Ok(ret)
            },
        )))
    }

    fn identity(&self) -> Option<TableInfo> {
        Some(self.target_info.clone())
    }
}

fn make_update_key_builders(
    ctx: &TempDbContext,
    target_info: &TableInfo,
    extract_map: &BTreeMap<String, Expr>,
) -> Result<KeyBuilderSet> {
    let ret = match target_info {
        TableInfo::Node(n) => {
            let key_builder = n
                .keys
                .iter()
                .map(|v| v.make_extractor(extract_map))
                .collect::<Vec<_>>();
            let val_builder = n
                .vals
                .iter()
                .map(|v| v.make_extractor(extract_map))
                .collect::<Vec<_>>();
            (key_builder, val_builder, None)
        }
        TableInfo::Edge(e) => {
            let src = ctx.get_table_info(e.src_id)?.into_node()?;
            let dst = ctx.get_table_info(e.dst_id)?.into_node()?;
            let fwd_edge_part = [(Expr::Const(Value::Bool(true)), Typing::Any)];
            let key_builder = fwd_edge_part
                .into_iter()
                .chain(src.keys.iter().map(|v| v.make_extractor(extract_map)))
                .chain(dst.keys.iter().map(|v| v.make_extractor(extract_map)))
                .chain(e.keys.iter().map(|v| v.make_extractor(extract_map)))
                .collect::<Vec<_>>();
            let val_builder = e
                .vals
                .iter()
                .map(|v| v.make_extractor(extract_map))
                .collect::<Vec<_>>();
            (key_builder, val_builder, None)
        }
        _ => unreachable!(),
    };
    Ok(ret)
}
