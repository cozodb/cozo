use crate::algebra::op::{
    build_binding_map_from_info, make_key_builders, parse_chain, InterpretContext,
    RelationalAlgebra,
};
use crate::algebra::parser::{assert_rule, build_relational_expr, AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetEvalContext};
use crate::ddl::reify::{AssocInfo, DdlContext, TableInfo};
use crate::parser::{Pairs, Rule};
use crate::runtime::options::default_write_options;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const NAME_DELETE: &str = "Delete";

pub(crate) struct DeleteOp<'a> {
    pub(crate) source: RaBox<'a>,
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) main_info: TableInfo,
    pub(crate) assoc_infos: Vec<AssocInfo>,
    pub(crate) delete_main: bool,
}

impl<'a> DeleteOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_DELETE.to_string());
        let source = match prev {
            Some(v) => v,
            None => build_relational_expr(ctx, args.next().ok_or_else(not_enough_args)?)?,
        };

        let chain = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .unwrap();
        assert_rule(&chain, Rule::chain, NAME_DELETE, 1)?;
        let mut chain = parse_chain(chain)?;
        if chain.len() != 1 {
            return Err(MutationError::WrongSpecification.into());
        }
        let chain_el = chain.pop().unwrap();
        let mut chain_el_names = chain_el.assocs;
        chain_el_names.insert(chain_el.target);
        let mut binding = chain_el.binding;
        if !binding.starts_with('@') {
            return Err(MutationError::WrongSpecification.into());
        }
        let mut assocs = vec![];
        let mut main = vec![];
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
        let delete_main = match main.len() {
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

        Ok(Self {
            source,
            ctx,
            main_info: main,
            assoc_infos: assocs,
            delete_main,
        })
    }
}

impl<'b> RelationalAlgebra for DeleteOp<'b> {
    fn name(&self) -> &str {
        NAME_DELETE
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        self.source.binding_map()
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let parent_bmap = self.source.binding_map()?.inner_map;
        if parent_bmap.len() != 1 {
            return Err(MutationError::SourceUnsuitableForMutation(
                self.source.name().to_string(),
                NAME_DELETE.to_string(),
            )
            .into());
        }
        let (_, extract_map) = parent_bmap.into_iter().next().unwrap();
        let extract_map = extract_map
            .into_iter()
            .map(|(k, v)| (k, Expr::TupleSetIdx(v)))
            .collect::<BTreeMap<_, _>>();
        let (key_builder, _, _) = make_key_builders(self.ctx, &self.main_info, &extract_map)?;
        let mut table_ids_to_delete = self
            .assoc_infos
            .iter()
            .map(|info| info.tid)
            .collect::<Vec<_>>();
        if self.delete_main {
            table_ids_to_delete.push(self.main_info.table_id());
        }
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
            let mut key = eval_ctx.eval_to_tuple(0, &key_builder)?;
            for tid in &table_ids_to_delete {
                key.overwrite_prefix(tid.id);
                if tid.in_root {
                    txn.del(&key)?;
                } else {
                    temp_db.del(&w_opts, &key)?;
                }
            }
            Ok(tset)
        });

        Ok(Box::new(iter))
    }

    fn identity(&self) -> Option<TableInfo> {
        Some(self.main_info.clone())
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum MutationError {
    #[error("Source relation {0} is unsuitable for {1}")]
    SourceUnsuitableForMutation(String, String),

    #[error("Wrong specification of mutation target")]
    WrongSpecification,
}
