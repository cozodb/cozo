use crate::algebra::op::{InterpretContext, RelationalAlgebra};
use crate::algebra::parser::RaBox;
use crate::context::TempDbContext;
use crate::data::expr::{Expr, StaticExpr};
use crate::data::tuple::{OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::data::value::Value;
use crate::ddl::reify::{AssocInfo, TableInfo};
use crate::runtime::options::default_read_options;
use anyhow::Result;
use std::collections::BTreeSet;

pub(crate) const NAME_ASSOC: &str = "Assoc";

pub(crate) struct AssocOp<'a> {
    pub(crate) ctx: &'a TempDbContext<'a>,
    pub(crate) source: RaBox<'a>,
    pub(crate) assoc_infos: Vec<AssocInfo>,
    pub(crate) key_extractors: Vec<StaticExpr>,
    pub(crate) binding: String,
}

impl<'a> AssocOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        source: RaBox<'a>,
        binding: &str,
        table_info: &TableInfo,
        assoc_infos: Vec<AssocInfo>,
    ) -> Result<Self> {
        let mut key_extractors = vec![];
        match table_info {
            TableInfo::Node(info) => {
                for col in &info.keys {
                    key_extractors.push(Expr::FieldAcc(
                        col.name.clone(),
                        Expr::Variable(binding.to_string()).into(),
                    ))
                }
            }
            TableInfo::Edge(info) => {
                let src = ctx.get_table_info(info.src_id)?;
                let src = src.as_node()?;
                let dst = ctx.get_table_info(info.dst_id)?;
                let dst = dst.as_node()?;
                key_extractors.push(Expr::Const(Value::Int(info.src_id.int_for_storage())));
                for col in &src.keys {
                    key_extractors.push(Expr::FieldAcc(
                        "_src_".to_string() + &col.name,
                        Expr::Variable(binding.to_string()).into(),
                    ));
                }
                key_extractors.push(Expr::Const(Value::Bool(true)));
                for col in &dst.keys {
                    key_extractors.push(Expr::FieldAcc(
                        "_dst_".to_string() + &col.name,
                        Expr::Variable(binding.to_string()).into(),
                    ));
                }
                for col in &info.keys {
                    key_extractors.push(Expr::FieldAcc(
                        col.name.clone(),
                        Expr::Variable(binding.to_string()).into(),
                    ));
                }
            }
            _ => unreachable!(),
        }
        Ok(AssocOp {
            ctx,
            source,
            assoc_infos,
            key_extractors,
            binding: binding.to_string(),
        })
    }
}

impl<'b> RelationalAlgebra for AssocOp<'b> {
    fn name(&self) -> &str {
        NAME_ASSOC
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        self.source.bindings()
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let mut binding_map = self.source.binding_map()?;
        let mvi = binding_map.val_size;
        let sub_map = binding_map.inner_map.entry(self.binding.clone()).or_default();
        for (i, info) in self.assoc_infos.iter().enumerate() {
            for (j, col) in info.vals.iter().enumerate() {
                sub_map.insert(
                    col.name.to_string(),
                    TupleSetIdx {
                        is_key: false,
                        t_set: i + mvi,
                        col_idx: j,
                    },
                );
            }
        }
        Ok(binding_map)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let parent_iter = self.source.iter()?;
        let key_extractors = self.key_extractors.clone();
        let assoc_ids = self
            .assoc_infos
            .iter()
            .map(|info| info.tid)
            .collect::<Vec<_>>();
        let mut key_tuple = OwnTuple::with_null_prefix();
        let txn = self.ctx.txn.clone();
        let temp_db = self.ctx.sess.temp.clone();
        let r_opts = default_read_options();
        let iter = parent_iter.map(move |tset| -> Result<TupleSet> {
            key_tuple.truncate_all();
            let mut tset = tset?;
            for ke in &key_extractors {
                let v = ke.row_eval(&tset)?;
                key_tuple.push_value(&v);
            }
            for id in &assoc_ids {
                key_tuple.overwrite_prefix(id.id);
                let res = if id.in_root {
                    txn.get_owned(&r_opts, &key_tuple)?
                } else {
                    temp_db.get_owned(&r_opts, &key_tuple)?
                };
                if let Some(slice) = res {
                    tset.push_val(Tuple::new(slice).into())
                } else {
                    tset.push_val(OwnTuple::empty_tuple().into())
                }
            }
            Ok(tset)
        });
        Ok(Box::new(iter))
    }

    fn identity(&self) -> Option<TableInfo> {
        self.source.identity()
    }
}
