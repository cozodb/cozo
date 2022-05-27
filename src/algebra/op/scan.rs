use crate::algebra::op::{AssocOp, ChainEl, ChainPart, InterpretContext, RelationalAlgebra};
use crate::algebra::parser::{AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::data::value::Value;
use crate::ddl::reify::{AssocInfo, TableInfo};
use anyhow::Result;
use cozorocks::IteratorPtr;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct TableScan<'a> {
    ctx: &'a TempDbContext<'a>,
    binding: String,
    table_info: TableInfo,
    // assoc_infos: Vec<AssocInfo>,
}

impl<'a> TableScan<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        el: &ChainEl,
        only_one: bool,
    ) -> Result<RaBox<'a>> {
        let table_id = ctx
            .resolve_table(&el.target)
            .ok_or_else(|| AlgebraParseError::TableNotFound(el.target.to_string()))?;
        let table_info = ctx.get_table_info(table_id)?;
        match el.part {
            ChainPart::Node => {
                if only_one {
                    table_info
                        .as_node()
                        .map(|_| ())
                        .or_else(|_| table_info.as_edge().map(|_| ()))?;
                } else {
                    table_info.as_node()?;
                }
            }
            ChainPart::Edge { .. } => {
                table_info.as_edge()?;
            }
        }
        let mut assoc_infos = vec![];
        for assoc_name in &el.assocs {
            let tid = ctx
                .resolve_table(assoc_name)
                .ok_or_else(|| AlgebraParseError::TableNotFound(assoc_name.to_string()))?;
            let tinfo = ctx.get_table_info(tid)?.into_assoc()?;
            if tinfo.src_id != table_info.table_id() {
                return Err(AlgebraParseError::NoAssociation(
                    el.target.to_string(),
                    assoc_name.to_string(),
                )
                .into());
            }
            assoc_infos.push(tinfo);
        }
        let mut ret = RaBox::TableScan(Box::new(Self {
            ctx,
            binding: el.binding.clone(),
            table_info: table_info.clone(),
        }));
        if !assoc_infos.is_empty() {
            ret = RaBox::AssocOp(Box::new(AssocOp::build(
                ctx,
                ret,
                &el.binding,
                &table_info,
                assoc_infos,
            )?))
        }
        Ok(ret)
    }
}

pub(crate) const NAME_TABLE_SCAN: &str = "TableScan";

impl<'b> RelationalAlgebra for TableScan<'b> {
    fn name(&self) -> &str {
        NAME_TABLE_SCAN
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        let inner = build_binding_map_from_info(self.ctx, &self.table_info, &[])?;
        Ok(BindingMap {
            inner_map: BTreeMap::from([(self.binding.clone(), inner)]),
            key_size: 1,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let tid = self.table_info.table_id();
        let iter = if tid.in_root {
            self.ctx.txn.iterator(&self.ctx.sess.r_opts_main)
        } else {
            self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp)
        };
        let start_key = OwnTuple::with_prefix(tid.id);
        iter.seek(&start_key);
        let iterator = ScanTableIterator {
            inner: iter,
            started: false,
        };
        Ok(Box::new(iterator))
    }

    fn identity(&self) -> Option<TableInfo> {
        Some(self.table_info.clone())
    }
}

pub(crate) struct ScanTableIterator {
    inner: IteratorPtr,
    started: bool,
}

impl Iterator for ScanTableIterator {
    type Item = Result<TupleSet>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.inner.next();
        } else {
            self.started = true;
        }
        while let Some((k, v)) = self.inner.pair() {
            let v = Tuple::new(v);
            if let Ok(DataKind::Data) = v.data_kind() {
                let k = Tuple::new(k);
                let mut tset = TupleSet::default();
                tset.push_key(k.into());
                tset.push_val(v.into());
                return Some(Ok(tset));
            }
            self.inner.next();
        }
        None
    }
}

pub(crate) fn build_binding_map_from_info(
    ctx: &TempDbContext,
    info: &TableInfo,
    assoc_infos: &[AssocInfo],
) -> Result<BTreeMap<String, TupleSetIdx>> {
    let mut binding_map_inner = BTreeMap::new();
    match info {
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
            let src = ctx.get_table_info(e.src_id)?.into_node()?;
            let dst = ctx.get_table_info(e.dst_id)?.into_node()?;
            for (i, k) in src.keys.iter().enumerate() {
                binding_map_inner.insert(
                    "_src_".to_string() + &k.name,
                    TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: i + 1,
                    },
                );
            }
            for (i, k) in dst.keys.iter().enumerate() {
                binding_map_inner.insert(
                    "_dst_".to_string() + &k.name,
                    TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: i + 1 + src.keys.len(),
                    },
                );
            }
            for (i, k) in e.keys.iter().enumerate() {
                binding_map_inner.insert(
                    k.name.clone(),
                    TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: i + 1 + src.keys.len() + dst.keys.len(),
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
