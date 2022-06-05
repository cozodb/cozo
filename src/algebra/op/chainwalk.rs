use crate::algebra::op::{
    build_hop_it, build_selection_iter, build_starting_it, check_chain_dup_binding,
    drop_temp_table, parse_chain, parse_walk_conditions_and_collectors, resolve_walk_chain,
    ChainPart, ClusterIterator, HoppingEls, RelationalAlgebra, StartingEl, WalkElOp, WalkError,
};
use crate::algebra::parser::{AlgebraParseError, RaBox};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{BindingMap, TupleSet, TupleSetIdx};
use crate::ddl::reify::{DdlContext, TableInfo};
use crate::parser::Pairs;
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::Result;
use chrono::format::Item;
use cozorocks::PinnableSlicePtr;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) const NAME_CHAIN_WALK: &str = "Chain";

pub(crate) struct ChainWalkOp<'a> {
    ctx: &'a TempDbContext<'a>,
    starting: StartingEl,
    hops: Vec<HoppingEls>,
    extraction_map: BTreeMap<String, Expr>,
    binding: String,
    binding_maps: Vec<BindingMap>,
    loop_filters: Vec<WalkElOp>,
    temp_table_id: AtomicU32,
}

impl<'a> ChainWalkOp<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<RaBox<'a>>,
        mut args: Pairs,
    ) -> Result<Self> {
        if !matches!(prev, None) {
            return Err(AlgebraParseError::Unchainable(NAME_CHAIN_WALK.to_string()).into());
        }
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(NAME_CHAIN_WALK.to_string());
        let arg = args.next().ok_or_else(not_enough_args)?;

        let chain = arg.into_inner().next().ok_or_else(not_enough_args)?;

        let chain = parse_chain(chain)?;

        if chain.is_empty() {
            return Err(not_enough_args().into());
        }

        if chain.first().unwrap().part != ChainPart::Node
            || chain.last().unwrap().part != ChainPart::Node
        {
            return Err(WalkError::Chain.into());
        }

        if chain.first().unwrap().target != chain.last().unwrap().target {
            return Err(WalkError::Chain.into());
        }

        check_chain_dup_binding(&chain)?;

        let (mut starting_el, mut hops, binding_maps) = resolve_walk_chain(ctx, chain)?;

        let (binding, extraction_map, continue_ops) = parse_walk_conditions_and_collectors(
            ctx,
            args,
            true,
            &mut starting_el,
            &mut hops,
            binding_maps.last().unwrap(),
        )?;

        Ok(Self {
            ctx,
            starting: starting_el,
            hops,
            extraction_map,
            binding,
            binding_maps,
            loop_filters: continue_ops,
            temp_table_id: Default::default(),
        })
    }
}

impl<'a> Drop for ChainWalkOp<'a> {
    fn drop(&mut self) {
        // let tid = self.temp_table_id.load(Ordering::SeqCst);
        // if tid > 0 {
        //     let it = self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp);
        //     let key = OwnTuple::with_prefix(tid);
        //     // it.seek(&key);
        //     for (k, v) in it.iter_rows(key) {
        //         let k = Tuple::new(k);
        //         dbg!(k);
        //     }
        // }
        drop_temp_table(self.ctx, self.temp_table_id.load(Ordering::SeqCst));
    }
}

const SEEN_MARKER: i64 = -1;
const RESULT_MARKER: i64 = -2;

impl<'b> RelationalAlgebra for ChainWalkOp<'b> {
    fn name(&self) -> &str {
        NAME_CHAIN_WALK
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        Ok(BindingMap {
            inner_map: BTreeMap::from([(
                self.binding.clone(),
                self.extraction_map
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
                    .collect(),
            )]),
            key_size: 0,
            val_size: 1,
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let mut temp_table_id = self.temp_table_id.load(Ordering::SeqCst);
        if temp_table_id == 0 {
            let temp_id = self.ctx.gen_table_id()?.id;
            self.temp_table_id.store(temp_id, Ordering::SeqCst);
            temp_table_id = temp_id;
        }

        let (starting_it, node_keys_extractors) =
            build_starting_it(self.ctx, &self.starting, &self.binding_maps)?;

        let temp_db = self.ctx.sess.temp.clone();
        let mut key_tuple = OwnTuple::with_prefix(temp_table_id);
        let mut val_tuple = OwnTuple::with_null_prefix();
        let w_opts = default_write_options();
        for tset in starting_it {
            let tset = tset?;
            let starting_key = tset.keys.first().unwrap();
            key_tuple.truncate_all();
            key_tuple.push_int(SEEN_MARKER);
            key_tuple.push_bytes(starting_key.as_ref());
            temp_db.put(&w_opts, &key_tuple, &[])?;
            key_tuple.truncate_all();
            key_tuple.push_int(0);
            key_tuple.push_bytes(starting_key.as_ref());
            val_tuple.truncate_all();
            tset.append_encode_as_tuple(&mut val_tuple);
            temp_db.put(&w_opts, &key_tuple, &val_tuple)?;
        }
        for i in 0i64.. {
            let it = self.ctx.sess.temp.iterator(&self.ctx.sess.r_opts_temp);
            let mut key = OwnTuple::with_prefix(temp_table_id);
            key.push_int(i);
            // it.seek(&key);
            let starting_it = it.iter_prefix(key).map(|(_k, v)| {
                // let k = Tuple::new(k);
                let v = Tuple::new(v);
                let tset = TupleSet::decode_from_tuple(&v);
                tset
            });

            let mut it: Box<dyn Iterator<Item = Result<TupleSet>>> = Box::new(starting_it);
            let mut last_node_keys_extractors = node_keys_extractors.clone();
            let mut met_pivot = self.starting.pivot;
            let mut final_truncate_kv_size: (usize, usize) = if self.starting.pivot {
                let bmap = self.binding_maps.first().unwrap();
                (bmap.key_size, bmap.val_size)
            } else {
                (0, 0)
            };

            for (hop_id, hop) in self.hops.iter().enumerate() {
                it = build_hop_it(
                    self.ctx,
                    &self.binding_maps,
                    it,
                    hop_id,
                    hop,
                    &mut last_node_keys_extractors,
                    &mut met_pivot,
                    &mut final_truncate_kv_size,
                )?;
            }

            let r_opts = default_read_options();
            let mut temp_slice = PinnableSlicePtr::default();
            let mut has_new = false;

            for tset in it {
                // whatever it is, write it to result
                let tset = tset?;
                let first_key = tset.keys.first().unwrap();
                let last_key = tset.keys.last().unwrap();
                key_tuple.truncate_all();
                key_tuple.push_int(RESULT_MARKER);
                key_tuple.push_bytes(first_key.as_ref());
                key_tuple.push_bytes(last_key.as_ref());
                val_tuple.truncate_all();
                tset.append_encode_as_tuple(&mut val_tuple);
                temp_db.put(&w_opts, &key_tuple, &val_tuple)?;

                if !matches!(last_key.data_kind(), Ok(DataKind::Empty)) {
                    key_tuple.truncate_all();
                    key_tuple.push_int(SEEN_MARKER);
                    key_tuple.push_bytes(last_key.as_ref());
                    if !temp_db.get(&r_opts, &key_tuple, &mut temp_slice)? {
                        has_new = true;
                        temp_db.put(&w_opts, &key_tuple, &[])?;

                        key_tuple.truncate_all();
                        key_tuple.push_int(i + 1);
                        key_tuple.push_bytes(last_key.as_ref());
                        val_tuple.truncate_all();
                        let key_size = self.binding_maps.first().unwrap().key_size;
                        let val_size = self.binding_maps.first().unwrap().val_size;
                        let keys = &tset.keys[tset.keys.len() - key_size .. tset.keys.len()];
                        let vals =  &tset.vals[tset.vals.len() - val_size .. tset.vals.len()];
                        let new_tset = TupleSet {
                            keys: keys.to_vec(),
                            vals: vals.to_vec()
                        };
                        new_tset.append_encode_as_tuple(&mut val_tuple);
                        temp_db.put(&w_opts, &key_tuple, &val_tuple)?;
                    }
                    // dbg!((i, first_key, last_key));
                }
            }

            dbg!(i);
            if !has_new {
                break;
            }
        }

        //
        // // it = Box::new(it.map(|v| {
        // //     if let Ok(tset) = &v {
        // //         if let Some(ending_key) = tset.keys.last() {
        // //             if !matches!(ending_key.data_kind(), Ok(DataKind::Empty)) {
        // //                 dbg!(ending_key);
        // //             }
        // //         }
        // //     }
        // //     v
        // // }));
        //
        // let temp_db = self.ctx.sess.temp.clone();
        // let mut write_tuple = OwnTuple::with_prefix(temp_table_id);
        // let w_opts = default_write_options();
        // let r_opts = default_read_options();
        // let mut temp_slice = PinnableSlicePtr::default();
        //
        // for tset in it {
        //     let tset = tset?;
        //     if let Some(last_key) = tset.keys.last() {
        //         let first_key = tset.keys.first().unwrap();
        //
        //         if !matches!(last_key.data_kind(), Ok(DataKind::Empty)) {
        //             write_tuple.truncate_all();
        //             write_tuple.push_bool(true);
        //             write_tuple.push_bytes(last_key.as_ref());
        //             if temp_db.get(&r_opts, &write_tuple, &mut temp_slice)? {
        //                 dbg!("SEEN", first_key, last_key);
        //             } else {
        //                 dbg!("NEW", first_key, last_key);
        //                 temp_db.put(&w_opts, &write_tuple, &[])?;
        //             }
        //         }
        //     }
        // }

        todo!()
        //
        //
        // it = Box::new(ClusterIterator {
        //     source: it,
        //     last_tuple: None,
        //     output_cache: false,
        //     key_len: final_truncate_kv_size.0,
        // });
        //
        // let iter =
        //     build_selection_iter(self.ctx, it, &self.extraction_map, final_truncate_kv_size)?;
        //
        // Ok(Box::new(iter))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
