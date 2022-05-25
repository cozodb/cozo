use crate::algebra::op::{RelationalAlgebra, TableInfoByIdCache, TableInfoByNameCache};
use crate::algebra::parser::{assert_rule, AlgebraParseError};
use crate::context::TempDbContext;
use crate::data::expr::Expr;
use crate::data::tuple::{DataKind, OwnTuple};
use crate::data::tuple_set::{BindingMap, TableId, TupleSet, TupleSetIdx};
use crate::data::value::Value;
use crate::ddl::reify::TableInfo;
use crate::parser::text_identifier::{build_name_in_def, parse_table_with_assocs};
use crate::parser::{CozoParser, Pairs, Rule};
use crate::runtime::options::{default_read_options, default_write_options};
use anyhow::{Context, Result};
use cozorocks::PinnableSlicePtr;
use pest::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(crate) const NAME_TAGGED_INSERTION: &str = "InsertTagged";
pub(crate) const NAME_TAGGED_UPSERT: &str = "UpsertTagged";

type TaggedInsertionSet = (
    OwnTuple,
    OwnTuple,
    Option<OwnTuple>,
    Vec<(TableId, OwnTuple)>,
);

pub(crate) struct TaggedInsertion<'a> {
    ctx: &'a TempDbContext<'a>,
    // source: Arc<Vec<StaticValue>>,
    values: BTreeMap<TableId, Vec<TaggedInsertionSet>>,
    tally: BTreeMap<String, usize>,
    binding: String,
    upsert: bool,
}

impl<'a> TaggedInsertion<'a> {
    pub(crate) fn build(
        ctx: &'a TempDbContext<'a>,
        prev: Option<Arc<dyn RelationalAlgebra + 'a>>,
        mut args: Pairs,
        upsert: bool,
    ) -> Result<Self> {
        let a_name = if upsert {
            NAME_TAGGED_UPSERT
        } else {
            NAME_TAGGED_INSERTION
        };
        if !matches!(prev, None) {
            return Err(AlgebraParseError::Unchainable(a_name.to_string()).into());
        }
        let not_enough_args = || AlgebraParseError::NotEnoughArguments(a_name.to_string());
        let values = args
            .next()
            .ok_or_else(not_enough_args)?
            .into_inner()
            .next()
            .ok_or_else(not_enough_args)?;
        assert_rule(&values, Rule::expr, a_name, 0)?;
        let values = Expr::try_from(values)?
            .interpret_eval(ctx)?
            .to_static()
            .into_vec()
            .map_err(|e| {
                AlgebraParseError::WrongArgumentType(a_name.to_string(), 0, format!("{:?}", e))
            })?;
        let (values, tally) = Self::make_values(values, ctx)?;

        let binding = match args.next() {
            None => "_".to_string(),
            Some(pair) => {
                let pair = CozoParser::parse(Rule::name_in_def_all, pair.as_str())?
                    .next()
                    .unwrap();
                build_name_in_def(pair, true)?
            }
        };
        Ok(Self {
            ctx,
            binding,
            upsert,
            values,
            tally,
        })
    }

    #[allow(clippy::type_complexity)]
    fn make_values(
        source: Vec<Value>,
        ctx: &'a TempDbContext<'a>,
    ) -> Result<(
        BTreeMap<TableId, Vec<TaggedInsertionSet>>,
        BTreeMap<String, usize>,
    )> {
        let mut collected = BTreeMap::new();
        let mut tally = BTreeMap::new();

        let mut cache = TableInfoByNameCache {
            ctx,
            cache: Default::default(),
        };

        let mut id_cache = TableInfoByIdCache {
            ctx,
            cache: Default::default(),
        };

        let mut key_buffer = String::new();
        for value in source.iter() {
            let gen_err = || AlgebraParseError::ValueError(value.clone().to_static());
            let d_map = value
                .get_map()
                .ok_or_else(gen_err)
                .context("Value must be a dict")?;
            let targets = d_map
                .get("_type")
                .ok_or_else(gen_err)
                .context("`_type` must be present on maps")?
                .get_str()
                .ok_or_else(gen_err)
                .context("`_type` must be Text")?;
            let (main, assocs) =
                parse_table_with_assocs(targets).context("Parsing table name failed")?;
            let main_info = cache.get_info(&main).context("Getting main info failed")?;
            let (key_tuple, val_tuple, inv_key_tuple) =
                match main_info.as_ref() {
                    TableInfo::Node(n) => {
                        *tally.entry(n.name.to_string()).or_default() += 1;
                        let mut key_tuple = OwnTuple::with_prefix(n.tid.id);
                        for col in &n.keys {
                            let k = &col.name as &str;
                            let val = d_map.get(k).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).context("type coercion failed")?;
                            key_tuple.push_value(&val);
                        }

                        let mut val_tuple = OwnTuple::with_data_prefix(DataKind::Data);
                        for col in &n.vals {
                            let k = &col.name as &str;
                            let val = d_map.get(k).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val)?;
                            val_tuple.push_value(&val);
                        }

                        (key_tuple, val_tuple, None)
                    }
                    TableInfo::Edge(e) => {
                        *tally.entry(e.name.to_string()).or_default() += 1;
                        let src = id_cache.get_info(e.src_id)?;
                        let dst = id_cache.get_info(e.dst_id)?;
                        let mut key_tuple = OwnTuple::with_prefix(e.tid.id);
                        key_tuple.push_int(e.src_id.id as i64);
                        let mut inv_key_tuple = OwnTuple::with_prefix(e.tid.id);
                        inv_key_tuple.push_int(e.dst_id.id as i64);
                        let mut val_tuple = OwnTuple::with_data_prefix(DataKind::Data);

                        for col in &src.as_node()?.keys {
                            key_buffer.clear();
                            key_buffer += "_src_";
                            key_buffer += &col.name;
                            let val = d_map.get(&key_buffer as &str).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).with_context(|| {
                                format!("Coercion failed {:?} {:?}", col, d_map)
                            })?;
                            key_tuple.push_value(&val);
                        }

                        key_tuple.push_bool(true);

                        for col in &dst.as_node()?.keys {
                            key_buffer.clear();
                            key_buffer += "_dst_";
                            key_buffer += &col.name;
                            let val = d_map.get(&key_buffer as &str).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).with_context(|| {
                                format!("Coercion failed {:?} {:?}", col, d_map)
                            })?;
                            key_tuple.push_value(&val);
                            inv_key_tuple.push_value(&val);
                        }

                        inv_key_tuple.push_bool(false);

                        for col in &src.as_node()?.keys {
                            key_buffer.clear();
                            key_buffer += "_src_";
                            key_buffer += &col.name;
                            let val = d_map.get(&key_buffer as &str).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).with_context(|| {
                                format!("Coercion failed {:?} {:?}", col, d_map)
                            })?;
                            inv_key_tuple.push_value(&val);
                        }

                        for col in &e.keys {
                            let k = &col.name as &str;
                            let val = d_map.get(k).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).with_context(|| {
                                format!("Coercion failed {:?} {:?}", col, d_map)
                            })?;
                            key_tuple.push_value(&val);
                        }

                        for col in &e.vals {
                            let k = &col.name as &str;
                            let val = d_map.get(k).unwrap_or(&Value::Null);
                            let val = col.typing.coerce_ref(val).with_context(|| {
                                format!("Coercion failed {:?} {:?}", col, d_map)
                            })?;
                            val_tuple.push_value(&val);
                        }

                        (key_tuple, val_tuple, Some(inv_key_tuple))
                    }
                    _ => return Err(AlgebraParseError::WrongTableKind(main_info.table_id()).into()),
                };
            let mut assoc_vecs = vec![];
            for assoc_name in assocs.into_iter() {
                let assoc_info = cache.get_info(&assoc_name)?;
                let assoc_info = assoc_info.as_assoc()?;
                *tally.entry(assoc_info.name.to_string()).or_default() += 1;
                if assoc_info.src_id != main_info.table_id() {
                    return Err(AlgebraParseError::NoAssociation(main, assoc_name).into());
                }
                let mut assoc_tuple = OwnTuple::with_data_prefix(DataKind::Data);
                for col in &assoc_info.vals {
                    let k = &col.name as &str;
                    let val = d_map.get(k).unwrap_or(&Value::Null);
                    let val = col.typing.coerce_ref(val)?;
                    assoc_tuple.push_value(&val);
                }
                assoc_vecs.push((assoc_info.tid, assoc_tuple));
            }

            let cur_table = collected
                .entry(main_info.table_id())
                .or_insert_with(Vec::new);
            cur_table.push((key_tuple, val_tuple, inv_key_tuple, assoc_vecs));
        }

        Ok((collected, tally))
    }
}

impl<'b> RelationalAlgebra for TaggedInsertion<'b> {
    fn name(&self) -> &str {
        if self.upsert {
            NAME_TAGGED_UPSERT
        } else {
            NAME_TAGGED_INSERTION
        }
    }

    fn bindings(&self) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::from([self.binding.clone()]))
    }

    fn binding_map(&self) -> Result<BindingMap> {
        Ok(BTreeMap::from([(
            self.binding.clone(),
            BTreeMap::from([
                (
                    "table".to_string(),
                    TupleSetIdx {
                        is_key: true,
                        t_set: 0,
                        col_idx: 0,
                    },
                ),
                (
                    "n".to_string(),
                    TupleSetIdx {
                        is_key: false,
                        t_set: 0,
                        col_idx: 0,
                    },
                ),
            ]),
        )]))
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<TupleSet>> + 'a>> {
        let mut temp_slice = PinnableSlicePtr::default();
        let r_opts = default_read_options();
        let w_opts = default_write_options();
        for (tid, rows) in &self.values {
            for (key, val, inv_key, assocs) in rows {
                if !self.upsert {
                    let exists = if tid.in_root {
                        self.ctx.txn.get(&r_opts, key, &mut temp_slice)?
                    } else {
                        self.ctx.sess.temp.get(&r_opts, key, &mut temp_slice)?
                    };
                    if exists {
                        return Err(AlgebraParseError::KeyConflict(key.to_owned()).into());
                    }
                }
                if tid.in_root {
                    self.ctx.txn.put(key, val)?;
                } else {
                    self.ctx.sess.temp.put(&w_opts, key, val)?;
                }
                if let Some(ik) = inv_key {
                    if tid.in_root {
                        self.ctx.txn.put(ik, key)?;
                    } else {
                        self.ctx.sess.temp.put(&w_opts, ik, key)?;
                    }
                }
                if !assocs.is_empty() {
                    let mut k = key.clone();
                    for (aid, v) in assocs {
                        k.overwrite_prefix(aid.id);
                        if aid.in_root {
                            self.ctx.txn.put(&k, v)?;
                        } else {
                            self.ctx.sess.temp.put(&w_opts, &k, v)?;
                        }
                    }
                }
            }
        }
        Ok(Box::new(self.tally.iter().map(|(name, n)| {
            let mut key_tuple = OwnTuple::with_prefix(0);
            let mut val_tuple = OwnTuple::with_prefix(0);
            key_tuple.push_str(name);
            val_tuple.push_int(*n as i64);
            Ok(TupleSet {
                keys: vec![key_tuple.into()],
                vals: vec![val_tuple.into()],
            })
        })))
    }

    fn identity(&self) -> Option<TableInfo> {
        None
    }
}
