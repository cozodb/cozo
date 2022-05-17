use crate::db::engine::Session;
use crate::db::table::TableId;
use crate::error::{CozoError, Result};
use crate::parser::text_identifier::build_name_in_def;
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::tuple::{OwnTuple, SliceTuple, Tuple};
use crate::relation::typing::Typing;
use crate::relation::value::Value;
use pest::iterators::{Pair, Pairs};
use std::collections::HashSet;

const STORAGE_ID_START: i64 = 10000;

impl<'s> Session<'s> {
    pub fn encode_definable_key(&self, name: &str, in_root: bool) -> OwnTuple {
        let depth_code = if in_root {
            0
        } else {
            self.get_stack_depth() as i64
        };
        let mut tuple = Tuple::with_null_prefix();
        tuple.push_str(name);
        tuple.push_int(depth_code);
        tuple
    }
    fn parse_cols(&self, pair: Pair<Rule>) -> Result<(Typing, Typing)> {
        let col_res = pair
            .into_inner()
            .map(|p| {
                let mut ps = p.into_inner();
                let mut name_ps = ps.next().unwrap().into_inner();
                let is_key;
                let mut name_p = name_ps.next().unwrap();
                match name_p.as_rule() {
                    Rule::key_marker => {
                        is_key = true;
                        name_p = name_ps.next().unwrap();
                    }
                    _ => is_key = false,
                }
                let name = build_name_in_def(name_p, true)?;
                let type_p = Typing::from_pair(ps.next().unwrap(), Some(self))?;
                Ok((is_key, name, type_p))
            })
            .collect::<Result<Vec<_>>>()?;
        let all_names = col_res.iter().map(|(_, n, _)| n).collect::<HashSet<_>>();
        if all_names.len() != col_res.len() {
            return Err(CozoError::DuplicateNames(
                col_res
                    .iter()
                    .map(|(_, n, _)| n.to_string())
                    .collect::<Vec<_>>(),
            ));
        }
        let (keys, cols): (Vec<_>, Vec<_>) = col_res.iter().partition(|(is_key, _, _)| *is_key);
        let keys_typing = Typing::NamedTuple(
            keys.iter()
                .map(|(_, n, t)| (n.to_string(), t.clone()))
                .collect(),
        );
        let vals_typing = Typing::NamedTuple(
            cols.iter()
                .map(|(_, n, t)| (n.to_string(), t.clone()))
                .collect(),
        );
        Ok((keys_typing, vals_typing))
    }
    #[allow(clippy::type_complexity)]
    fn parse_definition(
        &self,
        pair: Pair<Rule>,
        in_root: bool,
    ) -> Result<(bool, (String, OwnTuple, Vec<OwnTuple>))> {
        Ok(match pair.as_rule() {
            Rule::node_def => (true, self.parse_node_def(pair.into_inner(), in_root)?),
            Rule::edge_def => (true, self.parse_edge_def(pair.into_inner(), in_root)?),
            Rule::associate_def => (true, self.parse_assoc_def(pair.into_inner(), in_root)?),
            Rule::index_def => todo!(),
            Rule::type_def => (false, self.parse_type_def(pair.into_inner(), in_root)?),
            _ => unreachable!(),
        })
    }
    fn parse_assoc_def(
        &self,
        mut pairs: Pairs<Rule>,
        in_root: bool,
    ) -> Result<(String, OwnTuple, Vec<OwnTuple>)> {
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let src_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let src_tbl = match self.resolve(&src_name)? {
            Some(res) => res,
            None => return Err(CozoError::UndefinedType(src_name)),
        };
        let (_kind, src_global, src_id) = Self::extract_table_id(src_tbl)?;
        if in_root && !src_global {
            return Err(CozoError::LogicError(
                "Cannot have global edge with local nodes".to_string(),
            ));
        }

        let (keys_typing, vals_typing) = self.parse_cols(pairs.next().unwrap())?;
        if keys_typing.to_string() != "{}" {
            return Err(CozoError::LogicError(
                "Cannot have keys in assoc".to_string(),
            ));
        }

        let mut tuple = Tuple::with_data_prefix(DataKind::Assoc);
        tuple.push_bool(src_global);
        tuple.push_int(src_id);
        tuple.push_str(vals_typing.to_string());

        let mut for_src = Tuple::with_prefix(0);
        for_src.push_null();
        for_src.push_str(&src_name);
        for_src.push_int(if in_root {
            0
        } else {
            self.get_stack_depth() as i64
        });
        for_src.push_int(DataKind::Assoc as i64);
        for_src.push_str(&name);

        let mut for_src_i = Tuple::with_prefix(0);
        for_src_i.push_null();
        for_src_i.push_int(if in_root {
            0
        } else {
            self.get_stack_depth() as i64
        });
        for_src_i.push_str(&src_name);
        for_src_i.push_int(DataKind::Assoc as i64);
        for_src_i.push_str(&name);

        Ok((name, tuple, vec![for_src, for_src_i]))
    }
    fn parse_type_def(
        &self,
        mut pairs: Pairs<Rule>,
        _in_root: bool,
    ) -> Result<(String, OwnTuple, Vec<OwnTuple>)> {
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let typ = Typing::from_pair(pairs.next().unwrap(), Some(self))?;
        let mut data = Tuple::with_data_prefix(DataKind::Type);
        data.push_str(typ.to_string());
        Ok((name, data, vec![]))
    }

    fn parse_edge_def(
        &self,
        mut pairs: Pairs<Rule>,
        in_root: bool,
    ) -> Result<(String, OwnTuple, Vec<OwnTuple>)> {
        let src_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let src_tbl = match self.resolve(&src_name)? {
            Some(res) => res,
            None => return Err(CozoError::UndefinedType(src_name)),
        };
        let (kind, src_global, src_id) = Self::extract_table_id(src_tbl)?;
        if in_root && !src_global {
            return Err(CozoError::LogicError(
                "Cannot have global edge with local nodes".to_string(),
            ));
        }
        if kind != DataKind::Node {
            return Err(CozoError::UnexpectedDataKind(kind));
        }
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let dst_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let dst_tbl = match self.resolve(&dst_name)? {
            Some(res) => res,
            None => return Err(CozoError::UndefinedType(dst_name)),
        };
        let (kind, dst_global, dst_id) = Self::extract_table_id(dst_tbl)?;
        if in_root && !dst_global {
            return Err(CozoError::LogicError(
                "Cannot have global edge with local nodes".to_string(),
            ));
        }
        if kind != DataKind::Node {
            return Err(CozoError::UnexpectedDataKind(kind));
        }
        let (keys_typing, vals_typing) = match pairs.next() {
            Some(p) => self.parse_cols(p)?,
            None => (Typing::NamedTuple(vec![]), Typing::NamedTuple(vec![])),
        };

        let mut tuple = Tuple::with_data_prefix(DataKind::Edge);
        tuple.push_bool(src_global);
        tuple.push_int(src_id);
        tuple.push_bool(dst_global);
        tuple.push_int(dst_id);
        tuple.push_str(keys_typing.to_string());
        tuple.push_str(vals_typing.to_string());
        tuple.push_null(); // TODO default values for keys
        tuple.push_null(); // TODO default values for cols

        let mut index_data = Vec::with_capacity(2);

        let mut for_src = Tuple::with_prefix(0);
        for_src.push_null();
        for_src.push_str(&src_name);
        for_src.push_int(if in_root {
            0
        } else {
            self.get_stack_depth() as i64
        });
        for_src.push_int(DataKind::Edge as i64);
        for_src.push_str(&name);

        index_data.push(for_src);

        let mut for_src_i = Tuple::with_prefix(0);
        for_src_i.push_null();
        for_src_i.push_int(if in_root {
            0
        } else {
            self.get_stack_depth() as i64
        });
        for_src_i.push_str(&src_name);
        for_src_i.push_int(DataKind::Edge as i64);
        for_src_i.push_str(&name);

        index_data.push(for_src_i);

        if dst_name != src_name {
            let mut for_dst = Tuple::with_prefix(0);
            for_dst.push_null();
            for_dst.push_str(&dst_name);
            for_dst.push_int(if in_root {
                0
            } else {
                self.get_stack_depth() as i64
            });
            for_dst.push_int(DataKind::Edge as i64);
            for_dst.push_str(&name);

            index_data.push(for_dst);

            let mut for_dst_i = Tuple::with_prefix(0);
            for_dst_i.push_null();
            for_dst_i.push_int(if in_root {
                0
            } else {
                self.get_stack_depth() as i64
            });
            for_dst_i.push_str(&dst_name);
            for_dst_i.push_int(DataKind::Edge as i64);
            for_dst_i.push_str(&name);

            index_data.push(for_dst_i);
        }

        Ok((name, tuple, index_data))
    }

    fn extract_table_id<T: AsRef<[u8]>>(src_tbl: Tuple<T>) -> Result<(DataKind, bool, i64)> {
        let kind = src_tbl.data_kind()?;
        match kind {
            DataKind::Data | DataKind::Val | DataKind::Type => {
                return Err(CozoError::UnexpectedDataKind(kind))
            }
            _ => {}
        };
        let is_global = src_tbl.get_bool(0).expect("Data corrupt");
        let table_id = src_tbl.get_int(1).expect("Data corrupt");
        Ok((kind, is_global, table_id))
    }
    fn parse_node_def(
        &self,
        mut pairs: Pairs<Rule>,
        _in_root: bool,
    ) -> Result<(String, OwnTuple, Vec<OwnTuple>)> {
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let col_pair = pairs.next().unwrap();
        let (keys_typing, vals_typing) = self.parse_cols(col_pair)?;
        let mut tuple = Tuple::with_data_prefix(DataKind::Node);
        tuple.push_str(keys_typing.to_string());
        tuple.push_str(vals_typing.to_string());
        tuple.push_null(); // TODO default values for keys
        tuple.push_null(); // TODO default values for cols
        Ok((name, tuple, vec![]))
    }
    pub fn run_definition(&mut self, pair: Pair<Rule>) -> Result<()> {
        let in_root = match pair.as_rule() {
            Rule::global_def => true,
            Rule::local_def => false,
            r => panic!("Encountered definition with rule {:?}", r),
        };

        let (need_id, (name, mut tuple, assoc_defs)) =
            self.parse_definition(pair.into_inner().next().unwrap(), in_root)?;
        if need_id {
            let id = self.get_next_storage_id(in_root)?;
            tuple = tuple.insert_values_at(0, &[in_root.into(), id.into()]);
            let mut id_key = Tuple::with_null_prefix();
            id_key.push_bool(true);
            id_key.push_int(id);
            self.define_raw_key(&id_key, Some(&tuple), in_root).unwrap();
        }
        for t in assoc_defs {
            self.define_raw_key(&t, None, in_root).unwrap();
        }
        self.define_data(&name, tuple, in_root)
    }

    pub fn get_next_storage_id(&self, in_root: bool) -> Result<i64> {
        let mut key_entry = Tuple::with_null_prefix();
        key_entry.push_null();
        let db_res = if in_root {
            self.txn.get(true, &self.perm_cf, &key_entry)
        } else {
            self.txn.get(false, &self.temp_cf, &key_entry)
        };
        let u = if let Some(en) = db_res? {
            if let Value::Int(u) = Tuple::new(en).get(0).unwrap() {
                u
            } else {
                panic!("Unexpected value in storage id");
            }
        } else {
            STORAGE_ID_START
        };
        let mut new_data = Tuple::with_null_prefix();
        new_data.push_int(u + 1);
        if in_root {
            self.txn.put(true, &self.perm_cf, key_entry, new_data)?;
        } else {
            self.txn.put(false, &self.temp_cf, key_entry, new_data)?;
        }
        Ok(u + 1)
    }
    pub fn make_table_id_valid(&self, tid: &mut TableId) -> Result<()> {
        if !tid.is_valid() {
            tid.id = self.get_next_storage_id(tid.in_root)?;
        }
        Ok(())
    }

    pub fn table_data(&self, id: i64, in_root: bool) -> Result<Option<SliceTuple>> {
        let mut key = Tuple::with_null_prefix();
        key.push_bool(true);
        key.push_int(id);
        if in_root {
            let data = self.txn.get(true, &self.perm_cf, key)?;
            Ok(data.map(Tuple::new))
        } else {
            let data = self.txn.get(false, &self.temp_cf, key)?;
            Ok(data.map(Tuple::new))
        }
    }

    pub fn resolve_related_tables(&self, name: &str) -> Result<Vec<(String, SliceTuple)>> {
        let mut prefix = Tuple::with_prefix(0);
        prefix.push_null();
        prefix.push_str(name);
        let mut assocs = vec![];

        let it = self.txn.iterator(true, &self.perm_cf);
        it.seek(&prefix);
        while let Some(val) = unsafe { it.key() } {
            let cur = Tuple::new(val);
            if !cur.starts_with(&prefix) {
                break;
            }
            let name = cur
                .get_text(4)
                .ok_or_else(|| CozoError::LogicError("Bad data".to_string()))?;
            if let Some(data) = self.resolve(&name)? {
                if data.data_kind()? == DataKind::Assoc {
                    assocs.push((name.to_string(), data));
                }
            }
            it.next();
        }

        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&prefix);
        while let Some(val) = unsafe { it.key() } {
            let cur = Tuple::new(val);
            if !cur.starts_with(&prefix) {
                break;
            }
            let name = cur
                .get_text(4)
                .ok_or_else(|| CozoError::LogicError("Bad data".to_string()))?;
            if let Some(data) = self.resolve(&name)? {
                if data.data_kind()? == DataKind::Assoc {
                    assocs.push((name.to_string(), data));
                }
            }
            it.next();
        }

        Ok(assocs)
    }

    pub fn delete_defined(&mut self, name: &str, in_root: bool) -> Result<()> {
        let key = self.encode_definable_key(name, in_root);
        if in_root {
            self.txn.del(true, &self.perm_cf, key)?;
        } else {
            let it = self.txn.iterator(false, &self.temp_cf);
            it.seek(&key);
            if let Some(found_key) = unsafe { it.key() } {
                let found_key_tuple = Tuple::new(found_key);
                if found_key_tuple.starts_with(&key) {
                    let mut ikey = Tuple::with_null_prefix();
                    ikey.push_value(&found_key_tuple.get(1).unwrap());
                    ikey.push_value(&found_key_tuple.get(0).unwrap());
                    self.txn.del(false, &self.temp_cf, found_key_tuple)?;
                    self.txn.del(false, &self.temp_cf, ikey)?;
                }
            }
        }
        // TODO cleanup if the thing deleted is a table

        Ok(())
    }
}
