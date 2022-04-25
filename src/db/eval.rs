use std::collections::{BTreeMap, HashSet};
use std::process::id;
use pest::iterators::{Pair, Pairs};
use cozorocks::{SlicePtr, StatusCode};
use crate::db::engine::{Session};
use crate::relation::table::{Table};
use crate::relation::tuple::{OwnTuple, Tuple};
use crate::relation::typing::Typing;
use crate::relation::value::Value;
use crate::error::{CozoError, Result};
use crate::error::CozoError::UnexpectedDataKind;
use crate::relation::data::DataKind;
use crate::parser::Rule;
use crate::parser::text_identifier::build_name_in_def;

pub trait Environment<T: AsRef<[u8]>> where Self: Sized {
    fn get_next_storage_id(&mut self, in_root: bool) -> Result<u32>;
    fn get_stack_depth(&self) -> i32;
    fn push_env(&mut self);
    fn pop_env(&mut self) -> Result<()>;
    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) -> Result<()> {
        let mut data = Tuple::with_data_prefix(DataKind::Value);
        data.push_value(val);
        self.define_data(name, data, in_root)
    }

    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool) -> Result<()> {
        let mut data = Tuple::with_data_prefix(DataKind::TypeAlias);
        data.push_str(typ.to_string());
        self.define_data(name, data, in_root)
    }

    fn define_table(&mut self, table: &Table, in_root: bool) -> Result<()> {
        todo!()
    }
    fn resolve(&self, name: &str) -> Result<Option<Tuple<T>>>;
    fn delete_defined(&mut self, name: &str, in_root: bool) -> Result<()>;
    fn define_data(&mut self, name: &str, data: OwnTuple, in_root: bool) -> Result<()>;
    fn encode_definable_key(&self, name: &str, in_root: bool) -> OwnTuple {
        let depth_code = if in_root { 0 } else { self.get_stack_depth() as i64 };
        let mut tuple = Tuple::with_null_prefix();
        tuple.push_str(name);
        tuple.push_int(depth_code);
        tuple
    }
    fn parse_definition(&self, pair: Pair<Rule>) -> Result<(String, OwnTuple)> {
        match pair.as_rule() {
            Rule::node_def => self.parse_node_def(pair.into_inner()),
            Rule::edge_def => self.parse_edge_def(pair.into_inner()),
            Rule::associate_def => todo!(),
            Rule::index_def => todo!(),
            Rule::type_def => todo!(),
            _ => unreachable!()
        }
    }
    fn parse_cols(&self, pair: Pair<Rule>) -> Result<(Typing, Typing)> {
        let col_res = pair.into_inner().map(|p| {
            let mut ps = p.into_inner();
            let mut name_ps = ps.next().unwrap().into_inner();
            let is_key;
            let mut name_p = name_ps.next().unwrap();
            match name_p.as_rule() {
                Rule::key_marker => {
                    is_key = true;
                    name_p = name_ps.next().unwrap();
                }
                _ => { is_key = false }
            }
            let name = build_name_in_def(name_p, true)?;
            let type_p = Typing::from_pair(ps.next().unwrap(), Some(self))?;
            Ok((is_key, name, type_p))
        }).collect::<Result<Vec<_>>>()?;
        let all_names = col_res.iter().map(|(_, n, _)| n).collect::<HashSet<_>>();
        if all_names.len() != col_res.len() {
            return Err(CozoError::DuplicateNames(col_res.iter().map(|(_, n, _)| n.to_string()).collect::<Vec<_>>()));
        }
        let (keys, cols): (Vec<_>, Vec<_>) = col_res.iter().partition(|(is_key, _, _)| *is_key);
        let keys_typing = Typing::NamedTuple(keys.iter().map(|(_, n, t)| (n.to_string(), t.clone())).collect());
        let vals_typing = Typing::NamedTuple(cols.iter().map(|(_, n, t)| (n.to_string(), t.clone())).collect());
        Ok((keys_typing, vals_typing))
    }
    fn parse_edge_def(&self, mut pairs: Pairs<Rule>) -> Result<(String, OwnTuple)> {
        let src_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let src_tbl = match self.resolve(&src_name)? {
            Some(res) => res,
            None => return Err(CozoError::UndefinedType(src_name))
        };
        let (kind, src_id) = Self::extract_table_id(src_tbl)?;
        if kind != DataKind::Node {
            return Err(CozoError::UnexpectedDataKind(kind));
        }
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let dst_name = build_name_in_def(pairs.next().unwrap(), true)?;
        let dst_tbl = match self.resolve(&dst_name)? {
            Some(res) => res,
            None => return Err(CozoError::UndefinedType(dst_name))
        };
        let (kind, dst_id) = Self::extract_table_id(dst_tbl)?;
        if kind != DataKind::Node {
            return Err(CozoError::UnexpectedDataKind(kind));
        }
        let (keys_typing, vals_typing) = match pairs.next() {
            Some(p) => self.parse_cols(p)?,
            None => (Typing::NamedTuple(vec![]), Typing::NamedTuple(vec![]))
        };
        let mut tuple = Tuple::with_data_prefix(DataKind::Edge);
        tuple.push_str(keys_typing.to_string());
        tuple.push_str(vals_typing.to_string());
        tuple.push_null(); // TODO default values for keys
        tuple.push_null(); // TODO default values for cols
        tuple.push_uint(src_id);
        tuple.push_uint(dst_id);
        Ok((name, tuple))
    }

    fn extract_table_id(src_tbl: Tuple<T>) -> Result<(DataKind, u64)> {
        let kind = src_tbl.data_kind()?;
        let id_idx = match kind {
            DataKind::DataTuple => return Err(CozoError::UnexpectedDataKind(kind)),
            DataKind::Node => 4,
            DataKind::Edge => 6,
            DataKind::Associate => todo!(),
            DataKind::Index => todo!(),
            DataKind::Value => return Err(CozoError::UnexpectedDataKind(kind)),
            DataKind::TypeAlias => return Err(CozoError::UnexpectedDataKind(kind)),
        };
        let table_id = match src_tbl.get(id_idx).expect("Data corrupt") {
            Value::UInt(u) => u,
            _ => panic!("Data corrupt")
        };
        Ok((kind, table_id))
    }
    fn parse_node_def(&self, mut pairs: Pairs<Rule>) -> Result<(String, OwnTuple)> {
        let name = build_name_in_def(pairs.next().unwrap(), true)?;
        let col_pair = pairs.next().unwrap();
        let (keys_typing, vals_typing) = self.parse_cols(col_pair)?;
        let mut tuple = Tuple::with_data_prefix(DataKind::Node);
        tuple.push_str(keys_typing.to_string());
        tuple.push_str(vals_typing.to_string());
        tuple.push_null(); // TODO default values for keys
        tuple.push_null(); // TODO default values for cols
        Ok((name, tuple))
    }
    fn run_definition(&mut self, pair: Pair<Rule>) -> Result<()> {
        let in_root = match pair.as_rule() {
            Rule::global_def => true,
            Rule::local_def => false,
            _ => unreachable!()
        };

        let (name, mut tuple) = self.parse_definition(pair.into_inner().next().unwrap())?;
        let id = self.get_next_storage_id(in_root)?;
        tuple.push_uint(id as u64);
        self.define_data(&name, tuple, in_root)
    }
}

pub struct MemoryEnv {
    root: BTreeMap<String, OwnTuple>,
    stack: Vec<BTreeMap<String, OwnTuple>>,
    max_storage_id: u32,
}

impl Default for MemoryEnv {
    fn default() -> Self {
        MemoryEnv { root: BTreeMap::default(), stack: vec![BTreeMap::default()], max_storage_id: 0 }
    }
}

impl Environment<Vec<u8>> for MemoryEnv {
    fn get_next_storage_id(&mut self, _in_root: bool) -> Result<u32> {
        self.max_storage_id += 1;
        Ok(self.max_storage_id)
    }

    fn get_stack_depth(&self) -> i32 {
        -(self.stack.len() as i32)
    }

    fn push_env(&mut self) {
        self.stack.push(BTreeMap::default());
    }

    fn pop_env(&mut self) -> Result<()> {
        if self.stack.len() > 1 {
            self.stack.pop();
        }
        Ok(())
    }

    fn resolve(&self, name: &str) -> Result<Option<OwnTuple>> {
        for layer in self.stack.iter() {
            if let Some(res) = layer.get(name) {
                return Ok(Some(res.clone()));
            }
        }
        Ok(self.root.get(name).cloned())
    }

    fn delete_defined(&mut self, name: &str, in_root: bool) -> Result<()> {
        if in_root {
            self.root.remove(name);
        } else {
            for layer in self.stack.iter_mut().rev() {
                if let Some(_) = layer.remove(name) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn define_data(&mut self, name: &str, data: OwnTuple, in_root: bool) -> Result<()> {
        if in_root {
            self.root.insert(name.to_string(), data);
        } else {
            let last = self.stack.last_mut().unwrap();
            last.insert(name.to_string(), data);
        }
        Ok(())
    }
}


impl<'a> Environment<SlicePtr> for Session<'a> {
    fn get_next_storage_id(&mut self, in_root: bool) -> Result<u32> {
        // TODO: deal with wrapping problem
        let mut key_entry = Tuple::with_null_prefix();
        key_entry.push_null();
        let db_res = if in_root {
            self.txn.get(true, &self.perm_cf, &key_entry)
        } else {
            self.txn.get(false, &self.temp_cf, &key_entry)
        };
        let u = if let Some(en) = db_res? {
            if let Value::UInt(u) = Tuple::new(en).get(0).unwrap() {
                u
            } else {
                panic!("Unexpected value in storage id");
            }
        } else { 0 };
        let mut new_data = Tuple::with_null_prefix();
        new_data.push_uint(u + 1);
        if in_root {
            self.txn.put(true, &self.perm_cf, key_entry, new_data)?;
        } else {
            self.txn.put(false, &self.temp_cf, key_entry, new_data)?;
        }
        Ok((u + 1) as u32)
    }

    fn get_stack_depth(&self) -> i32 {
        self.stack_depth
    }

    fn push_env(&mut self) {
        self.stack_depth -= 1;
    }

    fn pop_env(&mut self) -> Result<()> {
        // Remove all stuff starting with the stack depth from the temp session
        let mut prefix = Tuple::with_null_prefix();
        prefix.push_int(self.stack_depth as i64);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&prefix);
        for val in it.keys() {
            let cur = Tuple::new(val);
            if cur.starts_with(&prefix) {
                if let Some(name) = cur.get(1) {
                    let mut ikey = Tuple::with_null_prefix();
                    ikey.push_value(&name);
                    ikey.push_int(self.stack_depth as i64);

                    self.txn.del(false, &self.temp_cf, cur)?;
                    self.txn.del(false, &self.temp_cf, ikey)?;
                }
            } else {
                break;
            }
        }

        if self.stack_depth != 0 {
            self.stack_depth += 1;
        }
        Ok(())
    }

    fn resolve(&self, name: &str) -> Result<Option<Tuple<SlicePtr>>> {
        let mut tuple = Tuple::with_null_prefix();
        tuple.push_str(name);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&tuple);
        if let Some((tk, vk)) = it.pair() {
            let k = Tuple::new(tk);
            if k.starts_with(&tuple) {
                return Ok(Some(Tuple::new(vk)));
            }
        }
        let root_key = self.encode_definable_key(name, true);
        let res = self.txn.get(true, &self.perm_cf, root_key).map(|v| v.map(Tuple::new))?;
        Ok(res)
    }

    fn delete_defined(&mut self, name: &str, in_root: bool) -> Result<()> {
        let key = self.encode_definable_key(name, in_root);
        if in_root {
            self.txn.del(true, &self.perm_cf, key)?;
        } else {
            let it = self.txn.iterator(false, &self.temp_cf);
            it.seek(&key);
            if let Some(found_key) = it.key() {
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

    fn define_data(&mut self, name: &str, data: OwnTuple, in_root: bool) -> Result<()> {
        let key = self.encode_definable_key(name, in_root);
        if in_root {
            self.txn.put(true, &self.perm_cf, key, data)?;
        } else {
            let mut ikey = Tuple::with_null_prefix();
            ikey.push_int(self.stack_depth as i64);
            ikey.push_str(name);
            self.txn.put(false, &self.temp_cf, key, data)?;
            self.txn.put(false, &self.temp_cf, ikey, "")?;
        }
        Ok(())
    }
}


impl<'a> Session<'a> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;
    use pest::Parser as PestParser;
    use crate::db::eval::MemoryEnv;

    #[test]
    fn node() {
        let s = r#"
            create node "Person" {
                *id: Int,
                name: Text,
                email: ?Text,
                habits: ?[?Text]
            }

            create edge (Person)-[Friend]->(Person) {
                relation: ?Text
            }
        "#;
        let mut parsed = Parser::parse(Rule::file, s).unwrap();
        let first_t = parsed.next().unwrap();
        let second_t = parsed.next().unwrap();
        let mut env = MemoryEnv::default();
        env.run_definition(first_t).unwrap();
        println!("{:?}", env.resolve("Person"));
        env.run_definition(second_t).unwrap();
        println!("{:?}", env.resolve("Friend"));
    }
}