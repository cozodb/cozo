use cozorocks::SlicePtr;
use crate::db::engine::{Session};
use crate::relation::table::{Table};
use crate::relation::tuple::{Tuple};
use crate::relation::typing::Typing;
use crate::relation::value::Value;
use crate::error::Result;
use crate::relation::data::DataKind;

pub trait Environment<T: AsRef<[u8]>> {
    fn push_env(&mut self);
    fn pop_env(&mut self) -> Result<()>;
    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) -> Result<()>;
    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool) -> Result<()>;
    fn define_table(&mut self, table: &Table, in_root: bool) -> Result<()>;
    fn resolve(&self, name: &str) -> Result<Option<Tuple<T>>>;
    fn delete_defined(&mut self, name: &str, in_root: bool) -> Result<()>;
}


impl<'a> Session<'a> {
    fn encode_definable_key(&self, name: &str, in_root: bool) -> Tuple<Vec<u8>> {
        let depth_code = if in_root { 0 } else { self.stack_depth as i64 };
        let mut tuple = Tuple::with_null_prefix();
        tuple.push_str(name);
        tuple.push_int(depth_code);
        tuple
    }

    fn define_data(&mut self, name: &str, data: Tuple<Vec<u8>>, in_root: bool) -> Result<()> {
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


impl<'a> Environment<SlicePtr> for Session<'a> {
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

    fn resolve(&self, name: &str) -> Result<Option<Tuple<SlicePtr>>> {
        let mut tuple = Tuple::with_null_prefix();
        tuple.push_str(name);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&tuple);
        Ok(match it.pair() {
            None => {
                None
            }
            Some((tk, vk)) => {
                let k = Tuple::new(tk);
                if k.starts_with(&tuple) {
                    println!("Resolved to key {:?}", k);
                    Some(Tuple::new(vk))
                } else {
                    None
                }
            }
        })
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
}