use cozorocks::SlicePtr;
use crate::db::engine::{Session};
use crate::relation::table::{DataKind, Table};
use crate::relation::tuple::{Tuple};
use crate::relation::typing::Typing;
use crate::relation::value::Value;

pub trait Environment<T: AsRef<[u8]>> {
    fn push_env(&mut self);
    fn pop_env(&mut self);
    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool);
    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool);
    fn define_table(&mut self, table: &Table, in_root: bool);
    fn resolve(&mut self, name: &str) -> Option<Tuple<T>>;
    fn delete_defined(&mut self, name: &str, in_root: bool);
}


impl<'a> Session<'a> {
    fn encode_definable_key(&self, name: &str, in_root: bool) -> Tuple<Vec<u8>> {
        let depth_code = if in_root { 0 } else { self.stack_depth as i64 };
        let mut tuple = Tuple::with_prefix(0);
        tuple.push_str(name);
        tuple.push_int(depth_code);
        tuple
    }
}


impl<'a> Environment<SlicePtr> for Session<'a> {
    fn push_env(&mut self) {
        self.stack_depth -= 1;
    }

    fn pop_env(&mut self) {
        if self.stack_depth == 0 {
            return;
        }
        // Remove all stuff starting with the stack depth from the temp session
        let mut prefix = Tuple::with_prefix(0);
        prefix.push_int(self.stack_depth as i64);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&prefix);
        for val in it.keys() {
            let cur = Tuple::new(val);
            if cur.starts_with(&prefix) {
                let name = cur.get(1).unwrap();
                let mut ikey = Tuple::with_prefix(0);
                ikey.push_value(&name);
                ikey.push_int(self.stack_depth as i64);

                self.txn.del(false, &self.temp_cf, cur).unwrap();
                self.txn.del(false, &self.temp_cf, ikey).unwrap();
            } else {
                break;
            }
        }

        self.stack_depth += 1;
    }

    fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) {
        let key = self.encode_definable_key(name, in_root);
        let mut data = Tuple::with_prefix(DataKind::Value as u32);
        data.push_value(val);
        if in_root {
            self.txn.put(true, &self.perm_cf, key, data).unwrap();
        } else {
            let mut ikey = Tuple::with_prefix(0);
            ikey.push_int(self.stack_depth as i64);
            ikey.push_str(name);
            self.txn.put(false, &self.temp_cf, key, data).unwrap();
            self.txn.put(false, &self.temp_cf, ikey, "").unwrap();
        }
    }

    fn define_type_alias(&mut self, name: &str, typ: &Typing, in_root: bool) {
        todo!()
    }

    fn define_table(&mut self, table: &Table, in_root: bool) {
        todo!()
    }

    fn resolve(&mut self, name: &str) -> Option<Tuple<SlicePtr>> {
        let mut tuple = Tuple::with_prefix(0);
        tuple.push_str(name);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&tuple);
        match it.pair() {
            None => {
                None
            }
            Some((tk, vk)) => {
                let k = Tuple::new(tk);
                if k.starts_with(&tuple) {
                    println!("Resolved to key {:?}", k);
                    let vt = Tuple::new(vk);
                    // let v = vt.iter().collect::<Vec<_>>();
                    Some(vt)
                } else {
                    None
                }
            }
        }
    }

    fn delete_defined(&mut self, name: &str, in_root: bool) {
        todo!()
    }
}