use cozorocks::SlicePtr;
use crate::db::engine::Session;
use crate::relation::value::Value;
use crate::error::{CozoError, Result};
use crate::relation::data::DataKind;
use crate::relation::tuple::{OwnTuple, SliceTuple, Tuple};

/// # layouts for sector 0
///
/// `[Null]`: stores information about table_ids
/// `[Text, Int]`: contains definable data and depth info
/// `[Int, Text]`: inverted index for depth info
/// `[Null, Text, Int, Int, Text]` inverted index for related tables
/// `[Null, Int, Text, Int, Text]` inverted index for related tables
/// `[True, Int]` table info, value is key


impl<'s> Session<'s> {
    pub fn define_variable(&mut self, name: &str, val: &Value, in_root: bool) -> Result<()> {
        let mut data = Tuple::with_data_prefix(DataKind::Val);
        data.push_value(val);
        self.define_data(name, data, in_root)
    }

    pub fn define_data(&mut self, name: &str, data: OwnTuple, in_root: bool) -> Result<()> {
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

    pub fn key_exists(&self, key: &OwnTuple, in_root: bool) -> Result<bool> {
        let res = self.txn.get(in_root, if in_root { &self.perm_cf } else { &self.temp_cf }, key)?;
        Ok(res.is_some())
    }

    pub fn del_key(&self, key: &OwnTuple, in_root: bool) -> Result<()> {
        self.txn.del(in_root, if in_root { &self.perm_cf } else { &self.temp_cf }, key)?;
        Ok(())
    }

    pub fn define_raw_key(&self, key: &OwnTuple, value: Option<&OwnTuple>, in_root: bool) -> Result<()> {
        if in_root {
            match value {
                None => {
                    self.txn.put(true, &self.perm_cf, key, "")?;
                }
                Some(v) => {
                    self.txn.put(true, &self.perm_cf, key, &v)?;
                }
            }
        } else {
            match value {
                None => {
                    self.txn.put(false, &self.temp_cf, key, "")?;
                }
                Some(v) => {
                    self.txn.put(false, &self.temp_cf, key, &v)?;
                }
            }
        }
        Ok(())
    }

    pub fn resolve_value(&self, name: &str) -> Result<Option<Value>> {
        match self.resolve(name)? {
            None => Ok(None),
            Some(t) => {
                match t.data_kind()? {
                    DataKind::Val => Ok(Some(t.get(0)
                        .ok_or_else(|| CozoError::LogicError("Corrupt".to_string()))?
                        .to_static())),
                    k => Err(CozoError::UnexpectedDataKind(k))
                }
            }
        }
    }
    pub fn get_stack_depth(&self) -> i32 {
        self.stack_depth
    }

    pub fn push_env(&mut self) -> Result<()> {
        if self.stack_depth <= -1024 {
            return Err(CozoError::LogicError("Stack overflow in env".to_string()));
        }
        self.stack_depth -= 1;
        Ok(())
    }

    pub fn pop_env(&mut self) -> Result<()> {
        // Remove all stuff starting with the stack depth from the temp session
        let mut prefix = Tuple::with_null_prefix();
        prefix.push_int(self.stack_depth as i64);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&prefix);
        let mut to_delete = vec![];
        while let Some(val) = it.key() {
            let cur = Tuple::new(val);
            if cur.starts_with(&prefix) {
                if let Some(name) = cur.get(1) {
                    let mut ikey = Tuple::with_null_prefix();
                    ikey.push_value(&name);
                    ikey.push_int(self.stack_depth as i64);

                    let data = self.txn.get(false, &self.temp_cf, &ikey)?
                        .ok_or_else(|| CozoError::LogicError("Bad format for ikey".to_string()))?;
                    let data = Tuple::new(data);
                    match data.data_kind()? {
                        DataKind::Node |
                        DataKind::Edge |
                        DataKind::Assoc |
                        DataKind::Index => {
                            let id = data.get_int(1).ok_or_else(|| CozoError::LogicError("Bad table index".to_string()))?;
                            let mut rkey = Tuple::with_null_prefix();
                            rkey.push_bool(true);
                            rkey.push_int(id);
                            self.txn.del(false, &self.temp_cf, rkey)?;
                            let range_start = Tuple::with_prefix(id as u32);
                            let mut range_end = Tuple::with_prefix(id as u32);
                            range_end.seal_with_sentinel();
                            self.txn.del_range(&self.temp_cf, range_start, range_end)?;
                        }
                        _ => {}
                    }
                    to_delete.push(cur.data.as_ref().to_vec());
                    to_delete.push(ikey.data.to_vec());
                }
                it.next();
            } else {
                break;
            }
        }

        let mut prefix = Tuple::with_null_prefix();
        prefix.push_null();
        prefix.push_int(self.stack_depth as i64);
        let it = self.txn.iterator(false, &self.temp_cf);
        it.seek(&prefix);
        while let Some(val) = it.key() {
            let cur = Tuple::new(val);
            if cur.starts_with(&prefix) {
                let mut ikey = Tuple::with_prefix(cur.get_prefix());
                ikey.push_null();
                ikey.push_str(cur.get_text(2).unwrap());
                ikey.push_int(cur.get_int(1).unwrap());
                for k in cur.iter().skip(3) {
                    ikey.push_value(&k);
                }

                to_delete.push(cur.data.as_ref().to_vec());
                to_delete.push(ikey.data.to_vec());
                it.next();
            } else {
                break;
            }
        }

        if self.stack_depth != 0 {
            self.stack_depth += 1;
        }

        for d in to_delete {
            self.txn.del(false, &self.temp_cf, &d)?;
        }

        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Result<Option<SliceTuple>> {
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
}