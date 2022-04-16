use crate::error::{CozoError, Result};
use cozo_rocks::*;
use crate::value::{cozo_comparator_v1};


pub struct RocksStorage {
    pub db: DB,
    #[allow(dead_code)]
    path: String,
}

impl RocksStorage {
    #[allow(unused_variables)]
    pub fn new(path: String) -> Result<Self> {
        let options = Options::default()
            .increase_parallelism()
            .optimize_level_style_compaction()
            .set_create_if_missing(true)
            .set_comparator("cozo_comparator_v1", cozo_comparator_v1);

        let db = DB::open(options, path.as_ref())?;
        Ok(RocksStorage {db, path})
    }

    #[allow(unused_variables)]
    pub fn delete(&mut self) -> Result<()> {
        // unimplemented!()
        // drop(self.db.take());
        // DB::destroy(&make_options(), &self.path)?;
        Ok(())
    }

    #[allow(unused_variables)]
    pub fn put_global(&self, k: &[u8], v: &[u8]) -> Result<()> {
        // let db = self.db.as_ref().ok_or(DatabaseClosed)?;
        let default_cf = self.db.get_cf_handle("default")?;
        self.db.put(k, v, &default_cf, None)?;

        Ok(())
    }
    #[allow(unused_variables)]
    pub fn create_table(&mut self, name: &str, _global: bool) -> Result<()> {
        match self.db.create_column_family(name) {
            Ok(_) => Ok(()),
            Err(s) if s.bridge_code == StatusBridgeCode::EXISTING_ERROR => Ok(()),
            Err(e) => Err(CozoError::Storage(e))
        }
    }
    #[allow(unused_variables)]
    pub fn drop_table(&mut self, name: &str, _global: bool) -> Result<()> {
        self.db.drop_column_family(name)?;
        Ok(())
    }
}

pub trait Storage {}

pub struct DummyStorage;

impl Storage for DummyStorage {}

impl Storage for RocksStorage {}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;
    use crate::value::{ByteArrayBuilder, cozo_comparator_v1, Value};

    #[test]
    fn import() {
        use cozo_rocks::*;
        let options = Options::default()
            .increase_parallelism()
            .optimize_level_style_compaction()
            .set_create_if_missing(true)
            .set_comparator("cozo_comparator_v1", cozo_comparator_v1);

        let db = DB::open(options,
                          "xxyyzz.db".as_ref()).unwrap();

        let mut builder = ByteArrayBuilder::default();
        builder.build_value(&Value::RefString("A key"));
        let key = builder;

        let mut builder = ByteArrayBuilder::default();
        builder.build_value(&Value::RefString("Another key"));
        let key2 = builder;
        let cf = db.get_cf_handle("default").unwrap();
        println!("{:?}", db.get_column_family_names());

        let val = db.get(&key, &cf, None).unwrap();
        println!("before anything {}", val.is_none());

        db.put(&key, "A motherfucking value!!! 👋👋👋", &cf, None).unwrap();
        let batch = WriteBatch::default();
        batch.put(&key2, "Another motherfucking value!!! 👋👋👋", &cf).unwrap();
        db.write(batch, None).unwrap();
        // db.put("Yes man", "A motherfucking value!!! 👋👋👋", None).unwrap();
        let val = db.get(&key, &cf, None).unwrap().unwrap();
        println!("1 {}", from_utf8(val.as_ref()).unwrap());
        let val = db.get(&key2, &cf, None).unwrap().unwrap();
        // let val = val.as_bytes();
        println!("2 {}", from_utf8(val.as_ref()).unwrap());
        let val = db.get(&key, &cf, None).unwrap().unwrap();
        println!("3 {}", from_utf8(val.as_ref()).unwrap());
        println!("4 {}", from_utf8(db.get(&key, &cf, None).unwrap().unwrap().as_ref()).unwrap());
    }
}