use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock};
use crate::error::{CozoError, Result};
use cozo_rocks::*;
use crate::env::Environment;
use crate::value::{cozo_comparator_v1};


pub struct RocksStorage {
    pub db: DB,
    #[allow(dead_code)]
    path: String,
    last_local_id: AtomicUsize,
    pub root_env: RwLock<Environment>,
}

const DEFAULT_CF: &str = "default";
const SCRATCH_CF: &str = "scratch";
const COMPARATOR_NAME: &str = "cozo_comparator_v1";

impl RocksStorage {
    #[allow(unused_variables)]
    pub fn new(path: String) -> Result<Self> {
        let options = Options::default()
            .increase_parallelism()
            .optimize_level_style_compaction()
            .set_create_if_missing(true)
            .set_comparator(COMPARATOR_NAME, cozo_comparator_v1);

        let db = DB::open(options, path.as_ref())?;
        (match db.create_column_family(SCRATCH_CF) {
            Err(s) if s.bridge_code == StatusBridgeCode::EXISTING_ERROR => Ok(()),
            v => v
        })?;
        let mut env = Environment::default();
        env.define_base_types();
        Ok(RocksStorage {
            db,
            path,
            last_local_id: AtomicUsize::new(0),
            root_env: RwLock::new(env),
        })
    }

    #[allow(unused_variables)]
    pub fn delete_storage(self) -> Result<()> {
        let path = self.path.clone();
        drop(self);
        fs::remove_dir_all(path)?;
        Ok(())
    }

    #[allow(unused_variables)]
    pub fn put_global(&self, k: &[u8], v: &[u8]) -> Result<()> {
        let default_cf = self.db.get_cf_handle(DEFAULT_CF)?;
        self.db.put(k, v, &default_cf, None)?;

        Ok(())
    }
    #[allow(unused_variables)]
    pub fn create_table(&self, name: &str, _global: bool) -> Result<()> {
        match self.db.create_column_family(table_name_to_cf_name(name)) {
            Ok(_) => Ok(()),
            Err(s) if s.bridge_code == StatusBridgeCode::EXISTING_ERROR => Ok(()),
            Err(e) => Err(CozoError::Storage(e))
        }
    }
    #[allow(unused_variables)]
    pub fn drop_table(&self, name: &str, _global: bool) -> Result<()> {
        self.db.drop_column_family(table_name_to_cf_name(name))?;
        Ok(())
    }

    pub fn get_next_local_id(&self, global: bool) -> usize {
        if global {
            0
        } else {
            self.last_local_id.fetch_add(1, Ordering::Relaxed) + 1
        }
    }
}

#[inline]
fn table_name_to_cf_name(name: &str) -> String {
    format!("${}", name)
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
        println!("{:?}", db.all_cf_names());

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