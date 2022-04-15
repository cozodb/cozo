use crate::error::Result;


pub struct Storage {
    pub db: Option<()>,
    #[allow(dead_code)]
    path: String,
}

impl Storage {
    pub fn no_storage() -> Self {
        Self { db: None, path: "".to_string() }
    }
    #[allow(unused_variables)]
    pub fn new(path: String) -> Result<Self> {
        unimplemented!()
        // let options = make_options();
        // let cfs = match DB::list_cf(&options, &path) {
        //     Ok(cfs) => { cfs }
        //     Err(_) => { vec![] }
        // };
        // let cfs = cfs.into_iter().map(|name| {
        //     ColumnFamilyDescriptor::new(name, make_options())
        // });
        // let db = DB::open_cf_descriptors(&options, &path, cfs)?;
        // Ok(Storage { db: Some(db), path })
    }
    #[allow(unused_variables)]
    pub fn delete(&mut self) -> Result<()> {
        unimplemented!()
        // drop(self.db.take());
        // DB::destroy(&make_options(), &self.path)?;
        // Ok(())
    }
    #[allow(unused_variables)]
    pub fn put_global(&self, k: &[u8], v: &[u8]) -> Result<()> {
        // let db = self.db.as_ref().ok_or(DatabaseClosed)?;
        // db.put(k, v)?;
        unimplemented!()
        // Ok(())
    }
    #[allow(unused_variables)]
    pub fn create_table(&mut self, name: &str, _global: bool) -> Result<()> {
        unimplemented!()
        // let db = self.db.as_mut().ok_or(DatabaseClosed)?;
        // db.create_cf(name, &make_options())?;
        // Ok(())
    }
    #[allow(unused_variables)]
    pub fn drop_table(&mut self, name: &str, _global: bool) -> Result<()> {
        unimplemented!()
        // let db = self.db.as_mut().ok_or(DatabaseClosed)?;
        // db.drop_cf(name)?;
        // Ok(())
    }
}

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