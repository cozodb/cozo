use rocksdb::{DB, Options, ColumnFamilyDescriptor};
use crate::error::Result;
use crate::value::cozo_comparator_v1;


pub struct Storage {
    db: Option<DB>,
    options: Options,
    path: String,
}

impl Storage {
    pub fn new(path: String) -> Result<Self> {
        let mut options = Options::default();

        options.create_missing_column_families(true);
        options.create_if_missing(true);
        options.set_comparator("cozo_comparator_v1", cozo_comparator_v1);

        let main_cf = ColumnFamilyDescriptor::new("main", options.clone());
        let temp_cf = ColumnFamilyDescriptor::new("temp", options.clone());
        let db = DB::open_cf_descriptors(&options, &path, vec![main_cf, temp_cf])?;

        Ok(Storage { db: Some(db), options, path })
    }
    pub fn delete(&mut self) -> Result<()> {
        drop(self.db.take());
        DB::destroy(&self.options, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage() {
        let mut s = Storage::new("_path_for_rocksdb_storage".to_string()).unwrap();
        s.delete().unwrap();
    }
}