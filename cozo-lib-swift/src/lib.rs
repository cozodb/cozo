use cozo::*;

#[swift_bridge::bridge]
mod ffi {
    extern "Rust" {
        type DbInstance;

        fn new_db(kind: &str, path: &str, options: &str) -> Option<DbInstance>;

        #[swift_bridge(associated_to = DbInstance)]
        fn run_script_str(&self, payload: &str, params: &str) -> String;
        fn export_relations_str(&self, relations_str: &str) -> String;
        fn import_relation_str(&self, data: &str) -> String;
        fn backup_db_str(&self, out_file: &str) -> String;
        fn restore_backup_str(&self, in_file: &str) -> String;
    }
}

fn new_db(kind: &str, path: &str, options: &str) -> Option<DbInstance> {
    let options = if options.is_empty() { "{}" } else { options };
    match DbInstance::new_with_str(kind, path, options) {
        Ok(db) => Some(db),
        Err(err) => {
            eprintln!("{}", err);
            None
        }
    }
}
