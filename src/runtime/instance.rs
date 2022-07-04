// use crate::data::compare::DB_KEY_PREFIX_LEN;
// use anyhow::Result;
// use cozorocks::*;
// use std::sync::atomic::{AtomicU32, AtomicU64};
// use std::sync::{Arc, Mutex};
//
// pub struct DbInstance {
//x     pub destroy_on_close: bool,
//     db: SharedPtr<DbBridge>,
//x     db_opts: UniquePtr<Options>,
//x     tdb_opts: Option<UniquePtr<TransactionDBOptions>>,
//x     odb_opts: Option<UniquePtr<OptimisticTransactionDBOptions>>,
//x     path: String,
//     last_attr_id: Arc<AtomicU32>,
//     last_ent_id: Arc<AtomicU64>,
//     last_tx_id: Arc<AtomicU64>,
//     sessions: Mutex<Vec<Arc<Mutex<SessionHandle>>>>,
// }
//
// struct SessionHandle {
//     id: usize,
//     temp: SharedPtr<DbBridge>,
//     status: SessionStatus,
// }
//
// #[derive(Eq, PartialEq, Debug, Clone, Copy)]
// pub enum SessionStatus {
//     Prepared,
//     Running,
//     Completed,
// }
//
// impl DbInstance {
//     pub fn new(path: &str, optimistic: bool, destroy_on_close: bool) -> Result<Self> {
//         let mut db_opts = Options::new().within_unique_ptr();
//         set_opts_create_if_missing(db_opts.pin_mut(), true);
//         set_opts_bloom_filter(db_opts.pin_mut(), 10., true);
//         set_opts_capped_prefix_extractor(db_opts.pin_mut(), DB_KEY_PREFIX_LEN);
//
//         let (db, tdb_opts, odb_opts) = if optimistic {
//             let o = new_odb_opts();
//             // let db = DbBridge::new_odb(path, &db_opts, &o)?;
//             let db = todo!();
//             (db, None, Some(o))
//         } else {
//             let o = new_tdb_opts();
//             let db = DbBridge::new_tdb(path, &db_opts, &o)?;
//             // let db = todo!();
//             (db, Some(new_tdb_opts()), None)
//         };
//         //
//         // Ok(Self {
//         //     db,
//         //     db_opts,
//         //     tdb_opts,
//         //     odb_opts,
//         //     path: path.to_string(),
//         //     destroy_on_close,
//         //     last_attr_id: Arc::new(Default::default()),
//         //     last_ent_id: Arc::new(Default::default()),
//         //     last_tx_id: Arc::new(Default::default()),
//         //     sessions: Mutex::new(vec![]),
//         // })
//         todo!()
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::data::compare::RUSTY_COMPARATOR;
//     use super::*;
//
//     #[test]
//     fn test_create() {
//         let mut opts = Options::new().within_unique_ptr();
//         // set_opts_comparator(opts.pin_mut(), &RUSTY_COMPARATOR);
//         set_opts_create_if_missing(opts.pin_mut(), true);
//         set_opts_bloom_filter(opts.pin_mut(), 10., true);
//         set_opts_capped_prefix_extractor(opts.pin_mut(), DB_KEY_PREFIX_LEN);
//         let db_ = DbBridge::new_raw_db("_test", &opts).unwrap();
//         //
//         // let o = new_odb_opts();
//         // let db = DbBridge::new_odb("_test2", &opts, &o).unwrap();
//         //
//         // let o = new_tdb_opts();
//         // let db = DbBridge::new_tdb("_test21", &opts, &o).unwrap();
//         //
//         //
//         // dbg!(12345);
//
//         // let db = DbInstance::new("_test3", false, true).unwrap();
//     }
// }
