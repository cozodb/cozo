// single engine per db storage
// will be shared among threads


use cozorocks::*;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;

struct EngineOptions {
    cmp: RustComparatorPtr,
    options: OptionsPtr,
    t_options: TDBOptions,
    path: String,
}

pub struct Engine {
    db: DBPtr,
    options_store: Box<EngineOptions>,
    session_handles: Vec<Arc<SessionHandle>>,
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

impl Engine {
    pub fn new(path: String, optimistic: bool) -> Result<Self> {
        let t_options = if optimistic {
            TDBOptions::Optimistic(OptimisticTransactionDBOptionsPtr::default())
        } else {
            TDBOptions::Pessimistic(TransactionDBOptionsPtr::default())
        };
        let mut options = OptionsPtr::default();
        let cmp = RustComparatorPtr::new("cozo_cmp_v1", crate::relation::key_order::compare);
        options.set_comparator(&cmp).increase_parallelism().optimize_level_style_compaction().set_create_if_missing(true);
        let e_options = Box::new(EngineOptions { cmp, options, t_options, path });
        let db = DBPtr::open(&e_options.options, &e_options.t_options, &e_options.path)?;
        db.drop_non_default_cfs();
        Ok(Self {
            db,
            options_store: e_options,
            session_handles: vec![]
        })
    }
    pub fn session(&self) -> Session {
        // find a handle if there is one available
        // otherwise create
        todo!()
    }
}

pub struct Session<'a> {
    engine: &'a Engine,
    stack_depth: i32, // zero or negative
}
// every session has its own column family to play with
// metadata are stored in table 0

pub struct SessionHandle {
    cf_ident: String,
    status: SessionStatus,
    table_idx: AtomicUsize
}

pub enum SessionStatus {
    Prepared,
    Running,
    Completed,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;

    #[test]
    fn test_create() {
        let p1= "_test_db_create1";
        let p2 = "_test_db_create2";
        let p3 = "_test_db_create3";
        {
            {
                let engine = Engine::new(p1.to_string(), true);
                assert!(engine.is_ok());
                let engine = Engine::new(p2.to_string(), true);
                assert!(engine.is_ok());
                let engine = Engine::new(p3.to_string(), true);
                assert!(engine.is_ok());
                let engine2 = Engine::new(p1.to_string(), false);
                assert!(engine2.is_err());
            }
            let engine2 = Engine::new(p2.to_string(), false);
            assert!(engine2.is_ok());
            let engine2 = Engine::new(p3.to_string(), false);
            assert!(engine2.is_ok());
        }
        let _ = fs::remove_dir_all(p1);
        let _ = fs::remove_dir_all(p2);
        let _ = fs::remove_dir_all(p3);
    }
}