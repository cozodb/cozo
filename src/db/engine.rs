// single engine per db storage
// will be shared among threads


use cozorocks::*;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use uuid::v1::{Context, Timestamp};
use rand::Rng;

struct EngineOptions {
    cmp: RustComparatorPtr,
    options: OptionsPtr,
    t_options: TDBOptions,
    path: String,
    uuid_ctx: Context,
}

pub struct Engine {
    db: DBPtr,
    options_store: Box<EngineOptions>,
    session_handles: RwLock<Vec<Arc<RwLock<SessionHandle>>>>,
}

unsafe impl Send for Engine {}

unsafe impl Sync for Engine {}

impl Engine {
    pub fn new(path: String, optimistic: bool) -> Result<Self> {
        let t_options = if optimistic {
            TDBOptions::Optimistic(OTxnDBOptionsPtr::default())
        } else {
            TDBOptions::Pessimistic(PTxnDBOptionsPtr::default())
        };
        let cmp = RustComparatorPtr::new("cozo_cmp_v1", crate::relation::key_order::compare);
        let mut options = OptionsPtr::default();
        options
            .set_comparator(&cmp)
            .increase_parallelism()
            .optimize_level_style_compaction()
            .set_create_if_missing(true)
            .set_paranoid_checks(false);
        let mut rng = rand::thread_rng();
        let uuid_ctx = Context::new(rng.gen());

        let e_options = Box::new(EngineOptions {
            cmp,
            options,
            t_options,
            path,
            uuid_ctx,
        });
        let db = DBPtr::open(&e_options.options, &e_options.t_options, &e_options.path)?;
        db.drop_non_default_cfs();
        Ok(Self {
            db,
            options_store: e_options,
            session_handles: RwLock::new(vec![]),
        })
    }
    pub fn session(&self) -> Session {
        // find a handle if there is one available
        // otherwise create a new one
        let old_handle = self.session_handles.read().unwrap().iter().find(|v| {
            match v.read() {
                Ok(content) => content.status == SessionStatus::Completed,
                Err(_) => false
            }
        }).cloned();
        let handle = match old_handle {
            None => {
                let now = SystemTime::now();
                let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                let ts = Timestamp::from_unix(
                    &self.options_store.uuid_ctx,
                    since_epoch.as_secs(),
                    since_epoch.subsec_nanos(),
                );
                let mut rng = rand::thread_rng();
                let id = Uuid::new_v1(ts, &[rng.gen(), rng.gen(), rng.gen(), rng.gen(), rng.gen(), rng.gen()]).unwrap();
                let cf_ident = id.to_string();
                self.db.create_cf(&self.options_store.options, &cf_ident).unwrap();

                let ret = Arc::new(RwLock::new(SessionHandle {
                    cf_ident,
                    status: SessionStatus::Prepared,
                    table_count: 0,
                }));
                self.session_handles.write().unwrap().push(ret.clone());
                ret
            }
            Some(h) => h.clone()
        };

        return Session {
            engine: self,
            stack_depth: 0,
            handle,
        };
    }
}

pub struct Session<'a> {
    engine: &'a Engine,
    stack_depth: i32,
    // zero or negative
    handle: Arc<RwLock<SessionHandle>>,
}
// every session has its own column family to play with
// metadata are stored in table 0

#[derive(Clone, Debug)]
pub struct SessionHandle {
    cf_ident: String,
    status: SessionStatus,
    table_count: usize,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum SessionStatus {
    Prepared,
    Running,
    Completed,
}

#[cfg(test)]
mod tests {
    use std::{fs, thread};
    use super::*;

    #[test]
    fn test_create() {
        let p1 = "_test_db_create1";
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
            let engine2 = Arc::new(Engine::new(p3.to_string(), false).unwrap());
            {
                for _i in 0..10 {
                    let _sess = engine2.session();
                }
                let handles = engine2.session_handles.read().unwrap();
                println!("got handles {}", handles.len());
                let cf_ident = &handles.first().unwrap().read().unwrap().cf_ident;
                println!("Opening ok {}", cf_ident);
                let cf = engine2.db.get_cf(cf_ident).unwrap();
                assert!(!cf.is_null());
                println!("Getting CF ok");
            }
            let mut thread_handles = vec![];

            println!("concurrent");
            for i in 0..10 {
                let engine = engine2.clone();
                thread_handles.push(thread::spawn(move || {
                    println!("In thread {}", i);
                    let _sess = engine.session();
                    println!("In thread {} end", i);
                }))
            }


            for t in thread_handles {
                t.join().unwrap();
            }
            println!("All OK");
            {
                let handles = engine2.session_handles.read().unwrap();
                println!("got handles {:#?}", handles.iter().map(|h| h.read().unwrap().cf_ident.to_string()).collect::<Vec<_>>());
            }
        }
        let _ = fs::remove_dir_all(p1);
        let _ = fs::remove_dir_all(p2);
        let _ = fs::remove_dir_all(p3);
    }
}