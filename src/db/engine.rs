// single engine per db storage
// will be shared among threads


use std::collections::BTreeMap;
use cozorocks::*;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use uuid::v1::{Context, Timestamp};
use rand::Rng;
use crate::error::{CozoError, Result};
use crate::error::CozoError::{Poisoned, SessionErr};
use crate::relation::tuple::Tuple;
use crate::relation::value::{StaticValue, Value};

pub struct EngineOptions {
    cmp: RustComparatorPtr,
    options: OptionsPtr,
    t_options: TDBOptions,
    path: String,
    uuid_ctx: Context,
}

pub struct Engine {
    pub db: DBPtr,
    pub options_store: Box<EngineOptions>,
    session_handles: Mutex<Vec<Arc<RwLock<SessionHandle>>>>,
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
        let cmp = RustComparatorPtr::new(
            "cozo_cmp_v1",
            crate::relation::key_order::compare,
            false);
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
            session_handles: Mutex::new(vec![]),
        })
    }
    pub fn session(&self) -> Result<Session> {
        // find a handle if there is one available
        // otherwise create a new one
        let mut guard = self.session_handles.lock().map_err(|_| CozoError::Poisoned)?;
        let old_handle = guard.iter().find(|v| {
            match v.read() {
                Ok(content) => content.status == SessionStatus::Completed,
                Err(_) => false
            }
        }).cloned();
        let handle = match old_handle {
            None => {
                let now = SystemTime::now();
                let since_epoch = now.duration_since(UNIX_EPOCH)?;
                let ts = Timestamp::from_unix(
                    &self.options_store.uuid_ctx,
                    since_epoch.as_secs(),
                    since_epoch.subsec_nanos(),
                );
                let mut rng = rand::thread_rng();
                let id = Uuid::new_v1(ts, &[rng.gen(), rng.gen(), rng.gen(), rng.gen(), rng.gen(), rng.gen()])?;
                let cf_ident = id.to_string();
                self.db.create_cf(&self.options_store.options, &cf_ident)?;

                let ret = Arc::new(RwLock::new(SessionHandle {
                    cf_ident,
                    status: SessionStatus::Prepared,
                }));
                guard.push(ret.clone());
                ret
            }
            Some(h) => h
        };

        let mut sess = Session {
            engine: self,
            stack_depth: 0,
            txn: TransactionPtr::null(),
            perm_cf: SharedPtr::null(),
            temp_cf: SharedPtr::null(),
            handle,
            params: BTreeMap::default()
        };
        sess.start()?;
        Ok(sess)
    }
}

pub struct Session<'a, 'b> {
    pub engine: &'a Engine,
    pub stack_depth: i32,
    pub handle: Arc<RwLock<SessionHandle>>,
    pub txn: TransactionPtr,
    pub perm_cf: SharedPtr<ColumnFamilyHandle>,
    pub temp_cf: SharedPtr<ColumnFamilyHandle>,
    pub params: BTreeMap<String, &'b str>,
}
// every session has its own column family to play with
// metadata are stored in table 0

impl<'a, 'b> Session<'a, 'b> {
    pub fn start(&mut self) -> Result<()> {
        self.perm_cf = self.engine.db.default_cf();
        assert!(!self.perm_cf.is_null());
        self.temp_cf = self.engine.db.get_cf(&self.handle.read().map_err(|_| Poisoned)?.cf_ident).ok_or(SessionErr)?;
        assert!(!self.temp_cf.is_null());
        let t_options = match self.engine.options_store.t_options {
            TDBOptions::Pessimistic(_) => {
                TransactOptions::Pessimistic(PTxnOptionsPtr::default())
            }
            TDBOptions::Optimistic(_) => {
                TransactOptions::Optimistic(OTxnOptionsPtr::new(&self.engine.options_store.cmp))
            }
        };
        let mut r_opts = ReadOptionsPtr::default();
        r_opts.set_total_order_seek(true);
        let mut rx_opts = ReadOptionsPtr::default();
        rx_opts.set_total_order_seek(true);
        let w_opts = WriteOptionsPtr::default();
        let mut wx_opts = WriteOptionsPtr::default();
        wx_opts.set_disable_wal(true);
        self.txn = self.engine.db.make_transaction(t_options, r_opts, rx_opts, w_opts, wx_opts);
        if self.txn.is_null() {
            panic!("Starting session failed as opening transaction failed");
        }
        self.handle.write().map_err(|_| Poisoned)?.status = SessionStatus::Running;
        Ok(())
    }
    pub fn commit(&mut self) -> Result<()> {
        self.txn.commit()?;
        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.txn.rollback()?;
        Ok(())
    }
    pub fn finish_work(&mut self) -> Result<()> {
        self.txn.del_range(&self.temp_cf, Tuple::with_null_prefix(), Tuple::max_tuple())?;
        let mut options = FlushOptionsPtr::default();
        options.set_allow_write_stall(true).set_flush_wait(true);
        self.txn.flush(&self.temp_cf, options)?;
        Ok(())
    }
}

impl<'a, 't> Drop for Session<'a, 't> {
    fn drop(&mut self) {
        if let Err(e) = self.finish_work() {
            eprintln!("Dropping session failed {:?}", e);
        }
        if let Ok(mut h) = self.handle.write().map_err(|_| Poisoned) {
            h.status = SessionStatus::Completed;
        } else {
            eprintln!("Accessing lock of session handle failed");
        }
    }
}

#[derive(Clone, Debug)]
pub struct SessionHandle {
    cf_ident: String,
    status: SessionStatus,
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
    use crate::db::eval::Environment;
    use crate::relation::tuple::Tuple;
    use super::*;

    #[test]
    fn push_get() {
        {
            let engine = Engine::new("_push_get".to_string(), false).unwrap();
            let sess = engine.session().unwrap();
            for i in (-80..-40).step_by(10) {
                let mut ikey = Tuple::with_null_prefix();
                ikey.push_int(i);
                ikey.push_str("pqr");
                println!("in {:?} {:?}", ikey, ikey.data);
                sess.txn.put(true, &sess.perm_cf, &ikey, &ikey).unwrap();
                let out = sess.txn.get(true, &sess.perm_cf, &ikey).unwrap();
                let out = out.as_ref().map(Tuple::new);
                println!("out {:?}", out);
            }
            let it = sess.txn.iterator(true, &sess.perm_cf);
            it.to_first();
            for (key, val) in it.iter() {
                println!("a: {:?} {:?}", key.as_ref(), val.as_ref());
                println!("v: {:?} {:?}", Tuple::new(key), Tuple::new(val));
            }
        }
        let _ = fs::remove_dir_all("_push_get");
    }

    #[test]
    fn test_create() {
        let p1 = "_test_db_create1";
        let p2 = "_test_db_create2";
        let p3 = "_test_db_create3";
        {
            {
                let engine = Engine::new(p1.to_string(), true);
                assert!(engine.is_ok());
                let engine = Engine::new(p2.to_string(), false);
                assert!(engine.is_ok());
                let engine = Engine::new(p3.to_string(), true);
                assert!(engine.is_ok());
                let engine2 = Engine::new(p1.to_string(), false);
                assert!(engine2.is_err());
                println!("create OK");
            }
            let engine2 = Engine::new(p2.to_string(), false);
            assert!(engine2.is_ok());
            println!("start ok");
            let engine2 = Arc::new(Engine::new(p3.to_string(), false).unwrap());
            {
                for _i in 0..10 {
                    let _sess = engine2.session().unwrap();
                }
                println!("sess OK");
                let handles = engine2.session_handles.lock().unwrap();
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
                    let mut sess = engine.session().unwrap();
                    println!("In thread {} {}", i, sess.handle.read().unwrap().cf_ident);
                    let gname = format!("abc{}", i);
                    for _ in 0..10000 {
                        sess.push_env();
                        sess.define_variable(&gname, &"xyz".into(), true).unwrap();
                        sess.define_variable("pqr", &"xyz".into(), false).unwrap();
                    }
                    if i & 1 == 0 {
                        sess.commit().unwrap();
                    }
                    println!("pqr {:?}", sess.resolve("pqr"));
                    println!("uvw {:?}", sess.resolve("uvw"));
                    println!("aaa {} {:?}", &gname, sess.resolve(&gname));
                    let it = sess.txn.iterator(false, &sess.temp_cf);
                    it.to_first();
                    // for (key, val) in it.iter() {
                    //     println!("a: {:?} {:?}", key.as_ref(), val.as_ref());
                    //     println!("v: {:?}", Tuple::new(key));
                    // }
                    for _ in 0..5000 {
                        sess.pop_env().unwrap();
                    }
                    // if let Err(e) = sess.commit() {
                    //     println!("Err {} with {:?}", i, e);
                    // } else {
                    //     println!("OK!!!! {}", i);
                    //     sess.commit().unwrap();
                    //     sess.commit().unwrap();
                    //     println!("OK!!!!!!!! {}", i);
                    // }
                    // sess.commit().unwrap();
                    // sess.commit().unwrap();
                    println!("pqr {:?}", sess.resolve("pqr"));
                    println!("In thread {} end", i);
                }))
            }


            for t in thread_handles {
                t.join().unwrap();
            }
            println!("All OK");
            {
                let handles = engine2.session_handles.lock().unwrap();
                println!("got handles {:#?}", handles.iter().map(|h| h.read().unwrap().cf_ident.to_string()).collect::<Vec<_>>());
            }
        }
        let _ = fs::remove_dir_all(p1);
        let _ = fs::remove_dir_all(p2);
        let _ = fs::remove_dir_all(p3);
    }
}