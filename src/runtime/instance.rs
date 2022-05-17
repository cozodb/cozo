use std::{mem, result};
use std::collections::{BTreeMap, BTreeSet};
use cozorocks::{BridgeError, DbPtr, destroy_db, OptionsPtrShared, PinnableSlicePtr, ReadOptionsPtr, TDbOptions, TransactionPtr, TransactOptions, WriteOptionsPtr};
use std::sync::{Arc, LockResult, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicU32, Ordering};
use lazy_static::lazy_static;
use log::error;
use crate::data::expr::StaticExpr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple, TupleError};
use crate::data::tuple_set::MIN_TABLE_ID_BOUND;
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use crate::runtime::instance::DbInstanceError::TableDoesNotExist;
use crate::runtime::options::{default_options, default_read_options, default_txn_db_options, default_txn_options, default_write_options};

#[derive(thiserror::Error, Debug)]
pub enum DbInstanceError {
    #[error(transparent)]
    DbBridge(#[from] BridgeError),

    #[error("Cannot obtain session lock")]
    SessionLock,

    #[error(transparent)]
    Tuple(#[from] TupleError),

    #[error("Cannot obtain table access lock")]
    TableAccessLock,

    #[error("Cannot obtain table mutation lock")]
    TableMutationLock,

    #[error("Table does not exist: {0}")]
    TableDoesNotExist(u32),
}

type Result<T> = result::Result<T, DbInstanceError>;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum SessionStatus {
    Prepared,
    Running,
    Completed,
}


struct SessionHandle {
    id: usize,
    db: DbPtr,
    next_table_id: u32,
    status: SessionStatus,
}

type TableLock = Arc<RwLock<()>>;

pub struct DbInstance {
    pub(crate) main: DbPtr,
    options: OptionsPtrShared,
    tdb_options: TDbOptions,
    path: String,
    session_handles: Mutex<Vec<Arc<Mutex<SessionHandle>>>>,
    optimistic: bool,
    destroy_on_close: bool,
    table_locks: TableLock,
}

impl DbInstance {
    pub fn new(path: &str, optimistic: bool) -> Result<Self> {
        let options = default_options().make_shared();
        let tdb_options = default_txn_db_options(optimistic);
        let main = DbPtr::open(&options, &tdb_options, path)?;
        Ok(Self {
            options,
            tdb_options,
            main,
            optimistic,
            path: path.to_string(),
            session_handles: vec![].into(),
            destroy_on_close: false,
            table_locks: Default::default(),
        })
    }
}

impl DbInstance {
    pub fn session(&self) -> Result<Session> {
        let mut handles = self.session_handles.lock()
            .map_err(|_| DbInstanceError::SessionLock)?;
        let handle = handles.iter().find_map(|handle| {
            match handle.try_lock() {
                Ok(inner) => {
                    if inner.status == SessionStatus::Completed {
                        let db = inner.db.clone();
                        let idx = inner.id;
                        Some((db, idx, handle))
                    } else {
                        None
                    }
                }
                Err(_) => None
            }
        });
        let (temp, handle) = match handle {
            None => {
                let idx = handles.len();
                let temp_path = self.get_session_storage_path(idx);
                let temp = DbPtr::open_non_txn(
                    &self.options,
                    &temp_path)?;
                let handle = Arc::new(Mutex::new(SessionHandle {
                    status: SessionStatus::Prepared,
                    id: idx,
                    db: temp.clone(),
                    next_table_id: MIN_TABLE_ID_BOUND,
                }));
                handles.push(handle.clone());

                (temp, handle)
            }
            Some((db, _, handle)) => (db, handle.clone())
        };

        drop(handles);

        let mut w_opts_temp = default_write_options();
        w_opts_temp.set_disable_wal(true);

        Ok(Session {
            main: self.main.clone(),
            temp,
            session_handle: handle,
            optimistic: self.optimistic,
            w_opts_main: default_write_options(),
            w_opts_temp,
            r_opts_main: default_read_options(),
            r_opts_temp: default_read_options(),
            stack: vec![],
            cur_table_id: 0.into(),
            params: Default::default(),
            table_locks: self.table_locks.clone(),
        })
    }

    pub fn set_destroy_on_close(&mut self, v: bool) {
        self.destroy_on_close = v;
    }

    fn get_session_storage_path(&self, idx: usize) -> String {
        format!("{}_sess_{}", self.path, idx)
    }
}

impl Drop for DbInstance {
    fn drop(&mut self) {
        if let Err(e) = self.main.close() {
            error!("Encountered error on closing main DB {:?}", e);
        }
        let mut to_wipe = 0;
        match self.session_handles.lock() {
            Ok(mut handles) => {
                to_wipe = handles.len();
                while let Some(handle) = handles.pop() {
                    match handle.lock() {
                        Ok(handle) => {
                            if let Err(e) = handle.db.close() {
                                error!("Encountered error on closing temp DB {:?}", e);
                            }
                        }
                        Err(e) => {
                            error!("Cannot obtain handles for DbInstance on drop {:?}", e)
                        }
                    }
                }
            }
            Err(e) => {
                error!("Cannot obtain handles for DbInstance on drop {:?}", e)
            }
        }
        for i in 0..to_wipe {
            let path = self.get_session_storage_path(i);
            if let Err(e) = destroy_db(&self.options, &path) {
                error!("Encountered error on destroying temp DB {:?}", e);
            }
        }
        if self.destroy_on_close {
            let mut temp = unsafe { DbPtr::null() };
            mem::swap(&mut temp, &mut self.main);
            drop(temp);
            if let Err(e) = destroy_db(&self.options, &self.path) {
                error!("Encountered error on destroying temp DB {:?}", e);
            }
        }
    }
}

enum SessionDefinable {
    Value(StaticValue),
    Expr(StaticExpr),
    Typing(Typing),
    // TODO
}

type SessionStackFrame = BTreeMap<String, SessionDefinable>;

pub struct Session {
    pub(crate) main: DbPtr,
    pub(crate) temp: DbPtr,
    pub(crate) r_opts_main: ReadOptionsPtr,
    pub(crate) r_opts_temp: ReadOptionsPtr,
    pub(crate) w_opts_main: WriteOptionsPtr,
    pub(crate) w_opts_temp: WriteOptionsPtr,
    optimistic: bool,
    cur_table_id: AtomicU32,
    stack: Vec<SessionStackFrame>,
    params: BTreeMap<String, StaticValue>,
    session_handle: Arc<Mutex<SessionHandle>>,
    table_locks: TableLock,
}

pub(crate) struct InterpretContext<'a> {
    session: &'a Session,
}

impl<'a> InterpretContext<'a> {
    pub(crate) fn resolve(&self, key: impl AsRef<str>) {}
    pub(crate) fn resolve_value(&self, key: impl AsRef<str>) {}
    pub(crate) fn resolve_typing(&self, key: impl AsRef<str>) {
        todo!()
    }
    // also for expr, table, etc..
}

impl Session {
    pub fn start(mut self) -> Result<Self> {
        {
            self.push_env();
            let mut handle = self.session_handle.lock()
                .map_err(|_| DbInstanceError::SessionLock)?;
            handle.status = SessionStatus::Running;
            self.cur_table_id = handle.next_table_id.into();
        }
        Ok(self)
    }
    pub(crate) fn push_env(&mut self) {
        self.stack.push(BTreeMap::new());
    }
    pub(crate) fn pop_env(&mut self) {
        if self.stack.len() > 1 {
            self.stack.pop();
        }
    }
    fn clear_data(&self) -> Result<()> {
        self.temp.del_range(
            &self.w_opts_temp,
            Tuple::with_null_prefix(),
            Tuple::max_tuple(),
        )?;
        Ok(())
    }
    pub fn stop(&mut self) -> Result<()> {
        self.clear_data()?;
        let mut handle = self.session_handle.lock()
            .map_err(|_| {
                error!("failed to stop interpreter");
                DbInstanceError::SessionLock
            })?;
        handle.next_table_id = self.cur_table_id.load(Ordering::SeqCst);
        handle.status = SessionStatus::Completed;
        Ok(())
    }

    pub(crate) fn get_next_temp_table_id(&self) -> u32 {
        let mut res = self.cur_table_id.fetch_add(1, Ordering::SeqCst);
        while res.wrapping_add(1) < MIN_TABLE_ID_BOUND {
            res = self.cur_table_id.fetch_add(MIN_TABLE_ID_BOUND, Ordering::SeqCst);
        }
        res + 1
    }

    pub(crate) fn txn(&self, w_opts: Option<WriteOptionsPtr>) -> TransactionPtr {
        self.main.txn(default_txn_options(self.optimistic),
                      w_opts.unwrap_or_else(default_write_options))
    }

    pub(crate) fn get_next_main_table_id(&self) -> Result<u32> {
        let txn = self.txn(None);
        let key = MAIN_DB_TABLE_ID_SEQ_KEY.as_ref();
        let cur_id = match txn.get_owned(&self.r_opts_main, key)? {
            None => {
                let val = OwnTuple::from(
                    (DataKind::Data, &[(MIN_TABLE_ID_BOUND as i64).into()]));
                txn.put(key, &val)?;
                MIN_TABLE_ID_BOUND
            }
            Some(pt) => {
                let pt = Tuple::from(pt);
                let prev_id = pt.get_int(0)?;
                let val = OwnTuple::from((DataKind::Data, &[(prev_id + 1).into()]));
                txn.put(key, &val)?;
                (prev_id + 1) as u32
            }
        };
        txn.commit()?;
        Ok(cur_id + 1)
    }
    pub(crate) fn table_access_guard(&self, ids: BTreeSet<u32>) -> Result<RwLockReadGuard<()>> {
            self.table_locks.try_read().map_err(|_| DbInstanceError::TableAccessLock)
    }
    pub(crate) fn table_mutation_guard(&self, ids: BTreeSet<u32>) -> Result<RwLockWriteGuard<()>> {
        self.table_locks.write().map_err(|_| DbInstanceError::TableAccessLock)
    }
}

lazy_static! {
    static ref MAIN_DB_TABLE_ID_SEQ_KEY: OwnTuple = OwnTuple::from((0u32, &[Value::Null]));
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            error!("failed to drop session {:?}", e);
        }
    }
}


#[cfg(test)]
mod tests {
    use std::time::Instant;
    use crate::logger::init_test_logger;
    use super::*;
    use crate::runtime::instance::DbInstance;

    fn test_send<T: Send>(_x: T) {}

    #[test]
    fn creation() -> Result<()> {
        init_test_logger();

        let start = Instant::now();
        let mut db = DbInstance::new("_test", false)?;
        db.set_destroy_on_close(true);
        dbg!(start.elapsed());
        let start = Instant::now();
        let mut db2 = DbInstance::new("_test2", true)?;
        db2.set_destroy_on_close(true);
        for _ in 0..100 {
            let i1 = db2.session()?.start()?;
            dbg!(i1.get_next_temp_table_id());
            dbg!(i1.get_next_main_table_id()?);
            test_send(i1);
        }
        dbg!(start.elapsed());
        Ok(())
    }
}