use std::{mem, result};
use cozorocks::{BridgeError, DbPtr, destroy_db, OptionsPtrShared, TDbOptions};
use std::sync::{Arc, LockResult, Mutex, PoisonError};
use log::error;
use crate::data::tuple::Tuple;
use crate::runtime::options::{default_options, default_txn_options, default_write_options};

#[derive(thiserror::Error, Debug)]
pub enum DbInstanceError {
    #[error(transparent)]
    DbBridgeError(#[from] BridgeError),

    #[error("Cannot obtain session lock")]
    SessionLockError,
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
    status: SessionStatus,
}

pub struct DbInstance {
    pub(crate) main: DbPtr,
    options: OptionsPtrShared,
    tdb_options: TDbOptions,
    path: String,
    session_handles: Mutex<Vec<Arc<Mutex<SessionHandle>>>>,
    destroy_on_close: bool,
}

impl DbInstance {
    pub fn new(path: &str, optimistic: bool) -> Result<Self> {
        let options = default_options().make_shared();
        let tdb_options = default_txn_options(optimistic);
        let main = DbPtr::open(&options, &tdb_options, path)?;
        Ok(Self {
            options,
            tdb_options,
            main,
            path: path.to_string(),
            session_handles: vec![].into(),
            destroy_on_close: false,
        })
    }
}

impl DbInstance {
    pub fn session(&self) -> Result<Session> {
        let mut handles = self.session_handles.lock()
            .map_err(|_| DbInstanceError::SessionLockError)?;
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
                }));
                handles.push(handle.clone());

                (temp, handle)
            }
            Some((db, _, handle)) => (db, handle.clone())
        };

        drop(handles);

        Ok(Session {
            main: self.main.clone(),
            temp,
            session_handle: handle,
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

pub struct Session {
    pub(crate) main: DbPtr,
    pub(crate) temp: DbPtr,
    session_handle: Arc<Mutex<SessionHandle>>,
}

impl Session {
    pub fn start(&mut self) -> Result<()> {
        let mut handle = self.session_handle.lock()
            .map_err(|_| DbInstanceError::SessionLockError)?;
        handle.status = SessionStatus::Running;
        Ok(())
    }
    fn clear_data(&self) -> Result<()> {
        let w_opts = default_write_options();
        self.temp
            .del_range(&w_opts, Tuple::with_null_prefix(), Tuple::max_tuple())?;
        // self.temp.compact_all()?;
        Ok(())
    }
    pub fn stop(&mut self) -> Result<()> {
        self.clear_data()?;
        let mut handle = self.session_handle.lock()
            .map_err(|_| {
                error!("failed to stop interpreter");
                DbInstanceError::SessionLockError
            })?;
        handle.status = SessionStatus::Completed;
        Ok(())
    }
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

    fn test_send_sync<T: Send + Sync>(_x: T) {}

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
        for _ in 0..1000 {
            let i1 = db2.session()?;
            test_send_sync(i1);
        }
        dbg!(start.elapsed());
        Ok(())
    }
}