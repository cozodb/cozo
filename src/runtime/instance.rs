use crate::data::tuple::TupleError;
use crate::data::tuple_set::MIN_TABLE_ID_BOUND;
use crate::ddl::parser::DdlParseError;
use crate::ddl::reify::DdlReifyError;
use crate::parser::Rule;
use crate::runtime::options::*;
use crate::runtime::session::Session;
use cozorocks::*;
use log::error;
use std::sync::{Arc, Mutex};
use std::{mem, result};

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

    #[error("Name conflict {0}")]
    NameConflict(String),

    #[error("Parse error {0}")]
    Parse(String),

    #[error(transparent)]
    DdlParse(#[from] DdlParseError),

    #[error(transparent)]
    Reify(#[from] DdlReifyError),

    #[error("Attempt to write when read-only")]
    WriteReadOnlyConflict,
}

impl From<pest::error::Error<Rule>> for DbInstanceError {
    fn from(err: pest::error::Error<Rule>) -> Self {
        DbInstanceError::Parse(format!("{:?}", err))
    }
}

type Result<T> = result::Result<T, DbInstanceError>;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum SessionStatus {
    Prepared,
    Running,
    Completed,
}

pub(crate) struct SessionHandle {
    id: usize,
    db: DbPtr,
    pub(crate) next_table_id: u32,
    pub(crate) status: SessionStatus,
}

pub struct DbInstance {
    pub(crate) main: DbPtr,
    options: OptionsPtrShared,
    _tdb_options: TDbOptions,
    path: String,
    session_handles: Mutex<Vec<Arc<Mutex<SessionHandle>>>>,
    optimistic: bool,
    destroy_on_close: bool,
}

impl DbInstance {
    pub fn new(path: &str, optimistic: bool) -> Result<Self> {
        let options = default_options().make_shared();
        let tdb_options = default_txn_db_options(optimistic);
        let main = DbPtr::open(&options, &tdb_options, path)?;
        Ok(Self {
            options,
            _tdb_options: tdb_options,
            main,
            optimistic,
            path: path.to_string(),
            session_handles: vec![].into(),
            destroy_on_close: false,
        })
    }
}

impl DbInstance {
    pub fn session(&self) -> Result<Session> {
        let mut handles = self
            .session_handles
            .lock()
            .map_err(|_| DbInstanceError::SessionLock)?;
        let handle = handles.iter().find_map(|handle| match handle.try_lock() {
            Ok(inner) => {
                if inner.status == SessionStatus::Completed {
                    let db = inner.db.clone();
                    let idx = inner.id;
                    Some((db, idx, handle))
                } else {
                    None
                }
            }
            Err(_) => None,
        });
        let (temp, handle) = match handle {
            None => {
                let idx = handles.len();
                let temp_path = self.get_session_storage_path(idx);
                let temp = DbPtr::open_non_txn(&self.options, &temp_path)?;
                let handle = Arc::new(Mutex::new(SessionHandle {
                    status: SessionStatus::Prepared,
                    id: idx,
                    db: temp.clone(),
                    next_table_id: MIN_TABLE_ID_BOUND,
                }));
                handles.push(handle.clone());

                (temp, handle)
            }
            Some((db, _, handle)) => (db, handle.clone()),
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
            tables: Default::default(),
            table_assocs: Default::default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger::init_test_logger;
    use crate::runtime::instance::DbInstance;
    use std::time::Instant;

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
