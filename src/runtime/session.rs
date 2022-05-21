use crate::data::expr::StaticExpr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{TableId, MIN_TABLE_ID_BOUND};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use crate::ddl::reify::TableInfo;
use crate::runtime::instance::{DbInstanceError, SessionHandle, SessionStatus, TableLock};
use crate::runtime::options::{default_txn_options, default_write_options};
use cozorocks::{DbPtr, ReadOptionsPtr, TransactionPtr, WriteOptionsPtr};
use lazy_static::lazy_static;
use log::error;
use std::collections::{BTreeMap, BTreeSet};
use std::result;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLockReadGuard, RwLockWriteGuard};

type Result<T> = result::Result<T, DbInstanceError>;

pub(crate) enum SessionDefinable {
    Value(StaticValue),
    Expr(StaticExpr),
    Typing(Typing),
    Table(u32), // TODO
}

pub(crate) type SessionStackFrame = BTreeMap<String, SessionDefinable>;
pub(crate) type TableAssocMap = BTreeMap<DataKind, BTreeMap<TableId, BTreeSet<u32>>>;

pub struct Session {
    pub(crate) main: DbPtr,
    pub(crate) temp: DbPtr,
    pub(crate) r_opts_main: ReadOptionsPtr,
    pub(crate) r_opts_temp: ReadOptionsPtr,
    pub(crate) w_opts_main: WriteOptionsPtr,
    pub(crate) w_opts_temp: WriteOptionsPtr,
    pub(crate) optimistic: bool,
    pub(crate) cur_table_id: AtomicU32,
    pub(crate) stack: Vec<SessionStackFrame>,
    pub(crate) params: BTreeMap<String, StaticValue>,
    pub(crate) session_handle: Arc<Mutex<SessionHandle>>,
    pub(crate) table_locks: TableLock,
    pub(crate) tables: BTreeMap<u32, TableInfo>,
    pub(crate) table_assocs: TableAssocMap,
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
            let mut handle = self
                .session_handle
                .lock()
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
        let mut handle = self.session_handle.lock().map_err(|_| {
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
            res = self
                .cur_table_id
                .fetch_add(MIN_TABLE_ID_BOUND, Ordering::SeqCst);
        }
        res + 1
    }

    pub(crate) fn txn(&self, w_opts: Option<WriteOptionsPtr>) -> TransactionPtr {
        self.main.txn(
            default_txn_options(self.optimistic),
            w_opts.unwrap_or_else(default_write_options),
        )
    }

    pub(crate) fn get_next_main_table_id(&self) -> Result<u32> {
        let txn = self.txn(None);
        let key = MAIN_DB_TABLE_ID_SEQ_KEY.as_ref();
        let cur_id = match txn.get_owned(&self.r_opts_main, key)? {
            None => {
                let val = OwnTuple::from((DataKind::Data, &[(MIN_TABLE_ID_BOUND as i64).into()]));
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
        self.table_locks
            .try_read()
            .map_err(|_| DbInstanceError::TableAccessLock)
    }
    pub(crate) fn table_mutation_guard(&self, ids: BTreeSet<u32>) -> Result<RwLockWriteGuard<()>> {
        self.table_locks
            .write()
            .map_err(|_| DbInstanceError::TableAccessLock)
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
