use crate::data::expr::StaticExpr;
use crate::data::tuple::{DataKind, OwnTuple, Tuple};
use crate::data::tuple_set::{TableId, MIN_TABLE_ID_BOUND};
use crate::data::typing::Typing;
use crate::data::value::{StaticValue, Value};
use crate::ddl::parser::DdlSchema;
use crate::ddl::reify::{DdlContext, DdlReifyError, TableInfo};
use crate::parser::{CozoParser, Pair, Rule};
use crate::runtime::instance::{DbInstanceError, SessionHandle, SessionStatus};
use crate::runtime::options::{default_txn_options, default_write_options};
use cozorocks::{DbPtr, ReadOptionsPtr, TransactionPtr, WriteOptionsPtr};
use lazy_static::lazy_static;
use log::error;
use pest::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::result;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

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
    pub(crate) tables: BTreeMap<u32, TableInfo>,
    pub(crate) table_assocs: TableAssocMap,
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
        if !self.stack.is_empty() {
            let popped_frame = self.stack.pop().unwrap();
            for (_k, v) in popped_frame.into_iter() {
                if let SessionDefinable::Table(id) = v {
                    self.undefine_temp_table(id);
                }
            }
        }
        if self.stack.is_empty() {
            self.push_env()
        }
    }
    fn undefine_temp_table(&mut self, id: u32) {
        // remove table
        self.tables.remove(&id);

        // remove assoc info
        for assoc_map in self.table_assocs.values_mut() {
            // remove as key
            assoc_map.remove(&TableId { in_root: false, id });
            for set in assoc_map.values_mut() {
                // remove as val
                set.remove(&id);
            }
        }
        // range delete associated data
        let start_key = OwnTuple::with_prefix(id);
        let mut end_key = OwnTuple::with_prefix(id);
        end_key.seal_with_sentinel();
        if let Err(e) = self.temp.del_range(&self.w_opts_temp, start_key, end_key) {
            error!("Undefine temp table failed: {:?}", e)
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

    pub(crate) fn get_next_main_table_id(&self) -> result::Result<u32, DdlReifyError> {
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

    pub fn set_params(&mut self, params: BTreeMap<String, StaticValue>) {
        self.params.extend(params);
    }

    pub fn unset_param(&mut self, name: &str) {
        self.params.remove(name);
    }

    pub fn run_script(&mut self, script: impl AsRef<str>) -> Result<Value> {
        let script = script.as_ref();
        let pair = CozoParser::parse(Rule::script, script)?
            .next()
            .ok_or_else(|| DbInstanceError::Parse(script.to_string()))?;
        match pair.as_rule() {
            Rule::query => self.execute_query(pair),
            Rule::persist_block => self.execute_persist_block(pair),
            _ => Err(DbInstanceError::Parse(script.to_string())),
        }
    }
    fn execute_query(&mut self, pair: Pair) -> Result<Value> {
        let mut ctx = self.temp_ctx();
        for pair in pair.into_inner() {
            let schema = DdlSchema::try_from(pair)?;
            ctx.build_table(schema)?;
        }
        ctx.commit()?;
        Ok(Value::Null)
    }
    fn execute_persist_block(&mut self, pair: Pair) -> Result<Value> {
        let mut ctx = self.main_ctx();
        for pair in pair.into_inner() {
            let schema = DdlSchema::try_from(pair)?;
            ctx.build_table(schema)?;
        }
        ctx.commit()?;
        Ok(Value::Null)
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
pub(crate) mod tests {
    use crate::data::tuple::Tuple;
    use crate::runtime::options::default_read_options;
    use crate::DbInstance;

    const HR_TEST_SCRIPT: &str = include_str!("../../test_data/hr.cozo");

    pub(crate) fn persist_hr_test() -> String {
        format!("persist! {{\n{}\n}}", HR_TEST_SCRIPT)
    }

    #[test]
    fn test_script() {
        let mut db = DbInstance::new("_test_session", false).unwrap();
        db.set_destroy_on_close(true);
        let mut sess = db.session().unwrap().start().unwrap();
        sess.run_script(HR_TEST_SCRIPT).unwrap();
        sess.run_script(persist_hr_test()).unwrap();
        sess.run_script(persist_hr_test()).unwrap();
        dbg!(&sess.tables);
        let mut opts = default_read_options();
        opts.set_total_order_seek(true);
        let it = sess.main.iterator(&opts);
        it.to_first();
        while it.is_valid() {
            let (k, v) = it.pair().unwrap();
            dbg!((Tuple::new(k), Tuple::new(v)));
            it.next();
        }
    }
}
