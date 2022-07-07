use crate::data::compare::{rusty_cmp, DB_KEY_PREFIX_LEN};
use crate::data::encode::encode_tx;
use crate::data::id::TxId;
use crate::runtime::transact::{SessionTx, TxLog};
use anyhow::Result;
use cozorocks::{DbBuilder, DbIter, RocksDb};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Db {
    db: RocksDb,
    last_attr_id: Arc<AtomicU64>,
    last_ent_id: Arc<AtomicU64>,
    last_tx_id: Arc<AtomicU64>,
    n_sessions: Arc<AtomicUsize>,
    session_id: usize,
}

impl Debug for Db {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Db<session {}, attrs {:?}, entities {:?}, txs {:?}, sessions: {:?}>",
            self.session_id, self.last_tx_id, self.last_ent_id, self.last_tx_id, self.n_sessions
        )
    }
}

impl Db {
    pub fn build(builder: DbBuilder) -> Result<Self> {
        let db = builder
            .use_bloom_filter(true, 10., true)
            .use_capped_prefix_extractor(true, DB_KEY_PREFIX_LEN)
            .use_custom_comparator("cozo_rusty_cmp", rusty_cmp, false)
            .build()?;
        Ok(Self {
            db,
            last_attr_id: Arc::new(Default::default()),
            last_ent_id: Arc::new(Default::default()),
            last_tx_id: Arc::new(Default::default()),
            n_sessions: Arc::new(Default::default()),
            session_id: Default::default(),
        })
    }

    pub fn new_session(&self) -> Result<Self> {
        if self.session_id == 0 {
            self.load_last_ids()?;
        }

        let old_count = self.n_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(Self {
            db: self.db.clone(),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            n_sessions: self.n_sessions.clone(),
            session_id: old_count + 1,
        })
    }

    fn load_last_ids(&self) -> Result<()> {
        let mut tx = self.transact(None)?;
        self.last_tx_id.store(tx.r_tx_id.0, Ordering::Release);
        self.last_attr_id
            .store(tx.load_last_attr_id()?.0, Ordering::Release);
        self.last_ent_id
            .store(tx.load_last_entity_id()?.0, Ordering::Release);
        Ok(())
    }
    pub(crate) fn transact(&self, at: Option<TxId>) -> Result<SessionTx> {
        let tx_id = at.unwrap_or(TxId::ZERO);
        let mut ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            r_tx_id: tx_id,
            w_tx_id: None,
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            temp_entity_to_perm: Default::default(),
            eid_by_attr_val_cache: Default::default(),
            touched_eids: Default::default(),
        };
        if at.is_none() {
            let tid = ret.load_last_tx_id()?;
            ret.r_tx_id = tid;
        }
        Ok(ret)
    }
    pub(crate) fn transact_write(&self) -> Result<SessionTx> {
        let last_tx_id = self.last_tx_id.fetch_add(1, Ordering::AcqRel);
        let cur_tx_id = TxId(last_tx_id + 1);

        let ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            r_tx_id: cur_tx_id,
            w_tx_id: Some(cur_tx_id),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            temp_entity_to_perm: Default::default(),
            eid_by_attr_val_cache: Default::default(),
            touched_eids: Default::default(),
        };
        Ok(ret)
    }
    pub(crate) fn total_iter(&self) -> DbIter {
        let mut it = self.db.transact().start().iterator().start();
        it.seek_to_start();
        it
    }
    pub(crate) fn find_tx_before_timestamp_millis(&self, ts: i64) -> Result<Option<TxLog>> {
        // binary search
        let lower_bound = encode_tx(TxId::MAX_SYS);
        let upper_bound = encode_tx(TxId::MAX_USER);

        // both are inclusive bounds
        let mut lower_found = TxId::MIN_USER;
        let mut upper_found = TxId::MAX_USER;
        let mut it = self
            .transact_write()?
            .tx
            .iterator()
            .lower_bound(&lower_bound)
            .upper_bound(&upper_bound)
            .start();

        loop {
            let needle = TxId((lower_found.0 + upper_found.0) / 2);
            let current = encode_tx(needle);
            it.seek(&current);
            match it.val()? {
                Some(v_slice) => {
                    let log = TxLog::decode(v_slice)?;
                    let found_ts = log.timestamp;
                    if found_ts == ts || needle == upper_found || needle == lower_found {
                        return Ok(Some(log));
                    }
                    if found_ts < ts {
                        lower_found = log.id;
                        continue;
                    }
                    if found_ts > ts {
                        upper_found = TxId(log.id.0 - 1);
                        continue;
                    }
                }
                None => return Ok(None),
            }
        }
    }
}
