use std::fmt::{Debug, Formatter};
use std::sync::atomic::Ordering;

use miette::{bail, miette, IntoDiagnostic, Result};
use rmp_serde::Serializer;
use serde::Serialize;

use cozorocks::CfHandle::Snd;
use cozorocks::{DbIter, RocksDb, Tx};

use crate::data::symb::Symbol;
use crate::data::tuple::{EncodedTuple, Tuple};
use crate::data::value::DataValue;
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Debug,
    serde_derive::Serialize,
    serde_derive::Deserialize,
    PartialOrd,
    Ord,
)]
pub(crate) struct ViewRelId(pub(crate) u64);

impl ViewRelId {
    pub(crate) fn new(u: u64) -> Result<Self> {
        if u > 2u64.pow(6 * 8) {
            bail!("StoredRelId overflow: {}", u)
        } else {
            Ok(Self(u))
        }
    }
    pub(crate) fn next(&self) -> Result<Self> {
        Self::new(self.0 + 1)
    }
    pub(crate) const SYSTEM: Self = Self(0);
    pub(crate) fn raw_encode(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub(crate) fn raw_decode(src: &[u8]) -> Result<Self> {
        if src.len() < 8 {
            bail!("cannot decode bytes as StoredRelId: {:x?}", src)
        } else {
            let u = u64::from_be_bytes([
                src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
            ]);
            Self::new(u)
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) enum ViewRelKind {
    Manual,
    AutoByCount,
}

#[derive(Clone, Eq, PartialEq, Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct ViewRelMetadata {
    pub(crate) name: Symbol,
    pub(crate) id: ViewRelId,
    pub(crate) arity: usize,
    pub(crate) kind: ViewRelKind,
}

#[derive(Clone)]
pub(crate) struct ViewRelStore {
    view_db: RocksDb,
    pub(crate) metadata: ViewRelMetadata,
}

impl Debug for ViewRelStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ViewStore<{}>", self.metadata.name)
    }
}

impl ViewRelStore {
    pub(crate) fn scan_all(&self) -> Result<impl Iterator<Item = Result<Tuple>>> {
        let lower = Tuple::default().encode_as_key(self.metadata.id);
        let upper = Tuple::default().encode_as_key(self.metadata.id.next()?);
        Ok(ViewRelIterator::new(&self.view_db, &lower, &upper))
    }

    pub(crate) fn scan_prefix(&self, prefix: &Tuple) -> impl Iterator<Item = Result<Tuple>> {
        let mut upper = prefix.0.clone();
        upper.push(DataValue::Bot);
        let prefix_encoded = prefix.encode_as_key(self.metadata.id);
        let upper_encoded = Tuple(upper).encode_as_key(self.metadata.id);
        ViewRelIterator::new(&self.view_db, &prefix_encoded, &upper_encoded)
    }
    pub(crate) fn scan_bounded_prefix(
        &self,
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut lower_t = prefix.clone();
        lower_t.0.extend_from_slice(lower);
        let mut upper_t = prefix.clone();
        upper_t.0.extend_from_slice(upper);
        upper_t.0.push(DataValue::Bot);
        let lower_encoded = lower_t.encode_as_key(self.metadata.id);
        let upper_encoded = upper_t.encode_as_key(self.metadata.id);
        ViewRelIterator::new(&self.view_db, &lower_encoded, &upper_encoded)
    }
}

struct ViewRelIterator {
    inner: DbIter,
    started: bool,
}

impl ViewRelIterator {
    fn new(db: &RocksDb, lower: &[u8], upper: &[u8]) -> Self {
        let mut inner = db
            .transact()
            .start()
            .iterator(Snd)
            .upper_bound(upper)
            .start();
        inner.seek(lower);
        Self {
            inner,
            started: false,
        }
    }
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        if self.started {
            self.inner.next()
        } else {
            self.started = true;
        }
        Ok(match self.inner.key()? {
            None => None,
            Some(k_slice) => Some(EncodedTuple(k_slice).decode()?),
        })
    }
}

impl Iterator for ViewRelIterator {
    type Item = Result<Tuple>;
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

impl SessionTx {
    pub(crate) fn view_exists(&self, name: &Symbol) -> Result<bool> {
        let key = DataValue::Str(name.0.clone());
        let encoded = Tuple(vec![key]).encode_as_key(ViewRelId::SYSTEM);
        let vtx = self.view_db.transact().start();
        Ok(vtx.exists(&encoded, false, Snd)?)
    }
    pub(crate) fn create_view_rel(&self, mut meta: ViewRelMetadata) -> Result<ViewRelStore> {
        let key = DataValue::Str(meta.name.0.clone());
        let encoded = Tuple(vec![key]).encode_as_key(ViewRelId::SYSTEM);

        let mut vtx = self.view_db.transact().set_snapshot(true).start();
        if vtx.exists(&encoded, true, Snd)? {
            bail!(
                "cannot create view {}: one with the same name already exists",
                meta.name
            )
        };
        let last_id = self.view_store_id.fetch_add(1, Ordering::SeqCst);
        meta.id = ViewRelId::new(last_id + 1)?;
        vtx.put(&encoded, &meta.id.raw_encode(), Snd)?;
        let name_key =
            Tuple(vec![DataValue::Str(meta.name.0.clone())]).encode_as_key(ViewRelId::SYSTEM);

        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val)).unwrap();
        vtx.put(&name_key, &meta_val, Snd)?;

        let tuple = Tuple(vec![DataValue::Null]);
        let t_encoded = tuple.encode_as_key(ViewRelId::SYSTEM);
        vtx.put(&t_encoded, &meta.id.raw_encode(), Snd)?;
        vtx.commit()?;
        Ok(ViewRelStore {
            view_db: self.view_db.clone(),
            metadata: meta,
        })
    }
    pub(crate) fn get_view_rel(&self, name: &Symbol) -> Result<ViewRelStore> {
        let vtx = self.view_db.transact().start();
        self.do_get_view_rel(name, &vtx)
    }
    fn do_get_view_rel(&self, name: &Symbol, vtx: &Tx) -> Result<ViewRelStore> {
        let key = DataValue::Str(name.0.clone());
        let encoded = Tuple(vec![key]).encode_as_key(ViewRelId::SYSTEM);

        let found = vtx
            .get(&encoded, true, Snd)?
            .ok_or_else(|| miette!("cannot find stored view {}", name))?;
        let metadata: ViewRelMetadata = rmp_serde::from_slice(&found).into_diagnostic()?;
        Ok(ViewRelStore {
            view_db: self.view_db.clone(),
            metadata,
        })
    }
    pub(crate) fn destroy_view_rel(&self, name: &Symbol) -> Result<()> {
        let mut vtx = self.view_db.transact().start();
        let store = self.do_get_view_rel(name, &vtx)?;
        let key = DataValue::Str(name.0.clone());
        let encoded = Tuple(vec![key]).encode_as_key(ViewRelId::SYSTEM);
        vtx.del(&encoded, Snd)?;
        let lower_bound = Tuple::default().encode_as_key(store.metadata.id);
        let upper_bound = Tuple::default().encode_as_key(store.metadata.id.next()?);
        self.view_db.range_del(&lower_bound, &upper_bound, Snd)?;
        vtx.commit()?;
        Ok(())
    }
}
