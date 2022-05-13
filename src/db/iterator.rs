use crate::db::engine::Session;
use crate::db::eval::{compare_tuple_by_keys, tuple_eval};
use crate::db::plan::{ExecPlan, TableRowGetter};
use crate::db::table::{ColId, TableId};
use crate::error::CozoError::LogicError;
use crate::error::Result;
use crate::relation::data::{DataKind, EMPTY_DATA};
use crate::relation::table::MegaTuple;
use crate::relation::tuple::{CowSlice, CowTuple, OwnTuple, Tuple};
use crate::relation::value::Value;
use cozorocks::IteratorPtr;
use std::cmp::Ordering;
use std::{iter, mem};

// Implementation notice
// Never define `.next()` recursively for iterators below, otherwise stackoverflow is almost
// guaranteed (but may not show for test data)

pub struct SortingMaterialization<'a> {
    pub(crate) source: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) ordering: &'a [(bool, Value<'a>)],
    pub(crate) sess: &'a Session<'a>,
    pub(crate) sorted: bool,
    pub(crate) temp_table_id: u32,
    pub(crate) skv_len: (usize, usize, usize),
    pub(crate) sorted_it: IteratorPtr<'a>,
}

impl<'a> SortingMaterialization<'a> {
    fn sort(&mut self) -> Result<()> {
        self.temp_table_id = self.sess.get_next_storage_id(false)? as u32;
        let mut key_cache = OwnTuple::with_prefix(self.temp_table_id);
        let mut val_cache = OwnTuple::with_data_prefix(DataKind::Data);
        let mut kv_len = (0, 0);
        for nxt in self.source.by_ref() {
            let m_tuple = nxt?;
            kv_len = (m_tuple.keys.len(), m_tuple.vals.len());
            key_cache.truncate_all();
            val_cache.truncate_all();
            for (is_asc, val) in self.ordering {
                let sort_val = tuple_eval(&val, &m_tuple)?;
                if *is_asc {
                    key_cache.push_value(&sort_val);
                } else {
                    key_cache.push_reverse_value(&sort_val)
                }
            }
            for kt in &m_tuple.keys {
                key_cache.push_bytes(kt);
            }
            for vt in &m_tuple.vals {
                val_cache.push_bytes(vt);
            }
            self.sess
                .txn
                .put(false, &self.sess.temp_cf, &key_cache, &val_cache)?;
        }
        self.sorted_it.refresh()?;
        key_cache.truncate_all();
        self.sorted_it.seek(&key_cache);
        self.skv_len = (self.ordering.len(), kv_len.0, kv_len.1);
        self.sorted = true;
        Ok(())
    }
}

impl<'a> Drop for SortingMaterialization<'a> {
    fn drop(&mut self) {
        let range_start = Tuple::with_prefix(self.temp_table_id);
        let mut range_end = Tuple::with_prefix(self.temp_table_id);
        range_end.seal_with_sentinel();
        if let Err(e) = self
            .sess
            .txn
            .del_range(&self.sess.temp_cf, range_start, range_end)
        {
            eprintln!("Error when dropping SortingMaterialization: {:?}", e)
        }
    }
}

impl<'a> Iterator for SortingMaterialization<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.sorted {
            if let Err(e) = self.sort() {
                return Some(Err(e));
            }
        } else {
            self.sorted_it.next();
        }
        match unsafe { self.sorted_it.pair() } {
            None => None,
            Some((kt, vt)) => {
                let kt = Tuple::new(kt);
                let vt = Tuple::new(vt);
                let mut mt = MegaTuple {
                    keys: Vec::with_capacity(self.skv_len.1),
                    vals: Vec::with_capacity(self.skv_len.2),
                };
                for k in kt.iter().skip(self.skv_len.0) {
                    match k {
                        Value::Bytes(b) => {
                            let v = b.into_owned();
                            let v = OwnTuple::new(v);
                            mt.keys.push(v.into());
                        }
                        _ => return Some(Err(LogicError("Wrong type in sorted".to_string()))),
                    }
                }
                if mt.keys.len() != self.skv_len.1 {
                    return Some(Err(LogicError("Wrong key len in sorted".to_string())));
                }
                for v in vt.iter() {
                    match v {
                        Value::Bytes(b) => {
                            let v = b.into_owned();
                            let v = OwnTuple::new(v);
                            mt.vals.push(v.into());
                        }
                        _ => return Some(Err(LogicError("Wrong type in sorted".to_string()))),
                    }
                }
                if mt.vals.len() != self.skv_len.2 {
                    return Some(Err(LogicError("Wrong val len in sorted".to_string())));
                }
                Some(Ok(mt))
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum NodeEdgeChainKind {
    Fwd,
    Bwd,
    Bidi,
}

pub struct LimiterIterator<'a> {
    pub(crate) source: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) current: usize,
}

impl<'a> Iterator for LimiterIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.source.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(val)) => {
                    self.current += 1;
                    if self.current <= self.offset {
                        continue;
                    } else if self.current > self.limit + self.offset {
                        return None;
                    } else {
                        return Some(Ok(val));
                    }
                }
            }
        }
    }
}

pub struct NodeToEdgeChainJoinIterator<'a> {
    // TODO associates, right_outer
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right_it: IteratorPtr<'a>,
    pub(crate) right_getter: TableRowGetter<'a>,
    pub(crate) kind: NodeEdgeChainKind,
    pub(crate) left_key_len: usize,
    pub(crate) right_key_cache: Option<OwnTuple>,
    pub(crate) edge_front_table_id: u32,
    pub(crate) left_cache: Option<MegaTuple>,
    pub(crate) last_right_key_cache: Option<CowTuple>,
}

impl<'a> Iterator for NodeToEdgeChainJoinIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            if self.left_cache.is_none() {
                self.left_cache = match self.left.next() {
                    None => return None,
                    Some(Ok(v)) => Some(v),
                    Some(Err(e)) => return Some(Err(e)),
                };
            }
            match &self.left_cache {
                None => return None,
                Some(left_tuple) => {
                    if self.right_key_cache.is_none() {
                        let left_key = left_tuple.keys.last().unwrap();
                        let mut right_key_cache = self.right_getter.key_cache.clone();
                        right_key_cache.truncate_all();
                        right_key_cache.push_int(self.edge_front_table_id as i64);
                        for v in left_key.iter() {
                            right_key_cache.push_value(&v);
                        }
                        self.right_key_cache = Some(right_key_cache);
                    }
                    let right_key_cache = match &self.right_key_cache {
                        Some(v) => v,
                        _ => unreachable!(),
                    };

                    let mut started = false;

                    match &self.last_right_key_cache {
                        None => {
                            self.right_it.seek(right_key_cache);
                        }
                        Some(v) => {
                            started = true;
                            self.right_it.seek(v);
                        }
                    }

                    self.last_right_key_cache = None;

                    let mut is_first_loop = true;
                    'inner: while let Some((r_key, r_val)) = {
                        if !started {
                            started = true;
                        } else {
                            self.right_it.next();
                        }
                        unsafe { self.right_it.pair() }
                    } {
                        let r_key = Tuple::new(r_key);
                        if !r_key.starts_with(right_key_cache) {
                            self.right_key_cache = None;
                            if is_first_loop {
                                self.left_cache = None;
                            }
                            // left join return here
                            // if self {
                            //
                            // }
                            continue 'outer;
                        } else {
                            is_first_loop = false;
                            let is_edge_forward = r_key.get_bool(self.left_key_len + 1).unwrap();
                            match self.kind {
                                NodeEdgeChainKind::Fwd => {
                                    if is_edge_forward {
                                        self.last_right_key_cache = Some(r_key.clone().into());
                                        let mut left_tuple = left_tuple.clone();
                                        left_tuple.keys.push(r_key.into());
                                        left_tuple.vals.push(Tuple::new(r_val).into());
                                        return Some(Ok(left_tuple));
                                    } else {
                                        continue 'inner;
                                    }
                                }
                                NodeEdgeChainKind::Bwd => {
                                    if !is_edge_forward {
                                        let real_r_key = Tuple::new(r_val);
                                        match self.right_getter.get_with_tuple(&real_r_key) {
                                            Ok(None) => unreachable!(),
                                            Ok(Some(v)) => {
                                                self.last_right_key_cache = Some(r_key.into());
                                                let mut left_tuple = left_tuple.clone();
                                                left_tuple.keys.push(real_r_key.into());
                                                left_tuple.vals.push(v.into());
                                                return Some(Ok(left_tuple));
                                            }
                                            Err(e) => return Some(Err(e)),
                                        }
                                    } else {
                                        continue 'inner;
                                    }
                                }
                                NodeEdgeChainKind::Bidi => {
                                    todo!()
                                }
                            }
                        }
                    }
                    // iterator goes out of the table
                    return None;
                }
            }
        }
    }
}

pub struct EdgeToNodeChainJoinIterator<'a> {
    // TODO associates, right_outer
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right: TableRowGetter<'a>,
    pub(crate) left_outer: bool,
    pub(crate) key_start_idx: usize,
    pub(crate) key_end_idx: usize,
}

impl<'a> Iterator for EdgeToNodeChainJoinIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.left.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(mut left_tuple)) => {
                    self.right.reset();
                    let key_iter = (self.key_start_idx..self.key_end_idx)
                        .map(|i| left_tuple.keys.last().unwrap().get(i).unwrap());
                    match self.right.get_with_iter(key_iter) {
                        Ok(v) => {
                            match v {
                                None => {
                                    if self.left_outer {
                                        left_tuple.keys.push(OwnTuple::empty_tuple().into());
                                        left_tuple.vals.push(OwnTuple::empty_tuple().into());
                                        return Some(Ok(left_tuple));
                                    }
                                    // else fall through, go to the next iteration
                                }
                                Some(right_val) => {
                                    left_tuple.keys.push(self.right.key_cache.clone().into());
                                    left_tuple.vals.push(right_val.into());
                                    return Some(Ok(left_tuple));
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            };
        }
    }
}

pub struct KeyedUnionIterator<'a> {
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
}

impl<'a> Iterator for KeyedUnionIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut left_cache = match self.left.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t,
        };

        let mut right_cache = match self.right.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t,
        };

        loop {
            let cmp_res = left_cache.all_keys_cmp(&right_cache);
            match cmp_res {
                Ordering::Equal => {
                    return Some(Ok(left_cache));
                }
                Ordering::Less => {
                    // Advance the left one
                    match self.left.next() {
                        None => return None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => {
                            left_cache = t;
                        }
                    };
                }
                Ordering::Greater => {
                    // Advance the right one
                    match self.right.next() {
                        None => return None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => {
                            right_cache = t;
                        }
                    };
                }
            }
        }
    }
}

pub struct KeyedDifferenceIterator<'a> {
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right_cache: Option<MegaTuple>,
    pub(crate) started: bool,
}

impl<'a> Iterator for KeyedDifferenceIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.right_cache = match self.right.next() {
                None => None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(t)) => Some(t),
            };

            self.started = true;
        }

        let mut left_cache = match self.left.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t,
        };

        loop {
            let right = match &self.right_cache {
                None => {
                    // right is exhausted, so all left ones can be returned
                    return Some(Ok(left_cache));
                }
                Some(r) => r,
            };
            let cmp_res = left_cache.all_keys_cmp(right);
            match cmp_res {
                Ordering::Equal => {
                    // Discard, since we are interested in difference
                    left_cache = match self.left.next() {
                        None => return None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => t,
                    };
                    self.right_cache = match self.right.next() {
                        None => None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => Some(t),
                    };
                }
                Ordering::Less => {
                    // the left one has no match, so return it
                    return Some(Ok(left_cache));
                }
                Ordering::Greater => {
                    // Advance the right one
                    match self.right.next() {
                        None => self.right_cache = None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => {
                            self.right_cache = Some(t);
                        }
                    };
                }
            }
        }
    }
}

pub struct BagsUnionIterator<'a> {
    pub(crate) bags: Vec<Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>>,
    pub(crate) current: usize,
}

impl<'a> Iterator for BagsUnionIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur_it = self.bags.get_mut(self.current).unwrap();
            match cur_it.next() {
                None => {
                    if self.current == self.bags.len() - 1 {
                        return None;
                    } else {
                        self.current += 1;
                    }
                }
                v => return v,
            }
        }
    }
}

pub struct NodeIterator<'a> {
    pub(crate) it: &'a IteratorPtr<'a>,
    pub(crate) started: bool,
}

impl<'a> Iterator for NodeIterator<'a> {
    type Item = Result<MegaTuple>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        unsafe {
            self.it.pair().map(|(k, v)| {
                Ok(MegaTuple {
                    keys: vec![Tuple::new(k).into()],
                    vals: vec![Tuple::new(v).into()],
                })
            })
        }
    }
}

pub struct EdgeIterator<'a> {
    pub(crate) it: &'a IteratorPtr<'a>,
    pub(crate) started: bool,
    pub(crate) src_table_id: i64,
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        loop {
            match unsafe { self.it.pair() } {
                None => return None,
                Some((k, v)) => {
                    let kt = Tuple::new(k);
                    let vt = Tuple::new(v);
                    if kt.get_int(0) != Some(self.src_table_id) {
                        return None;
                    }
                    if matches!(vt.data_kind(), Ok(DataKind::Data)) {
                        return Some(Ok(MegaTuple {
                            keys: vec![kt.into()],
                            vals: vec![vt.into()],
                        }));
                    } else {
                        self.it.next();
                    }
                }
            }
        }
    }
}

pub struct EdgeKeyOnlyBwdIterator<'a> {
    pub(crate) it: &'a IteratorPtr<'a>,
    pub(crate) started: bool,
    pub(crate) dst_table_id: i64,
}

impl<'a> Iterator for EdgeKeyOnlyBwdIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        loop {
            match unsafe { self.it.pair() } {
                None => return None,
                Some((_k, rev_k)) => {
                    let rev_k_tuple = Tuple::new(rev_k);
                    if rev_k_tuple.get_int(0) != Some(self.dst_table_id) {
                        return None;
                    }
                    if !matches!(rev_k_tuple.data_kind(), Ok(DataKind::Edge)) {
                        self.it.next()
                    } else {
                        return Some(Ok(MegaTuple {
                            keys: vec![rev_k_tuple.into()],
                            vals: vec![],
                        }));
                    }
                }
            }
        }
    }
}

pub struct KeySortedWithAssocIterator<'a> {
    pub(crate) main: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) associates: Vec<NodeIterator<'a>>,
    pub(crate) buffer: Vec<Option<(CowTuple, CowTuple)>>,
}

impl<'a> Iterator for KeySortedWithAssocIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.main.next() {
            None => None, // main exhausted, we are finished
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(MegaTuple { mut keys, mut vals })) => {
                // extract key from main
                let k = match keys.pop() {
                    None => return Some(Err(LogicError("Empty keys".to_string()))),
                    Some(k) => k,
                };
                let l = self.associates.len();
                // initialize vector for associate values
                let mut assoc_vals: Vec<Option<CowTuple>> =
                    iter::repeat_with(|| None).take(l).collect();
                // let l = assoc_vals.len();
                #[allow(clippy::needless_range_loop)]
                for i in 0..l {
                    // for each associate
                    let cached = self.buffer.get(i).unwrap();
                    // if no cache, try to get cache filled first
                    if matches!(cached, None) {
                        let assoc_data = self.associates.get_mut(i).unwrap().next().map(|mt| {
                            mt.map(|mut mt| (mt.keys.pop().unwrap(), mt.vals.pop().unwrap()))
                        });
                        match assoc_data {
                            None => self.buffer[i] = None,
                            Some(Ok(data)) => self.buffer[i] = Some(data),
                            Some(Err(e)) => return Some(Err(e)),
                        }
                    }

                    // if we have cache
                    while let Some((ck, _)) = self.buffer.get(i).unwrap() {
                        match k.key_part_cmp(ck) {
                            Ordering::Less => {
                                // target key less than cache key, no value for current iteration
                                break;
                            }
                            Ordering::Equal => {
                                // target key equals cache key, we put it into collected values
                                let (_, v) = mem::replace(&mut self.buffer[i], None).unwrap();
                                assoc_vals[i] = Some(v);
                                break;
                            }
                            Ordering::Greater => {
                                // target key greater than cache key, meaning that the source has holes (maybe due to filtering)
                                // get a new one into buffer
                                let assoc_data =
                                    self.associates.get_mut(i).unwrap().next().map(|mt| {
                                        mt.map(|mut mt| {
                                            (mt.keys.pop().unwrap(), mt.vals.pop().unwrap())
                                        })
                                    });
                                match assoc_data {
                                    None => self.buffer[i] = None,
                                    Some(Ok(data)) => self.buffer[i] = Some(data),
                                    Some(Err(e)) => return Some(Err(e)),
                                }
                            }
                        }
                    }
                }
                vals.extend(assoc_vals.into_iter().map(|v| match v {
                    None => CowTuple::new(CowSlice::Own(EMPTY_DATA.into())),
                    Some(v) => v,
                }));
                Some(Ok(MegaTuple {
                    keys: vec![k],
                    vals,
                }))
            }
        }
    }
}

pub struct OuterMergeJoinIterator<'a> {
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) left_outer: bool,
    pub(crate) right_outer: bool,
    pub(crate) left_keys: &'a [(TableId, ColId)],
    pub(crate) right_keys: &'a [(TableId, ColId)],
    pub(crate) left_len: (usize, usize),
    pub(crate) right_len: (usize, usize),
    pub(crate) left_cache: Option<MegaTuple>,
    pub(crate) right_cache: Option<MegaTuple>,
    pub(crate) pull_left: bool,
    pub(crate) pull_right: bool,
}

impl<'a> Iterator for OuterMergeJoinIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pull_left {
            self.left_cache = match self.left.next() {
                None => None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(t)) => Some(t),
            };
            self.pull_left = false;
        }

        if self.pull_right {
            self.right_cache = match self.right.next() {
                None => None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(t)) => Some(t),
            };
            self.pull_right = false;
        }

        let make_empty_tuple = |is_left: bool| -> MegaTuple {
            let lengths = if is_left {
                self.left_len
            } else {
                self.right_len
            };
            let keys = iter::repeat_with(|| OwnTuple::empty_tuple().into())
                .take(lengths.0)
                .collect();
            let vals = iter::repeat_with(|| OwnTuple::empty_tuple().into())
                .take(lengths.1)
                .collect();
            MegaTuple { keys, vals }
        };

        loop {
            let left_cache = match &self.left_cache {
                None => {
                    return match &self.right_cache {
                        None => None,
                        Some(_) => {
                            if self.right_outer {
                                self.pull_right = true;
                                let mut tuple = make_empty_tuple(true);
                                let right = self.right_cache.take().unwrap();
                                tuple.extend(right);
                                Some(Ok(tuple))
                            } else {
                                None
                            }
                        }
                    };
                }
                Some(t) => t,
            };
            let right_cache = match &self.right_cache {
                None => {
                    return match &self.left_cache {
                        None => None,
                        Some(_) => {
                            if self.left_outer {
                                self.pull_left = true;
                                let tuple = make_empty_tuple(false);
                                let mut left = self.right_cache.take().unwrap();
                                left.extend(tuple);
                                Some(Ok(left))
                            } else {
                                None
                            }
                        }
                    };
                }
                Some(t) => t,
            };
            let cmp_res = match compare_tuple_by_keys(
                (left_cache, self.left_keys),
                (right_cache, self.right_keys),
            ) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            match cmp_res {
                Ordering::Equal => {
                    // Both are present
                    self.pull_left = true;
                    self.pull_right = true;
                    let mut left = self.left_cache.take().unwrap();
                    let right = self.right_cache.take().unwrap();
                    left.extend(right);
                    return Some(Ok(left));
                }
                Ordering::Less => {
                    // Advance the left one
                    if self.left_outer {
                        self.pull_left = true;
                        let right = make_empty_tuple(false);
                        let mut left = self.left_cache.take().unwrap();
                        left.extend(right);
                        return Some(Ok(left));
                    } else {
                        match self.left.next() {
                            None => return None,
                            Some(Err(e)) => return Some(Err(e)),
                            Some(Ok(t)) => {
                                self.left_cache = Some(t);
                            }
                        };
                    }
                }
                Ordering::Greater => {
                    // Advance the right one
                    if self.right_outer {
                        self.pull_right = true;
                        let mut left = make_empty_tuple(true);
                        let right = self.right_cache.take().unwrap();
                        left.extend(right);
                        return Some(Ok(left));
                    } else {
                        match self.right.next() {
                            None => return None,
                            Some(Err(e)) => return Some(Err(e)),
                            Some(Ok(t)) => {
                                self.right_cache = Some(t);
                            }
                        };
                    }
                }
            }
        }
    }
}

pub struct MergeJoinIterator<'a> {
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) right: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) left_keys: &'a [(TableId, ColId)],
    pub(crate) right_keys: &'a [(TableId, ColId)],
}

impl<'a> Iterator for MergeJoinIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut left_cache = match self.left.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t,
        };

        let mut right_cache = match self.right.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t,
        };

        loop {
            let cmp_res = match compare_tuple_by_keys(
                (&left_cache, self.left_keys),
                (&right_cache, self.right_keys),
            ) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            match cmp_res {
                Ordering::Equal => {
                    left_cache.extend(right_cache);
                    return Some(Ok(left_cache));
                }
                Ordering::Less => {
                    // Advance the left one
                    match self.left.next() {
                        None => return None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => {
                            left_cache = t;
                        }
                    };
                }
                Ordering::Greater => {
                    // Advance the right one
                    match self.right.next() {
                        None => return None,
                        Some(Err(e)) => return Some(Err(e)),
                        Some(Ok(t)) => {
                            right_cache = t;
                        }
                    };
                }
            }
        }
    }
}

pub struct CartesianProdIterator<'a> {
    pub(crate) left: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) left_cache: MegaTuple,
    pub(crate) right_source: &'a ExecPlan<'a>,
    pub(crate) right: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
}

impl<'a> Iterator for CartesianProdIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left_cache.is_empty() {
            self.left_cache = match self.left.next() {
                None => return None,
                Some(Ok(v)) => v,
                Some(Err(e)) => return Some(Err(e)),
            }
        }
        let r_tpl = match self.right.next() {
            None => {
                self.right = match self.right_source.iter() {
                    Ok(it) => it,
                    Err(e) => return Some(Err(e)),
                };
                self.left_cache = match self.left.next() {
                    None => return None,
                    Some(Ok(v)) => v,
                    Some(Err(e)) => return Some(Err(e)),
                };
                match self.right.next() {
                    // early return in case right is empty
                    None => return None,
                    Some(Ok(r_tpl)) => r_tpl,
                    Some(Err(e)) => return Some(Err(e)),
                }
            }
            Some(Ok(r_tpl)) => r_tpl,
            Some(Err(e)) => return Some(Err(e)),
        };
        let mut ret = self.left_cache.clone();
        ret.keys.extend(r_tpl.keys);
        ret.vals.extend(r_tpl.vals);
        Some(Ok(ret))
    }
}

pub struct FilterIterator<'a> {
    pub(crate) it: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) filter: &'a Value<'a>,
}

impl<'a> Iterator for FilterIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        for t in self.it.by_ref() {
            match t {
                Ok(t) => match tuple_eval(self.filter, &t) {
                    Ok(Value::Bool(true)) => {
                        return Some(Ok(t));
                    }
                    Ok(Value::Bool(false)) | Ok(Value::Null) => {}
                    Ok(_v) => {
                        return Some(Err(LogicError("Unexpected type in filter".to_string())));
                    }
                    Err(e) => return Some(Err(e)),
                },
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub struct OutputIterator<'a> {
    pub(crate) it: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) transform: &'a Value<'a>,
}

impl<'a> OutputIterator<'a> {
    pub fn new(it: &'a ExecPlan<'a>, transform: &'a Value<'a>) -> Result<Self> {
        Ok(Self {
            it: it.iter()?,
            transform,
        })
    }
}

impl<'a> Iterator for OutputIterator<'a> {
    type Item = Result<Value<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(t)) => Some(tuple_eval(self.transform, &t).map(|v| v.to_static())),
        }
    }
}

pub struct EvalIterator<'a> {
    pub(crate) it: Box<dyn Iterator<Item = Result<MegaTuple>> + 'a>,
    pub(crate) keys: &'a [(String, Value<'a>)],
    pub(crate) vals: &'a [(String, Value<'a>)],
}

pub const EVAL_TEMP_PREFIX: u32 = u32::MAX - 1;

impl<'a> Iterator for EvalIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(t)) => {
                let mut key_tuple = OwnTuple::with_prefix(EVAL_TEMP_PREFIX);
                let mut val_tuple = OwnTuple::with_data_prefix(DataKind::Data);
                for k in self.keys {
                    match tuple_eval(&k.1, &t) {
                        Ok(v) => key_tuple.push_value(&v),
                        Err(e) => return Some(Err(e)),
                    }
                }
                for k in self.vals {
                    match tuple_eval(&k.1, &t) {
                        Ok(v) => val_tuple.push_value(&v),
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Ok(MegaTuple {
                    keys: vec![key_tuple.into()],
                    vals: vec![val_tuple.into()],
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::engine::Engine;
    use crate::db::iterator::{ExecPlan, OutputIterator};
    use crate::db::query::FromEl;
    use crate::db::table::TableInfo;
    use crate::error::Result;
    use crate::parser::{Parser, Rule};
    use crate::relation::data::DataKind;
    use crate::relation::value::Value;
    use pest::Parser as PestParser;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Instant;

    #[test]
    fn pair_value() -> Result<()> {
        let s = "{x: e.first_name ++ ' ' ++ e.last_name, y: [e.a, e.b ++ e.c]}";
        let res = Parser::parse(Rule::expr, s).unwrap().next().unwrap();
        let v = Value::from_pair(res)?;
        println!("{:#?}", v);
        Ok(())
    }

    #[test]
    fn plan() -> Result<()> {
        let db_path = "_test_db_plan";
        let engine = Engine::new(db_path.to_string(), true).unwrap();
        {
            let mut sess = engine.session().unwrap();
            let start = Instant::now();
            let s = fs::read_to_string("test_data/hr.cozo").unwrap();

            for p in Parser::parse(Rule::file, &s).unwrap() {
                if p.as_rule() == Rule::EOI {
                    break;
                }
                sess.run_definition(p).unwrap();
            }
            sess.commit().unwrap();

            let data = fs::read_to_string("test_data/hr.json").unwrap();
            let value = Value::parse_str(&data).unwrap();
            let s = "insert $data;";
            let p = Parser::parse(Rule::file, &s).unwrap().next().unwrap();
            let params = BTreeMap::from([("$data".into(), value)]);

            assert!(sess.run_mutation(p.clone(), &params).is_ok());
            sess.commit().unwrap();
            let start2 = Instant::now();

            let s = "from e:Employee";
            let p = Parser::parse(Rule::from_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            let from_pat = match sess.parse_from_pattern(p).unwrap().pop().unwrap() {
                FromEl::Simple(s) => s,
                FromEl::Chain(_) => panic!(),
            };
            let s = "where e.id >= 100, e.id <= 105 || e.id == 110";
            let p = Parser::parse(Rule::where_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            let where_pat = sess.parse_where_pattern(p).unwrap();

            let s = r#"select {id: e.id,
            full_name: e.first_name ++ ' ' ++ e.last_name, bibio_name: e.last_name ++ ', '
            ++ e.first_name ++ ': ' ++ (e.phone_number ~ 'N.A.')}"#;
            let p = Parser::parse(Rule::select_pattern, s)
                .unwrap()
                .next()
                .unwrap();
            let sel_pat = sess.parse_select_pattern(p).unwrap();
            let sel_vals = Value::Dict(
                sel_pat
                    .vals
                    .into_iter()
                    .map(|(k, v)| (k.into(), v))
                    .collect(),
            );
            let amap = sess.node_accessor_map(&from_pat.binding, &from_pat.info);
            let (_, vals) = sess
                .partial_eval(sel_vals, &Default::default(), &amap)
                .unwrap();
            let (_, where_vals) = sess
                .partial_eval(where_pat, &Default::default(), &amap)
                .unwrap();
            println!(
                "{:#?}",
                sess.cnf_with_table_refs(where_vals.clone(), &Default::default(), &amap)
            );
            let (vcoll, mut rel_tbls) =
                Value::extract_relevant_tables([vals, where_vals].into_iter()).unwrap();
            let mut vcoll = vcoll.into_iter();
            let vals = vcoll.next().unwrap();
            let where_vals = vcoll.next().unwrap();
            println!("VALS AFTER 2  {} {}", vals, where_vals);

            println!("{:?}", from_pat);
            println!("{:?}", amap);
            println!("{:?}", rel_tbls);

            let tbl = rel_tbls.pop().unwrap();
            let it = sess.iter_node(tbl);
            let it = ExecPlan::FilterItPlan {
                filter: where_vals,
                source: it.into(),
            };
            let it = OutputIterator::new(&it, &vals)?;
            for val in it {
                println!("{}", val?);
            }
            let duration = start.elapsed();
            let duration2 = start2.elapsed();
            println!("Time elapsed {:?} {:?}", duration, duration2);
            let dummy_tinfo = TableInfo {
                kind: DataKind::Data,
                table_id: Default::default(),
                src_table_id: Default::default(),
                dst_table_id: Default::default(),
                data_keys: Default::default(),
                key_typing: vec![],
                val_typing: vec![],
                src_key_typing: vec![],
                dst_key_typing: vec![],
                associates: vec![],
            };
            let it = ExecPlan::KeySortedWithAssocItPlan {
                main: Box::new(sess.iter_node(tbl)),
                associates: vec![
                    (dummy_tinfo.clone(), sess.raw_iterator(true).into()),
                    (dummy_tinfo.clone(), sess.raw_iterator(true).into()),
                    (dummy_tinfo.clone(), sess.raw_iterator(true).into()),
                ],
                binding: None,
            };
            {
                for el in it.iter()? {
                    println!("{:?}", el?);
                }
            }
            println!("XXXXX");
            {
                for el in it.iter()? {
                    println!("{:?}", el?);
                }
            }
            let mut it = sess.iter_node(tbl);
            for _ in 0..2 {
                it = ExecPlan::CartesianProdItPlan {
                    left: Box::new(it),
                    right: Box::new(sess.iter_node(tbl)),
                }
            }

            let start = Instant::now();

            println!("Now cartesian product");
            let mut n = 0;
            for el in it.iter()? {
                let el = el.unwrap();
                // if n % 4096 == 0 {
                //     println!("{}: {:?}", n, el)
                // }
                let _x = el
                    .keys
                    .into_iter()
                    .map(|v| v.iter().map(|_v| ()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let _y = el
                    .vals
                    .into_iter()
                    .map(|v| v.iter().map(|_v| ()).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                n += 1;
            }
            let duration = start.elapsed();
            println!(
                "{} items per second",
                1e9 * (n as f64) / (duration.as_nanos() as f64)
            );

            let s = r##"from e:Employee
            where e.id >= 100, e.id <= 105 || e.id == 110
            select {id: e.id,
            full_name: e.first_name ++ ' ' ++ e.last_name, bibio_name: e.last_name ++ ', '
            ++ e.first_name ++ ': ' ++ (e.phone_number ~ 'N.A.')}"##;

            let parsed = Parser::parse(Rule::relational_query, s)?.next().unwrap();
            let plan = sess.query_to_plan(parsed)?;
            println!("{:?}", plan);
            let plan = sess.reify_output_plan(plan)?;
            println!("{:?}", plan);
            for val in plan.iter()? {
                println!("{}", val?)
            }

            let s = r##"from hj:HasJob
            where hj.salary < 5000 || hj._dst_id == 19
            select {src_id: hj._src_id, dst_id: hj._dst_id, salary: hj.salary, hire_date: hj.hire_date}"##;

            let parsed = Parser::parse(Rule::relational_query, s)?.next().unwrap();
            let plan = sess.query_to_plan(parsed)?;
            println!("{:?}", plan);
            let plan = sess.reify_output_plan(plan)?;
            println!("{:?}", plan);
            for val in plan.iter()? {
                println!("{}", val?)
            }

            let start = Instant::now();

            let s = r##"from e1:Employee, e2:Employee
            where e1.id == e2.id - 10
            select { fid: e1.id, fname: e1.first_name, sid: e2.id, sname: e2.first_name }"##;

            let parsed = Parser::parse(Rule::relational_query, s)?.next().unwrap();
            let plan = sess.query_to_plan(parsed)?;
            let plan = sess.reify_output_plan(plan)?;
            for val in plan.iter()? {
                println!("{}", val?)
            }

            let duration = start.elapsed();
            println!("Time elapsed {:?}", duration);

            let start = Instant::now();

            let s = r##"from (e:Employee)-[hj:HasJob]->(j:Job)
            where j.id == 16
            select { eid: e.id, jid: j.id, fname: e.first_name, salary: hj.salary, job: j.title }"##;

            let parsed = Parser::parse(Rule::relational_query, s)?.next().unwrap();
            let plan = sess.query_to_plan(parsed)?;
            let plan = sess.reify_output_plan(plan)?;
            for val in plan.iter()? {
                println!("{}", val?)
            }
            let duration = start.elapsed();
            println!("Time elapsed {:?}", duration);

            let start = Instant::now();

            let s = r##"from (j:Job)<-[hj:HasJob]-(e:Employee)
            where j.id >= 16
            select { eid: e.id, jid: j.id, fname: e.first_name, salary: hj.salary, job: j.title }
            ordered [j.id: desc, e.id]"##;

            let parsed = Parser::parse(Rule::relational_query, s)?.next().unwrap();
            let plan = sess.query_to_plan(parsed)?;
            let plan = sess.reify_output_plan(plan)?;
            for val in plan.iter()? {
                println!("{}", val?)
            }
            let duration = start.elapsed();
            println!("Time elapsed {:?}", duration);
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
        Ok(())
    }
}
