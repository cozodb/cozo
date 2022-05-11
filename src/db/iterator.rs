use crate::db::eval::{compare_tuple_by_keys, tuple_eval};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::error::CozoError::LogicError;
use crate::error::Result;
use crate::relation::data::{DataKind, EMPTY_DATA};
use crate::relation::table::MegaTuple;
use crate::relation::tuple::{CowSlice, CowTuple, OwnTuple, Tuple};
use crate::relation::value::Value;
use cozorocks::IteratorPtr;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::{iter, mem};

pub enum IteratorSlot<'a> {
    Dummy,
    Reified(IteratorPtr<'a>),
}

impl<'a> Debug for IteratorSlot<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IteratorSlot::Dummy { .. } => write!(f, "DummyIterator"),
            IteratorSlot::Reified(_) => write!(f, "BaseIterator"),
        }
    }
}

impl<'a> From<IteratorPtr<'a>> for IteratorSlot<'a> {
    fn from(it: IteratorPtr<'a>) -> Self {
        Self::Reified(it)
    }
}

impl<'a> IteratorSlot<'a> {
    pub fn try_get(&self) -> Result<&IteratorPtr<'a>> {
        match self {
            IteratorSlot::Dummy => Err(LogicError("Cannot iter over dummy".to_string())),
            IteratorSlot::Reified(r) => Ok(r),
        }
    }
}

#[derive(Debug)]
pub enum ExecPlan<'a> {
    NodeItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: String,
    },
    EdgeItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
        binding: String,
    },
    EdgeKeyOnlyBwdItPlan {
        it: IteratorSlot<'a>,
        info: TableInfo,
    },
    // EdgeBwdIt { it: IteratorPtr<'a>, sess: &'a Session<'a>, tid: u32 },
    // IndexIt {it: ..}
    KeySortedWithAssocItPlan {
        main: Box<ExecPlan<'a>>,
        associates: Vec<(u32, IteratorSlot<'a>)>,
    },
    CartesianProdItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    MergeJoinItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
        left_keys: Vec<(TableId, ColId)>,
        right_keys: Vec<(TableId, ColId)>,
    },
    OuterMergeJoinItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
        left_keys: Vec<(TableId, ColId)>,
        right_keys: Vec<(TableId, ColId)>,
        left_outer: bool,
        right_outer: bool,
        left_len: (usize, usize),
        right_len: (usize, usize),
    },
    KeyedUnionItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    KeyedDifferenceItPlan {
        left: Box<ExecPlan<'a>>,
        right: Box<ExecPlan<'a>>,
    },
    FilterItPlan {
        source: Box<ExecPlan<'a>>,
        filter: Value<'a>,
    },
    EvalItPlan {
        source: Box<ExecPlan<'a>>,
        keys: Vec<(String, Value<'a>)>,
        vals: Vec<(String, Value<'a>)>,
    },
    BagsUnionIt {
        bags: Vec<ExecPlan<'a>>,
    },
}

#[derive(Debug)]
pub struct OutputItPlan<'a> {
    pub source: ExecPlan<'a>,
    pub value: Value<'a>,
}

impl<'a> OutputItPlan<'a> {
    pub fn iter(&self) -> Result<OutputIterator> {
        Ok(OutputIterator {
            it: self.source.iter()?,
            transform: &self.value,
        })
    }
}

impl<'a> ExecPlan<'a> {
    pub fn iter(&'a self) -> Result<Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>> {
        match self {
            ExecPlan::NodeItPlan { it, info, .. } => {
                let it = it.try_get()?;
                let prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                it.seek(prefix_tuple);

                Ok(Box::new(NodeIterator { it, started: false }))
            }
            ExecPlan::EdgeItPlan { it, info, .. } => {
                let it = it.try_get()?;
                let mut prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                prefix_tuple.push_int(info.src_table_id.id);
                it.seek(prefix_tuple);

                Ok(Box::new(EdgeIterator { it, started: false, src_table_id: info.src_table_id.id }))
            }
            ExecPlan::EdgeKeyOnlyBwdItPlan { it, info } => {
                let it = it.try_get()?;
                let mut prefix_tuple = OwnTuple::with_prefix(info.table_id.id as u32);
                prefix_tuple.push_int(info.dst_table_id.id);
                it.seek(prefix_tuple);

                Ok(Box::new(EdgeKeyOnlyBwdIterator { it, started: false, dst_table_id: info.dst_table_id.id }))
            }
            ExecPlan::KeySortedWithAssocItPlan { main, associates } => {
                let buffer = iter::repeat_with(|| None).take(associates.len()).collect();
                let associates = associates
                    .iter()
                    .map(|(tid, it)| {
                        it.try_get().map(|it| {
                            let prefix_tuple = OwnTuple::with_prefix(*tid);
                            it.seek(prefix_tuple);

                            NodeIterator { it, started: false }
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(KeySortedWithAssocIterator {
                    main: main.iter()?,
                    associates,
                    buffer,
                }))
            }
            ExecPlan::CartesianProdItPlan { left, right } => Ok(Box::new(CartesianProdIterator {
                left: left.iter()?,
                left_cache: MegaTuple::empty_tuple(),
                right_source: right.as_ref(),
                right: right.as_ref().iter()?,
            })),
            ExecPlan::FilterItPlan { source: it, filter } => Ok(Box::new(FilterIterator {
                it: it.iter()?,
                filter,
            })),
            ExecPlan::EvalItPlan {
                source: it,
                keys,
                vals
            } => Ok(Box::new(EvalIterator {
                it: it.iter()?,
                keys,
                vals,
            })),
            ExecPlan::MergeJoinItPlan {
                left,
                right,
                left_keys,
                right_keys,
            } => Ok(Box::new(MergeJoinIterator {
                left: left.iter()?,
                right: right.iter()?,
                left_keys,
                right_keys,
            })),
            ExecPlan::OuterMergeJoinItPlan {
                left,
                right,
                left_keys,
                right_keys,
                left_outer,
                right_outer,
                left_len,
                right_len,
            } => Ok(Box::new(OuterMergeJoinIterator {
                left: left.iter()?,
                right: right.iter()?,
                left_outer: *left_outer,
                right_outer: *right_outer,
                left_keys,
                right_keys,
                left_len: *left_len,
                right_len: *right_len,
                left_cache: None,
                right_cache: None,
                pull_left: true,
                pull_right: true,
            })),
            ExecPlan::KeyedUnionItPlan { left, right } => Ok(Box::new(KeyedUnionIterator {
                left: left.iter()?,
                right: right.iter()?,
            })),
            ExecPlan::KeyedDifferenceItPlan { left, right } => Ok(Box::new(KeyedDifferenceIterator {
                left: left.iter()?,
                right: right.iter()?,
                right_cache: None,
                started: false,
            })),
            ExecPlan::BagsUnionIt { bags } => {
                let bags = bags.iter().map(|i| i.iter()).collect::<Result<Vec<_>>>()?;
                Ok(Box::new(BagsUnionIterator { bags, current: 0 }))
            }
        }
    }
}

pub struct KeyedUnionIterator<'a> {
    left: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
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
    left: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    right_cache: Option<MegaTuple>,
    started: bool,
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
    bags: Vec<Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>>,
    current: usize,
}

impl<'a> Iterator for BagsUnionIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        let cur_it = self.bags.get_mut(self.current).unwrap();
        match cur_it.next() {
            None => {
                if self.current == self.bags.len() - 1 {
                    None
                } else {
                    self.current += 1;
                    self.next()
                }
            }
            v => v,
        }
    }
}

pub struct NodeIterator<'a> {
    it: &'a IteratorPtr<'a>,
    started: bool,
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
    it: &'a IteratorPtr<'a>,
    started: bool,
    src_table_id: i64,
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
    it: &'a IteratorPtr<'a>,
    started: bool,
    dst_table_id: i64,
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
    main: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    associates: Vec<NodeIterator<'a>>,
    buffer: Vec<Option<(CowTuple, CowTuple)>>,
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
                let mut assoc_vals: Vec<Option<CowTuple>> = iter::repeat_with(|| None).take(l).collect();
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
    left: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    left_outer: bool,
    right_outer: bool,
    left_keys: &'a [(TableId, ColId)],
    right_keys: &'a [(TableId, ColId)],
    left_len: (usize, usize),
    right_len: (usize, usize),
    left_cache: Option<MegaTuple>,
    right_cache: Option<MegaTuple>,
    pull_left: bool,
    pull_right: bool,
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
    left: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    left_keys: &'a [(TableId, ColId)],
    right_keys: &'a [(TableId, ColId)],
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
    left: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    left_cache: MegaTuple,
    right_source: &'a ExecPlan<'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
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
    it: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    filter: &'a Value<'a>,
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
    it: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    transform: &'a Value<'a>,
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
    it: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    keys: &'a [(String, Value<'a>)],
    vals: &'a [(String, Value<'a>)],
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
    use crate::error::Result;
    use crate::parser::{Parser, Rule};
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
            let sel_vals = Value::Dict(sel_pat.vals.into_iter().map(|(k, v)| (k.into(), v)).collect());
            let amap = sess.node_accessor_map(
                &from_pat.binding,
                &from_pat.info,
            );
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
                println!("{}", val.unwrap());
            }
            let duration = start.elapsed();
            let duration2 = start2.elapsed();
            println!("Time elapsed {:?} {:?}", duration, duration2);
            let it = ExecPlan::KeySortedWithAssocItPlan {
                main: Box::new(sess.iter_node(tbl)),
                associates: vec![
                    (tbl.id as u32, sess.raw_iterator(true).into()),
                    (tbl.id as u32, sess.raw_iterator(true).into()),
                    (tbl.id as u32, sess.raw_iterator(true).into()),
                ],
            };
            {
                for el in it.iter()? {
                    println!("{:?}", el);
                }
            }
            println!("XXXXX");
            {
                for el in it.iter()? {
                    println!("{:?}", el);
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
                println!("{:?}", val)
            }
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
        Ok(())
    }
}
