use std::collections::BTreeMap;
use std::{iter, mem};
use std::cmp::Ordering;
use pest::iterators::Pair;
use cozorocks::{IteratorPtr};
use crate::db::engine::Session;
use crate::db::eval::{compare_tuple_by_keys, tuple_eval};
use crate::db::query::{FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::error::CozoError::LogicError;
use crate::relation::value::{StaticValue, Value};
use crate::parser::Rule;
use crate::error::{Result};
use crate::relation::data::{DataKind, EMPTY_DATA};
use crate::relation::table::MegaTuple;
use crate::relation::tuple::{CowSlice, CowTuple, OwnTuple, Tuple};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum QueryPlan {
    Union {
        args: Vec<QueryPlan>
    },
    Intersection {
        args: Vec<QueryPlan>
    },
    Difference {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
    },
    Projection {
        arg: Box<QueryPlan>,
        projection: Selection,
    },
    Grouping {
        arg: Box<QueryPlan>,
        projection: Selection,
    },
    InnerJoinGroup {
        args: Vec<QueryPlan>,
    },
    InnerJoin {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        left_key: Vec<String>,
        right_key: Vec<String>,
    },
    OuterJoin {
        join_type: OuterJoinType,
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        left_key: Vec<String>,
        right_key: Vec<String>,
    },
    Filter {
        rel: Box<QueryPlan>,
        filter: StaticValue,
    },
    BaseRelation {
        table: String,
        binding: String,
        // accessors: AccessorMap,
        info: TableInfo,
    },
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum OuterJoinType {
    LeftJoin,
    RightJoin,
    FullOuterJoin,
}


pub type AccessorMap = BTreeMap<String, BTreeMap<String, (TableId, ColId)>>;

impl<'a> Session<'a> {
    pub fn query_to_plan(&self, pair: Pair<Rule>) -> Result<()> {
        let mut pairs = pair.into_inner();
        let from_data = self.parse_from_pattern(pairs.next().unwrap())?;
        let mut nxt = pairs.next().unwrap();
        let where_data = match nxt.as_rule() {
            Rule::where_pattern => {
                let r = self.parse_where_pattern(nxt)?.to_static();
                nxt = pairs.next().unwrap();
                r
            }
            _ => true.into()
        };
        let select_data = self.parse_select_pattern(nxt)?;
        let plan = self.convert_from_data_to_plan(from_data)?;
        let plan = self.convert_where_data_to_plan(plan, where_data)?;
        let plan = self.convert_select_data_to_plan(plan, select_data)?;
        println!("{:#?}", plan);
        Ok(())
    }
    fn convert_from_data_to_plan(&self, mut from_data: Vec<FromEl>) -> Result<QueryPlan> {
        let res = match from_data.pop().unwrap() {
            FromEl::Simple(el) => {
                println!("{:#?}", self.base_relation_to_accessor_map(&el.table, &el.binding, &el.info));
                QueryPlan::BaseRelation {
                    table: el.table,
                    binding: el.binding,
                    info: el.info,
                }
            }
            FromEl::Chain(_) => todo!()
        };
        Ok(res)
    }
    fn base_relation_to_accessor_map(&self, _table: &str, binding: &str, info: &TableInfo) -> AccessorMap {
        let mut ret = BTreeMap::new();
        for (i, (k, _)) in info.key_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (true, i).into()));
        }
        for (i, (k, _)) in info.val_typing.iter().enumerate() {
            ret.insert(k.into(), (info.table_id, (false, i).into()));
        }
        for assoc in &info.associates {
            for (i, (k, _)) in assoc.key_typing.iter().enumerate() {
                ret.insert(k.into(), (assoc.table_id, (true, i).into()));
            }
            for (i, (k, _)) in assoc.val_typing.iter().enumerate() {
                ret.insert(k.into(), (assoc.table_id, (false, i).into()));
            }
        }
        BTreeMap::from([(binding.to_string(), ret)])
    }
    fn convert_where_data_to_plan(&self, plan: QueryPlan, where_data: StaticValue) -> Result<QueryPlan> {
        let where_data = self.partial_eval(where_data, &Default::default(), &Default::default());
        let plan = match where_data?.1 {
            Value::Bool(true) => plan,
            v => {
                QueryPlan::Filter { rel: Box::new(plan), filter: v }
            }
        };
        Ok(plan)
    }
    fn convert_select_data_to_plan(&self, plan: QueryPlan, select_data: Selection) -> Result<QueryPlan> {
        Ok(QueryPlan::Projection { arg: Box::new(plan), projection: select_data })
    }

    pub fn raw_iterator(&self, in_root: bool) -> IteratorPtr {
        if in_root {
            self.txn.iterator(true, &self.perm_cf)
        } else {
            self.txn.iterator(false, &self.temp_cf)
        }
    }

    pub fn iter_node(&self, tid: TableId) -> MegaTupleIt {
        let it = self.raw_iterator(tid.in_root);
        MegaTupleIt::NodeIt { it, tid: tid.id as u32 }
    }
}

pub enum MegaTupleIt<'a> {
    NodeIt { it: IteratorPtr<'a>, tid: u32 },
    EdgeIt { it: IteratorPtr<'a>, tid: u32 },
    EdgeKeyOnlyBwdIt { it: IteratorPtr<'a>, tid: u32 },
    // EdgeBwdIt { it: IteratorPtr<'a>, sess: &'a Session<'a>, tid: u32 },
    // IndexIt {it: ..}
    KeySortedWithAssocIt { main: Box<MegaTupleIt<'a>>, associates: Vec<(u32, IteratorPtr<'a>)> },
    CartesianProdIt { left: Box<MegaTupleIt<'a>>, right: Box<MegaTupleIt<'a>> },
    MergeJoinIt { left: Box<MegaTupleIt<'a>>, right: Box<MegaTupleIt<'a>>, left_keys: Vec<(TableId, ColId)>, right_keys: Vec<(TableId, ColId)> },
    KeyedUnionIt { left: Box<MegaTupleIt<'a>>, right: Box<MegaTupleIt<'a>> },
    KeyedDifferenceIt { left: Box<MegaTupleIt<'a>>, right: Box<MegaTupleIt<'a>> },
    FilterIt { it: Box<MegaTupleIt<'a>>, filter: Value<'a> },
    EvalIt { it: Box<MegaTupleIt<'a>>, keys: Vec<Value<'a>>, vals: Vec<Value<'a>>, prefix: u32 },
    BagsUnionIt { bags: Vec<MegaTupleIt<'a>> },
}

impl<'a> MegaTupleIt<'a> {
    pub fn iter(&'a self) -> Box<dyn Iterator<Item=Result<MegaTuple>> + 'a> {
        match self {
            MegaTupleIt::NodeIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                Box::new(NodeIterator {
                    it,
                    started: false,
                })
            }
            MegaTupleIt::EdgeIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                Box::new(EdgeIterator {
                    it,
                    started: false,
                })
            }
            MegaTupleIt::EdgeKeyOnlyBwdIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                Box::new(EdgeKeyOnlyBwdIterator {
                    it,
                    started: false,
                })
            }
            MegaTupleIt::KeySortedWithAssocIt { main, associates } => {
                let buffer = iter::repeat_with(|| None).take(associates.len()).collect();
                let associates = associates.into_iter().map(|(tid, it)| {
                    let prefix_tuple = OwnTuple::with_prefix(*tid);
                    it.seek(prefix_tuple);

                    NodeIterator {
                        it,
                        started: false,
                    }
                }).collect();
                Box::new(KeySortedWithAssocIterator {
                    main: Box::new(main.iter()),
                    associates,
                    buffer,
                })
            }
            MegaTupleIt::CartesianProdIt { left, right } => {
                Box::new(CartesianProdIterator {
                    left: Box::new(left.iter()),
                    left_cache: MegaTuple::empty_tuple(),
                    right_source: right.as_ref(),
                    right: Box::new(right.as_ref().iter()),
                })
            }
            MegaTupleIt::FilterIt { it, filter } => {
                Box::new(FilterIterator {
                    it: Box::new(it.iter()),
                    filter,
                })
            }
            MegaTupleIt::EvalIt { it, keys, vals, prefix } => {
                Box::new(EvalIterator {
                    it: Box::new(it.iter()),
                    keys,
                    vals,
                    prefix: *prefix,
                })
            }
            MegaTupleIt::MergeJoinIt { left, right, left_keys, right_keys } => {
                Box::new(MergeJoinIterator {
                    left: Box::new(left.iter()),
                    right: Box::new(right.iter()),
                    left_keys,
                    right_keys,
                })
            }
            MegaTupleIt::KeyedUnionIt { .. } => {
                todo!()
            }
            MegaTupleIt::KeyedDifferenceIt { .. } => {
                todo!()
            }
            MegaTupleIt::BagsUnionIt { bags } => {
                let bags = bags.iter().map(|i| i.iter()).collect();
                Box::new(BagsUnionIterator {
                    bags,
                    current: 0
                })
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
            v => v
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
        self.it.pair().map(|(k, v)| {
            Ok(MegaTuple {
                keys: vec![Tuple::new(k).into()],
                vals: vec![Tuple::new(v).into()],
            })
        })
    }
}

pub struct EdgeIterator<'a> {
    it: &'a IteratorPtr<'a>,
    started: bool,
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
            match self.it.pair() {
                None => return None,
                Some((k, v)) => {
                    let vt = Tuple::new(v);
                    if matches!(vt.data_kind(), Ok(DataKind::Edge)) {
                        self.it.next()
                    } else {
                        let kt = Tuple::new(k);
                        return Some(Ok(MegaTuple {
                            keys: vec![kt.into()],
                            vals: vec![vt.into()],
                        }));
                    }
                }
            }
        }
    }
}

pub struct EdgeKeyOnlyBwdIterator<'a> {
    it: &'a IteratorPtr<'a>,
    started: bool,
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
            match self.it.pair() {
                None => return None,
                Some((_k, rev_k)) => {
                    let rev_k_tuple = Tuple::new(rev_k);
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
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(MegaTuple { mut keys, mut vals })) => {
                // extract key from main
                let k = match keys.pop() {
                    None => return Some(Err(LogicError("Empty keys".to_string()))),
                    Some(k) => k
                };
                let l = self.associates.len();
                // initialize vector for associate values
                let mut assoc_vals: Vec<Option<CowTuple>> = iter::repeat_with(|| None).take(l).collect();
                let l = assoc_vals.len();
                for i in 0..l {
                    // for each associate
                    let cached = self.buffer.get(i).unwrap();
                    // if no cache, try to get cache filled first
                    if matches!(cached, None) {
                        let assoc_data = self.associates.get_mut(i).unwrap().next()
                            .map(|mt| {
                                mt.map(|mut mt| {
                                    (mt.keys.pop().unwrap(), mt.vals.pop().unwrap())
                                })
                            });
                        match assoc_data {
                            None => {
                                self.buffer[i] = None
                            }
                            Some(Ok(data)) => {
                                self.buffer[i] = Some(data)
                            }
                            Some(Err(e)) => return Some(Err(e))
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
                                assoc_vals[i] = Some(v.into());
                                break;
                            }
                            Ordering::Greater => {
                                // target key greater than cache key, meaning that the source has holes (maybe due to filtering)
                                // get a new one into buffer
                                let assoc_data = self.associates.get_mut(i).unwrap().next()
                                    .map(|mt| {
                                        mt.map(|mut mt| {
                                            (mt.keys.pop().unwrap(), mt.vals.pop().unwrap())
                                        })
                                    });
                                match assoc_data {
                                    None => {
                                        self.buffer[i] = None
                                    }
                                    Some(Ok(data)) => {
                                        self.buffer[i] = Some(data)
                                    }
                                    Some(Err(e)) => return Some(Err(e))
                                }
                            }
                        }
                    }
                }
                vals.extend(assoc_vals.into_iter().map(|v|
                    match v {
                        None => {
                            CowTuple::new(CowSlice::Own(EMPTY_DATA.into()))
                        }
                        Some(v) => v
                    }));
                Some(Ok(MegaTuple {
                    keys: vec![k],
                    vals,
                }))
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
            Some(Ok(t)) => t
        };

        let mut right_cache = match self.right.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(t)) => t
        };

        loop {
            let cmp_res = match compare_tuple_by_keys((&left_cache, self.left_keys),
                                                      (&right_cache, self.right_keys)) {
                Ok(r) => r,
                Err(e) => return Some(Err(e))
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
    right_source: &'a MegaTupleIt<'a>,
    right: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
}

impl<'a> Iterator for CartesianProdIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left_cache.is_empty() {
            self.left_cache = match self.left.next() {
                None => return None,
                Some(Ok(v)) => v,
                Some(Err(e)) => return Some(Err(e))
            }
        }
        let r_tpl = match self.right.next() {
            None => {
                self.right = Box::new(self.right_source.iter());
                self.left_cache = match self.left.next() {
                    None => return None,
                    Some(Ok(v)) => v,
                    Some(Err(e)) => return Some(Err(e))
                };
                match self.right.next() {
                    // early return in case right is empty
                    None => return None,
                    Some(Ok(r_tpl)) => r_tpl,
                    Some(Err(e)) => return Some(Err(e))
                }
            }
            Some(Ok(r_tpl)) => r_tpl,
            Some(Err(e)) => return Some(Err(e))
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
                Ok(t) => {
                    match tuple_eval(self.filter, &t) {
                        Ok(Value::Bool(true)) => {
                            return Some(Ok(t));
                        }
                        Ok(Value::Bool(false)) | Ok(Value::Null) => {}
                        Ok(_v) => return Some(Err(LogicError("Unexpected type in filter".to_string()))),
                        Err(e) => return Some(Err(e))
                    }
                }
                Err(e) => return Some(Err(e))
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
    pub fn new(it: &'a MegaTupleIt<'a>, transform: &'a Value<'a>) -> Self {
        Self {
            it: Box::new(it.iter()),
            transform,
        }
    }
}

impl<'a> Iterator for OutputIterator<'a> {
    type Item = Result<Value<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(t)) => Some(tuple_eval(self.transform, &t).map(|v| v.to_static()))
        }
    }
}

pub struct EvalIterator<'a> {
    it: Box<dyn Iterator<Item=Result<MegaTuple>> + 'a>,
    keys: &'a [Value<'a>],
    vals: &'a [Value<'a>],
    prefix: u32,
}

impl<'a> Iterator for EvalIterator<'a> {
    type Item = Result<MegaTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(t)) => {
                let mut key_tuple = OwnTuple::with_prefix(self.prefix);
                let mut val_tuple = OwnTuple::with_data_prefix(DataKind::Data);
                for k in self.keys {
                    match tuple_eval(k, &t) {
                        Ok(v) => key_tuple.push_value(&v),
                        Err(e) => return Some(Err(e))
                    }
                }
                for k in self.vals {
                    match tuple_eval(k, &t) {
                        Ok(v) => val_tuple.push_value(&v),
                        Err(e) => return Some(Err(e))
                    }
                }
                Some(Ok(MegaTuple { keys: vec![key_tuple.into()], vals: vec![val_tuple.into()] }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Instant;
    use crate::db::engine::Engine;
    use crate::parser::{Parser, Rule};
    use pest::Parser as PestParser;
    use crate::db::plan::{MegaTupleIt, OutputIterator};
    use crate::db::query::FromEl;
    use crate::relation::value::Value;
    use crate::error::Result;

    #[test]
    fn pair_value() -> Result<()> {
        let s = "{x: e.first_name ++ ' ' ++ e.last_name, y: [e.a, e.b ++ e.c]}";
        let res = Parser::parse(Rule::expr, s).unwrap().next().unwrap();
        let v = Value::from_pair(res)?;
        println!("{:#?}", v);
        Ok(())
    }

    #[test]
    fn plan() {
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
            let p = Parser::parse(Rule::from_pattern, s).unwrap().next().unwrap();
            let from_pat = match sess.parse_from_pattern(p).unwrap().pop().unwrap() {
                FromEl::Simple(s) => s,
                FromEl::Chain(_) => panic!()
            };
            let s = "where e.id >= 100, e.id <= 105 || e.id == 110";
            let p = Parser::parse(Rule::where_pattern, s).unwrap().next().unwrap();
            let where_pat = sess.parse_where_pattern(p).unwrap();

            let s = r#"select {id: e.id,
            full_name: e.first_name ++ ' ' ++ e.last_name, bibio_name: e.last_name ++ ', '
            ++ e.first_name ++ ': ' ++ (e.phone_number ~ 'N.A.')}"#;
            let p = Parser::parse(Rule::select_pattern, s).unwrap().next().unwrap();
            let sel_pat = sess.parse_select_pattern(p).unwrap();
            let amap = sess.base_relation_to_accessor_map(&from_pat.table, &from_pat.binding, &from_pat.info);
            let (_, vals) = sess.partial_eval(sel_pat.vals, &Default::default(), &amap).unwrap();
            let (_, where_vals) = sess.partial_eval(where_pat, &Default::default(), &amap).unwrap();
            println!("{:#?}", sess.cnf_with_table_refs(where_vals.clone(), &Default::default(), &amap));
            let (vcoll, mut rel_tbls) = Value::extract_relevant_tables([vals, where_vals].into_iter()).unwrap();
            let mut vcoll = vcoll.into_iter();
            let vals = vcoll.next().unwrap();
            let where_vals = vcoll.next().unwrap();
            println!("VALS AFTER 2  {} {}", vals, where_vals);

            println!("{:?}", from_pat);
            println!("{:?}", amap);
            println!("{:?}", rel_tbls);

            let tbl = rel_tbls.pop().unwrap();
            let it = sess.iter_node(tbl);
            let it = MegaTupleIt::FilterIt { filter: where_vals, it: it.into() };
            let it = OutputIterator::new(&it, &vals);
            for val in it {
                println!("{}", val.unwrap());
            }
            let duration = start.elapsed();
            let duration2 = start2.elapsed();
            println!("Time elapsed {:?} {:?}", duration, duration2);
            let it = MegaTupleIt::KeySortedWithAssocIt {
                main: Box::new(sess.iter_node(tbl)),
                associates: vec![(tbl.id as u32, sess.raw_iterator(true)),
                                 (tbl.id as u32, sess.raw_iterator(true)),
                                 (tbl.id as u32, sess.raw_iterator(true))],
            };
            {
                for el in it.iter() {
                    println!("{:?}", el);
                }
            }
            println!("XXXXX");
            {
                for el in it.iter() {
                    println!("{:?}", el);
                }
            }
            let mut it = sess.iter_node(tbl);
            for _ in 0..3 {
                it = MegaTupleIt::CartesianProdIt {
                    left: Box::new(it),
                    right: Box::new(sess.iter_node(tbl)),
                }
            }

            let start = Instant::now();

            println!("Now cartesian product");
            let mut n = 0;
            for el in it.iter() {
                let el = el.unwrap();
                // if n % 4096 == 0 {
                //     println!("{}: {:?}", n, el)
                // }
                let _x = el.keys.into_iter().map(|v| v.iter().map(|_v| ()).collect::<Vec<_>>()).collect::<Vec<_>>();
                let _y = el.vals.into_iter().map(|v| v.iter().map(|_v| ()).collect::<Vec<_>>()).collect::<Vec<_>>();
                n += 1;
            }
            let duration = start.elapsed();
            println!("{} items per second", 1e9 * (n as f64) / (duration.as_nanos() as f64));
            // let a = sess.iter_table(tbl);
            // let ac = (&a).into_iter().count();
            // println!("{}", ac);
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}