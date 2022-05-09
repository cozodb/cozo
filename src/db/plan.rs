use std::collections::BTreeMap;
use std::{iter, mem};
use std::cmp::Ordering;
use pest::iterators::Pair;
use cozorocks::{IteratorPtr};
use crate::db::engine::Session;
use crate::db::query::{FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::relation::value::{StaticValue, Value};
use crate::parser::Rule;
use crate::error::Result;
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
}

impl<'a> IntoIterator for &'a MegaTupleIt<'a> {
    type Item = MegaTuple;
    type IntoIter = MegaTupleIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            MegaTupleIt::NodeIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                MegaTupleIterator::NodeIterator {
                    it,
                    started: false,
                }
            }
            MegaTupleIt::EdgeIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                MegaTupleIterator::EdgeIterator {
                    it,
                    started: false,
                }
            }
            MegaTupleIt::EdgeKeyOnlyBwdIt { it, tid } => {
                let prefix_tuple = OwnTuple::with_prefix(*tid);
                it.seek(prefix_tuple);

                MegaTupleIterator::EdgeKeyOnlyBwdIterator {
                    it,
                    started: false,
                }
            }
            MegaTupleIt::KeySortedWithAssocIt { main, associates } => {
                let buffer = iter::repeat_with(|| None).take(associates.len()).collect();
                let associates = associates.into_iter().map(|(tid, it)| {
                    let prefix_tuple = OwnTuple::with_prefix(*tid);
                    it.seek(prefix_tuple);

                    MegaTupleIterator::NodeIterator {
                        it,
                        started: false,
                    }
                }).collect();
                MegaTupleIterator::KeySortedWithAssocIterator {
                    main: Box::new(main.as_ref().into_iter()),
                    associates,
                    buffer,
                }
            }
            MegaTupleIt::CartesianProdIt { left, right } => {
                MegaTupleIterator::CartesianProdIterator {
                    left: Box::new(left.as_ref().into_iter()),
                    left_cache: MegaTuple::empty_tuple(),
                    right_source: right.as_ref(),
                    right: Box::new(right.as_ref().into_iter()),
                }
            }
            MegaTupleIt::MergeJoinIt { .. } => todo!(),
        }
    }
}

pub enum MegaTupleIterator<'a> {
    NodeIterator { it: &'a IteratorPtr<'a>, started: bool },
    EdgeIterator { it: &'a IteratorPtr<'a>, started: bool },
    EdgeKeyOnlyBwdIterator { it: &'a IteratorPtr<'a>, started: bool },
    KeySortedWithAssocIterator { main: Box<MegaTupleIterator<'a>>, associates: Vec<MegaTupleIterator<'a>>, buffer: Vec<Option<(CowTuple, CowTuple)>> },
    CartesianProdIterator {
        left: Box<MegaTupleIterator<'a>>,
        left_cache: MegaTuple,
        right_source: &'a MegaTupleIt<'a>,
        right: Box<MegaTupleIterator<'a>>,
    },
}

impl<'a> Iterator for MegaTupleIterator<'a> {
    type Item = MegaTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MegaTupleIterator::NodeIterator { it, started } => {
                if *started {
                    it.next();
                } else {
                    *started = true;
                }
                it.pair().map(|(k, v)| {
                    MegaTuple {
                        keys: vec![Tuple::new(k).into()],
                        vals: vec![Tuple::new(v).into()],
                    }
                })
            }
            MegaTupleIterator::EdgeIterator { it, started } => {
                if *started {
                    it.next();
                } else {
                    *started = true;
                }
                loop {
                    match it.pair() {
                        None => return None,
                        Some((k, v)) => {
                            let vt = Tuple::new(v);
                            if matches!(vt.data_kind(), Ok(DataKind::Edge)) {
                                it.next()
                            } else {
                                let kt = Tuple::new(k);
                                return Some(MegaTuple {
                                    keys: vec![kt.into()],
                                    vals: vec![vt.into()],
                                });
                            }
                        }
                    }
                }
            }
            MegaTupleIterator::EdgeKeyOnlyBwdIterator { it, started } => {
                if *started {
                    it.next();
                } else {
                    *started = true;
                }
                loop {
                    match it.pair() {
                        None => return None,
                        Some((_k, rev_k)) => {
                            let rev_k_tuple = Tuple::new(rev_k);
                            if !matches!(rev_k_tuple.data_kind(), Ok(DataKind::Edge)) {
                                it.next()
                            } else {
                                return Some(MegaTuple {
                                    keys: vec![rev_k_tuple.into()],
                                    vals: vec![],
                                });
                            }
                        }
                    }
                }
            }
            MegaTupleIterator::KeySortedWithAssocIterator { main, associates, buffer } => {
                // first get a tuple from main
                match main.next() {
                    None => None, // main exhausted, we are finished
                    Some(MegaTuple { mut keys, mut vals }) => {
                        // extract key from main
                        let k = keys.pop().unwrap();
                        let l = associates.len();
                        // initialize vector for associate values
                        let mut assoc_vals: Vec<Option<CowTuple>> = iter::repeat_with(|| None).take(l).collect();
                        let l = assoc_vals.len();
                        for i in 0..l {
                            // for each associate
                            let cached = buffer.get(i).unwrap();
                            // if no cache, try to get cache filled first
                            if matches!(cached, None) {
                                let assoc_data = associates.get_mut(i).unwrap().next()
                                    .map(|mut mt| (mt.keys.pop().unwrap(), mt.vals.pop().unwrap()));
                                buffer[i] = assoc_data;
                            }

                            // if we have cache
                            while let Some((ck, _)) = buffer.get(i).unwrap() {
                                match k.key_part_cmp(ck) {
                                    Ordering::Less => {
                                        // target key less than cache key, no value for current iteration
                                        break;
                                    }
                                    Ordering::Equal => {
                                        // target key equals cache key, we put it into collected values
                                        let (_, v) = mem::replace(&mut buffer[i], None).unwrap();
                                        assoc_vals[i] = Some(v.into());
                                        break;
                                    }
                                    Ordering::Greater => {
                                        // target key greater than cache key, meaning that the source has holes (maybe due to filtering)
                                        // get a new one into buffer
                                        let assoc_data = associates.get_mut(i).unwrap().next()
                                            .map(|mut mt| (mt.keys.pop().unwrap(), mt.vals.pop().unwrap()));
                                        buffer[i] = assoc_data;
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
                        Some(MegaTuple {
                            keys: vec![k],
                            vals,
                        })
                    }
                }
            }
            MegaTupleIterator::CartesianProdIterator { left, left_cache, right, right_source } => {
                if left_cache.is_empty() {
                    *left_cache = match left.next() {
                        None => return None,
                        Some(v) => v
                    }
                }
                let r_tpl = match right.next() {
                    None => {
                        *right = Box::new((*right_source).into_iter());
                        *left_cache = match left.next() {
                            None => return None,
                            Some(v) => v
                        };
                        match right.next() {
                            // early return in case right is empty
                            None => return None,
                            Some(r_tpl) => r_tpl
                        }
                    }
                    Some(r_tpl) => r_tpl
                };
                let mut ret = left_cache.clone();
                ret.keys.extend(r_tpl.keys);
                ret.vals.extend(r_tpl.vals);
                Some(ret)
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
    use crate::db::plan::{MegaTupleIt};
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
            for tuple in &it {
                match sess.tuple_eval(&where_vals, &tuple).unwrap() {
                    Value::Bool(true) => {
                        let extracted = sess.tuple_eval(&vals, &tuple).unwrap();
                        println!("{}", extracted);
                    }
                    Value::Null |
                    Value::Bool(_) => {
                        println!("  Ignore {:?}", &tuple);
                    }
                    _ => panic!("Bad type")
                }
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
                for el in &it {
                    println!("{:?}", el);
                }
            }
            println!("XXXXX");
            {
                for el in &it {
                    println!("{:?}", el);
                }
            }
            let mut it = sess.iter_node(tbl);
            for _ in 0..2 {
                it = MegaTupleIt::CartesianProdIt {
                    left: Box::new(it),
                    right: Box::new(sess.iter_node(tbl)),
                }
            }

            let start = Instant::now();

            println!("Now cartesian product");
            let mut n = 0;
            for el in &it {
                if n % 4096 == 0 {
                    println!("{}: {:?}", n, el)
                }
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