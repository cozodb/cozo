use std::collections::BTreeMap;
use std::{iter, mem};
use std::vec::IntoIter;
use chrono::format::Item;
use pest::iterators::Pair;
use cozorocks::{IteratorPtr, SlicePtr};
use crate::db::engine::Session;
use crate::db::query::{FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::relation::value::{StaticValue, Value};
use crate::parser::Rule;
use crate::error::Result;
use crate::relation::data::EMPTY_DATA;
use crate::relation::table::MegaTuple;
use crate::relation::tuple::{CowSlice, CowTuple, OwnTuple, SliceTuple, Tuple};

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

    pub fn iter_table(&self, tid: TableId) -> TableRowIterable {
        let it = if tid.in_root {
            self.txn.iterator(true, &self.perm_cf)
        } else {
            self.txn.iterator(false, &self.temp_cf)
        };
        TableRowIterable::new(it, tid.id as u32)
    }
}

pub struct TableRowIterable<'a> {
    it: IteratorPtr<'a>,
    prefix: u32,
}

impl<'a> TableRowIterable<'a> {
    pub fn new(it: IteratorPtr<'a>, prefix: u32) -> Self {
        Self {
            it,
            prefix,
        }
    }
}

impl<'a> IntoIterator for &'a TableRowIterable<'a> {
    type Item = (SliceTuple, SliceTuple);
    type IntoIter = TableRowIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let prefix_tuple = OwnTuple::with_prefix(self.prefix);
        self.it.seek(prefix_tuple);

        Self::IntoIter { it: &self.it, started: false }
    }
}

pub struct TableRowIterator<'a> {
    it: &'a IteratorPtr<'a>,
    started: bool,
}

impl<'a> Iterator for TableRowIterator<'a> {
    type Item = (SliceTuple, SliceTuple);

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        self.it.pair().map(|(k, v)| (Tuple::new(k), Tuple::new(v)))
    }
}

pub struct TableRowWithAssociatesIterable<'a> {
    main: TableRowIterable<'a>,
    associates: Vec<TableRowIterable<'a>>,
}

impl<'a> TableRowWithAssociatesIterable<'a> {
    pub fn new(main: TableRowIterable<'a>, associates: Vec<TableRowIterable<'a>>) -> Self {
        Self { main, associates }
    }
}

pub struct TableRowWithAssociatesIterator<'a> {
    main: TableRowIterator<'a>,
    associates: Vec<TableRowIterator<'a>>,
    buffer: Vec<Option<(SliceTuple, SliceTuple)>>,
}

impl<'a> IntoIterator for &'a TableRowWithAssociatesIterable<'a> {
    type Item = MegaTuple;
    type IntoIter = TableRowWithAssociatesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            main: (&self.main).into_iter(),
            associates: self.associates.iter().map(|v| v.into_iter()).collect(),
            buffer: iter::repeat_with(|| None).take(self.associates.len()).collect(),
        }
    }
}

pub enum MegaTupleIterable<'a> {
    TableRowIt(IteratorPtr<'a>),
    TableRowWithAssocIt(IteratorPtr<'a>, Vec<IteratorPtr<'a>>),
    CartesianProdIt(Box<MegaTupleIterable<'a>>, Box<MegaTupleIterable<'a>>),
}

impl<'a> Iterator for TableRowWithAssociatesIterator<'a> {
    type Item = MegaTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self.main.next() {
            None => None,
            Some((k, v)) => {
                let l = self.associates.len();
                let mut assoc_vals: Vec<Option<CowTuple>> = iter::repeat_with(|| None).take(l).collect();
                let l = assoc_vals.len();
                for i in 0..l {
                    let cached = self.buffer.get(i).unwrap();
                    let cached = if matches!(cached, None) {
                        self.buffer[i] = self.associates.get_mut(i).unwrap().next();
                        self.buffer.get(i).unwrap()
                    } else {
                        cached
                    };
                    if let Some((ck, _)) = cached {
                        if k.key_part_eq(ck) {
                            let (_, v) = mem::replace(&mut self.buffer[i], None).unwrap();
                            assoc_vals[i] = Some(v.into())
                        }
                    }
                }
                let mut vals: Vec<CowTuple> = Vec::with_capacity(assoc_vals.len());
                vals.push(v.into());
                vals.extend(assoc_vals.into_iter().map(|v|
                    match v {
                        None => {
                            CowTuple::new(CowSlice::Own(EMPTY_DATA.into()))
                        }
                        Some(v) => v
                    }));
                Some(MegaTuple {
                    keys: vec![k.into()],
                    vals,
                })
            }
        }
    }
}


pub struct CartesianProductIterable<A, B> {
    left: A,
    right: B,
}

impl<'a, A, B, AI, BI> IntoIterator for &'a CartesianProductIterable<A, B>
    where &'a A: IntoIterator<Item=MegaTuple, IntoIter=AI>,
          &'a B: IntoIterator<Item=MegaTuple, IntoIter=BI>,
          AI: Iterator<Item=MegaTuple>,
          BI: Iterator<Item=MegaTuple> {
    type Item = MegaTuple;
    type IntoIter = CartesianProductIterator<'a, B, AI, BI>;

    fn into_iter(self) -> Self::IntoIter {
        let mut left = (&self.left).into_iter();
        let left_cache = left.next();
        Self::IntoIter {
            right_source: &self.right,
            left,
            right: (&self.right).into_iter(),
            left_cache,
        }
    }
}

pub struct CartesianProductIterator<'a, B, AI, BI>
    where &'a B: IntoIterator<Item=MegaTuple, IntoIter=BI> {
    right_source: &'a B,
    left: AI,
    right: BI,
    left_cache: Option<MegaTuple>,
}

impl<'a, B, AI, BI> Iterator for CartesianProductIterator<'a, B, AI, BI>
    where &'a B: IntoIterator<Item=MegaTuple, IntoIter=BI>,
          AI: Iterator<Item=MegaTuple>,
          BI: Iterator<Item=MegaTuple> {
    type Item = MegaTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.left_cache {
            None => None,
            Some(t) => {
                match self.right.next() {
                    Some(t2) => {
                        let mut keys = t.keys.clone();
                        keys.extend(t2.keys);
                        let mut vals = t.vals.clone();
                        vals.extend(t2.vals);
                        Some(MegaTuple { keys, vals })
                    }
                    None => {
                        self.left_cache = self.left.next();
                        self.right = self.right_source.into_iter();
                        self.next()
                    }
                }
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
    use crate::db::plan::{CartesianProductIterable, TableRowWithAssociatesIterable, TableRowWithAssociatesIterator};
    use crate::db::query::FromEl;
    use crate::db::table::TableId;
    use crate::relation::value::Value;
    use crate::error::Result;
    use crate::relation::tuple::{OwnTuple, Tuple};

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
            for (k, v) in &sess.iter_table(tbl) {
                let tpair = [(k, v)];
                match sess.tuple_eval(&where_vals, &tpair).unwrap() {
                    Value::Bool(true) => {
                        let extracted = sess.tuple_eval(&vals, &tpair).unwrap();
                        println!("{}", extracted);
                    }
                    Value::Null |
                    Value::Bool(_) => {
                        println!("  Ignore {:?}", &tpair);
                    }
                    _ => panic!("Bad type")
                }
            }
            let duration = start.elapsed();
            let duration2 = start2.elapsed();
            println!("Time elapsed {:?} {:?}", duration, duration2);
            let a = sess.iter_table(tbl);
            let b = sess.iter_table(tbl);
            let c = sess.iter_table(tbl);
            let it = TableRowWithAssociatesIterable::new(a, vec![b, c]);
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
            let a = sess.iter_table(tbl);
            let a = TableRowWithAssociatesIterable::new(a, vec![]);
            let b = sess.iter_table(tbl);
            let b = TableRowWithAssociatesIterable::new(b, vec![]);
            let c_it = CartesianProductIterable { left: a, right: b };
            let c = sess.iter_table(tbl);
            let c = TableRowWithAssociatesIterable::new(c, vec![]);
            let mut c_it = CartesianProductIterable { left: c, right: c_it };

            let start = Instant::now();

            println!("Now cartesian product");

            let mut n = 0;

            for el in &c_it {
                // if i % 4096 == 0 {
                //     println!("{}: {:?}", i, el)
                // }
                n += el.keys.len();
            }
            let duration = start.elapsed();
            println!("Time elapsed {:?} for {}", duration, n / 3);
            let a = sess.iter_table(tbl);
            let ac = (&a).into_iter().count();
            println!("{}", ac);
        }
        drop(engine);
        let _ = fs::remove_dir_all(db_path);
    }
}