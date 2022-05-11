use crate::db::engine::Session;
use crate::db::iterator::{ExecPlan, IteratorSlot, OutputIt};
use crate::db::query::{FromEl, Selection};
use crate::db::table::{ColId, TableId, TableInfo};
use crate::error::Result;
use crate::parser::Rule;
use crate::relation::value::{StaticValue, Value};
use cozorocks::IteratorPtr;
use pest::iterators::Pair;
use std::collections::BTreeMap;

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum OuterJoinType {
    LeftJoin,
    RightJoin,
    FullOuterJoin,
}

pub type AccessorMap = BTreeMap<String, BTreeMap<String, (TableId, ColId)>>;

impl<'a> Session<'a> {
    pub fn realize_intermediate_plan(&self, _plan: ExecPlan) -> ExecPlan {
        todo!()
    }
    pub fn realize_output_plan(&self, _plan: ExecPlan) -> OutputIt {
        todo!()
    }
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
            _ => true.into(),
        };
        let select_data = self.parse_select_pattern(nxt)?;
        let plan = self.convert_from_data_to_plan(from_data)?;
        let plan = self.convert_where_data_to_plan(plan, where_data)?;
        let plan = self.convert_select_data_to_plan(plan, select_data)?;
        println!("{:#?}", plan);
        Ok(())
    }
    fn convert_from_data_to_plan(&self, mut from_data: Vec<FromEl>) -> Result<ExecPlan> {
        let _res = match from_data.pop().unwrap() {
            FromEl::Simple(el) => {
                println!(
                    "{:#?}",
                    self.base_relation_to_accessor_map(&el.table, &el.binding, &el.info)
                );
                todo!()
                // QueryPlan::BaseRelation {
                //     table: el.table,
                //     binding: el.binding,
                //     info: el.info,
                // }
            }
            FromEl::Chain(_) => todo!(),
        };
        // Ok(res)
        // todo!()
    }
    pub(crate) fn base_relation_to_accessor_map(
        &self,
        _table: &str,
        binding: &str,
        info: &TableInfo,
    ) -> AccessorMap {
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
    fn convert_where_data_to_plan(
        &self,
        plan: ExecPlan,
        where_data: StaticValue,
    ) -> Result<ExecPlan> {
        let where_data = self.partial_eval(where_data, &Default::default(), &Default::default());
        let _plan = match where_data?.1 {
            Value::Bool(true) => plan,
            _v => {
                todo!()
                // QueryPlan::Filter { rel: Box::new(plan), filter: v }
            }
        };
        // Ok(plan)
        todo!()
    }
    fn convert_select_data_to_plan(
        &self,
        _plan: ExecPlan,
        _select_data: Selection,
    ) -> Result<ExecPlan> {
        // Ok(MegaTupleIt::Projection { arg: Box::new(plan), projection: select_data })
        todo!()
    }

    pub fn raw_iterator(&self, in_root: bool) -> IteratorPtr {
        if in_root {
            self.txn.iterator(true, &self.perm_cf)
        } else {
            self.txn.iterator(false, &self.temp_cf)
        }
    }

    pub fn iter_node(&self, tid: TableId) -> ExecPlan {
        let it = self.raw_iterator(tid.in_root);
        ExecPlan::NodeIt {
            it: IteratorSlot::Reified(it),
            tid: tid.id as u32,
        }
    }
}
