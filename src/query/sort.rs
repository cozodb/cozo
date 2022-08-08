use std::cmp::Reverse;
use std::collections::BTreeMap;

use anyhow::Result;
use itertools::Itertools;

use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::query::SortDir;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn sort_and_collect(
        &mut self,
        original: TempStore,
        sorters: &[(Symbol, SortDir)],
        head: &[Symbol],
    ) -> Result<TempStore> {
        let head_indices: BTreeMap<_, _> = head.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let idx_sorters = sorters
            .iter()
            .map(|(k, dir)| (head_indices[k], *dir))
            .collect_vec();
        let ret = self.new_temp_store();
        for (idx, tuple) in original.scan_all().enumerate() {
            let tuple = tuple?;
            let mut key = idx_sorters
                .iter()
                .map(|(idx, dir)| {
                    let mut val = tuple.0[*idx].clone();
                    if *dir == SortDir::Dsc {
                        val = DataValue::DescVal(Reverse(Box::new(val)));
                    }
                    val
                })
                .collect_vec();
            key.push(DataValue::from(idx as i64));
            let key = Tuple(key);
            let encoded_key = key.encode_as_key_for_epoch(ret.id, 0);
            let encoded_val = tuple.encode_as_key_for_epoch(ret.id, 0);
            ret.db.put(&encoded_key, &encoded_val)?;
        }
        Ok(ret)
    }
}
