use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::data::expr::Expr;
use crate::data::program::MagicAlgoRuleArg;
use crate::data::symb::Symbol;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) mod page_rank;

pub(crate) trait AlgoImpl {
    fn name(&self) -> Symbol;
    fn arity(&self) -> usize;
    fn run(
        &self,
        tx: &mut SessionTx,
        rels: Vec<MagicAlgoRuleArg>,
        opts: &BTreeMap<Symbol, Expr>,
    ) -> DerivedRelStore;
}

pub(crate) fn get_algo(name: &str) -> Result<Arc<dyn AlgoImpl>> {
    match name {
        "page_rank" => todo!(),
        name => bail!("algorithm '{}' not found", name),
    }
}
