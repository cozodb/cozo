use std::collections::BTreeMap;

use anyhow::{bail, Result};

use crate::data::expr::Expr;
use crate::data::program::TripleDir;
use crate::data::symb::Symbol;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;
use crate::runtime::view::ViewRelStore;

pub(crate) mod page_rank;

#[derive(Debug, Clone)]
pub(crate) enum AlgoInputRel {
    InMem(DerivedRelStore),
    View(ViewRelStore),
    Triple(Symbol, TripleDir),
}

pub(crate) trait AlgoImpl {
    fn name(&self) -> Symbol;
    fn arity(&self) -> usize;
    fn run(
        &self,
        tx: &mut SessionTx,
        rels: Vec<AlgoInputRel>,
        opts: &BTreeMap<Symbol, Expr>,
    ) -> DerivedRelStore;
}

pub(crate) fn get_impl(name: &str) -> Result<Box<dyn AlgoImpl>> {
    match name {
        "page_rank" => todo!(),
        name => bail!("algorithm '{}' not found", name),
    }
}
