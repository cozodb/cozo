use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct ShortestPathAStar;

impl AlgoImpl for ShortestPathAStar {
    fn run(
        &mut self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'shortest_path_astar' requires edges relation"))?;
        let nodes = rels.get(0).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires nodes relation as second argument")
        })?;
        let starting = rels.get(0).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires starting relation as third argument")
        })?;
        let ending = rels.get(0).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires ending relation as fourth argument")
        })?;
        let heuristic = opts
            .get("heuristic")
            .ok_or_else(|| anyhow!("'heuristic' option required for 'shortest_path_astar'"))?;
        let heuristic_is_consistent = match opts.get("heuristic_is_consistent") {
            None => true,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(expr) => bail!("unexpected option 'heuristic_is_consistent' for 'shortest_path_astar': {:?}, boolean required", expr)
        };

        todo!()
    }
}
