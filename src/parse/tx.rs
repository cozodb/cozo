use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use miette::{bail, ensure, miette, Result};
use smartstring::{LazyCompact, SmartString};

use crate::data::id::{EntityId, Validity};
use crate::data::program::InputProgram;
use crate::data::symb::Symbol;
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::parse::expr::{build_expr, parse_string};
use crate::parse::{Pair, Pairs, Rule};
use crate::parse::query::parse_query;

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TxAction {
    Put,
    Retract,
}

impl Display for TxAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum EntityRep {
    Id(EntityId),
    UserTempId(SmartString<LazyCompact>),
    PullByKey(Symbol, DataValue),
}

impl EntityRep {
    fn as_datavalue(&self) -> DataValue {
        match self {
            EntityRep::Id(i) => DataValue::from(i.0 as i64),
            EntityRep::UserTempId(s) => DataValue::Str(s.clone()),
            EntityRep::PullByKey(attr, data) => {
                DataValue::List(vec![DataValue::Str(attr.0.clone()), data.clone()])
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Quintuple {
    pub(crate) entity: EntityRep,
    pub(crate) attr_name: Symbol,
    pub(crate) value: DataValue,
    pub(crate) action: TxAction,
    pub(crate) validity: Option<Validity>,
}

pub(crate) struct TripleTx {
    pub(crate) quintuples: Vec<Quintuple>,
    pub(crate) before: Vec<InputProgram>,
    pub(crate) after: Vec<InputProgram>,
}

pub(crate) fn parse_tx(
    src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<TripleTx> {
    let mut quintuples = vec![];
    let mut before = vec![];
    let mut after = vec![];
    let mut temp_id_serial = 0;

    for pair in src {
        match pair.as_rule() {
            Rule::EOI => {}
            Rule::tx_clause => {
                parse_tx_clause(pair, param_pool, &mut temp_id_serial, &mut quintuples)?
            }
            Rule::tx_before_ensure_script => {
                let p = parse_query(pair.into_inner(), param_pool)?;
                before.push(p);
            },
            Rule::tx_after_ensure_script => {
                let p = parse_query(pair.into_inner(), param_pool)?;
                after.push(p);
            },
            _ => unreachable!(),
        }
    }
    Ok(TripleTx {
        quintuples,
        before,
        after,
    })
}

fn parse_tx_clause(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
    coll: &mut Vec<Quintuple>,
) -> Result<()> {
    let mut src = pair.into_inner();
    let nxt = src.next().unwrap();
    let mut op = TxAction::Put;
    let mut vld = None;
    let map_p = match nxt.as_rule() {
        Rule::tx_map => nxt,
        Rule::tx_put => {
            let n = src.next().unwrap();
            match n.as_rule() {
                Rule::expr => {
                    let vld_expr = build_expr(n, param_pool)?.eval_to_const()?;
                    vld = Some(Validity::try_from(vld_expr)?);
                    src.next().unwrap()
                }
                Rule::tx_map => n,
                _ => unreachable!(),
            }
        }
        Rule::tx_retract => {
            op = TxAction::Retract;
            let n = src.next().unwrap();
            match n.as_rule() {
                Rule::expr => {
                    let vld_expr = build_expr(n, param_pool)?.eval_to_const()?;
                    vld = Some(Validity::try_from(vld_expr)?);
                    src.next().unwrap()
                }
                Rule::tx_map => n,
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };

    parse_tx_map(map_p, op, vld, param_pool, temp_id_serial, coll)?;
    Ok(())
}

fn parse_tx_map(
    map_p: Pair<'_>,
    op: TxAction,
    vld: Option<Validity>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
    coll: &mut Vec<Quintuple>,
) -> Result<EntityRep> {
    let mut identifier = None;
    for pair in map_p.clone().into_inner() {
        let mut src = pair.into_inner();
        let fst = src.next().unwrap();
        match fst.as_rule() {
            Rule::tx_ident_id => {
                ensure!(identifier.is_none(), "duplicate specification of key");
                let expr = build_expr(src.next().unwrap(), param_pool)?;
                let c = expr.eval_to_const()?;
                let c = c
                    .get_non_neg_int()
                    .ok_or_else(|| miette!("integer id required, got {:?}", c))?;
                let eid = EntityId(c);
                ensure!(eid.is_perm(), "entity id invalid: {:?}", eid);
                identifier = Some(EntityRep::Id(eid))
            }
            Rule::tx_ident_temp_id => {
                ensure!(identifier.is_none(), "duplicate specification of key");
                let expr = build_expr(src.next().unwrap(), param_pool)?;
                let c = expr.eval_to_const()?;
                let c = c
                    .get_string()
                    .ok_or_else(|| miette!("tid requires string, got {:?}", c))?;
                identifier = Some(EntityRep::UserTempId(SmartString::from(c)))
            }
            Rule::tx_ident_key => {
                ensure!(identifier.is_none(), "duplicate specification of key");
                let expr = build_expr(src.next().unwrap(), param_pool)?;
                let c = expr.eval_to_const()?;
                let c = match c {
                    DataValue::List(l) => l,
                    v => bail!("key requires a list, got {:?}", v),
                };
                ensure!(c.len() == 2, "key requires a list of length 2");
                let mut c = c.into_iter();
                let attr = match c.next().unwrap() {
                    DataValue::Str(s) => Symbol(s),
                    v => bail!("attr name requires a string, got {:?}", v),
                };
                let val = c.next().unwrap();
                identifier = Some(EntityRep::PullByKey(attr, val))
            }
            _ => {}
        }
    }
    let identifier = identifier.unwrap_or_else(|| {
        *temp_id_serial += 1;
        let s_id = format!(
            "{}{}{}",
            LARGEST_UTF_CHAR, *temp_id_serial, LARGEST_UTF_CHAR
        );
        EntityRep::UserTempId(SmartString::from(s_id))
    });
    for pair in map_p.into_inner() {
        parse_tx_pair(pair, op, vld, param_pool, temp_id_serial, &identifier, coll)?;
    }
    Ok(identifier)
}

fn parse_tx_pair(
    pair: Pair<'_>,
    op: TxAction,
    vld: Option<Validity>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
    parent_id: &EntityRep,
    coll: &mut Vec<Quintuple>,
) -> Result<()> {
    let mut src = pair.into_inner();
    let fst = src.next().unwrap();
    let (attr_name, is_multi) = match fst.as_rule() {
        Rule::compound_ident_with_maybe_star => {
            if fst.as_str().starts_with('*') {
                (Symbol::from(fst.as_str().strip_prefix('*').unwrap()), true)
            } else {
                (Symbol::from(fst.as_str()), false)
            }
        }
        Rule::raw_string | Rule::s_quoted_string | Rule::quoted_string => {
            let s = parse_string(fst)?;
            if s.starts_with('*') {
                (Symbol::from(s.as_str().strip_prefix('*').unwrap()), true)
            } else {
                (Symbol::from(s.as_str()), false)
            }
        }
        Rule::tx_ident_id | Rule::tx_ident_temp_id | Rule::tx_ident_key => return Ok(()),
        _ => unreachable!(),
    };

    let tx_val = src.next().unwrap();

    if is_multi {
        match tx_val.as_rule() {
            Rule::tx_list => {
                for sub_val in tx_val.into_inner() {
                    parse_tx_val(
                        sub_val,
                        op,
                        attr_name.clone(),
                        vld,
                        param_pool,
                        temp_id_serial,
                        parent_id,
                        coll,
                    )?;
                }
            }
            Rule::expr | Rule::tx_map => {
                bail!("multi elements require a list")
            }
            _ => unreachable!(),
        }
    } else {
        parse_tx_val(
            tx_val,
            op,
            attr_name,
            vld,
            param_pool,
            temp_id_serial,
            parent_id,
            coll,
        )?;
    }

    Ok(())
}

fn parse_tx_val(
    pair: Pair<'_>,
    op: TxAction,
    attr_name: Symbol,
    vld: Option<Validity>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
    parent_id: &EntityRep,
    coll: &mut Vec<Quintuple>,
) -> Result<()> {
    match pair.as_rule() {
        Rule::expr => {
            let expr = build_expr(pair, param_pool)?;
            let value = expr.eval_to_const()?;
            coll.push(Quintuple {
                entity: parent_id.clone(),
                attr_name,
                value,
                action: op,
                validity: vld,
            })
        }
        Rule::tx_list => {
            let mut list_coll = vec![];
            for el in pair.into_inner() {
                match el.as_rule() {
                    Rule::tx_map => bail!("map not allowed here"),
                    Rule::expr => {
                        let expr = build_expr(el, param_pool)?;
                        let value = expr.eval_to_const()?;
                        list_coll.push(value)
                    }
                    _ => unreachable!(),
                }
            }
            coll.push(Quintuple {
                entity: parent_id.clone(),
                attr_name,
                value: DataValue::List(list_coll),
                action: op,
                validity: vld,
            })
        }
        Rule::tx_map => {
            let id = parse_tx_map(pair, op, vld, param_pool, temp_id_serial, coll)?;
            coll.push(Quintuple {
                entity: parent_id.clone(),
                attr_name,
                value: id.as_datavalue(),
                action: op,
                validity: vld,
            })
        }
        _ => unreachable!(),
    }
    Ok(())
}
