use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use either::{Either, Left, Right};
use miette::{bail, ensure, miette, Result};
use smartstring::{LazyCompact, SmartString};

use crate::data::id::{EntityId, Validity};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::script::expr::{build_expr, parse_string};
use crate::parse::script::{Pair, Pairs, Rule};


#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TxAction {
    Put,
    Retract,
    // RetractAllEA,
    // RetractAllE,
    // Ensure,
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
    SysTempId(usize),
    PullByKey(Symbol, DataValue),
}

#[derive(Debug)]
pub(crate) struct Quintuple {
    pub(crate) entity: EntityRep,
    pub(crate) attr_name: Symbol,
    pub(crate) value: DataValue,
    pub(crate) action: TxAction,
    pub(crate) validity: Option<Validity>,
}

pub(crate) fn parse_tx(
    src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<Vec<Quintuple>> {
    let mut ret = vec![];
    let mut temp_id_serial = 0;

    for pair in src {
        if pair.as_rule() == Rule::EOI {
            break;
        }
        ret.extend(parse_tx_clause(pair, param_pool, &mut temp_id_serial)?);
    }
    Ok(ret)
}

fn parse_tx_clause(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
) -> Result<Vec<Quintuple>> {
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
    let mut identifier = None;
    let mut entities = vec![];
    for pair in map_p.into_inner() {
        match parse_tx_pair(pair, param_pool)? {
            Left(entity) => {
                if identifier.is_some() {
                    bail!("duplicate id specified")
                }
                identifier = Some(entity)
            }
            Right(ent) => entities.push(ent),
        }
    }
    let identifier = identifier.unwrap_or_else(|| {
        *temp_id_serial += 1;
        EntityRep::SysTempId(*temp_id_serial)

    });
    let mut coll = vec![];
    for (attr, val) in entities {
        coll.push(Quintuple {
            entity: identifier.clone(),
            attr_name: attr,
            value: val,
            action: op,
            validity: vld.clone(),
        })
    }
    Ok(coll)
}

fn parse_tx_pair(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<Either<EntityRep, (Symbol, DataValue)>> {
    let mut src = pair.into_inner();
    let fst = src.next().unwrap();
    Ok(match fst.as_rule() {
        Rule::compound_ident => {
            let name = Symbol::from(fst.as_str());
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            let c = expr.eval_to_const()?;
            Right((name, c))
        }
        Rule::raw_string | Rule::s_quoted_string | Rule::quoted_string => {
            let name = Symbol(parse_string(fst)?);
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            let c = expr.eval_to_const()?;
            Right((name, c))
        }
        Rule::tx_ident_id => {
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            let c = expr.eval_to_const()?;
            let c = c
                .get_non_neg_int()
                .ok_or_else(|| miette!("integer id required, got {:?}", c))?;
            let eid = EntityId(c);
            ensure!(eid.is_perm(), "entity id invalid: {:?}", eid);
            Left(EntityRep::Id(eid))
        }
        Rule::tx_ident_temp_id => {
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            let c = expr.eval_to_const()?;
            let c = c
                .get_string()
                .ok_or_else(|| miette!("tid requires string, got {:?}", c))?;
            Left(EntityRep::UserTempId(SmartString::from(c)))
        }
        Rule::tx_ident_key => {
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
            Left(EntityRep::PullByKey(attr, val))
        }
        _ => unreachable!(),
    })
}
//
// fn parse_tx_el(src: Pair<'_>) -> Result<JsonValue> {
//     match src.as_rule() {
//         Rule::tx_map => parse_tx_map(src),
//         Rule::tx_list => parse_tx_list(src),
//         Rule::expr => build_expr::<NoWrapConst>(src),
//         Rule::neg_num => Ok(JsonValue::from_str(src.as_str()).into_diagnostic()?),
//         _ => unreachable!(),
//     }
// }
//
// fn parse_tx_list(src: Pair<'_>) -> Result<JsonValue> {
//     src.into_inner().map(parse_tx_el).try_collect()
// }
