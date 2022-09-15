use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use log::trace;
use miette::{bail, Diagnostic, ensure, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::functions::OP_LIST;
use crate::data::id::{EntityId, Validity};
use crate::data::program::InputProgram;
use crate::data::symb::Symbol;
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::parse::{ExtractSpan, Pair, Pairs, ParseError, Rule, SourceSpan};
use crate::parse::expr::{build_expr, InvalidExpression, parse_string};
use crate::parse::query::parse_query;

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TxAction {
    Put,
    Retract,
    RetractAll,
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
    PullByKey(SmartString<LazyCompact>, DataValue),
}

impl EntityRep {
    fn as_datavalue(&self) -> DataValue {
        match self {
            EntityRep::Id(i) => DataValue::uuid(i.0),
            EntityRep::UserTempId(s) => DataValue::Str(s.clone()),
            EntityRep::PullByKey(attr, data) => {
                DataValue::List(vec![DataValue::Str(attr.clone()), data.clone()])
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
            }
            Rule::tx_after_ensure_script => {
                let p = parse_query(pair.into_inner(), param_pool)?;
                after.push(p);
            }
            _ => unreachable!(),
        }
    }
    trace!("Quintuples {:?}", quintuples);
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
                    let vld_expr = build_expr(n, param_pool)?;
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
                    let vld_expr = build_expr(n, param_pool)?;
                    vld = Some(Validity::try_from(vld_expr)?);
                    src.next().unwrap()
                }
                Rule::tx_map => n,
                _ => unreachable!(),
            }
        }
        Rule::tx_retract_all => {
            op = TxAction::RetractAll;
            let n = src.next().unwrap();
            match n.as_rule() {
                Rule::expr => {
                    let vld_expr = build_expr(n, param_pool)?;
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

#[derive(Debug, Error, Diagnostic)]
#[error("Duplicate specification of key in tx map")]
#[diagnostic(code(parser::tx_dup_key))]
#[diagnostic(help("'_id' or '_tid' can appear only once"))]
struct DupKeySpecError(#[label] SourceSpan);

fn parse_tx_map(
    map_p: Pair<'_>,
    op: TxAction,
    vld: Option<Validity>,
    param_pool: &BTreeMap<String, DataValue>,
    temp_id_serial: &mut usize,
    coll: &mut Vec<Quintuple>,
) -> Result<EntityRep> {
    let mut identifier = None;
    let whole_span = map_p.extract_span();
    for pair in map_p.clone().into_inner() {
        let mut src = pair.into_inner();
        let fst = src.next().unwrap();
        match fst.as_rule() {
            Rule::tx_ident_id => {
                ensure!(identifier.is_none(), DupKeySpecError(whole_span));
                let expr = parse_tx_val_inline(src.next().unwrap(), param_pool)?;
                let eid = expr.build_perm_eid()?;
                identifier = Some(EntityRep::Id(eid))
            }
            Rule::tx_ident_temp_id => {
                #[derive(Debug, Diagnostic, Error)]
                #[error("Bad temp id specified")]
                #[diagnostic(code(parser::bad_temp_id))]
                #[diagnostic(help("Temp ID must be given as a string"))]
                struct BadTempId(DataValue, #[label] SourceSpan);

                ensure!(identifier.is_none(), DupKeySpecError(whole_span));
                let expr = parse_tx_val_inline(src.next().unwrap(), param_pool)?;
                let span = expr.span();
                let c = expr.eval_to_const()?;
                let c = c.get_string().ok_or_else(|| BadTempId(c.clone(), span))?;
                identifier = Some(EntityRep::UserTempId(SmartString::from(c)))
            }
            Rule::tx_ident_key => {
                ensure!(identifier.is_none(), DupKeySpecError(whole_span));
                let expr_p = src.next().unwrap();
                let span = expr_p.extract_span();
                let expr = parse_tx_val_inline(expr_p, param_pool)?;
                let c = expr.eval_to_const()?;
                let c = match c {
                    DataValue::List(l) => l,
                    _ => bail!(ParseError { span }),
                };
                ensure!(c.len() == 2, ParseError { span });
                let mut c = c.into_iter();
                let attr = match c.next().unwrap() {
                    DataValue::Str(s) => s,
                    _ => bail!(ParseError { span }),
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
                (
                    Symbol::new(fst.as_str().strip_prefix('*').unwrap(), fst.extract_span()),
                    true,
                )
            } else {
                (Symbol::new(fst.as_str(), fst.extract_span()), false)
            }
        }
        Rule::raw_string | Rule::s_quoted_string | Rule::quoted_string => {
            let span = fst.extract_span();
            let s = parse_string(fst)?;
            if s.starts_with('*') {
                (
                    Symbol::new(s.as_str().strip_prefix('*').unwrap(), span),
                    true,
                )
            } else {
                (Symbol::new(s.as_str(), span), false)
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
                bail!(ParseError {
                    span: tx_val.extract_span()
                })
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
                    Rule::tx_map => bail!(ParseError {
                        span: el.extract_span()
                    }),
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


fn parse_tx_val_inline(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<Expr> {
    Ok(match pair.as_rule() {
        Rule::expr => {
            let mut expr = build_expr(pair, param_pool)?;
            expr.partial_eval()?;
            expr
        }
        Rule::tx_map => {
            bail!(InvalidExpression(pair.extract_span()))
        }
        Rule::tx_list => {
            let span = pair.extract_span();
            let list_coll = pair.into_inner().map(|p| parse_tx_val_inline(p, param_pool)).try_collect()?;
            Expr::Apply {
                op: &OP_LIST,
                args: list_coll,
                span,
            }
        }
        _ => unreachable!(),
    })
}
