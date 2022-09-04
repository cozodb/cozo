use std::borrow::BorrowMut;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::str::FromStr;

use miette::{bail, ensure, miette, IntoDiagnostic, Result};
use pest::Parser;
use crate::data::aggr::Aggregation;

use crate::data::expr::Expr;
use crate::data::id::Validity;
use crate::data::program::{AlgoApply, InputAtom, InputProgram, InputRule, InputRulesOrAlgo, MagicSymbol};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::query::{ConstRules, OutSpec, QueryOutOptions, SortDir, ViewOp};
use crate::runtime::view::{ViewRelId, ViewRelKind, ViewRelMetadata};

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Query(InputProgram),
}

pub(crate) fn parse_script(
    src: &str,
    param_pool: &BTreeMap<Symbol, DataValue>,
) -> Result<CozoScript> {
    let parsed = CozoScriptParser::parse(Rule::script, src)
        .into_diagnostic()?
        .next()
        .unwrap();
    Ok(match parsed.as_rule() {
        Rule::query_script => CozoScript::Query(parse_query(parsed.into_inner(), param_pool)?),
        Rule::schema_script => todo!(),
        Rule::tx_script => todo!(),
        Rule::sys_script => todo!(),
        _ => unreachable!(),
    })
}

fn parse_query(src: Pairs<'_>, param_pool: &BTreeMap<Symbol, DataValue>) -> Result<InputProgram> {
    let mut progs: BTreeMap<Symbol, InputRulesOrAlgo> = Default::default();
    let mut const_rules: ConstRules = Default::default();
    let mut out_opts: QueryOutOptions = Default::default();
    let default_vld = Validity::current();

    for pair in src {
        match pair.as_rule() {
            Rule::rule => {
                let (name, rule) = parse_rule(pair, param_pool, default_vld)?;
                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Rules(vec![rule]));
                    }
                    Entry::Occupied(mut e) => match e.get_mut() {
                        InputRulesOrAlgo::Rules(rs) => rs.push(rule),
                        InputRulesOrAlgo::Algo(_) => {
                            bail!("cannot mix rules and algo: {}", e.key())
                        }
                    },
                }
            }
            Rule::algo_rule => {
                let (name, apply) = parse_algo_rule(pair)?;
                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Algo(apply));
                    }
                    Entry::Occupied(e) => bail!("algo rule can only be defined once: {}", e.key()),
                }
            }
            Rule::const_rule => {
                let mut src = pair.into_inner();
                let name = src.next().unwrap().as_str();
                let mut data = build_expr(src.next().unwrap())?;
                let data = data.eval_to_const(param_pool)?;
                let data = match data {
                    DataValue::List(l) => l,
                    d => bail!(
                        "const rules must have body consisting of a list, got {:?}",
                        d
                    ),
                };

                ensure!(!data.is_empty(), "const rules cannot be empty for {}", name);

                match const_rules.entry(MagicSymbol::Muggle {
                    inner: Symbol::from(name),
                }) {
                    Entry::Vacant(e) => {
                        let mut tuples = vec![];
                        let mut last_len = None;
                        for row in data {
                            match row {
                                DataValue::List(tuple) => {
                                    if let Some(l) = &last_len {
                                        ensure!(*l == tuple.len(), "all rows in const rules must have the same length, got offending row {:?}", tuple);
                                    };
                                    last_len = Some(tuple.len());
                                    tuples.push(Tuple(tuple));
                                }
                                v => bail!("rows of const rules must be list, got {:?}", v),
                            }
                        }
                        e.insert(tuples);
                    }
                    Entry::Occupied(e) => {
                        bail!("const rule can be defined only once: {:?}", e.key())
                    }
                }
            }
            Rule::timeout_option => {
                let timeout = build_expr(pair)?
                    .eval_to_const(param_pool)?
                    .get_int()
                    .ok_or_else(|| miette!("timeout option must be an integer"))?;
                ensure!(timeout > 0, "timeout must be positive");
                out_opts.timeout = Some(timeout as u64);
            }
            Rule::limit_option => {
                let limit = parse_limit_or_offset(pair)?;
                out_opts.limit = Some(limit);
            }
            Rule::offset_option => {
                let offset = parse_limit_or_offset(pair)?;
                out_opts.offset = Some(offset);
            }
            Rule::sort_option => {
                for part in pair.into_inner() {
                    let mut var = "";
                    let mut dir = SortDir::Asc;
                    for a in part.into_inner() {
                        match a.as_rule() {
                            Rule::var => var = a.as_str(),
                            Rule::sort_asc => dir = SortDir::Asc,
                            Rule::sort_desc => dir = SortDir::Dsc,
                            _ => unreachable!(),
                        }
                    }
                    out_opts.sorters.push((Symbol::from(var), dir));
                }
            }
            Rule::out_option => {
                if out_opts.as_view.is_some() {
                    bail!("cannot use out spec with 'view'");
                }
                let out_spec = parse_out_option(pair.into_inner().next().unwrap())?;
                out_opts.out_spec = Some(out_spec);
            }
            Rule::view_option => {
                if out_opts.out_spec.is_some() {
                    bail!("cannot use out spec with 'view'");
                }
                let mut args = pair.into_inner();
                let op = match args.next().unwrap().as_rule() {
                    Rule::view_create => ViewOp::Create,
                    Rule::view_rederive => ViewOp::Rederive,
                    Rule::view_put => ViewOp::Put,
                    Rule::view_retract => ViewOp::Retract,
                    _ => unreachable!(),
                };

                let name = args.next().unwrap().as_str();
                let meta = ViewRelMetadata {
                    name: Symbol::from(name),
                    id: ViewRelId::SYSTEM,
                    arity: 0, // TODO
                    kind: ViewRelKind::Manual,
                };
                out_opts.as_view = Some((meta, op));
            }
            Rule::EOI => break,
            r => unreachable!("{:?}", r),
        }
    }

    if let Some((meta, _)) = out_opts.as_view.borrow_mut() {
        meta.arity = get_entry_arity(&progs)?;
    }

    Ok(InputProgram {
        prog: progs,
        const_rules,
        out_opts,
    })
}

fn get_entry_arity(prog: &BTreeMap<Symbol, InputRulesOrAlgo>) -> Result<usize> {
    Ok(
        match prog
            .get(&PROG_ENTRY)
            .ok_or_else(|| miette!("program entry point not found"))?
        {
            InputRulesOrAlgo::Rules(rules) => rules[0].head.len(),
            InputRulesOrAlgo::Algo(algo_apply) => algo_apply.arity()?,
        },
    )
}

fn parse_rule(src: Pair<'_>,param_pool: &BTreeMap<Symbol, DataValue>, default_vld: Validity) -> Result<(Symbol, InputRule)> {
    let mut src = src.into_inner();
    let head = src.next().unwrap();
    let (name, head, aggr) = parse_rule_head(head)?;
    let mut at = default_vld;
    let mut body = src.next().unwrap();
    if body.as_rule() == Rule::expr {
        let vld = build_expr(body)?.eval_to_const(param_pool)?;
        let vld = Validity::try_from(vld)?;
        at = vld;
        body = src.next().unwrap();
    }
    let mut body_clauses = vec![];
    for atom_src in body.into_inner() {
        body_clauses.push(parse_disjunction(atom_src)?)
    }

    Ok((name, InputRule {
        head,
        aggr,
        body: body_clauses,
        vld: at
    }))
}

fn parse_disjunction(pair: Pair<'_>) -> Result<InputAtom> {
    todo!()
}

fn parse_rule_head(src: Pair<'_>) -> Result<(Symbol, Vec<Symbol>, Vec<Option<(Aggregation, Vec<DataValue>)>>)> {
    // let mut src = src.into_inner();
    // let name = src.next().unwrap().as_str();
    // let args: Vec<_> = src.map(parse_rule_head_arg).try_collect()?;
    // Ok((name.to_string(), json!(args)))
    todo!()
}


fn parse_algo_rule(src: Pair<'_>) -> Result<(Symbol, AlgoApply)> {
    todo!()
    // let mut src = src.into_inner();
    // let out_symbol = src.next().unwrap().as_str();
    // let algo_name = &src.next().unwrap().as_str().strip_suffix('!').unwrap();
    // let mut algo_rels = vec![];
    // let mut algo_opts = Map::default();
    // for nxt in src {
    //     match nxt.as_rule() {
    //         Rule::algo_rel => {
    //             let inner = nxt.into_inner().next().unwrap();
    //             match inner.as_rule() {
    //                 Rule::algo_rule_rel => {
    //                     let mut els = inner.into_inner();
    //                     let name = els.next().unwrap().as_str();
    //                     let args = els.map(|v| v.as_str()).collect_vec();
    //                     algo_rels.push(json!({"rule": name, "rel_args": args}));
    //                 }
    //                 Rule::algo_view_rel => {
    //                     let mut els = inner.into_inner();
    //                     let name = els.next().unwrap().as_str().strip_prefix(':').unwrap();
    //                     let args = els.map(|v| v.as_str()).collect_vec();
    //                     algo_rels.push(json!({"view": name, "rel_args": args}));
    //                 }
    //                 Rule::algo_triple_rel => {
    //                     let mut els = inner.into_inner();
    //                     let fst = els.next().unwrap().as_str();
    //                     let mdl = els.next().unwrap();
    //                     let mut backward = false;
    //                     let ident = match mdl.as_rule() {
    //                         Rule::rev_triple_marker => {
    //                             backward = true;
    //                             els.next().unwrap().as_str()
    //                         }
    //                         Rule::compound_ident => mdl.as_str(),
    //                         _ => unreachable!(),
    //                     };
    //                     let snd = els.next().unwrap().as_str();
    //                     algo_rels.push(
    //                         json!({"triple": ident, "backward": backward, "rel_args": [fst, snd]}),
    //                     )
    //                 }
    //                 _ => unreachable!(),
    //             }
    //         }
    //         Rule::algo_opt_pair => {
    //             let mut inner = nxt.into_inner();
    //             let name = inner.next().unwrap().as_str();
    //             let val = inner.next().unwrap();
    //             let val = build_expr::<WrapConst>(val)?;
    //             algo_opts.insert(name.to_string(), val);
    //         }
    //         _ => unreachable!(),
    //     }
    // }
    // Ok(
    //     json!({"algo_out": out_symbol, "algo_name": algo_name, "relations": algo_rels, "options": algo_opts}),
    // )
}

pub(crate) fn build_expr(pair: Pair<'_>) -> Result<Expr> {
    // PREC_CLIMBER.climb(pair.into_inner(), build_unary::<T>, build_expr_infix)
    todo!()
}

fn parse_limit_or_offset(src: Pair<'_>) -> Result<usize> {
    let src = src.into_inner().next().unwrap().as_str();
    str2usize(src)
}

fn str2usize(src: &str) -> Result<usize> {
    Ok(usize::from_str(&src.replace('_', "")).into_diagnostic()?)
}

fn parse_out_option(src: Pair<'_>) -> Result<OutSpec> {
    // Ok(match src.as_rule() {
    //     Rule::out_list_spec => {
    //         let l: Vec<_> = src.into_inner().map(parse_pull_spec).try_collect()?;
    //         json!(l)
    //     }
    //     Rule::out_map_spec => {
    //         let m: Map<_, _> = src
    //             .into_inner()
    //             .map(|p| -> Result<(String, JsonValue)> {
    //                 let mut p = p.into_inner();
    //                 let name = p.next().unwrap().as_str();
    //                 let spec = parse_pull_spec(p.next().unwrap())?;
    //                 Ok((name.to_string(), spec))
    //             })
    //             .try_collect()?;
    //         json!(m)
    //     }
    //     _ => unreachable!(),
    // })
    todo!()
}
