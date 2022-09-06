use std::borrow::BorrowMut;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use either::Left;
use itertools::Itertools;
use miette::{bail, ensure, miette, Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoHandle;
use crate::data::aggr::{get_aggr, Aggregation};
use crate::data::expr::Expr;
use crate::data::id::Validity;
use crate::data::program::{
    AlgoApply, AlgoRuleArg, ConstRules, InputAtom, InputAttrTripleAtom, InputProgram,
    InputRelationApplyAtom, InputRule, InputRuleApplyAtom, InputRulesOrAlgo, InputTerm,
    MagicSymbol, QueryOutOptions, RelationOp, SortDir, TripleDir, Unification,
};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::expr::build_expr;
use crate::parse::pull::parse_out_options;
use crate::parse::{Pair, Pairs, Rule};
use crate::runtime::relation::{RelationId, RelationMetadata};

pub(crate) fn parse_query(
    src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<InputProgram> {
    let mut progs: BTreeMap<Symbol, InputRulesOrAlgo> = Default::default();
    let mut const_rules: ConstRules = Default::default();
    let mut out_opts: QueryOutOptions = Default::default();

    for pair in src {
        match pair.as_rule() {
            Rule::rule => {
                let (name, rule) = parse_rule(pair, param_pool)?;
                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Rules { rules: vec![rule] });
                    }
                    Entry::Occupied(mut e) => match e.get_mut() {
                        InputRulesOrAlgo::Rules { rules: rs } => rs.push(rule),
                        InputRulesOrAlgo::Algo { algo: _ } => {
                            bail!("cannot mix rules and algo: {}", e.key())
                        }
                    },
                }
            }
            Rule::algo_rule => {
                let (name, apply) = parse_algo_rule(pair, param_pool)?;
                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Algo { algo: apply });
                    }
                    Entry::Occupied(e) => bail!("algo rule can only be defined once: {}", e.key()),
                }
            }
            Rule::const_rule => {
                let mut src = pair.into_inner();
                let (name, head, aggr) = parse_rule_head(src.next().unwrap(), param_pool)?;
                ensure!(
                    aggr.iter().all(|v| v.is_none()),
                    "const rules cannot have aggregation application"
                );
                let data = build_expr(src.next().unwrap(), param_pool)?;
                let data = data.eval_to_const()?;
                let data = match data {
                    DataValue::List(l) => l,
                    d => bail!(
                        "const rules must have body consisting of a list, got {:?}",
                        d
                    ),
                };

                ensure!(!data.is_empty(), "const rules cannot be empty for {}", name);

                match const_rules.entry(MagicSymbol::Muggle { inner: name }) {
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
                        if let Some(l) = &last_len {
                            ensure!(
                                head.is_empty() || *l == head.len(),
                                "const head must have the same length as rows, or be empty"
                            );
                        }
                        e.insert((tuples, head));
                    }
                    Entry::Occupied(e) => {
                        bail!("const rule can be defined only once: {:?}", e.key())
                    }
                }
            }
            Rule::timeout_option => {
                let timeout = build_expr(pair, param_pool)?
                    .eval_to_const()?
                    .get_int()
                    .ok_or_else(|| miette!("timeout option must be an integer"))?;
                ensure!(timeout > 0, "timeout must be positive");
                out_opts.timeout = Some(timeout as u64);
            }
            Rule::limit_option => {
                let limit = build_expr(pair, param_pool)?
                    .eval_to_const()?
                    .get_non_neg_int()
                    .ok_or_else(|| miette!("limit requires a non-negative integer"))?;
                out_opts.limit = Some(limit as usize);
            }
            Rule::offset_option => {
                let offset = build_expr(pair, param_pool)?
                    .eval_to_const()?
                    .get_non_neg_int()
                    .ok_or_else(|| miette!("limit requires a non-negative integer"))?;
                out_opts.offset = Some(offset as usize);
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
                if out_opts.store_relation.is_some() {
                    bail!("cannot use out spec with 'relation'");
                }
                let (target, vld, specs) = parse_out_options(pair, param_pool)?;
                match out_opts.out_spec.entry(target) {
                    Entry::Vacant(e) => e.insert((specs, vld)),
                    Entry::Occupied(_) => {
                        bail!("cannot specify spec for the same target twice")
                    }
                };
            }
            Rule::relation_option => {
                let mut args = pair.into_inner();
                let op = match args.next().unwrap().as_rule() {
                    Rule::relation_create => RelationOp::Create,
                    Rule::relation_rederive => RelationOp::ReDerive,
                    Rule::relation_put => RelationOp::Put,
                    Rule::relation_retract => RelationOp::Retract,
                    _ => unreachable!(),
                };

                let name = args.next().unwrap().as_str();
                let meta = RelationMetadata {
                    name: Symbol::from(name),
                    id: RelationId::SYSTEM,
                    arity: 0,
                };
                out_opts.store_relation = Some((meta, op));
            }
            Rule::EOI => break,
            r => unreachable!("{:?}", r),
        }
    }

    if let Some((meta, _)) = out_opts.store_relation.borrow_mut() {
        meta.arity = get_entry_arity(&progs)?;
    }

    let prog = InputProgram {
        prog: progs,
        const_rules,
        out_opts,
    };

    let head_args = prog.get_entry_head().unwrap_or(&[]);
    for key in prog.out_opts.out_spec.keys() {
        ensure!(
            head_args.contains(key),
            "the pull target {} is not found",
            key
        );
    }

    Ok(prog)
}

fn get_entry_arity(prog: &BTreeMap<Symbol, InputRulesOrAlgo>) -> Result<usize> {
    Ok(
        match prog
            .get(&PROG_ENTRY)
            .ok_or_else(|| miette!("program entry point not found"))?
        {
            InputRulesOrAlgo::Rules { rules } => rules[0].head.len(),
            InputRulesOrAlgo::Algo { algo } => algo.arity()?,
        },
    )
}

fn parse_rule(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, InputRule)> {
    let mut src = src.into_inner();
    let head = src.next().unwrap();
    let (name, head, aggr) = parse_rule_head(head, param_pool)?;
    let mut at = None;
    let mut body = src.next().unwrap();
    if body.as_rule() == Rule::expr {
        let vld = build_expr(body, param_pool)?.eval_to_const()?;
        let vld = Validity::try_from(vld)?;
        at = Some(vld);
        body = src.next().unwrap();
    }
    let mut body_clauses = vec![];
    for atom_src in body.into_inner() {
        body_clauses.push(parse_disjunction(atom_src, param_pool)?)
    }

    Ok((
        name,
        InputRule {
            head,
            aggr,
            body: body_clauses,
            vld: at,
        },
    ))
}

fn parse_disjunction(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<InputAtom> {
    let res: Vec<_> = pair
        .into_inner()
        .map(|v| parse_atom(v, param_pool))
        .try_collect()?;
    Ok(if res.len() == 1 {
        res.into_iter().next().unwrap()
    } else {
        InputAtom::Disjunction(res)
    })
}

fn parse_atom(src: Pair<'_>, param_pool: &BTreeMap<String, DataValue>) -> Result<InputAtom> {
    Ok(match src.as_rule() {
        Rule::rule_body => {
            let grouped: Vec<_> = src
                .into_inner()
                .map(|v| parse_disjunction(v, param_pool))
                .try_collect()?;
            InputAtom::Conjunction(grouped)
        }
        Rule::disjunction => parse_disjunction(src, param_pool)?,
        Rule::triple => parse_triple(src, param_pool)?,
        Rule::negation => {
            let inner = parse_atom(src.into_inner().next().unwrap(), param_pool)?;
            InputAtom::Negation(inner.into())
        }
        Rule::expr => {
            let expr = build_expr(src, param_pool)?;
            InputAtom::Predicate(expr)
        }
        Rule::unify => {
            let mut src = src.into_inner();
            let var = src.next().unwrap().as_str();
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            InputAtom::Unification(Unification {
                binding: Symbol::from(var),
                expr,
                one_many_unif: false,
            })
        }
        Rule::unify_multi => {
            let mut src = src.into_inner();
            let var = src.next().unwrap().as_str();
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            InputAtom::Unification(Unification {
                binding: Symbol::from(var),
                expr,
                one_many_unif: true,
            })
        }
        Rule::rule_apply => {
            let mut src = src.into_inner();
            let name = src.next().unwrap().as_str();
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(|v| parse_rule_arg(v, param_pool))
                .try_collect()?;
            InputAtom::Rule(InputRuleApplyAtom {
                name: Symbol::from(name),
                args,
            })
        }
        Rule::relation_apply => {
            let mut src = src.into_inner();
            let name = &src.next().unwrap().as_str()[1..];
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(|v| parse_rule_arg(v, param_pool))
                .try_collect()?;
            InputAtom::Relation(InputRelationApplyAtom {
                name: Symbol::from(name),
                args,
            })
        }
        rule => unreachable!("{:?}", rule),
    })
}

fn parse_triple(src: Pair<'_>, param_pool: &BTreeMap<String, DataValue>) -> Result<InputAtom> {
    let mut src = src.into_inner();
    let e_p = src.next().unwrap();
    let attr_p = src.next().unwrap();
    let v_p = src.next().unwrap();
    Ok(InputAtom::AttrTriple(InputAttrTripleAtom {
        attr: Symbol::from(attr_p.as_str()),
        entity: parse_rule_arg(e_p, param_pool)?,
        value: parse_rule_arg(v_p, param_pool)?,
    }))
}

fn parse_rule_arg(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<InputTerm<DataValue>> {
    Ok(match src.as_rule() {
        Rule::expr => {
            let mut p = build_expr(src, param_pool)?;
            p.partial_eval()?;
            match p {
                Expr::Binding { var, .. } => InputTerm::Var(var),
                Expr::Const { val } => InputTerm::Const(val),
                _ => bail!("triple arg must either evaluate to a constant or a variable"),
            }
        }
        _ => unreachable!(),
    })
}

fn parse_rule_head(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(
    Symbol,
    Vec<Symbol>,
    Vec<Option<(Aggregation, Vec<DataValue>)>>,
)> {
    let mut src = src.into_inner();
    let name = src.next().unwrap().as_str();
    let mut args = vec![];
    let mut aggrs = vec![];
    for p in src {
        let (arg, aggr) = parse_rule_head_arg(p, param_pool)?;
        args.push(arg);
        aggrs.push(aggr);
    }
    Ok((Symbol::from(name), args, aggrs))
}

fn parse_rule_head_arg(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, Option<(Aggregation, Vec<DataValue>)>)> {
    let src = src.into_inner().next().unwrap();
    Ok(match src.as_rule() {
        Rule::var => (Symbol::from(src.as_str()), None),
        Rule::aggr_arg => {
            let mut inner = src.into_inner();
            let aggr_name = inner.next().unwrap().as_str();
            let var = inner.next().unwrap().as_str();
            let args: Vec<_> = inner
                .map(|v| -> Result<DataValue> { build_expr(v, param_pool)?.eval_to_const() })
                .try_collect()?;
            (
                Symbol::from(var),
                Some((
                    get_aggr(aggr_name)
                        .ok_or_else(|| miette!("cannot find aggregation"))?
                        .clone(),
                    args,
                )),
            )
        }
        _ => unreachable!(),
    })
}

fn parse_algo_rule(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, AlgoApply)> {
    let mut src = src.into_inner();
    let (out_symbol, head, aggr) = parse_rule_head(src.next().unwrap(), param_pool)?;
    ensure!(
        aggr.iter().all(|v| v.is_none()),
        "aggregation cannot be applied to algo rule head"
    );
    let mut name_pair = src.next().unwrap();
    let mut at = None;
    match name_pair.as_rule() {
        Rule::expr => {
            let vld = build_expr(name_pair, param_pool)?.eval_to_const()?;
            let vld = Validity::try_from(vld)?;
            at = Some(vld);
            name_pair = src.next().unwrap();
        }
        Rule::algo_ident => {}
        _ => unreachable!(),
    }
    let algo_name = &name_pair.as_str().strip_suffix('!').unwrap();
    let mut rule_args: Vec<AlgoRuleArg> = vec![];
    let mut options: BTreeMap<SmartString<LazyCompact>, Expr> = Default::default();

    for nxt in src {
        match nxt.as_rule() {
            Rule::algo_rel => {
                let inner = nxt.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::algo_rule_rel => {
                        let mut els = inner.into_inner();
                        let name = els.next().unwrap().as_str();
                        let bindings = els.map(|v| Symbol::from(v.as_str())).collect_vec();
                        rule_args.push(AlgoRuleArg::InMem {
                            name: Symbol::from(name),
                            bindings,
                        })
                    }
                    Rule::algo_relation_rel => {
                        let mut els = inner.into_inner();
                        let name = els.next().unwrap().as_str();
                        let bindings = els.map(|v| Symbol::from(v.as_str())).collect_vec();
                        rule_args.push(AlgoRuleArg::Stored {
                            name: Symbol::from(name.strip_prefix(':').unwrap()),
                            bindings,
                        })
                    }
                    Rule::algo_triple_rel => {
                        let mut els = inner.into_inner();
                        let fst = els.next().unwrap().as_str();
                        let mdl = els.next().unwrap();
                        let mut dir = TripleDir::Fwd;
                        let ident = match mdl.as_rule() {
                            Rule::rev_triple_marker => {
                                dir = TripleDir::Bwd;
                                els.next().unwrap().as_str()
                            }
                            Rule::compound_ident => mdl.as_str(),
                            _ => unreachable!(),
                        };
                        let snd = els.next().unwrap().as_str();
                        rule_args.push(AlgoRuleArg::Triple {
                            name: Symbol::from(ident),
                            bindings: vec![Symbol::from(fst), Symbol::from(snd)],
                            dir,
                        });
                    }
                    _ => unreachable!(),
                }
            }
            Rule::algo_opt_pair => {
                let mut inner = nxt.into_inner();
                let name = inner.next().unwrap().as_str();
                let val = inner.next().unwrap();
                let val = build_expr(val, param_pool)?;
                options.insert(SmartString::from(name), val);
            }
            _ => unreachable!(),
        }
    }

    let algo = AlgoHandle::new(algo_name);
    ensure!(
        head.is_empty() || algo.arity(Left(&rule_args), &options)? == head.len(),
        "algo head must have the same length as the return, or be omitted"
    );

    Ok((
        out_symbol,
        AlgoApply {
            algo,
            rule_args,
            options,
            head,
            vld: at,
        },
    ))
}
