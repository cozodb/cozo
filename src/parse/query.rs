use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use either::{Left, Right};
use itertools::Itertools;
use miette::{bail, ensure, Diagnostic, LabeledSpan, Report, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::algo::constant::Constant;
use crate::algo::AlgoHandle;
use crate::data::aggr::{parse_aggr, Aggregation};
use crate::data::expr::Expr;
use crate::data::program::{
    AlgoApply, AlgoRuleArg, InputAtom, InputNamedFieldRelationApplyAtom, InputProgram,
    InputRelationApplyAtom, InputRule, InputRuleApplyAtom, InputRulesOrAlgo, QueryAssertion,
    QueryOutOptions, RelationOp, SortDir, Unification,
};
use crate::data::relation::{ColType, ColumnDef, NullableColType, StoredRelationMetadata};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::value::DataValue;
use crate::parse::expr::build_expr;
use crate::parse::schema::parse_schema;
use crate::parse::{ExtractSpan, Pair, Pairs, Rule, SourceSpan};
use crate::runtime::relation::InputRelationHandle;

#[derive(Error, Diagnostic, Debug)]
#[error("Query option {0} is not constant")]
#[diagnostic(code(parser::option_not_constant))]
struct OptionNotConstantError(&'static str, #[label] SourceSpan, #[related] [Report; 1]);

#[derive(Error, Diagnostic, Debug)]
#[error("Query option {0} requires a non-negative integer")]
#[diagnostic(code(parser::option_not_non_neg))]
struct OptionNotNonNegIntError(&'static str, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Query option {0} requires a positive integer")]
#[diagnostic(code(parser::option_not_pos))]
struct OptionNotPosIntError(&'static str, #[label] SourceSpan);

#[derive(Debug)]
struct MultipleRuleDefinitionError(String, Vec<SourceSpan>);

#[derive(Debug, Error, Diagnostic)]
#[error("Multiple query output assertions defined")]
#[diagnostic(code(parser::multiple_out_assert))]
struct DuplicateQueryAssertion(#[label] SourceSpan);

impl Error for MultipleRuleDefinitionError {}

impl Display for MultipleRuleDefinitionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The rule '{0}' cannot have multiple definitions since it contains non-Horn clauses",
            self.0
        )
    }
}

impl Diagnostic for MultipleRuleDefinitionError {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("parser::mult_rule_def"))
    }
    fn labels(&self) -> Option<Box<dyn Iterator<Item = LabeledSpan> + '_>> {
        Some(Box::new(
            self.1.iter().map(|s| LabeledSpan::new_with_span(None, s)),
        ))
    }
}

fn merge_spans(symbs: &[Symbol]) -> SourceSpan {
    let mut fst = symbs.first().unwrap().span;
    for nxt in symbs.iter().skip(1) {
        fst = fst.merge(nxt.span);
    }
    fst
}

pub(crate) fn parse_query(
    src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<InputProgram> {
    let mut progs: BTreeMap<Symbol, InputRulesOrAlgo> = Default::default();
    let mut out_opts: QueryOutOptions = Default::default();
    let mut stored_relation = None;

    for pair in src {
        match pair.as_rule() {
            Rule::rule => {
                let (name, rule) = parse_rule(pair, param_pool)?;

                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Rules { rules: vec![rule] });
                    }
                    Entry::Occupied(mut e) => {
                        let key = e.key().to_string();
                        match e.get_mut() {
                            InputRulesOrAlgo::Rules { rules: rs } => {
                                #[derive(Debug, Error, Diagnostic)]
                                #[error("Rule {0} has multiple definitions with conflicting heads")]
                                #[diagnostic(code(parser::head_aggr_mismatch))]
                                #[diagnostic(help("The arity of each rule head must match. In addition, any aggregation \
                            applied must be the same."))]
                                struct RuleHeadMismatch(
                                    String,
                                    #[label] SourceSpan,
                                    #[label] SourceSpan,
                                );
                                let prev = rs.first().unwrap();
                                ensure!(prev.aggr == rule.aggr, {
                                    RuleHeadMismatch(
                                        key,
                                        merge_spans(&prev.head),
                                        merge_spans(&rule.head),
                                    )
                                });

                                rs.push(rule);
                            }
                            InputRulesOrAlgo::Algo { algo } => {
                                let algo_span = algo.span;
                                bail!(MultipleRuleDefinitionError(
                                    e.key().name.to_string(),
                                    vec![rule.span, algo_span]
                                ))
                            }
                        }
                    }
                }
            }
            Rule::algo_rule => {
                let rule_span = pair.extract_span();
                let (name, apply) = parse_algo_rule(pair, param_pool)?;

                match progs.entry(name) {
                    Entry::Vacant(e) => {
                        e.insert(InputRulesOrAlgo::Algo { algo: apply });
                    }
                    Entry::Occupied(e) => {
                        let found_name = e.key().name.to_string();
                        let mut found_span = match e.get() {
                            InputRulesOrAlgo::Rules { rules } => {
                                rules.iter().map(|r| r.span).collect_vec()
                            }
                            InputRulesOrAlgo::Algo { algo } => vec![algo.span],
                        };
                        found_span.push(rule_span);
                        bail!(MultipleRuleDefinitionError(found_name, found_span));
                    }
                }
            }
            Rule::const_rule => {
                let span = pair.extract_span();
                let mut src = pair.into_inner();
                let (name, head, aggr) = parse_rule_head(src.next().unwrap(), param_pool)?;

                if let Some(found) = progs.get(&name) {
                    let mut found_span = match found {
                        InputRulesOrAlgo::Rules { rules } => {
                            rules.iter().map(|r| r.span).collect_vec()
                        }
                        InputRulesOrAlgo::Algo { algo } => {
                            vec![algo.span]
                        }
                    };
                    found_span.push(span);
                    bail!(MultipleRuleDefinitionError(
                        name.name.to_string(),
                        found_span
                    ));
                }

                #[derive(Debug, Error, Diagnostic)]
                #[error("Constant rules cannot have aggregation application")]
                #[diagnostic(code(parser::aggr_in_const_rule))]
                struct AggrInConstRuleError(#[label] SourceSpan);

                for (a, v) in aggr.iter().zip(head.iter()) {
                    ensure!(a.is_none(), AggrInConstRuleError(v.span));
                }

                let data = build_expr(src.next().unwrap(), param_pool)?;
                let mut options = BTreeMap::new();
                options.insert(SmartString::from("data"), data);
                let handle = AlgoHandle {
                    name: Symbol::new("Constant", span),
                };
                let algo_impl = handle.get_impl()?;
                algo_impl.process_options(&mut options, span)?;
                let arity = algo_impl.arity(&options, &head, span)?;
                progs.insert(
                    name,
                    InputRulesOrAlgo::Algo {
                        algo: AlgoApply {
                            algo: handle,
                            rule_args: vec![],
                            options,
                            head,
                            arity,
                            span,
                            algo_impl,
                        },
                    },
                );
            }
            Rule::timeout_option => {
                let pair = pair.into_inner().next().unwrap();
                let span = pair.extract_span();
                let timeout = build_expr(pair, param_pool)?
                    .eval_to_const()
                    .map_err(|err| OptionNotConstantError("timeout", span, [err]))?
                    .get_non_neg_int()
                    .ok_or(OptionNotNonNegIntError("timeout", span))?;
                ensure!(timeout > 0, OptionNotPosIntError("timeout", span));
                out_opts.timeout = Some(timeout as u64);
            }
            Rule::limit_option => {
                let pair = pair.into_inner().next().unwrap();
                let span = pair.extract_span();
                let limit = build_expr(pair, param_pool)?
                    .eval_to_const()
                    .map_err(|err| OptionNotConstantError("limit", span, [err]))?
                    .get_non_neg_int()
                    .ok_or(OptionNotNonNegIntError("limit", span))?;
                out_opts.limit = Some(limit as usize);
            }
            Rule::offset_option => {
                let pair = pair.into_inner().next().unwrap();
                let span = pair.extract_span();
                let offset = build_expr(pair, param_pool)?
                    .eval_to_const()
                    .map_err(|err| OptionNotConstantError("offset", span, [err]))?
                    .get_non_neg_int()
                    .ok_or(OptionNotNonNegIntError("offset", span))?;
                out_opts.offset = Some(offset as usize);
            }
            Rule::sort_option => {
                for part in pair.into_inner() {
                    let mut var = "";
                    let mut dir = SortDir::Asc;
                    let mut span = part.extract_span();
                    for a in part.into_inner() {
                        match a.as_rule() {
                            Rule::out_arg => {
                                var = a.as_str();
                                span = a.extract_span();
                            }
                            Rule::sort_asc => dir = SortDir::Asc,
                            Rule::sort_desc => dir = SortDir::Dsc,
                            _ => unreachable!(),
                        }
                    }
                    out_opts.sorters.push((Symbol::new(var, span), dir));
                }
            }
            Rule::relation_option => {
                let span = pair.extract_span();
                let mut args = pair.into_inner();
                let op = match args.next().unwrap().as_rule() {
                    Rule::relation_create => RelationOp::Create,
                    Rule::relation_replace => RelationOp::Replace,
                    Rule::relation_put => RelationOp::Put,
                    Rule::relation_rm => RelationOp::Rm,
                    _ => unreachable!(),
                };

                let name_p = args.next().unwrap();
                let name = Symbol::new(name_p.as_str(), name_p.extract_span());
                match args.next() {
                    None => stored_relation = Some(Left((name, span, op))),
                    Some(schema_p) => {
                        let (metadata, key_bindings, dep_bindings) = parse_schema(schema_p)?;
                        stored_relation = Some(Right((
                            InputRelationHandle {
                                name,
                                metadata,
                                key_bindings,
                                dep_bindings,
                                span,
                            },
                            op,
                        )))
                    }
                }
            }
            Rule::assert_none_option => {
                ensure!(
                    out_opts.assertion.is_none(),
                    DuplicateQueryAssertion(pair.extract_span())
                );
                out_opts.assertion = Some(QueryAssertion::AssertNone(pair.extract_span()))
            }
            Rule::assert_some_option => {
                ensure!(
                    out_opts.assertion.is_none(),
                    DuplicateQueryAssertion(pair.extract_span())
                );
                out_opts.assertion = Some(QueryAssertion::AssertSome(pair.extract_span()))
            }
            Rule::EOI => break,
            r => unreachable!("{:?}", r),
        }
    }

    let mut prog = InputProgram {
        prog: progs,
        out_opts,
    };

    if prog.prog.is_empty() {
        if let Some((
            InputRelationHandle {
                key_bindings,
                dep_bindings,
                ..
            },
            RelationOp::Create,
        )) = &prog.out_opts.store_relation
        {
            let mut bindings = key_bindings.clone();
            bindings.extend_from_slice(dep_bindings);
            make_empty_const_rule(&mut prog, &bindings);
        }
    }

    // let head_arity = prog.get_entry_arity()?;

    match stored_relation {
        None => {}
        Some(Left((name, span, op))) => {
            let head = prog.get_entry_out_head()?;
            for symb in &head {
                symb.ensure_valid_field()?;
            }

            let metadata = StoredRelationMetadata {
                keys: head
                    .iter()
                    .map(|s| ColumnDef {
                        name: s.name.clone(),
                        typing: NullableColType {
                            coltype: ColType::Any,
                            nullable: true,
                        },
                        default_gen: None,
                    })
                    .collect(),
                non_keys: vec![],
            };

            let handle = InputRelationHandle {
                name,
                metadata,
                key_bindings: head,
                dep_bindings: vec![],
                span,
            };
            prog.out_opts.store_relation = Some((handle, op))
        }
        Some(Right(r)) => prog.out_opts.store_relation = Some(r),
    }

    if prog.prog.is_empty() {
        if let Some((handle, RelationOp::Create)) = &prog.out_opts.store_relation {
            let mut bindings = handle.dep_bindings.clone();
            bindings.extend_from_slice(&handle.key_bindings);
            make_empty_const_rule(&mut prog, &bindings);
        }
    }

    if !prog.out_opts.sorters.is_empty() {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Sort key '{0}' not found")]
        #[diagnostic(code(parser::sort_key_not_found))]
        struct SortKeyNotFound(String, #[label] SourceSpan);

        let head_args = prog.get_entry_out_head()?;

        for (sorter, _) in &prog.out_opts.sorters {
            ensure!(
                head_args.contains(sorter),
                SortKeyNotFound(sorter.to_string(), sorter.span)
            )
        }
    }

    Ok(prog)
}

fn parse_rule(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, InputRule)> {
    let span = src.extract_span();
    let mut src = src.into_inner();
    let head = src.next().unwrap();
    let head_span = head.extract_span();
    let (name, head, aggr) = parse_rule_head(head, param_pool)?;

    #[derive(Debug, Error, Diagnostic)]
    #[error("Horn-clause rule cannot have empty rule head")]
    #[diagnostic(code(parser::empty_horn_rule_head))]
    struct EmptyRuleHead(#[label] SourceSpan);

    ensure!(!head.is_empty(), EmptyRuleHead(head_span));
    let body = src.next().unwrap();
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
            span,
        },
    ))
}

fn parse_disjunction(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<InputAtom> {
    let span = pair.extract_span();
    let res: Vec<_> = pair
        .into_inner()
        .map(|v| parse_atom(v, param_pool))
        .try_collect()?;
    Ok(if res.len() == 1 {
        res.into_iter().next().unwrap()
    } else {
        InputAtom::Disjunction { inner: res, span }
    })
}

fn parse_atom(src: Pair<'_>, param_pool: &BTreeMap<String, DataValue>) -> Result<InputAtom> {
    Ok(match src.as_rule() {
        Rule::rule_body => {
            let span = src.extract_span();
            let grouped: Vec<_> = src
                .into_inner()
                .map(|v| parse_disjunction(v, param_pool))
                .try_collect()?;
            InputAtom::Conjunction {
                inner: grouped,
                span,
            }
        }
        Rule::disjunction => parse_disjunction(src, param_pool)?,
        Rule::negation => {
            let span = src.extract_span();
            let inner = parse_atom(src.into_inner().next().unwrap(), param_pool)?;
            InputAtom::Negation {
                inner: inner.into(),
                span,
            }
        }
        Rule::expr => {
            let expr = build_expr(src, param_pool)?;
            InputAtom::Predicate { inner: expr }
        }
        Rule::unify => {
            let span = src.extract_span();
            let mut src = src.into_inner();
            let var = src.next().unwrap();
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            InputAtom::Unification {
                inner: Unification {
                    binding: Symbol::new(var.as_str(), var.extract_span()),
                    expr,
                    one_many_unif: false,
                    span,
                },
            }
        }
        Rule::unify_multi => {
            let span = src.extract_span();
            let mut src = src.into_inner();
            let var = src.next().unwrap();
            let expr = build_expr(src.next().unwrap(), param_pool)?;
            InputAtom::Unification {
                inner: Unification {
                    binding: Symbol::new(var.as_str(), var.extract_span()),
                    expr,
                    one_many_unif: true,
                    span,
                },
            }
        }
        Rule::rule_apply => {
            let span = src.extract_span();
            let mut src = src.into_inner();
            let name = src.next().unwrap();
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(|v| build_expr(v, param_pool))
                .try_collect()?;
            InputAtom::Rule {
                inner: InputRuleApplyAtom {
                    name: Symbol::new(name.as_str(), name.extract_span()),
                    args,
                    span,
                },
            }
        }
        Rule::relation_apply => {
            let span = src.extract_span();
            let mut src = src.into_inner();
            let name = src.next().unwrap();
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(|v| build_expr(v, param_pool))
                .try_collect()?;
            InputAtom::Relation {
                inner: InputRelationApplyAtom {
                    name: Symbol::new(&name.as_str()[1..], name.extract_span()),
                    args,
                    span,
                },
            }
        }
        Rule::relation_named_apply => {
            let span = src.extract_span();
            let mut src = src.into_inner();
            let name_p = src.next().unwrap();
            let name = Symbol::new(&name_p.as_str()[1..], name_p.extract_span());
            let args = src
                .next()
                .unwrap()
                .into_inner()
                .map(|pair| -> Result<(SmartString<LazyCompact>, Expr)> {
                    let mut inner = pair.into_inner();
                    let name_p = inner.next().unwrap();
                    let name = SmartString::from(name_p.as_str());
                    let arg = match inner.next() {
                        Some(a) => build_expr(a, param_pool)?,
                        None => Expr::Binding {
                            var: Symbol::new(name.clone(), name_p.extract_span()),
                            tuple_pos: None,
                        },
                    };
                    Ok((name, arg))
                })
                .try_collect()?;
            InputAtom::NamedFieldRelation {
                inner: InputNamedFieldRelationApplyAtom { name, args, span },
            }
        }
        rule => unreachable!("{:?}", rule),
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
    let name = src.next().unwrap();
    let mut args = vec![];
    let mut aggrs = vec![];
    for p in src {
        let (arg, aggr) = parse_rule_head_arg(p, param_pool)?;
        args.push(arg);
        aggrs.push(aggr);
    }
    Ok((Symbol::new(name.as_str(), name.extract_span()), args, aggrs))
}

#[derive(Error, Diagnostic, Debug)]
#[diagnostic(code(parser::aggr_not_found))]
#[error("Aggregation '{0}' not found")]
struct AggrNotFound(String, #[label] SourceSpan);

fn parse_rule_head_arg(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<(Symbol, Option<(Aggregation, Vec<DataValue>)>)> {
    let src = src.into_inner().next().unwrap();
    Ok(match src.as_rule() {
        Rule::var => (Symbol::new(src.as_str(), src.extract_span()), None),
        Rule::aggr_arg => {
            let mut inner = src.into_inner();
            let aggr_p = inner.next().unwrap();
            let aggr_name = aggr_p.as_str();
            let var = inner.next().unwrap();
            let args: Vec<_> = inner
                .map(|v| -> Result<DataValue> { build_expr(v, param_pool)?.eval_to_const() })
                .try_collect()?;
            (
                Symbol::new(var.as_str(), var.extract_span()),
                Some((
                    parse_aggr(aggr_name)
                        .ok_or_else(|| AggrNotFound(aggr_name.to_string(), aggr_p.extract_span()))?
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

    #[derive(Debug, Error, Diagnostic)]
    #[error("Algorithm rule cannot be combined with aggregation")]
    #[diagnostic(code(parser::algo_aggr_conflict))]
    struct AggrInAlgoError(#[label] SourceSpan);

    for (a, v) in aggr.iter().zip(head.iter()) {
        ensure!(a.is_none(), AggrInAlgoError(v.span))
    }

    let name_pair = src.next().unwrap();
    let algo_name = &name_pair.as_str();
    let mut rule_args: Vec<AlgoRuleArg> = vec![];
    let mut options: BTreeMap<SmartString<LazyCompact>, Expr> = Default::default();
    let args_list = src.next().unwrap();
    let args_list_span = args_list.extract_span();

    for nxt in args_list.into_inner() {
        match nxt.as_rule() {
            Rule::algo_rel => {
                let inner = nxt.into_inner().next().unwrap();
                let span = inner.extract_span();
                match inner.as_rule() {
                    Rule::algo_rule_rel => {
                        let mut els = inner.into_inner();
                        let name = els.next().unwrap();
                        let bindings = els
                            .map(|v| Symbol::new(v.as_str(), v.extract_span()))
                            .collect_vec();
                        rule_args.push(AlgoRuleArg::InMem {
                            name: Symbol::new(name.as_str(), name.extract_span()),
                            bindings,
                            span,
                        })
                    }
                    Rule::algo_relation_rel => {
                        let mut els = inner.into_inner();
                        let name = els.next().unwrap();
                        let bindings = els
                            .map(|v| Symbol::new(v.as_str(), v.extract_span()))
                            .collect_vec();
                        rule_args.push(AlgoRuleArg::Stored {
                            name: Symbol::new(
                                name.as_str().strip_prefix(':').unwrap(),
                                name.extract_span(),
                            ),
                            bindings,
                            span,
                        })
                    }
                    Rule::algo_named_relation_rel => {
                        let mut els = inner.into_inner();
                        let name = els.next().unwrap();
                        let bindings = els
                            .map(|v| {
                                let mut vs = v.into_inner();
                                let kp = vs.next().unwrap();
                                let k = SmartString::from(kp.as_str());
                                let v = match vs.next() {
                                    Some(vp) => Symbol::new(vp.as_str(), vp.extract_span()),
                                    None => Symbol::new(k.clone(), kp.extract_span()),
                                };
                                (k, v)
                            })
                            .collect();

                        rule_args.push(AlgoRuleArg::NamedStored {
                            name: Symbol::new(
                                name.as_str().strip_prefix(':').unwrap(),
                                name.extract_span(),
                            ),
                            bindings,
                            span,
                        })
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

    let algo = AlgoHandle::new(algo_name, name_pair.extract_span());

    #[derive(Debug, Error, Diagnostic)]
    #[error("Algorithm rule head arity mismatch")]
    #[diagnostic(code(parser::algo_rule_head_arity_mismatch))]
    #[diagnostic(help("Expected arity: {0}, number of arguments given: {1}"))]
    struct AlgoRuleHeadArityMismatch(usize, usize, #[label] SourceSpan);

    let algo_impl = algo.get_impl()?;
    algo_impl.process_options(&mut options, args_list_span)?;
    let arity = algo_impl.arity(&options, &head, name_pair.extract_span())?;

    ensure!(
        head.is_empty() || arity == head.len(),
        AlgoRuleHeadArityMismatch(arity, head.len(), args_list_span)
    );

    Ok((
        out_symbol,
        AlgoApply {
            algo,
            rule_args,
            options,
            head,
            arity,
            span: args_list_span,
            algo_impl,
        },
    ))
}

fn make_empty_const_rule(prog: &mut InputProgram, bindings: &[Symbol]) {
    let entry_symbol = Symbol::new(PROG_ENTRY, Default::default());
    let mut options = BTreeMap::new();
    options.insert(
        SmartString::from("data"),
        Expr::Const {
            val: DataValue::List(vec![]),
            span: Default::default(),
        },
    );
    prog.prog.insert(
        entry_symbol.clone(),
        InputRulesOrAlgo::Algo {
            algo: AlgoApply {
                algo: AlgoHandle {
                    name: entry_symbol.clone(),
                },
                rule_args: vec![],
                options,
                head: bindings.to_vec(),
                arity: bindings.len(),
                span: Default::default(),
                algo_impl: Box::new(Constant),
            },
        },
    );
}
