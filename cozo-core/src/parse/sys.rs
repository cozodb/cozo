/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use miette::{bail, ensure, miette, Diagnostic, Result};
use ordered_float::OrderedFloat;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::program::InputProgram;
use crate::data::relation::VecElementType;
use crate::data::symb::Symbol;
use crate::data::value::{DataValue, ValidityTs};
use crate::fts::TokenizerConfig;
use crate::parse::expr::{build_expr, parse_string};
use crate::parse::query::parse_query;
use crate::parse::{ExtractSpan, Pairs, Rule, SourceSpan};
use crate::runtime::relation::AccessLevel;
use crate::{Expr, FixedRule};

#[derive(Debug)]
pub(crate) enum SysOp {
    Compact,
    ListColumns(Symbol),
    ListIndices(Symbol),
    ListRelations,
    ListRunning,
    ListFixedRules,
    KillRunning(u64),
    Explain(Box<InputProgram>),
    RemoveRelation(Vec<Symbol>),
    RenameRelation(Vec<(Symbol, Symbol)>),
    ShowTrigger(Symbol),
    SetTriggers(Symbol, Vec<String>, Vec<String>, Vec<String>),
    SetAccessLevel(Vec<Symbol>, AccessLevel),
    CreateIndex(Symbol, Symbol, Vec<Symbol>),
    CreateVectorIndex(HnswIndexConfig),
    CreateFtsIndex(FtsIndexConfig),
    CreateMinHashLshIndex(MinHashLshConfig),
    RemoveIndex(Symbol, Symbol),
    DescribeRelation(Symbol, SmartString<LazyCompact>)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FtsIndexConfig {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) extractor: String,
    pub(crate) tokenizer: TokenizerConfig,
    pub(crate) filters: Vec<TokenizerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MinHashLshConfig {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) extractor: String,
    pub(crate) tokenizer: TokenizerConfig,
    pub(crate) filters: Vec<TokenizerConfig>,
    pub(crate) n_gram: usize,
    pub(crate) n_perm: usize,
    pub(crate) false_positive_weight: OrderedFloat<f64>,
    pub(crate) false_negative_weight: OrderedFloat<f64>,
    pub(crate) target_threshold: OrderedFloat<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HnswIndexConfig {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) vec_dim: usize,
    pub(crate) dtype: VecElementType,
    pub(crate) vec_fields: Vec<SmartString<LazyCompact>>,
    pub(crate) distance: HnswDistance,
    pub(crate) ef_construction: usize,
    pub(crate) m_neighbours: usize,
    pub(crate) index_filter: Option<String>,
    pub(crate) extend_candidates: bool,
    pub(crate) keep_pruned_connections: bool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde_derive::Serialize, serde_derive::Deserialize,
)]
pub(crate) enum HnswDistance {
    L2,
    InnerProduct,
    Cosine,
}

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot interpret {0} as process ID")]
#[diagnostic(code(parser::not_proc_id))]
struct ProcessIdError(String, #[label] SourceSpan);

pub(crate) fn parse_sys(
    mut src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
    algorithms: &BTreeMap<String, Arc<Box<dyn FixedRule>>>,
    cur_vld: ValidityTs,
) -> Result<SysOp> {
    let inner = src.next().unwrap();
    Ok(match inner.as_rule() {
        Rule::compact_op => SysOp::Compact,
        Rule::running_op => SysOp::ListRunning,
        Rule::kill_op => {
            let i_expr = inner.into_inner().next().unwrap();
            let i_val = build_expr(i_expr, param_pool)?;
            let i_val = i_val.eval_to_const()?;
            let i_val = i_val
                .get_int()
                .ok_or_else(|| miette!("Process ID must be an integer"))?;
            SysOp::KillRunning(i_val as u64)
        }
        Rule::explain_op => {
            let prog = parse_query(
                inner.into_inner().next().unwrap().into_inner(),
                param_pool,
                algorithms,
                cur_vld,
            )?;
            SysOp::Explain(Box::new(prog))
        }
        Rule::describe_relation_op => {
            let mut inner = inner.into_inner();
            let rels_p = inner.next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            let description = match inner.next() {
                None => Default::default(),
                Some(desc_p) => parse_string(desc_p)?,
            };
            SysOp::DescribeRelation(rel, description)
        }
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rel = inner
                .into_inner()
                .map(|rels_p| Symbol::new(rels_p.as_str(), rels_p.extract_span()))
                .collect_vec();

            SysOp::RemoveRelation(rel)
        }
        Rule::list_columns_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::ListColumns(rel)
        }
        Rule::list_indices_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::ListIndices(rel)
        }
        Rule::rename_relations_op => {
            let rename_pairs = inner
                .into_inner()
                .map(|pair| {
                    let mut src = pair.into_inner();
                    let rels_p = src.next().unwrap();
                    let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
                    let rels_p = src.next().unwrap();
                    let new_rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
                    (rel, new_rel)
                })
                .collect_vec();
            SysOp::RenameRelation(rename_pairs)
        }
        Rule::access_level_op => {
            let mut ps = inner.into_inner();
            let access_level = match ps.next().unwrap().as_str() {
                "normal" => AccessLevel::Normal,
                "protected" => AccessLevel::Protected,
                "read_only" => AccessLevel::ReadOnly,
                "hidden" => AccessLevel::Hidden,
                _ => unreachable!(),
            };
            let mut rels = vec![];
            for rel_p in ps {
                let rel = Symbol::new(rel_p.as_str(), rel_p.extract_span());
                rels.push(rel)
            }
            SysOp::SetAccessLevel(rels, access_level)
        }
        Rule::trigger_relation_show_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::ShowTrigger(rel)
        }
        Rule::trigger_relation_op => {
            let mut src = inner.into_inner();
            let rels_p = src.next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            let mut puts = vec![];
            let mut rms = vec![];
            let mut replaces = vec![];
            for clause in src {
                let mut clause_inner = clause.into_inner();
                let op = clause_inner.next().unwrap();
                let script = clause_inner.next().unwrap();
                let script_str = script.as_str();
                parse_query(
                    script.into_inner(),
                    &Default::default(),
                    algorithms,
                    cur_vld,
                )?;
                match op.as_rule() {
                    Rule::trigger_put => puts.push(script_str.to_string()),
                    Rule::trigger_rm => rms.push(script_str.to_string()),
                    Rule::trigger_replace => replaces.push(script_str.to_string()),
                    r => unreachable!("{:?}", r),
                }
            }
            SysOp::SetTriggers(rel, puts, rms, replaces)
        }
        Rule::lsh_idx_op => {
            let inner = inner.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::index_create_adv => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    let mut filters = vec![];
                    let mut tokenizer = TokenizerConfig {
                        name: Default::default(),
                        args: Default::default(),
                    };
                    let mut extractor = "".to_string();
                    let mut extract_filter = "".to_string();
                    let mut n_gram = 1;
                    let mut n_perm = 200;
                    let mut target_threshold = 0.9;
                    let mut false_positive_weight = 1.0;
                    let mut false_negative_weight = 1.0;
                    for opt_pair in inner {
                        let mut opt_inner = opt_pair.into_inner();
                        let opt_name = opt_inner.next().unwrap();
                        let opt_val = opt_inner.next().unwrap();
                        match opt_name.as_str() {
                            "false_positive_weight" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                let v = expr.eval_to_const()?;
                                false_positive_weight = v.get_float().ok_or_else(|| {
                                    miette!("false_positive_weight must be a float")
                                })?;
                            }
                            "false_negative_weight" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                let v = expr.eval_to_const()?;
                                false_negative_weight = v.get_float().ok_or_else(|| {
                                    miette!("false_negative_weight must be a float")
                                })?;
                            }
                            "n_gram" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                let v = expr.eval_to_const()?;
                                n_gram = v
                                    .get_int()
                                    .ok_or_else(|| miette!("n_gram must be an integer"))?
                                    as usize;
                            }
                            "n_perm" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                let v = expr.eval_to_const()?;
                                n_perm = v
                                    .get_int()
                                    .ok_or_else(|| miette!("n_perm must be an integer"))?
                                    as usize;
                            }
                            "target_threshold" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                let v = expr.eval_to_const()?;
                                target_threshold = v
                                    .get_float()
                                    .ok_or_else(|| miette!("target_threshold must be a float"))?;
                            }
                            "extractor" => {
                                let mut ex = build_expr(opt_val, param_pool)?;
                                ex.partial_eval()?;
                                extractor = ex.to_string();
                            }
                            "extract_filter" => {
                                let mut ex = build_expr(opt_val, param_pool)?;
                                ex.partial_eval()?;
                                extract_filter = ex.to_string();
                            }
                            "tokenizer" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                match expr {
                                    Expr::UnboundApply { op, args, .. } => {
                                        let mut targs = vec![];
                                        for arg in args.iter() {
                                            let v = arg.clone().eval_to_const()?;
                                            targs.push(v);
                                        }
                                        tokenizer.name = op;
                                        tokenizer.args = targs;
                                    }
                                    Expr::Binding { var, .. } => {
                                        tokenizer.name = var.name;
                                        tokenizer.args = vec![];
                                    }
                                    _ => bail!("Tokenizer must be a symbol or a call for an existing tokenizer"),
                                }
                            }
                            "filters" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                match expr {
                                    Expr::Apply { op, args, .. } => {
                                        if op.name != "OP_LIST" {
                                            bail!("Filters must be a list of filters");
                                        }
                                        for arg in args.iter() {
                                            match arg {
                                                Expr::UnboundApply { op, args, .. } => {
                                                    let mut targs = vec![];
                                                    for arg in args.iter() {
                                                        let v = arg.clone().eval_to_const()?;
                                                        targs.push(v);
                                                    }
                                                    filters.push(TokenizerConfig {
                                                        name: op.clone(),
                                                        args: targs,
                                                    })
                                                }
                                                Expr::Binding { var, .. } => {
                                                    filters.push(TokenizerConfig {
                                                        name: var.name.clone(),
                                                        args: vec![],
                                                    })
                                                }
                                                _ => bail!("Tokenizer must be a symbol or a call for an existing tokenizer"),
                                            }
                                        }
                                    }
                                    _ => bail!("Filters must be a list of filters"),
                                }
                            }
                            _ => bail!("Unknown option {} for LSH index", opt_name.as_str()),
                        }
                    }
                    ensure!(
                        false_positive_weight > 0.,
                        "false_positive_weight must be positive"
                    );
                    ensure!(
                        false_negative_weight > 0.,
                        "false_negative_weight must be positive"
                    );
                    ensure!(n_gram > 0, "n_gram must be positive");
                    ensure!(n_perm > 0, "n_perm must be positive");
                    ensure!(
                        target_threshold > 0. && target_threshold < 1.,
                        "target_threshold must be between 0 and 1"
                    );
                    let total_weights = false_positive_weight + false_negative_weight;
                    false_positive_weight /= total_weights;
                    false_negative_weight /= total_weights;

                    if !extract_filter.is_empty() {
                        extractor = format!("if({}, {})", extract_filter, extractor);
                    }

                    let config = MinHashLshConfig {
                        base_relation: SmartString::from(rel.as_str()),
                        index_name: SmartString::from(name.as_str()),
                        extractor,
                        tokenizer,
                        filters,
                        n_gram,
                        n_perm,
                        false_positive_weight: false_positive_weight.into(),
                        false_negative_weight: false_negative_weight.into(),
                        target_threshold: target_threshold.into(),
                    };
                    SysOp::CreateMinHashLshIndex(config)
                }
                Rule::index_drop => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    SysOp::RemoveIndex(
                        Symbol::new(rel.as_str(), rel.extract_span()),
                        Symbol::new(name.as_str(), name.extract_span()),
                    )
                }
                r => unreachable!("{:?}", r),
            }
        }
        Rule::fts_idx_op => {
            let inner = inner.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::index_create_adv => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    let mut filters = vec![];
                    let mut tokenizer = TokenizerConfig {
                        name: Default::default(),
                        args: Default::default(),
                    };
                    let mut extractor = "".to_string();
                    let mut extract_filter = "".to_string();
                    for opt_pair in inner {
                        let mut opt_inner = opt_pair.into_inner();
                        let opt_name = opt_inner.next().unwrap();
                        let opt_val = opt_inner.next().unwrap();
                        match opt_name.as_str() {
                            "extractor" => {
                                let mut ex = build_expr(opt_val, param_pool)?;
                                ex.partial_eval()?;
                                extractor = ex.to_string();
                            }
                            "extract_filter" => {
                                let mut ex = build_expr(opt_val, param_pool)?;
                                ex.partial_eval()?;
                                extract_filter = ex.to_string();
                            }
                            "tokenizer" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                match expr {
                                    Expr::UnboundApply { op, args, .. } => {
                                        let mut targs = vec![];
                                        for arg in args.iter() {
                                            let v = arg.clone().eval_to_const()?;
                                            targs.push(v);
                                        }
                                        tokenizer.name = op;
                                        tokenizer.args = targs;
                                    }
                                    Expr::Binding { var, .. } => {
                                        tokenizer.name = var.name;
                                        tokenizer.args = vec![];
                                    }
                                    _ => bail!("Tokenizer must be a symbol or a call for an existing tokenizer"),
                                }
                            }
                            "filters" => {
                                let mut expr = build_expr(opt_val, param_pool)?;
                                expr.partial_eval()?;
                                match expr {
                                    Expr::Apply { op, args, .. } => {
                                        if op.name != "OP_LIST" {
                                            bail!("Filters must be a list of filters");
                                        }
                                        for arg in args.iter() {
                                            match arg {
                                                Expr::UnboundApply { op, args, .. } => {
                                                    let mut targs = vec![];
                                                    for arg in args.iter() {
                                                        let v = arg.clone().eval_to_const()?;
                                                        targs.push(v);
                                                    }
                                                    filters.push(TokenizerConfig {
                                                        name: op.clone(),
                                                        args: targs,
                                                    })
                                                }
                                                Expr::Binding { var, .. } => {
                                                    filters.push(TokenizerConfig {
                                                        name: var.name.clone(),
                                                        args: vec![],
                                                    })
                                                }
                                                _ => bail!("Tokenizer must be a symbol or a call for an existing tokenizer"),
                                            }
                                        }
                                    }
                                    _ => bail!("Filters must be a list of filters"),
                                }
                            }
                            _ => bail!("Unknown option {} for FTS index", opt_name.as_str()),
                        }
                    }
                    if !extract_filter.is_empty() {
                        extractor = format!("if({}, {})", extract_filter, extractor);
                    }
                    let config = FtsIndexConfig {
                        base_relation: SmartString::from(rel.as_str()),
                        index_name: SmartString::from(name.as_str()),
                        extractor,
                        tokenizer,
                        filters,
                    };
                    SysOp::CreateFtsIndex(config)
                }
                Rule::index_drop => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    SysOp::RemoveIndex(
                        Symbol::new(rel.as_str(), rel.extract_span()),
                        Symbol::new(name.as_str(), name.extract_span()),
                    )
                }
                r => unreachable!("{:?}", r),
            }
        }
        Rule::vec_idx_op => {
            let inner = inner.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::index_create_adv => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    // options
                    let mut vec_dim = 0;
                    let mut dtype = VecElementType::F32;
                    let mut vec_fields = vec![];
                    let mut distance = HnswDistance::L2;
                    let mut ef_construction = 0;
                    let mut m_neighbours = 0;
                    let mut index_filter = None;
                    let mut extend_candidates = false;
                    let mut keep_pruned_connections = false;

                    for opt_pair in inner {
                        let mut opt_inner = opt_pair.into_inner();
                        let opt_name = opt_inner.next().unwrap();
                        let opt_val = opt_inner.next().unwrap();
                        let opt_val_str = opt_val.as_str();
                        match opt_name.as_str() {
                            "dim" => {
                                let v = build_expr(opt_val, param_pool)?
                                    .eval_to_const()?
                                    .get_int()
                                    .ok_or_else(|| miette!("Invalid vec_dim: {}", opt_val_str))?;
                                ensure!(v > 0, "Invalid vec_dim: {}", v);
                                vec_dim = v as usize;
                            }
                            "ef_construction" | "ef" => {
                                let v = build_expr(opt_val, param_pool)?
                                    .eval_to_const()?
                                    .get_int()
                                    .ok_or_else(|| {
                                        miette!("Invalid ef_construction: {}", opt_val_str)
                                    })?;
                                ensure!(v > 0, "Invalid ef_construction: {}", v);
                                ef_construction = v as usize;
                            }
                            "m_neighbours" | "m" => {
                                let v = build_expr(opt_val, param_pool)?
                                    .eval_to_const()?
                                    .get_int()
                                    .ok_or_else(|| {
                                        miette!("Invalid m_neighbours: {}", opt_val_str)
                                    })?;
                                ensure!(v > 0, "Invalid m_neighbours: {}", v);
                                m_neighbours = v as usize;
                            }
                            "dtype" => {
                                dtype = match opt_val.as_str() {
                                    "F32" | "Float" => VecElementType::F32,
                                    "F64" | "Double" => VecElementType::F64,
                                    _ => {
                                        return Err(miette!("Invalid dtype: {}", opt_val.as_str()))
                                    }
                                }
                            }
                            "fields" => {
                                let fields = build_expr(opt_val, &Default::default())?;
                                vec_fields = fields.to_var_list()?;
                            }
                            "distance" | "dist" => {
                                distance = match opt_val.as_str().trim() {
                                    "L2" => HnswDistance::L2,
                                    "IP" => HnswDistance::InnerProduct,
                                    "Cosine" => HnswDistance::Cosine,
                                    _ => {
                                        return Err(miette!(
                                            "Invalid distance: {}",
                                            opt_val.as_str()
                                        ))
                                    }
                                }
                            }
                            "filter" => {
                                index_filter = Some(opt_val.as_str().to_string());
                            }
                            "extend_candidates" => {
                                extend_candidates = opt_val.as_str().trim() == "true";
                            }
                            "keep_pruned_connections" => {
                                keep_pruned_connections = opt_val.as_str().trim() == "true";
                            }
                            _ => return Err(miette!("Invalid option: {}", opt_name.as_str())),
                        }
                    }
                    if ef_construction == 0 {
                        bail!("ef_construction must be set");
                    }
                    if m_neighbours == 0 {
                        bail!("m_neighbours must be set");
                    }
                    SysOp::CreateVectorIndex(HnswIndexConfig {
                        base_relation: SmartString::from(rel.as_str()),
                        index_name: SmartString::from(name.as_str()),
                        vec_dim,
                        dtype,
                        vec_fields,
                        distance,
                        ef_construction,
                        m_neighbours,
                        index_filter,
                        extend_candidates,
                        keep_pruned_connections,
                    })
                }
                Rule::index_drop => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    SysOp::RemoveIndex(
                        Symbol::new(rel.as_str(), rel.extract_span()),
                        Symbol::new(name.as_str(), name.extract_span()),
                    )
                }
                r => unreachable!("{:?}", r),
            }
        }
        Rule::index_op => {
            let inner = inner.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::index_create => {
                    let span = inner.extract_span();
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    let cols = inner
                        .map(|p| Symbol::new(p.as_str(), p.extract_span()))
                        .collect_vec();

                    #[derive(Debug, Diagnostic, Error)]
                    #[error("index must have at least one column specified")]
                    #[diagnostic(code(parser::empty_index))]
                    struct EmptyIndex(#[label] SourceSpan);

                    ensure!(!cols.is_empty(), EmptyIndex(span));
                    SysOp::CreateIndex(
                        Symbol::new(rel.as_str(), rel.extract_span()),
                        Symbol::new(name.as_str(), name.extract_span()),
                        cols,
                    )
                }
                Rule::index_drop => {
                    let mut inner = inner.into_inner();
                    let rel = inner.next().unwrap();
                    let name = inner.next().unwrap();
                    SysOp::RemoveIndex(
                        Symbol::new(rel.as_str(), rel.extract_span()),
                        Symbol::new(name.as_str(), name.extract_span()),
                    )
                }
                _ => unreachable!(),
            }
        }
        Rule::list_fixed_rules => SysOp::ListFixedRules,
        r => unreachable!("{:?}", r),
    })
}
