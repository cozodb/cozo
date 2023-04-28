/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ordered_float::OrderedFloat;
use crate::fts::tokenizer::TextAnalyzer;
use smartstring::{LazyCompact, SmartString};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FtsLiteral {
    pub(crate) value: SmartString<LazyCompact>,
    pub(crate) is_prefix: bool,
    pub(crate) booster: OrderedFloat<f64>,
}

impl FtsLiteral {
    pub(crate) fn tokenize(self, tokenizer: &TextAnalyzer, coll: &mut Vec<Self>) {
        if self.is_prefix {
            coll.push(self);
            return;
        }

        let mut tokens = tokenizer.token_stream(&self.value);
        while let Some(t) = tokens.next() {
            coll.push(FtsLiteral {
                value: SmartString::from(&t.text),
                is_prefix: false,
                booster: self.booster,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FtsNear {
    pub(crate) literals: Vec<FtsLiteral>,
    pub(crate) distance: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FtsExpr {
    Literal(FtsLiteral),
    Near(FtsNear),
    And(Vec<FtsExpr>),
    Or(Vec<FtsExpr>),
    Not(Box<FtsExpr>, Box<FtsExpr>),
}

impl FtsExpr {
    // pub(crate) fn needs_idf(&self) -> bool {
    //     match self {
    //         FtsExpr::Literal(_) => false,
    //         FtsExpr::Near(_) => false,
    //         FtsExpr::And(exprs) => exprs.iter().any(|e| e.needs_idf()),
    //         FtsExpr::Or(_) => true,
    //         FtsExpr::Not(lhs, _) => lhs.needs_idf(),
    //     }
    // }

    pub(crate) fn tokenize(self, tokenizer: &TextAnalyzer) -> Self {
        self.do_tokenize(tokenizer).flatten()
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            FtsExpr::Literal(l) => {
                l.booster == 0. || l.value.is_empty()
            },
            FtsExpr::Near(FtsNear{ literals, .. }) => {literals.is_empty()}
            FtsExpr::And(v) => {v.is_empty()}
            FtsExpr::Or(v) => {v.is_empty()}
            FtsExpr::Not(lhs, _) => {lhs.is_empty()}
        }
    }

    pub(crate) fn flatten(self) -> Self {
        match self {
            FtsExpr::And(exprs) => {
                let mut flattened = vec![];
                for e in exprs {
                    match e.flatten() {
                        FtsExpr::And(es) => flattened.extend(es),
                        e => {
                            if !e.is_empty() {
                                flattened.push(e)
                            }
                        },
                    }
                }
                if flattened.len() == 1 {
                    flattened.into_iter().next().unwrap()
                } else {
                    FtsExpr::And(flattened)
                }
            }
            FtsExpr::Or(exprs) => {
                let mut flattened = vec![];
                for e in exprs {
                    match e.flatten() {
                        FtsExpr::Or(es) => flattened.extend(es),
                        e => {
                            if !e.is_empty() {
                                flattened.push(e)
                            }
                        },                    }
                }
                if flattened.len() == 1 {
                    flattened.into_iter().next().unwrap()
                } else {
                    FtsExpr::Or(flattened)
                }
            }
            FtsExpr::Not(lhs, rhs) => {
                let lhs = lhs.flatten();
                let rhs = rhs.flatten();
                if rhs.is_empty() {
                    lhs
                } else {
                    FtsExpr::Not(Box::new(lhs), Box::new(rhs))
                }
            }
            FtsExpr::Literal(l) => FtsExpr::Literal(l),
            FtsExpr::Near(n) => FtsExpr::Near(n),
        }
    }

    fn do_tokenize(self, tokenizer: &TextAnalyzer) -> Self {
        match self {
            FtsExpr::Literal(l) => {
                let mut tokens = vec![];
                l.tokenize(tokenizer, &mut tokens);
                if tokens.len() == 1 {
                    FtsExpr::Literal(tokens.into_iter().next().unwrap())
                } else {
                    FtsExpr::And(tokens.into_iter().map(FtsExpr::Literal).collect())
                }
            }
            FtsExpr::Near(FtsNear { literals, distance }) => {
                let mut tokens = vec![];
                for l in literals {
                    l.tokenize(tokenizer, &mut tokens);
                }
                FtsExpr::Near(FtsNear {
                    literals: tokens,
                    distance,
                })
            }
            FtsExpr::And(exprs) => FtsExpr::And(
                exprs
                    .into_iter()
                    .map(|e| e.do_tokenize(tokenizer))
                    .collect(),
            ),
            FtsExpr::Or(exprs) => FtsExpr::Or(
                exprs
                    .into_iter()
                    .map(|e| e.do_tokenize(tokenizer))
                    .collect(),
            ),
            FtsExpr::Not(lhs, rhs) => FtsExpr::Not(
                Box::new(lhs.do_tokenize(tokenizer)),
                Box::new(rhs.do_tokenize(tokenizer)),
            ),
        }
    }
}
