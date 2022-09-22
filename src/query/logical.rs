use std::collections::BTreeSet;

use itertools::Itertools;
use miette::{bail, Result};

use crate::data::expr::Expr;
use crate::data::program::{InputAtom, InputNamedFieldRelationApplyAtom, InputRelationApplyAtom, InputRuleApplyAtom, InputTerm, NormalFormAtom, NormalFormRelationApplyAtom, NormalFormRuleApplyAtom, TempSymbGen, Unification};
use crate::query::reorder::UnsafeNegation;
use crate::runtime::transact::SessionTx;

#[derive(Debug)]
pub(crate) struct Disjunction {
    pub(crate) inner: Vec<Conjunction>,
}

impl Disjunction {
    fn conjunctive_to_disjunctive_de_morgen(self, other: Self) -> Self {
        // invariants: self and other are both already in disjunctive normal form, which are to be conjuncted together
        // the return value must be in disjunctive normal form
        let mut ret = vec![];
        let right_args = other.inner.into_iter().map(|a| a.0).collect_vec();
        for left in self.inner {
            let left = left.0;
            for right in &right_args {
                let mut current = left.clone();
                current.extend_from_slice(right);
                ret.push(Conjunction(current))
            }
        }
        Disjunction { inner: ret }
    }
    fn singlet(atom: NormalFormAtom) -> Self {
        Disjunction {
            inner: vec![Conjunction(vec![atom])],
        }
    }
    fn conj(atoms: Vec<NormalFormAtom>) -> Self {
        Disjunction {
            inner: vec![Conjunction(atoms)],
        }
    }
}

#[derive(Debug)]
pub(crate) struct Conjunction(pub(crate) Vec<NormalFormAtom>);

impl InputAtom {
    pub(crate) fn negation_normal_form(self) -> Result<Self> {
        Ok(match self {
            a @ (InputAtom::Rule { .. }
            | InputAtom::NamedFieldRelation { .. }
            | InputAtom::Predicate { .. }
            | InputAtom::Relation { .. }) => a,
            InputAtom::Conjunction { inner: args, span } => InputAtom::Conjunction {
                inner: args
                    .into_iter()
                    .map(|a| a.negation_normal_form())
                    .try_collect()?,
                span,
            },
            InputAtom::Disjunction { inner: args, span } => InputAtom::Disjunction {
                inner: args
                    .into_iter()
                    .map(|a| a.negation_normal_form())
                    .try_collect()?,
                span,
            },
            InputAtom::Unification { inner: unif } => InputAtom::Unification { inner: unif },
            InputAtom::Negation { inner: arg, span } => match *arg {
                a @ (InputAtom::Rule { .. }
                | InputAtom::NamedFieldRelation { .. }
                | InputAtom::Relation { .. }) => InputAtom::Negation {
                    inner: Box::new(a),
                    span,
                },
                InputAtom::Predicate { inner: p } => InputAtom::Predicate {
                    inner: p.negate(span),
                },
                InputAtom::Negation { inner, .. } => inner.negation_normal_form()?,
                InputAtom::Conjunction { inner: args, .. } => InputAtom::Disjunction {
                    inner: args
                        .into_iter()
                        .map(|a| {
                            let span = a.span();
                            InputAtom::Negation {
                                inner: Box::new(a),
                                span,
                            }
                                .negation_normal_form()
                        })
                        .try_collect()?,
                    span,
                },
                InputAtom::Disjunction { inner: args, span } => InputAtom::Conjunction {
                    inner: args
                        .into_iter()
                        .map(|a| {
                            let span = a.span();
                            InputAtom::Negation {
                                inner: Box::new(a),
                                span,
                            }
                                .negation_normal_form()
                        })
                        .try_collect()?,
                    span,
                },
                InputAtom::Unification { inner } => {
                    bail!(UnsafeNegation(inner.span))
                }
            },
        })
    }

    pub(crate) fn disjunctive_normal_form(self, tx: &SessionTx) -> Result<Disjunction> {
        let neg_form = self.negation_normal_form()?;
        let mut gen = TempSymbGen::default();
        neg_form.do_disjunctive_normal_form(&mut gen, tx)
    }

    fn do_disjunctive_normal_form(
        self,
        gen: &mut TempSymbGen,
        tx: &SessionTx,
    ) -> Result<Disjunction> {
        // invariants: the input is already in negation normal form
        // the return value is a disjunction of conjunctions, with no nesting
        Ok(match self {
            InputAtom::Disjunction { inner: args, .. } => {
                let mut ret = vec![];
                for arg in args {
                    for a in arg.do_disjunctive_normal_form(gen, tx)?.inner {
                        ret.push(a);
                    }
                }
                Disjunction { inner: ret }
            }
            InputAtom::Conjunction { inner: args, .. } => {
                let mut args = args
                    .into_iter()
                    .map(|a| a.do_disjunctive_normal_form(gen, tx));
                let mut result = args.next().unwrap()?;
                for a in args {
                    result = result.conjunctive_to_disjunctive_de_morgen(a?)
                }
                result
            }
            InputAtom::Rule { inner: r } => r.normalize(false, gen),
            InputAtom::NamedFieldRelation { inner: InputNamedFieldRelationApplyAtom { name, mut args, span } } => {
                let stored = tx.get_relation(&name)?;
                let mut new_args = vec![];
                for (arg_name, _) in stored.metadata.keys.iter().chain(stored.metadata.dependents.iter()) {
                    let arg = args.remove(arg_name)
                        .unwrap_or_else(|| InputTerm::Var { name: gen.next(span) });
                    new_args.push(arg)
                }
                let r = InputRuleApplyAtom {
                    name,
                    args: new_args,
                    span,
                };
                r.normalize(false, gen)
            }
            InputAtom::Relation { inner: v } => v.normalize(false, gen),
            InputAtom::Predicate { inner: mut p } => {
                p.partial_eval()?;
                Disjunction::singlet(NormalFormAtom::Predicate(p))
            }
            InputAtom::Negation { inner: n, .. } => match *n {
                InputAtom::Rule { inner: r } => r.normalize(true, gen),
                InputAtom::Relation { inner: v } => v.normalize(true, gen),
                _ => unreachable!(),
            },
            InputAtom::Unification { inner: u } => {
                Disjunction::singlet(NormalFormAtom::Unification(u))
            }
        })
    }
}

impl InputRuleApplyAtom {
    fn normalize(self, is_negated: bool, gen: &mut TempSymbGen) -> Disjunction {
        let mut ret = Vec::with_capacity(self.args.len() + 1);
        let mut args = Vec::with_capacity(self.args.len());
        let mut seen_variables = BTreeSet::new();
        for arg in self.args {
            match arg {
                InputTerm::Var { name: kw } => {
                    if seen_variables.insert(kw.clone()) {
                        args.push(kw);
                    } else {
                        let dup = gen.next(kw.span);
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: dup.clone(),
                            expr: Expr::Binding {
                                var: kw,
                                tuple_pos: None,
                            },
                            one_many_unif: false,
                            span: dup.span,
                        });
                        ret.push(unif);
                        args.push(dup);
                    }
                }
                InputTerm::Const { val, span } => {
                    let kw = gen.next(span);
                    args.push(kw.clone());
                    let unif = NormalFormAtom::Unification(Unification {
                        binding: kw,
                        expr: Expr::Const { val, span },
                        one_many_unif: false,
                        span,
                    });
                    ret.push(unif)
                }
            }
        }

        ret.push(if is_negated {
            NormalFormAtom::NegatedRule(NormalFormRuleApplyAtom {
                name: self.name,
                args,
                span: self.span,
            })
        } else {
            NormalFormAtom::Rule(NormalFormRuleApplyAtom {
                name: self.name,
                args,
                span: self.span,
            })
        });
        Disjunction::conj(ret)
    }
}

impl InputRelationApplyAtom {
    fn normalize(self, is_negated: bool, gen: &mut TempSymbGen) -> Disjunction {
        let mut ret = Vec::with_capacity(self.args.len() + 1);
        let mut args = Vec::with_capacity(self.args.len());
        let mut seen_variables = BTreeSet::new();
        for arg in self.args {
            match arg {
                InputTerm::Var { name: kw } => {
                    if seen_variables.insert(kw.clone()) {
                        args.push(kw);
                    } else {
                        let span = kw.span;
                        let dup = gen.next(span);
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: dup.clone(),
                            expr: Expr::Binding {
                                var: kw,
                                tuple_pos: None,
                            },
                            one_many_unif: false,
                            span,
                        });
                        ret.push(unif);
                        args.push(dup);
                    }
                }
                InputTerm::Const { val, span } => {
                    let kw = gen.next(span);
                    args.push(kw.clone());
                    let unif = NormalFormAtom::Unification(Unification {
                        binding: kw,
                        expr: Expr::Const { val, span },
                        one_many_unif: false,
                        span,
                    });
                    ret.push(unif)
                }
            }
        }

        ret.push(if is_negated {
            NormalFormAtom::NegatedRelation(NormalFormRelationApplyAtom {
                name: self.name,
                args,
                span: self.span,
            })
        } else {
            NormalFormAtom::Relation(NormalFormRelationApplyAtom {
                name: self.name,
                args,
                span: self.span,
            })
        });
        Disjunction::conj(ret)
    }
}
