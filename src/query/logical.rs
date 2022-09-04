use std::collections::BTreeSet;

use miette::{bail, Result};
use itertools::Itertools;

use crate::data::expr::Expr;
use crate::data::program::{
    InputAtom, InputAttrTripleAtom, InputRuleApplyAtom, InputTerm, InputViewApplyAtom,
    NormalFormAtom, NormalFormAttrTripleAtom, NormalFormRuleApplyAtom, NormalFormViewApplyAtom,
    TempSymbGen, Unification,
};

#[derive(Debug)]
pub(crate) struct Disjunction(pub(crate) Vec<Conjunction>);

impl Disjunction {
    fn conjunctive_to_disjunctive_de_morgen(self, other: Self) -> Self {
        // invariants: self and other are both already in disjunctive normal form, which are to be conjuncted together
        // the return value must be in disjunctive normal form
        let mut ret = vec![];
        let right_args = other.0.into_iter().map(|a| a.0).collect_vec();
        for left in self.0 {
            let left = left.0;
            for right in &right_args {
                let mut current = left.clone();
                current.extend_from_slice(right);
                ret.push(Conjunction(current))
            }
        }
        Disjunction(ret)
    }
    fn singlet(atom: NormalFormAtom) -> Self {
        Disjunction(vec![Conjunction(vec![atom])])
    }
    fn conj(atoms: Vec<NormalFormAtom>) -> Self {
        Disjunction(vec![Conjunction(atoms)])
    }
}

#[derive(Debug)]
pub(crate) struct Conjunction(pub(crate) Vec<NormalFormAtom>);

impl InputAtom {
    pub(crate) fn negation_normal_form(self) -> Result<Self> {
        Ok(match self {
            a @ (InputAtom::AttrTriple(_)
            | InputAtom::Rule(_)
            | InputAtom::Predicate(_)
            | InputAtom::View(_)) => a,
            InputAtom::Conjunction(args) => InputAtom::Conjunction(
                args.into_iter()
                    .map(|a| a.negation_normal_form())
                    .try_collect()?,
            ),
            InputAtom::Disjunction(args) => InputAtom::Disjunction(
                args.into_iter()
                    .map(|a| a.negation_normal_form())
                    .try_collect()?,
            ),
            InputAtom::Unification(unif) => InputAtom::Unification(unif),
            InputAtom::Negation(arg) => match *arg {
                a @ (InputAtom::AttrTriple(_) | InputAtom::Rule(_) | InputAtom::View(_)) => {
                    InputAtom::Negation(Box::new(a))
                }
                InputAtom::Predicate(p) => InputAtom::Predicate(p.negate()),
                InputAtom::Negation(inner) => inner.negation_normal_form()?,
                InputAtom::Conjunction(args) => InputAtom::Disjunction(
                    args.into_iter()
                        .map(|a| InputAtom::Negation(Box::new(a)).negation_normal_form())
                        .try_collect()?,
                ),
                InputAtom::Disjunction(args) => InputAtom::Conjunction(
                    args.into_iter()
                        .map(|a| InputAtom::Negation(Box::new(a)).negation_normal_form())
                        .try_collect()?,
                ),
                InputAtom::Unification(unif) => {
                    bail!("unification not allowed in negation: {:?}", unif)
                }
            },
        })
    }

    pub(crate) fn disjunctive_normal_form(self) -> Result<Disjunction> {
        let neg_form = self.negation_normal_form()?;
        let mut gen = TempSymbGen::default();
        neg_form.do_disjunctive_normal_form(&mut gen)
    }

    fn do_disjunctive_normal_form(self, gen: &mut TempSymbGen) -> Result<Disjunction> {
        // invariants: the input is already in negation normal form
        // the return value is a disjunction of conjunctions, with no nesting
        Ok(match self {
            InputAtom::Disjunction(args) => {
                let mut ret = vec![];
                for arg in args {
                    for a in arg.do_disjunctive_normal_form(gen)?.0 {
                        ret.push(a);
                    }
                }
                Disjunction(ret)
            }
            InputAtom::Conjunction(args) => {
                let mut args = args.into_iter().map(|a| a.do_disjunctive_normal_form(gen));
                let mut result = args.next().unwrap()?;
                for a in args {
                    result = result.conjunctive_to_disjunctive_de_morgen(a?)
                }
                result
            }
            InputAtom::AttrTriple(a) => a.normalize(false, gen),
            InputAtom::Rule(r) => r.normalize(false, gen),
            InputAtom::View(v) => v.normalize(false, gen),
            InputAtom::Predicate(mut p) => {
                p.partial_eval(&Default::default())?;
                Disjunction::singlet(NormalFormAtom::Predicate(p))
            }
            InputAtom::Negation(n) => match *n {
                InputAtom::Rule(r) => r.normalize(true, gen),
                InputAtom::AttrTriple(r) => r.normalize(true, gen),
                InputAtom::View(v) => v.normalize(true, gen),
                _ => unreachable!(),
            },
            InputAtom::Unification(u) => Disjunction::singlet(NormalFormAtom::Unification(u)),
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
                InputTerm::Var(kw) => {
                    if seen_variables.insert(kw.clone()) {
                        args.push(kw);
                    } else {
                        let dup = gen.next();
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: dup.clone(),
                            expr: Expr::Binding(kw, None),
                            one_many_unif: false,
                        });
                        ret.push(unif);
                        args.push(dup);
                    }
                }
                InputTerm::Const(val) => {
                    let kw = gen.next();
                    args.push(kw.clone());
                    let unif = NormalFormAtom::Unification(Unification {
                        binding: kw,
                        expr: Expr::Const(val),
                        one_many_unif: false,
                    });
                    ret.push(unif)
                }
            }
        }

        ret.push(if is_negated {
            NormalFormAtom::NegatedRule(NormalFormRuleApplyAtom {
                name: self.name,
                args,
            })
        } else {
            NormalFormAtom::Rule(NormalFormRuleApplyAtom {
                name: self.name,
                args,
            })
        });
        Disjunction::conj(ret)
    }
}

impl InputAttrTripleAtom {
    fn normalize(self, is_negated: bool, gen: &mut TempSymbGen) -> Disjunction {
        let wrap = |atom| {
            if is_negated {
                NormalFormAtom::NegatedAttrTriple(atom)
            } else {
                NormalFormAtom::AttrTriple(atom)
            }
        };
        Disjunction::conj(match (self.entity, self.value) {
            (InputTerm::Const(eid), InputTerm::Const(val)) => {
                let ekw = gen.next();
                let vkw = gen.next();
                let atom = NormalFormAttrTripleAtom {
                    attr: self.attr,
                    entity: ekw.clone(),
                    value: vkw.clone(),
                };
                let ret = wrap(atom);
                let ue = NormalFormAtom::Unification(Unification {
                    binding: ekw,
                    expr: Expr::Const(eid.as_datavalue()),
                    one_many_unif: false,
                });
                let uv = NormalFormAtom::Unification(Unification {
                    binding: vkw,
                    expr: Expr::Const(val),
                    one_many_unif: false,
                });
                vec![ue, uv, ret]
            }
            (InputTerm::Var(ekw), InputTerm::Const(val)) => {
                let vkw = gen.next();
                let atom = NormalFormAttrTripleAtom {
                    attr: self.attr,
                    entity: ekw,
                    value: vkw.clone(),
                };
                let ret = wrap(atom);
                let uv = NormalFormAtom::Unification(Unification {
                    binding: vkw,
                    expr: Expr::Const(val),
                    one_many_unif: false,
                });
                vec![uv, ret]
            }
            (InputTerm::Const(eid), InputTerm::Var(vkw)) => {
                let ekw = gen.next();
                let atom = NormalFormAttrTripleAtom {
                    attr: self.attr,
                    entity: ekw.clone(),
                    value: vkw,
                };
                let ret = wrap(atom);
                let ue = NormalFormAtom::Unification(Unification {
                    binding: ekw,
                    expr: Expr::Const(eid.as_datavalue()),
                    one_many_unif: false,
                });
                vec![ue, ret]
            }
            (InputTerm::Var(ekw), InputTerm::Var(vkw)) => {
                if ekw == vkw {
                    let dup = gen.next();
                    let atom = NormalFormAttrTripleAtom {
                        attr: self.attr,
                        entity: ekw,
                        value: dup.clone(),
                    };
                    vec![
                        NormalFormAtom::Unification(Unification {
                            binding: dup,
                            expr: Expr::Binding(vkw, None),
                            one_many_unif: false,
                        }),
                        wrap(atom),
                    ]
                } else {
                    let ret = wrap(NormalFormAttrTripleAtom {
                        attr: self.attr,
                        entity: ekw,
                        value: vkw,
                    });
                    vec![ret]
                }
            }
        })
    }
}

impl InputViewApplyAtom {
    fn normalize(self, is_negated: bool, gen: &mut TempSymbGen) -> Disjunction {
        let mut ret = Vec::with_capacity(self.args.len() + 1);
        let mut args = Vec::with_capacity(self.args.len());
        let mut seen_variables = BTreeSet::new();
        for arg in self.args {
            match arg {
                InputTerm::Var(kw) => {
                    if seen_variables.insert(kw.clone()) {
                        args.push(kw);
                    } else {
                        let dup = gen.next();
                        let unif = NormalFormAtom::Unification(Unification {
                            binding: dup.clone(),
                            expr: Expr::Binding(kw, None),
                            one_many_unif: false,
                        });
                        ret.push(unif);
                        args.push(dup);
                    }
                }
                InputTerm::Const(val) => {
                    let kw = gen.next();
                    args.push(kw.clone());
                    let unif = NormalFormAtom::Unification(Unification {
                        binding: kw,
                        expr: Expr::Const(val),
                        one_many_unif: false,
                    });
                    ret.push(unif)
                }
            }
        }

        ret.push(if is_negated {
            NormalFormAtom::NegatedView(NormalFormViewApplyAtom {
                name: self.name,
                args,
            })
        } else {
            NormalFormAtom::View(NormalFormViewApplyAtom {
                name: self.name,
                args,
            })
        });
        Disjunction::conj(ret)
    }
}
