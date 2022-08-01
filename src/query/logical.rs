use anyhow::{bail, Result};
use itertools::Itertools;

use crate::data::expr::Expr;
use crate::data::program::{
    InputAtom, InputAttrTripleAtom, InputRuleApplyAtom, InputTerm, NormalFormAtom,
    NormalFormAttrTripleAtom, NormalFormRuleApplyAtom, TempKwGen, Unification,
};
use crate::data::value::DataValue;
use crate::query::compile::Atom;
use crate::EntityId;

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

pub(crate) struct Conjunction(pub(crate) Vec<NormalFormAtom>);

impl InputAtom {
    pub(crate) fn negation_normal_form(self) -> Result<Self> {
        Ok(match self {
            a @ (InputAtom::AttrTriple(_) | InputAtom::Rule(_) | InputAtom::Predicate(_)) => a,
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
                a @ (InputAtom::AttrTriple(_) | InputAtom::Rule(_)) => {
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
        let mut gen = TempKwGen::default();
        self.negation_normal_form()?
            .do_disjunctive_normal_form(&mut gen)
    }

    fn do_disjunctive_normal_form(self, gen: &mut TempKwGen) -> Result<Disjunction> {
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
            InputAtom::AttrTriple(a) => Disjunction::conj(a.normalize(false, gen)),
            InputAtom::Rule(r) => Disjunction::conj(r.normalize(false, gen)),
            InputAtom::Predicate(mut p) => {
                p.partial_eval()?;
                Disjunction::singlet(NormalFormAtom::Predicate(p))
            }
            InputAtom::Negation(n) => match *n {
                InputAtom::Rule(r) => Disjunction::conj(r.normalize(true, gen)),
                InputAtom::AttrTriple(r) => Disjunction::conj(r.normalize(true, gen)),
                _ => unreachable!(),
            },
            InputAtom::Unification(u) => Disjunction::singlet(NormalFormAtom::Unification(u)),
        })
    }
}

impl InputRuleApplyAtom {
    fn normalize(mut self, is_negated: bool, gen: &mut TempKwGen) -> Vec<NormalFormAtom> {
        let mut ret = Vec::with_capacity(self.args.len() + 1);
        let mut args = Vec::with_capacity(self.args.len());
        for arg in self.args {
            match arg {
                InputTerm::Var(kw) => args.push(kw),
                InputTerm::Const(val) => {
                    let kw = gen.next();
                    args.push(kw.clone());
                    let unif = NormalFormAtom::Unification(Unification {
                        binding: kw,
                        expr: Expr::Const(val),
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
        ret
    }
}

impl InputAttrTripleAtom {
    fn normalize(mut self, is_negated: bool, gen: &mut TempKwGen) -> Vec<NormalFormAtom> {
        let wrap = |atom| {
            if is_negated {
                NormalFormAtom::NegatedAttrTriple(atom)
            } else {
                NormalFormAtom::AttrTriple(atom)
            }
        };
        match (self.entity, self.value) {
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
                    expr: Expr::Const(DataValue::EnId(eid)),
                });
                let uv = NormalFormAtom::Unification(Unification {
                    binding: vkw,
                    expr: Expr::Const(val),
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
                    expr: Expr::Const(DataValue::EnId(eid)),
                });
                vec![ue, ret]
            }
            (InputTerm::Var(ekw), InputTerm::Var(vkw)) => {
                let ret = wrap(NormalFormAttrTripleAtom {
                    attr: self.attr,
                    entity: ekw,
                    value: vkw,
                });
                vec![ret]
            }
        }
    }
}

impl Atom {
    pub(crate) fn negation_normal_form(self) -> Self {
        match self {
            a @ (Atom::AttrTriple(_) | Atom::Rule(_) | Atom::Predicate(_)) => a,
            Atom::Conjunction(args) => {
                Atom::Conjunction(args.into_iter().map(|a| a.negation_normal_form()).collect())
            }
            Atom::Disjunction(args) => {
                Atom::Disjunction(args.into_iter().map(|a| a.negation_normal_form()).collect())
            }
            Atom::Negation(arg) => match *arg {
                a @ (Atom::AttrTriple(_) | Atom::Rule(_)) => Atom::Negation(Box::new(a)),
                Atom::Predicate(p) => Atom::Predicate(p.negate()),
                Atom::Negation(inner) => inner.negation_normal_form(),
                Atom::Conjunction(args) => Atom::Disjunction(
                    args.into_iter()
                        .map(|a| Atom::Negation(Box::new(a)).negation_normal_form())
                        .collect(),
                ),
                Atom::Disjunction(args) => Atom::Conjunction(
                    args.into_iter()
                        .map(|a| Atom::Negation(Box::new(a)).negation_normal_form())
                        .collect(),
                ),
            },
        }
    }
    pub(crate) fn disjunctive_normal_form(self) -> Vec<Vec<Self>> {
        match self.negation_normal_form().do_disjunctive_normal_form() {
            Atom::Disjunction(atoms) => atoms
                .into_iter()
                .map(|a| match a {
                    Atom::Conjunction(v) => v,
                    _ => unreachable!(),
                })
                .collect_vec(),
            _ => unreachable!(),
        }
    }
    fn get_disjunctive_args(self) -> Option<Vec<Self>> {
        match self {
            Atom::Disjunction(v) => Some(v),
            _ => None,
        }
    }
    fn get_conjunctive_args(self) -> Option<Vec<Self>> {
        match self {
            Atom::Conjunction(v) => Some(v),
            _ => None,
        }
    }
    fn do_disjunctive_normal_form(self) -> Self {
        // invariants: the input is already in negation normal form
        // the return value is a disjunction of conjunctions, with no nesting
        match self {
            // invariant: results is disjunction of conjunctions
            a @ (Atom::AttrTriple(_) | Atom::Rule(_) | Atom::Predicate(_) | Atom::Negation(_)) => {
                Atom::Disjunction(vec![Atom::Conjunction(vec![a])])
            }
            Atom::Disjunction(args) => {
                let mut ret = vec![];
                for arg in args {
                    for a in arg
                        .do_disjunctive_normal_form()
                        .get_disjunctive_args()
                        .unwrap()
                    {
                        ret.push(a);
                    }
                }
                Atom::Disjunction(ret)
            }
            Atom::Conjunction(args) => {
                let mut args = args.into_iter().map(|a| a.do_disjunctive_normal_form());
                let mut result = args.next().unwrap();
                for a in args {
                    result = result.conjunctive_to_disjunctive_de_morgen(a)
                }
                result
            }
        }
    }
    fn conjunctive_to_disjunctive_de_morgen(self, other: Self) -> Self {
        // invariants: self and other are both already in disjunctive normal form, which are to be conjuncted together
        // the return value must be in disjunctive normal form
        let mut ret = vec![];
        let right_args = other
            .get_disjunctive_args()
            .unwrap()
            .into_iter()
            .map(|a| a.get_conjunctive_args().unwrap())
            .collect_vec();
        for left in self.get_disjunctive_args().unwrap() {
            let left = left.get_conjunctive_args().unwrap();
            for right in &right_args {
                let mut current = left.clone();
                current.extend_from_slice(right);
                ret.push(Atom::Conjunction(current))
            }
        }
        Atom::Disjunction(ret)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::expr::Expr;
    use crate::data::value::DataValue;
    use crate::query::compile::Atom;

    #[test]
    fn normal_forms() {
        let a = Atom::Conjunction(vec![
            Atom::Disjunction(vec![
                Atom::Negation(Box::new(Atom::Conjunction(vec![
                    Atom::Negation(Box::new(Atom::Predicate(Expr::Const(DataValue::Int(1))))),
                    Atom::Predicate(Expr::Const(DataValue::Int(2))),
                ]))),
                Atom::Predicate(Expr::Const(DataValue::Int(3))),
            ]),
            Atom::Predicate(Expr::Const(DataValue::Int(4))),
        ]);
        dbg!(a.disjunctive_normal_form());
    }
}
