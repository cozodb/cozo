use itertools::Itertools;

use crate::query::compile::Atom;

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
