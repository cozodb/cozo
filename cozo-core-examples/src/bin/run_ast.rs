use std::collections::BTreeMap;

use cozo::{
    data::{
        functions::current_validity,
        program::{InputAtom, InputInlineRule, InputInlineRulesOrFixed, InputProgram, Unification},
        symb::PROG_ENTRY,
    },
    parse::{CozoScript, ImperativeStmt, ImperativeStmtClause},
    DataValue, DbInstance, Num, ScriptMutability, Symbol,
};

fn main() {
    let db = DbInstance::new("mem", "", Default::default()).unwrap();
    let sym_a = Symbol::new("a", Default::default());
    let script = CozoScript::Imperative(vec![ImperativeStmt::Program {
        prog: ImperativeStmtClause {
            prog: InputProgram {
                prog: {
                    let mut p = BTreeMap::new();
                    p.insert(
                        Symbol::new(PROG_ENTRY, Default::default()),
                        InputInlineRulesOrFixed::Rules {
                            rules: vec![InputInlineRule {
                                head: vec![sym_a.clone()],
                                aggr: vec![None],
                                body: vec![InputAtom::Unification {
                                    inner: Unification {
                                        binding: sym_a,
                                        expr: cozo::Expr::Const {
                                            val: DataValue::List(vec![
                                                DataValue::Num(Num::Int(1)),
                                                DataValue::Num(Num::Int(2)),
                                                DataValue::Num(Num::Int(3)),
                                            ]),
                                            span: Default::default(),
                                        },
                                        one_many_unif: true,
                                        span: Default::default(),
                                    },
                                }],
                                span: Default::default(),
                            }],
                        },
                    );
                    p
                },
                out_opts: Default::default(),
                disable_magic_rewrite: false,
            },
            store_as: None,
        },
    }]);
    let result = db
        .run_script_ast(script, current_validity(), ScriptMutability::Immutable)
        .unwrap();
    println!("{:?}", result);
}
