use cozo::{data::functions::current_validity, parse::parse_script, DbInstance, ScriptMutability};

fn main() {
    let db = DbInstance::new("mem", "", Default::default()).unwrap();
    let script = "?[a] := a in [1, 2, 3]";
    let cur_vld = current_validity();
    let script_ast =
        parse_script(script, &Default::default(), &db.get_fixed_rules(), cur_vld).unwrap();
    println!("AST: {:?}", script_ast);
    let result = db
        .run_script_ast(script_ast, cur_vld, ScriptMutability::Immutable)
        .unwrap();
    println!("Result: {:?}", result);
}
