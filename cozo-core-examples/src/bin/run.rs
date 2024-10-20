use cozo::{DbInstance, ScriptMutability};

fn main() {
    let db = DbInstance::new("mem", "", Default::default()).unwrap();
    let script = "?[a] := a in [1, 2, 3]";
    let result = db
        .run_script(script, Default::default(), ScriptMutability::Immutable)
        .unwrap();
    println!("{:?}", result);
}
