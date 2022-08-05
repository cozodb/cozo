use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;
