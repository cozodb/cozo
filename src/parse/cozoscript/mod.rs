use pest_derive::Parser;

pub(crate) mod number;
pub(crate) mod query;
pub(crate) mod string;
pub(crate) mod tx;
pub(crate) mod schema;

#[derive(Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;
