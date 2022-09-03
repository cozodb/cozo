use anyhow::Result;
use pest::Parser;

use crate::data::program::InputProgram;

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Query(InputProgram),
}

pub(crate) fn parse_script(src: &str) -> Result<CozoScript> {
    let parsed = CozoScriptParser::parse(Rule::script, src)?.next().unwrap();
    Ok(match parsed.as_rule() {
        Rule::query_script => CozoScript::Query(parse_query(parsed.into_inner())?),
        Rule::schema_script => todo!(),
        Rule::tx_script => todo!(),
        Rule::sys_script => todo!(),
        _ => unreachable!(),
    })
}

fn parse_query(src: Pairs<'_>) -> Result<InputProgram> {
    // let x = InputProgram {
    //     prog: Default::default(),
    // };
    todo!()
}
