use miette::{miette, bail, Result};

use crate::parse::cozoscript::number::parse_int;
use crate::parse::cozoscript::Pair;
use crate::parse::cozoscript::Rule;

fn parse_raw_string(pair: Pair<'_>) -> Result<String> {
    Ok(pair
        .into_inner()
        .into_iter()
        .next()
        .unwrap()
        .as_str()
        .to_string())
}

fn parse_quoted_string(pair: Pair<'_>) -> Result<String> {
    let pairs = pair.into_inner().next().unwrap().into_inner();
    let mut ret = String::with_capacity(pairs.as_str().len());
    for pair in pairs {
        let s = pair.as_str();
        match s {
            r#"\""# => ret.push('"'),
            r"\\" => ret.push('\\'),
            r"\/" => ret.push('/'),
            r"\b" => ret.push('\x08'),
            r"\f" => ret.push('\x0c'),
            r"\n" => ret.push('\n'),
            r"\r" => ret.push('\r'),
            r"\t" => ret.push('\t'),
            s if s.starts_with(r"\u") => {
                let code = parse_int(s, 16) as u32;
                let ch =
                    char::from_u32(code).ok_or_else(|| miette!("invalid UTF8 code {}", code))?;
                ret.push(ch);
            }
            s if s.starts_with('\\') => {
                bail!("invalid escape sequence {}", s);
            }
            s => ret.push_str(s),
        }
    }
    Ok(ret)
}

fn parse_s_quoted_string(pair: Pair<'_>) -> Result<String> {
    let pairs = pair.into_inner().next().unwrap().into_inner();
    let mut ret = String::with_capacity(pairs.as_str().len());
    for pair in pairs {
        let s = pair.as_str();
        match s {
            r#"\'"# => ret.push('\''),
            r"\\" => ret.push('\\'),
            r"\/" => ret.push('/'),
            r"\b" => ret.push('\x08'),
            r"\f" => ret.push('\x0c'),
            r"\n" => ret.push('\n'),
            r"\r" => ret.push('\r'),
            r"\t" => ret.push('\t'),
            s if s.starts_with(r"\u") => {
                let code = parse_int(s, 16) as u32;
                let ch =
                    char::from_u32(code).ok_or_else(|| miette!("invalid UTF8 code {}", code))?;
                ret.push(ch);
            }
            s if s.starts_with('\\') => {
                bail!("invalid escape sequence {}", s);
            }
            s => ret.push_str(s),
        }
    }
    Ok(ret)
}

pub(crate) fn parse_string(pair: Pair<'_>) -> Result<String> {
    match pair.as_rule() {
        Rule::quoted_string => Ok(parse_quoted_string(pair)?),
        Rule::s_quoted_string => Ok(parse_s_quoted_string(pair)?),
        Rule::raw_string => Ok(parse_raw_string(pair)?),
        Rule::ident => Ok(pair.as_str().to_string()),
        t => unreachable!("{:?}", t),
    }
}
