use pest::iterators::Pair;
use crate::parser::Rule;
use crate::error::{CozoError, Result};
use crate::parser::number::parse_int;

#[inline]
fn parse_raw_string(pair: Pair<Rule>) -> Result<String> {
    Ok(pair.into_inner().into_iter().next().unwrap().as_str().to_string())
}

#[inline]
fn parse_quoted_string(pair: Pair<Rule>) -> Result<String> {
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
                let ch = char::from_u32(code).ok_or(CozoError::InvalidUtfCode)?;
                ret.push(ch);
            }
            s if s.starts_with('\\') => return Err(CozoError::InvalidEscapeSequence),
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}


#[inline]
fn parse_s_quoted_string(pair: Pair<Rule>) -> Result<String> {
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
                let ch = char::from_u32(code).ok_or(CozoError::InvalidUtfCode)?;
                ret.push(ch);
            }
            s if s.starts_with('\\') => return Err(CozoError::InvalidEscapeSequence),
            s => ret.push_str(s)
        }
    }
    Ok(ret)
}

#[inline]
pub fn parse_string(pair: Pair<Rule>) -> Result<String> {
    match pair.as_rule() {
        Rule::quoted_string => Ok(parse_quoted_string(pair)?),
        Rule::s_quoted_string => Ok(parse_s_quoted_string(pair)?),
        Rule::raw_string => Ok(parse_raw_string(pair)?),
        Rule::ident => Ok(pair.as_str().to_string()),
        _ => unreachable!()
    }
}

pub fn parse_ident(pair: Pair<Rule>) -> String {
    pair.as_str().to_string()
}

pub fn build_name_in_def(pair: Pair<Rule>, forbid_underscore: bool) -> Result<String> {
    let inner = pair.into_inner().next().unwrap();
    let name = match inner.as_rule() {
        Rule::ident => parse_ident(inner),
        Rule::raw_string | Rule::s_quoted_string | Rule::quoted_string => parse_string(inner)?,
        _ => unreachable!()
    };
    if forbid_underscore && name.starts_with('_') {
        Err(CozoError::ReservedIdent)
    } else {
        Ok(name)
    }
}

pub fn parse_col_name(pair: Pair<Rule>) -> Result<(String, bool)> {
    let mut pairs = pair.into_inner();
    let mut is_key = false;
    let mut nxt_pair = pairs.next().unwrap();
    if nxt_pair.as_rule() == Rule::key_marker {
        is_key = true;
        nxt_pair = pairs.next().unwrap();
    }

    Ok((build_name_in_def(nxt_pair, true)?, is_key))
}
