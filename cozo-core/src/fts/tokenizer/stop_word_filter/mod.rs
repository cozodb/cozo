//! # Example
//! ```text
//! use tantivy::tokenizer::*;
//!
//! let tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!   .filter(StopWordFilter::remove(vec!["the".to_string(), "is".to_string()]));
//!
//! let mut stream = tokenizer.token_stream("the fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! ```
#[rustfmt::skip]
mod stopwords;

use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::{BoxTokenStream, Token, TokenFilter, TokenStream};
use miette::{bail, Result};

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub(crate) struct StopWordFilter {
    words: Arc<FxHashSet<String>>,
}

impl StopWordFilter {
    /// Creates a new [`StopWordFilter`] for the given [`Language`]
    ///
    /// Returns `Some` if a list of stop words is available and `None` otherwise.
    pub(crate) fn for_lang(language: &str) -> Result<Self> {
        let words = match language {
            "af" => stopwords::AF,
            "ar" => stopwords::AR,
            "hy" => stopwords::HY,
            "eu" => stopwords::EU,
            "bn" => stopwords::BN,
            "br" => stopwords::BR,
            "bg" => stopwords::BG,
            "ca" => stopwords::CA,
            "zh" => stopwords::ZH,
            "hr" => stopwords::HR,
            "cs" => stopwords::CS,
            "da" => stopwords::DA,
            "nl" => stopwords::NL,
            "en" => stopwords::EN,
            "eo" => stopwords::EO,
            "et" => stopwords::ET,
            "fi" => stopwords::FI,
            "fr" => stopwords::FR,
            "gl" => stopwords::GL,
            "de" => stopwords::DE,
            "el" => stopwords::EL,
            "gu" => stopwords::GU,
            "ha" => stopwords::HA,
            "he" => stopwords::HE,
            "hi" => stopwords::HI,
            "hu" => stopwords::HU,
            "id" => stopwords::ID,
            "ga" => stopwords::GA,
            "it" => stopwords::IT,
            "ja" => stopwords::JA,
            "ko" => stopwords::KO,
            "ku" => stopwords::KU,
            "la" => stopwords::LA,
            "lt" => stopwords::LT,
            "lv" => stopwords::LV,
            "ms" => stopwords::MS,
            "mr" => stopwords::MR,
            "no" => stopwords::NO,
            "fa" => stopwords::FA,
            "pl" => stopwords::PL,
            "pt" => stopwords::PT,
            "ro" => stopwords::RO,
            "ru" => stopwords::RU,
            "sk" => stopwords::SK,
            "sl" => stopwords::SL,
            "so" => stopwords::SO,
            "st" => stopwords::ST,
            "es" => stopwords::ES,
            "sw" => stopwords::SW,
            "sv" => stopwords::SV,
            "th" => stopwords::TH,
            "tl" => stopwords::TL,
            "tr" => stopwords::TR,
            "uk" => stopwords::UK,
            "ur" => stopwords::UR,
            "vi" => stopwords::VI,
            "yo" => stopwords::YO,
            "zu" => stopwords::ZU,
            _ => bail!("Unsupported language: {}", language),
        };

        Ok(Self::new(words.iter().map(|&word| word.to_owned())))
    }

    /// Creates a `StopWordFilter` given a list of words to remove
    pub(crate) fn new<W: IntoIterator<Item = String>>(words: W) -> StopWordFilter {
        StopWordFilter {
            words: Arc::new(words.into_iter().collect()),
        }
    }
}

pub(crate) struct StopWordFilterStream<'a> {
    words: Arc<FxHashSet<String>>,
    tail: BoxTokenStream<'a>,
}

impl TokenFilter for StopWordFilter {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        BoxTokenStream::from(StopWordFilterStream {
            words: self.words.clone(),
            tail: token_stream,
        })
    }
}

impl<'a> StopWordFilterStream<'a> {
    fn predicate(&self, token: &Token) -> bool {
        !self.words.contains(&token.text)
    }
}

impl<'a> TokenStream for StopWordFilterStream<'a> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::fts::tokenizer::tests::assert_token;
    use crate::fts::tokenizer::{SimpleTokenizer, StopWordFilter, TextAnalyzer, Token};

    #[test]
    fn test_stop_word() {
        let tokens = token_stream_helper("i am a cat. as yet i have no name.");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 3, "cat", 7, 10);
        assert_token(&tokens[1], 5, "yet", 15, 18);
        assert_token(&tokens[2], 7, "have", 21, 25);
        assert_token(&tokens[3], 8, "no", 26, 28);
        assert_token(&tokens[4], 9, "name", 29, 33);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let stops = vec![
            "a".to_string(),
            "as".to_string(),
            "am".to_string(),
            "i".to_string(),
        ];
        let a = TextAnalyzer::from(SimpleTokenizer).filter(StopWordFilter::new(stops));
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
