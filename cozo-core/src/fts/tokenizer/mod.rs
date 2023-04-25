/*
 * Code under this module is adapted from the Tantivy project
 * https://github.com/quickwit-oss/tantivy/tree/0.19.2/src/tokenizer
 * All code here are licensed under the MIT license, as in the original project.
 */

//! Tokenizer are in charge of chopping text into a stream of tokens
//! ready for indexing.
//!
//! You must define in your schema which tokenizer should be used for
//! each of your fields :
//!
//! ```text
//! use tantivy::schema::*;
//!
//! let mut schema_builder = Schema::builder();
//!
//! let text_options = TextOptions::default()
//!     .set_indexing_options(
//!         TextFieldIndexing::default()
//!             .set_tokenizer("en_stem")
//!             .set_index_option(IndexRecordOption::Basic)
//!     )
//!     .set_stored();
//!
//! let id_options = TextOptions::default()
//!     .set_indexing_options(
//!         TextFieldIndexing::default()
//!             .set_tokenizer("raw_ids")
//!             .set_index_option(IndexRecordOption::WithFreqsAndPositions)
//!     )
//!     .set_stored();
//!
//! schema_builder.add_text_field("title", text_options.clone());
//! schema_builder.add_text_field("text", text_options);
//! schema_builder.add_text_field("uuid", id_options);
//!
//! let schema = schema_builder.build();
//! ```
//!
//! By default, `tantivy` offers the following tokenizers:
//!
//! ## `default`
//!
//! `default` is the tokenizer that will be used if you do not
//! assign a specific tokenizer to your text field.
//! It will chop your text on punctuation and whitespaces,
//! removes tokens that are longer than 40 chars, and lowercase your text.
//!
//! ## `raw`
//! Does not actual tokenizer your text. It keeps it entirely unprocessed.
//! It can be useful to index uuids, or urls for instance.
//!
//! ## `en_stem`
//!
//! In addition to what `default` does, the `en_stem` tokenizer also
//! apply stemming to your tokens. Stemming consists in trimming words to
//! remove their inflection. This tokenizer is slower than the default one,
//! but is recommended to improve recall.
//!
//!
//! # Custom tokenizers
//!
//! You can write your own tokenizer by implementing the [`Tokenizer`] trait
//! or you can extend an existing [`Tokenizer`] by chaining it with several
//! [`TokenFilter`]s.
//!
//! For instance, the `en_stem` is defined as follows.
//!
//! ```text
//! use tantivy::tokenizer::*;
//!
//! let en_stem = TextAnalyzer::from(SimpleTokenizer)
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser)
//!     .filter(Stemmer::new(Language::English));
//! ```
//!
//! Once your tokenizer is defined, you need to
//! register it with a name in your index's [`TokenizerManager`].
//!
//! ```text
//! # use tantivy::schema::Schema;
//! # use tantivy::tokenizer::*;
//! # use tantivy::Index;
//! #
//! let custom_en_tokenizer = SimpleTokenizer;
//! # let schema = Schema::builder().build();
//! let index = Index::create_in_ram(schema);
//! index.tokenizers()
//!      .register("custom_en", custom_en_tokenizer);
//! ```
//!
//! If you built your schema programmatically, a complete example
//! could like this for instance.
//!
//! Note that tokens with a len greater or equal to
//! [`MAX_TOKEN_LEN`].
//!
//! # Example
//!
//! ```text
//! use tantivy::schema::{Schema, IndexRecordOption, TextOptions, TextFieldIndexing};
//! use tantivy::tokenizer::*;
//! use tantivy::Index;
//!
//! let mut schema_builder = Schema::builder();
//! let text_field_indexing = TextFieldIndexing::default()
//!     .set_tokenizer("custom_en")
//!     .set_index_option(IndexRecordOption::WithFreqsAndPositions);
//! let text_options = TextOptions::default()
//!     .set_indexing_options(text_field_indexing)
//!     .set_stored();
//! schema_builder.add_text_field("title", text_options);
//! let schema = schema_builder.build();
//! let index = Index::create_in_ram(schema);
//!
//! // We need to register our tokenizer :
//! let custom_en_tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser);
//! index
//!     .tokenizers()
//!     .register("custom_en", custom_en_tokenizer);
//! ```
mod alphanum_only;
mod ascii_folding_filter;
mod empty_tokenizer;
mod lower_caser;
mod ngram_tokenizer;
mod raw_tokenizer;
mod remove_long;
mod simple_tokenizer;
mod split_compound_words;
mod stemmer;
mod stop_word_filter;
mod tokenized_string;
mod tokenizer_impl;
mod whitespace_tokenizer;

pub(crate) use self::alphanum_only::AlphaNumOnlyFilter;
pub(crate) use self::ascii_folding_filter::AsciiFoldingFilter;
pub(crate) use self::lower_caser::LowerCaser;
pub(crate) use self::ngram_tokenizer::NgramTokenizer;
pub(crate) use self::raw_tokenizer::RawTokenizer;
pub(crate) use self::remove_long::RemoveLongFilter;
pub(crate) use self::simple_tokenizer::SimpleTokenizer;
pub(crate) use self::split_compound_words::SplitCompoundWords;
pub(crate) use self::stemmer::{Language, Stemmer};
pub(crate) use self::stop_word_filter::StopWordFilter;
// pub(crate) use self::tokenized_string::{PreTokenizedStream, PreTokenizedString};
pub(crate) use self::tokenizer_impl::{
    BoxTokenFilter, BoxTokenStream, TextAnalyzer, Token, TokenFilter, TokenStream, Tokenizer,
};
pub(crate) use self::whitespace_tokenizer::WhitespaceTokenizer;

#[cfg(test)]
pub(crate) mod tests {
    // use super::{
    //     Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, Token,
    // };
    // use crate::fts::tokenizer::TextAnalyzer;

    use crate::fts::tokenizer::Token;

    /// This is a function that can be used in tests and doc tests
    /// to assert a token's correctness.
    pub(crate) fn assert_token(token: &Token, position: usize, text: &str, from: usize, to: usize) {
        assert_eq!(
            token.position, position,
            "expected position {} but {:?}",
            position, token
        );
        assert_eq!(token.text, text, "expected text {} but {:?}", text, token);
        assert_eq!(
            token.offset_from, from,
            "expected offset_from {} but {:?}",
            from, token
        );
        assert_eq!(
            token.offset_to, to,
            "expected offset_to {} but {:?}",
            to, token
        );
    }
}
