/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::memcmp::MemCmpEncoder;
use crate::fts::cangjie::tokenizer::CangJieTokenizer;
use crate::fts::tokenizer::{
    AlphaNumOnlyFilter, AsciiFoldingFilter, BoxTokenFilter, Language, LowerCaser, NgramTokenizer,
    RawTokenizer, RemoveLongFilter, SimpleTokenizer, SplitCompoundWords, Stemmer, StopWordFilter,
    TextAnalyzer, Tokenizer, WhitespaceTokenizer,
};
use crate::DataValue;
use jieba_rs::Jieba;
use miette::{bail, ensure, miette, Result};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use smartstring::{LazyCompact, SmartString};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub(crate) mod ast;
pub(crate) mod cangjie;
pub(crate) mod indexing;
pub(crate) mod tokenizer;

#[derive(Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct FtsIndexManifest {
    pub(crate) base_relation: SmartString<LazyCompact>,
    pub(crate) index_name: SmartString<LazyCompact>,
    pub(crate) extractor: String,
    pub(crate) tokenizer: TokenizerConfig,
    pub(crate) filters: Vec<TokenizerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct TokenizerConfig {
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) args: Vec<DataValue>,
}

impl TokenizerConfig {
    // use sha256::digest;
    pub(crate) fn config_hash(&self, filters: &[Self]) -> impl AsRef<[u8]> {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        let mut args_vec = vec![];
        for arg in &self.args {
            args_vec.encode_datavalue(arg);
        }
        hasher.update(&args_vec);
        for filter in filters {
            hasher.update(filter.name.as_bytes());
            args_vec.clear();
            for arg in &filter.args {
                args_vec.encode_datavalue(arg);
            }
            hasher.update(&args_vec);
        }
        hasher.finalize_fixed()
    }
    pub(crate) fn build(&self, filters: &[Self]) -> Result<TextAnalyzer> {
        let tokenizer = self.construct_tokenizer()?;
        let token_filters = filters
            .iter()
            .map(|filter| filter.construct_token_filter())
            .collect::<Result<Vec<_>>>()?;
        Ok(TextAnalyzer {
            tokenizer,
            token_filters,
        })
    }
    pub(crate) fn construct_tokenizer(&self) -> Result<Box<dyn Tokenizer>> {
        Ok(match &self.name as &str {
            "Raw" => Box::new(RawTokenizer),
            "Simple" => Box::new(SimpleTokenizer),
            "Whitespace" => Box::new(WhitespaceTokenizer),
            "NGram" => {
                let min_gram = self
                    .args
                    .get(0)
                    .unwrap_or(&DataValue::from(1))
                    .get_int()
                    .ok_or_else(|| miette!("First argument `min_gram` must be an integer"))?;
                let max_gram = self
                    .args
                    .get(1)
                    .unwrap_or(&DataValue::from(min_gram))
                    .get_int()
                    .ok_or_else(|| miette!("Second argument `max_gram` must be an integer"))?;
                let prefix_only = self
                    .args
                    .get(2)
                    .unwrap_or(&DataValue::Bool(false))
                    .get_bool()
                    .ok_or_else(|| miette!("Third argument `prefix_only` must be a boolean"))?;
                ensure!(min_gram >= 1, "min_gram must be >= 1");
                ensure!(max_gram >= min_gram, "max_gram must be >= min_gram");
                Box::new(NgramTokenizer::new(
                    min_gram as usize,
                    max_gram as usize,
                    prefix_only,
                ))
            }
            "Cangjie" => {
                let hmm = match self.args.get(1) {
                    None => false,
                    Some(d) => d.get_bool().ok_or_else(|| {
                        miette!("Second argument `use_hmm` to Cangjie must be a boolean")
                    })?,
                };
                let option = match self.args.get(0) {
                    None => cangjie::options::TokenizerOption::Default { hmm },
                    Some(d) => {
                        let s = d.get_str().ok_or_else(|| {
                            miette!("First argument `kind` to Cangjie must be a string")
                        })?;
                        match s {
                            "default" => cangjie::options::TokenizerOption::Default { hmm },
                            "all" => cangjie::options::TokenizerOption::All,
                            "search" => cangjie::options::TokenizerOption::ForSearch { hmm },
                            "unicode" => cangjie::options::TokenizerOption::Unicode,
                            _ => bail!("Unknown Cangjie kind: {}", s),
                        }
                    }
                };
                Box::new(CangJieTokenizer {
                    worker: std::sync::Arc::new(Jieba::new()),
                    option,
                })
            }
            _ => bail!("Unknown tokenizer: {}", self.name),
        })
    }
    pub(crate) fn construct_token_filter(&self) -> Result<BoxTokenFilter> {
        Ok(match &self.name as &str {
            "AlphaNumOnly" => AlphaNumOnlyFilter.into(),
            "AsciiFolding" => AsciiFoldingFilter.into(),
            "LowerCase" | "Lowercase" => LowerCaser.into(),
            "RemoveLong" => RemoveLongFilter::limit(
                self.args
                    .get(0)
                    .ok_or_else(|| miette!("Missing first argument `min_length`"))?
                    .get_int()
                    .ok_or_else(|| miette!("First argument `min_length` must be an integer"))?
                    as usize,
            )
            .into(),
            "SplitCompoundWords" => {
                let mut list_values = Vec::new();
                match self
                    .args
                    .get(0)
                    .ok_or_else(|| miette!("Missing first argument `compound_words_list`"))?
                {
                    DataValue::List(l) => {
                        for v in l {
                            list_values.push(
                                v.get_str()
                                    .ok_or_else(|| {
                                        miette!("First argument `compound_words_list` must be a list of strings")
                                    })?,
                            );
                        }
                    }
                    _ => bail!("First argument `compound_words_list` must be a list of strings"),
                }
                SplitCompoundWords::from_dictionary(list_values)
                    .map_err(|e| miette!("Failed to load dictionary: {}", e))?
                    .into()
            }
            "Stemmer" => {
                let language = match self
                    .args
                    .get(0)
                    .ok_or_else(|| miette!("Missing first argument `language` to Stemmer"))?
                    .get_str()
                    .ok_or_else(|| {
                        miette!("First argument `language` to Stemmer must be a string")
                    })?
                    .to_lowercase()
                    .as_str()
                {
                    "arabic" => Language::Arabic,
                    "danish" => Language::Danish,
                    "dutch" => Language::Dutch,
                    "english" => Language::English,
                    "finnish" => Language::Finnish,
                    "french" => Language::French,
                    "german" => Language::German,
                    "greek" => Language::Greek,
                    "hungarian" => Language::Hungarian,
                    "italian" => Language::Italian,
                    "norwegian" => Language::Norwegian,
                    "portuguese" => Language::Portuguese,
                    "romanian" => Language::Romanian,
                    "russian" => Language::Russian,
                    "spanish" => Language::Spanish,
                    "swedish" => Language::Swedish,
                    "tamil" => Language::Tamil,
                    "turkish" => Language::Turkish,
                    lang => bail!("Unsupported language: {}", lang),
                };
                Stemmer::new(language).into()
            }
            "Stopwords" => {
                match self.args.get(0).ok_or_else(|| {
                    miette!("Filter Stopwords requires language name or a list of stopwords")
                })? {
                    DataValue::Str(name) => StopWordFilter::for_lang(name)?.into(),
                    DataValue::List(l) => {
                        let mut stopwords = Vec::new();
                        for v in l {
                            stopwords.push(
                                v.get_str()
                                    .ok_or_else(|| {
                                        miette!(
                                            "First argument `stopwords` must be a list of strings"
                                        )
                                    })?
                                    .to_string(),
                            );
                        }
                        StopWordFilter::new(stopwords).into()
                    }
                    _ => bail!("Filter Stopwords requires language name or a list of stopwords"),
                }
            }
            _ => bail!("Unknown token filter: {:?}", self.name),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct FtsIndexConfig {
    base_relation: SmartString<LazyCompact>,
    index_name: SmartString<LazyCompact>,
    fts_fields: Vec<SmartString<LazyCompact>>,
    tokenizer: TokenizerConfig,
    filters: Vec<TokenizerConfig>,
}

#[derive(Default)]
pub(crate) struct TokenizerCache {
    pub(crate) named_cache: RwLock<HashMap<SmartString<LazyCompact>, Arc<TextAnalyzer>>>,
    pub(crate) hashed_cache: RwLock<HashMap<Vec<u8>, Arc<TextAnalyzer>>>,
}

impl TokenizerCache {
    pub(crate) fn get(
        &self,
        tokenizer_name: &str,
        tokenizer: &TokenizerConfig,
        filters: &[TokenizerConfig],
    ) -> Result<Arc<TextAnalyzer>> {
        {
            let idx_cache = self.named_cache.read().unwrap();
            if let Some(analyzer) = idx_cache.get(tokenizer_name) {
                return Ok(analyzer.clone());
            }
        }
        let hash = tokenizer.config_hash(filters);
        {
            let hashed_cache = self.hashed_cache.read().unwrap();
            if let Some(analyzer) = hashed_cache.get(hash.as_ref()) {
                let mut idx_cache = self.named_cache.write().unwrap();
                idx_cache.insert(tokenizer_name.into(), analyzer.clone());
                return Ok(analyzer.clone());
            }
        }
        {
            let analyzer = Arc::new(tokenizer.build(filters)?);
            let mut hashed_cache = self.hashed_cache.write().unwrap();
            hashed_cache.insert(hash.as_ref().to_vec(), analyzer.clone());
            let mut idx_cache = self.named_cache.write().unwrap();
            idx_cache.insert(tokenizer_name.into(), analyzer.clone());
            Ok(analyzer)
        }
    }
}
