/// Tokenizer Option
#[derive(Debug, Clone)]
pub(crate) enum TokenizerOption {
    /// Cut the input text, return all possible words
    All,
    /// Cut the input text
    Default {
        /// `hmm`: enable HMM or not
        hmm: bool,
    },

    /// Cut the input text in search mode
    ForSearch {
        /// `hmm`: enable HMM or not
        hmm: bool,
    },
    /// Cut the input text into UTF-8 characters
    Unicode,
}
