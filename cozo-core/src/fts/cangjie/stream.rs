use crate::fts::tokenizer::Token;

#[derive(Debug)]
pub(crate) struct CangjieTokenStream<'a> {
    result: Vec<&'a str>,
    // Begin with 1
    index: usize,
    offset_from: usize,
    token: Token,
}

impl<'a> CangjieTokenStream<'a> {
    pub(crate) fn new(result: Vec<&'a str>) -> Self {
        CangjieTokenStream {
            result,
            index: 0,
            offset_from: 0,
            token: Token::default(),
        }
    }
}

impl<'a> crate::fts::tokenizer::TokenStream for CangjieTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if self.index < self.result.len() {
            let current_word = self.result[self.index];
            let offset_to = self.offset_from + current_word.len();

            self.token = Token {
                offset_from: self.offset_from,
                offset_to,
                position: self.index,
                text: current_word.to_string(),
                position_length: self.result.len(),
            };

            self.index += 1;
            self.offset_from = offset_to;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &crate::fts::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut crate::fts::tokenizer::Token {
        &mut self.token
    }
}
