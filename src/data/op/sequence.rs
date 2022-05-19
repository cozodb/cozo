use crate::data::op::Op;
use crate::data::value::Value;

pub(crate) struct SeqNext;

const NAME_OP_SEQ_NEXT: &str = "seq_next";

impl Op for SeqNext {
    fn arity(&self) -> Option<usize> {
        Some(1)
    }

    fn has_side_effect(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        NAME_OP_SEQ_NEXT
    }

    fn non_null_args(&self) -> bool {
        true
    }

    fn eval<'a>(&self, args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        todo!()
    }
}