use crate::data::op::Op;
use crate::data::value::Value;

pub(crate) struct OpGenUuidV1;

const NAME_OP_GEN_UUID_V1: &str = "gen_uuid_v1";

impl Op for OpGenUuidV1 {
    fn arity(&self) -> Option<usize> {
        Some(0)
    }

    fn has_side_effect(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        NAME_OP_GEN_UUID_V1
    }

    fn non_null_args(&self) -> bool {
        true
    }

    fn eval<'a>(&self, _args: Vec<Value<'a>>) -> crate::data::op::Result<Value<'a>> {
        todo!()
    }
}
