use std::fmt::{Debug, Formatter};

pub(crate) trait Op: Send + Sync {
    fn is_resolved(&self) -> bool;
    fn name(&self) -> &str;
}

pub(crate) trait AggOp: Send + Sync {
    fn is_resolved(&self) -> bool;
    fn name(&self) -> &str;
}

pub(crate) struct UnresolvedOp(pub String);

impl Op for UnresolvedOp {
    fn is_resolved(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        &self.0
    }
}

impl AggOp for UnresolvedOp {
    fn is_resolved(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        &self.0
    }
}

pub(crate) struct OpAdd;

impl Op for OpAdd {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "+"
    }
}

pub(crate) struct OpSub;

impl Op for OpSub {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "-"
    }
}

pub(crate) struct OpMul;

impl Op for OpMul {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "*"
    }
}

pub(crate) struct OpDiv;

impl Op for OpDiv {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "/"
    }
}

pub(crate) struct OpStrCat;

impl Op for OpStrCat {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "++"
    }
}

pub(crate) struct OpEq;

impl Op for OpEq {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "=="
    }
}

pub(crate) struct OpNe;

impl Op for OpNe {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "!="
    }
}

pub(crate) struct OpOr;

impl Op for OpOr {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "||"
    }
}

pub(crate) struct OpAnd;

impl Op for OpAnd {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "&&"
    }
}

pub(crate) struct OpMod;

impl Op for OpMod {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "%"
    }
}

pub(crate) struct OpGt;

impl Op for OpGt {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        ">"
    }
}

pub(crate) struct OpGe;

impl Op for OpGe {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        ">="
    }
}

pub(crate) struct OpLt;

impl Op for OpLt {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "<"
    }
}

pub(crate) struct OpLe;

impl Op for OpLe {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "<="
    }
}

pub(crate) struct OpPow;

impl Op for OpPow {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "**"
    }
}

pub(crate) struct OpCoalesce;

impl Op for OpCoalesce {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "~~"
    }
}

pub(crate) struct OpNegate;

impl Op for OpNegate {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "!"
    }
}

pub(crate) struct OpMinus;

impl Op for OpMinus {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "--"
    }
}

pub(crate) struct OpIsNull;

impl Op for OpIsNull {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "is_null"
    }
}

pub(crate) struct OpNotNull;

impl Op for OpNotNull {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "not_null"
    }
}

pub(crate) struct OpConcat;

impl Op for OpConcat {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "concat"
    }
}

pub(crate) struct OpMerge;

impl Op for OpMerge {
    fn is_resolved(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "merge"
    }
}
