pub(crate) trait Op {
    fn is_resolved(&self) -> bool;
    fn name(&self) -> &str;
}

pub(crate) trait AggOp {
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