
#[derive(PartialEq, Debug)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    Neg,
    Minus,
    Mod,
    Or,
    And,
    Coalesce,
    Pow,
    Call,
    IsNull,
    NotNull
}
