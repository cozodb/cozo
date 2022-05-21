use crate::data::value::StaticValue;

pub(crate) struct ValueSet {
    values: Vec<StaticValue>,
    default_table: Option<String>,
}
