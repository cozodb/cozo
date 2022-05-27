use crate::algebra::parser::RaBox;
use crate::ddl::reify::{EdgeInfo, NodeInfo};

pub(crate) const NAME_NODE_HOP: &str = "NodeHop";

pub(crate) struct NodeToNodeHop<'a> {
    source: RaBox<'a>,
    edge_info: EdgeInfo,
    target_info: NodeInfo,
    edge_binding: String,
    target_binding: String,
    left_outer: bool,
    right_outer: bool,
}

pub(crate) struct EdgeToEdgeHop<'a> {
    source: RaBox<'a>,
    target_info: EdgeInfo,
    target_binding: String,
}
