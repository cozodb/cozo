// single engine per db storage
// will be shared among threads



use std::sync::{Arc, RwLock};

pub struct Engine {
    db: (),
    session_handles: Vec<Arc<RwLock<SessionHandle>>>
}

impl Engine {
    pub fn new(_x: String) -> Self {
        todo!()
    }
    pub fn session(&self) -> Session {
        todo!()
    }
}

pub struct Session<'a> {
    engine: &'a Engine,
    stack_depth: i32, // zero or negative
}
// every session has its own column family to play with
// metadata are stored in table 0

pub struct SessionHandle {
    cf_ident: String,
    status: SessionStatus
}

pub enum SessionStatus {
    Prepared,
    Running,
    Completed
}