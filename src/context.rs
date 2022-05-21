use crate::data::eval::PartialEvalContext;
use crate::data::expr::Expr;
use crate::runtime::session::{Session, SessionDefinable};
use cozorocks::TransactionPtr;

pub(crate) struct MainDbContext<'a> {
    pub(crate) sess: &'a Session,
    pub(crate) txn: TransactionPtr,
}

pub(crate) struct TempDbContext<'a> {
    pub(crate) sess: &'a mut Session,
    pub(crate) txn: TransactionPtr,
}

impl<'a> PartialEvalContext for TempDbContext<'a> {
    fn resolve(&self, key: &str) -> Option<Expr> {
        if key.starts_with('$') {
            self.sess.params.get(key).cloned().map(Expr::Const)
        } else {
            for frame in &self.sess.stack {
                match frame.get(key) {
                    None => {}
                    Some(definable) => {
                        return match definable {
                            SessionDefinable::Value(v) => Some(Expr::Const(v.clone())),
                            SessionDefinable::Expr(expr) => Some(expr.clone()),
                            SessionDefinable::Typing(_) => None,
                            SessionDefinable::Table(_) => None,
                        };
                    }
                }
            }
            None
        }
    }
}

impl Session {
    pub(crate) fn main_ctx(&self) -> MainDbContext {
        let txn = self.txn(None);
        txn.set_snapshot();
        MainDbContext { sess: self, txn }
    }
    pub(crate) fn temp_ctx(&mut self) -> TempDbContext {
        let txn = self.txn(None);
        TempDbContext { sess: self, txn }
    }
}
