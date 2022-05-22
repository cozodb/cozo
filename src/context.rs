use crate::data::eval::PartialEvalContext;
use crate::data::expr::Expr;
use crate::runtime::session::{Definable, Session};
use cozorocks::TransactionPtr;

pub(crate) struct MainDbContext<'a> {
    pub(crate) sess: &'a Session,
    pub(crate) txn: TransactionPtr,
}

pub(crate) struct TempDbContext<'a> {
    pub(crate) sess: &'a mut Session,
    pub(crate) txn: TransactionPtr,
    pub(crate) writable_main: bool,
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
                            Definable::Value(v) => Some(Expr::Const(v.clone())),
                            Definable::Expr(expr) => Some(expr.clone()),
                            Definable::Table(_) => None,
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
    pub(crate) fn temp_ctx(&mut self, writable_main: bool) -> TempDbContext {
        let txn = self.txn(None);
        TempDbContext {
            sess: self,
            txn,
            writable_main,
        }
    }
}
