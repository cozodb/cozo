#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

pub use data::encode::EncodedVec;
pub use preprocess::attr::AttrTxItem;
pub use runtime::db::Db;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub(crate) mod data;
pub(crate) mod runtime;
pub(crate) mod transact;
pub(crate) mod utils;
pub(crate) mod preprocess;

