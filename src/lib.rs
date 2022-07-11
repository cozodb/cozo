#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(test)]
mod tests;

pub(crate) mod data;
pub(crate) mod runtime;
pub(crate) mod transact;
pub(crate) mod utils;

pub use runtime::instance::Db;
pub use data::tx_attr::AttrTxItem;
