#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub(crate) mod data;
pub(crate) mod runtime;
#[cfg(test)]
mod tests;

pub use runtime::instance::Db;