pub mod application;
pub mod detect;
pub mod bench;
pub mod market;
pub mod delivery;
pub mod model;
pub mod node;
pub mod performance;
pub mod projection;
pub mod store;
pub mod store_application;
pub mod store_delivery;
pub mod sync;
mod writer;

pub mod tokens;

pub use application::*;
pub use delivery::*;
pub use model::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("connect error: {0}")]
    Connect(String),
    #[error("node mismatch: {0}")]
    NodeMismatch(String),
    #[error("invalid {what}: {value}")]
    Invalid { what: &'static str, value: String },
}

pub type Result<T> = std::result::Result<T, Error>;
