//! devcloud STS: `AssumeRole` (inline session policies accepted, not
//! evaluated) and `GetCallerIdentity` over the AWS Query protocol. Stateless.

pub mod http;
pub mod server;
pub mod sigv4;

pub use server::{Config, Server};
