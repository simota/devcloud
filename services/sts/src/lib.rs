//! devcloud STS: `AssumeRole` and `GetCallerIdentity` over the AWS Query
//! protocol. In `strict` mode, requests signed with issued temporary
//! credentials are accepted and limited by their session policy.

pub mod http;
pub mod policy;
pub mod server;
pub mod sigv4;

pub use server::{Config, Server, Session};
