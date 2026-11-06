//! devcloud SSM Parameter Store: `PutParameter`, `GetParameter` (including
//! `SecureString` with `WithDecryption`), `DeleteParameter`, and
//! `DescribeParameters` over the AWS JSON 1.1 protocol, persisted to
//! `state.json`.

pub mod http;
pub mod server;
pub mod sigv4;

pub use server::{Config, Server};
