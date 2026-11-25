//! devcloud SSM Parameter Store: `PutParameter`, `GetParameter`, `GetParameters`,
//! `GetParametersByPath`, `GetParameterHistory`, `DeleteParameter`,
//! `DeleteParameters`, `DescribeParameters`, `LabelParameterVersion`,
//! `UnlabelParameterVersion`, `AddTagsToResource`, `RemoveTagsFromResource`,
//! and `ListTagsForResource` over the AWS JSON 1.1 protocol, persisted to
//! `state.json`.

pub mod http;
pub mod server;
pub mod sigv4;

pub use server::{Config, PolicyEvent, PolicyEventKind, Server};
