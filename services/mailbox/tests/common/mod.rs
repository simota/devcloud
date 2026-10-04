use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use devcloud_mail::http::HttpAuth;
use devcloud_mail::{Envelope, FileBlobStore, FileStore, Message, Service};
use devcloud_mailbox::http::{Request, Response};
use devcloud_mailbox::routes::{dispatch, App};

pub struct Fixture {
    pub app: Arc<App>,
    pub root: PathBuf,
}

impl Fixture {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "mailbox-tests-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let service = Arc::new(Service::new(Arc::new(FileStore::new(
            root.join("mail"),
            Arc::new(FileBlobStore::new(root.join("blobs"))),
        ))));
        let auth = HttpAuth {
            auth_mode: "relaxed".into(),
            username: String::new(),
            password: String::new(),
        };
        Self {
            app: Arc::new(App::new(
                service,
                auth,
                "mailhog.example".into(),
                Vec::new(),
            )),
            root,
        }
    }

    pub fn receive(&self, raw: &[u8]) -> Message {
        self.app
            .service
            .receive_from(
                Envelope {
                    from: "env@a.example".into(),
                    to: vec!["r1@b.example".into(), "r2@b.example".into()],
                },
                "client.example",
                raw,
            )
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub fn request(app: &App, method: &str, target: &str) -> Response {
    let mut req = Request::new(method, target);
    req.headers.insert("host".into(), "127.0.0.1:8025".into());
    dispatch(app, &req)
}

pub fn json(response: &Response) -> serde_json::Value {
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}
