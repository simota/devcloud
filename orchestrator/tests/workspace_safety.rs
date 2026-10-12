//! Exercise reset through the CLI so filesystem validation runs against an
//! isolated workspace without changing the test process's current directory.

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-reset-safety-{}-{}",
            std::process::id(),
            NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join(".devcloud/data")).unwrap();
        fs::create_dir_all(root.join("outside/target")).unwrap();
        fs::write(root.join(".devcloud/data/keep"), b"stored data").unwrap();
        fs::write(root.join("outside/target/keep"), b"unrelated data").unwrap();
        Self(root)
    }

    fn run(&self, command: &str) -> Output {
        Command::new(env!("CARGO_BIN_EXE_devcloud"))
            .arg(command)
            .current_dir(&self.0)
            .output()
            .unwrap()
    }

    fn assert_rejected_without_changes(&self, config: &str) {
        fs::write(self.0.join(".devcloud/config.yaml"), config).unwrap();
        let result = self.run("reset");
        assert!(
            !result.status.success(),
            "reset accepted unsafe configuration: {config}"
        );
        assert_eq!(
            fs::read(self.0.join("outside/target/keep")).unwrap(),
            b"unrelated data"
        );
        assert_eq!(
            fs::read(self.0.join(".devcloud/data/keep")).unwrap(),
            b"stored data",
            "every configured path must be validated before the first deletion"
        );
        assert_eq!(
            fs::read_to_string(self.0.join(".devcloud/config.yaml")).unwrap(),
            config
        );
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn reset_rejects_parent_traversal_in_every_storage_setting() {
    for config in [
        "storage:\n  path: .devcloud/../outside/target\n",
        "services:\n  pubsub:\n    dataDir: .devcloud/../outside/target\n",
        "services:\n  pubsub:\n    messageDataDir: .devcloud/../outside/target\n",
        "services:\n  redshift:\n    dataDir: ../../outside/target\n",
        "services:\n  redis:\n    dataDir: ../../outside/target\n",
    ] {
        Workspace::new().assert_rejected_without_changes(config);
    }
}

#[cfg(unix)]
#[test]
fn reset_rejects_intermediate_symlinks_before_deleting_any_storage() {
    for config in [
        "storage:\n  path: .devcloud/link/target\n",
        "services:\n  pubsub:\n    dataDir: .devcloud/link/target\n",
        "services:\n  pubsub:\n    messageDataDir: .devcloud/link/target\n",
        "services:\n  redshift:\n    dataDir: .devcloud/link/target\n",
        "services:\n  redis:\n    dataDir: .devcloud/link/target\n",
    ] {
        let workspace = Workspace::new();
        std::os::unix::fs::symlink("../outside", workspace.0.join(".devcloud/link")).unwrap();
        workspace.assert_rejected_without_changes(config);
    }
}

#[cfg(unix)]
#[test]
fn init_rejects_a_symlinked_workspace_root() {
    let workspace = Workspace::new();
    fs::remove_dir_all(workspace.0.join(".devcloud")).unwrap();
    std::os::unix::fs::symlink("outside", workspace.0.join(".devcloud")).unwrap();
    let result = workspace.run("init");
    assert!(!result.status.success());
    assert!(!workspace.0.join("outside/data").exists());
    assert!(!workspace.0.join("outside/config.yaml").exists());
    assert_eq!(
        fs::read(workspace.0.join("outside/target/keep")).unwrap(),
        b"unrelated data"
    );
}

#[cfg(unix)]
#[test]
fn init_does_not_write_through_a_symlinked_service_directory() {
    let workspace = Workspace::new();
    std::os::unix::fs::symlink(
        "../../outside/target",
        workspace.0.join(".devcloud/data/mail"),
    )
    .unwrap();
    let result = workspace.run("init");
    assert!(!result.status.success());
    assert!(!workspace.0.join("outside/target/index.json").exists());
    assert_eq!(
        fs::read(workspace.0.join("outside/target/keep")).unwrap(),
        b"unrelated data"
    );
}

#[cfg(unix)]
#[test]
fn init_does_not_write_through_a_dangling_state_file_symlink() {
    let workspace = Workspace::new();
    fs::create_dir_all(workspace.0.join(".devcloud/data/mail")).unwrap();
    std::os::unix::fs::symlink(
        "../../../outside/target/index.json",
        workspace.0.join(".devcloud/data/mail/index.json"),
    )
    .unwrap();
    let result = workspace.run("init");
    assert!(!result.status.success());
    assert!(!workspace.0.join("outside/target/index.json").exists());
    assert_eq!(
        fs::read(workspace.0.join("outside/target/keep")).unwrap(),
        b"unrelated data"
    );
}

#[test]
fn reset_recreates_valid_storage_and_preserves_configuration() {
    let workspace = Workspace::new();
    let config = "storage:\n  path: ./.devcloud//data\n";
    fs::write(workspace.0.join(".devcloud/config.yaml"), config).unwrap();
    let result = workspace.run("reset");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!workspace.0.join(".devcloud/data/keep").exists());
    assert!(workspace.0.join(".devcloud/data/s3/buckets").is_dir());
    assert!(workspace.0.join(".devcloud/data/mail/index.json").is_file());
    assert_eq!(
        fs::read_to_string(workspace.0.join(".devcloud/config.yaml")).unwrap(),
        config
    );
    assert_eq!(
        fs::read(workspace.0.join("outside/target/keep")).unwrap(),
        b"unrelated data"
    );
}
