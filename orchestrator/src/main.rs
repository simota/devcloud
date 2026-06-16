//! devcloud — native Rust orchestrator (single binary).
//!
//! Replaces the legacy `orchestrator` CLI and `internal/app` daemon. All service
//! engines are linked in as libraries and run as tokio tasks inside this one
//! process; there are no subprocesses (except the genuinely external
//! `redis-server`) and no `DEVCLOUD_<SVC>_ENGINE` seams.
//!
//! Subcommands mirror the legacy CLI: `init`, `up [service ...]`, `reset`, `dashboard`.

// Config ported from legacy `internal/app/config.rs`. Many fields are read only once
// their service is wired into the supervisor, so the module is dead-code-allowed
// to keep the crate warning-clean during incremental wiring (Phase 1).
// TODO(agent): drop this allow once all services are wired (task #5).
#[allow(dead_code)]
mod config;
mod services;
mod supervisor;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("devcloud: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str).unwrap_or("") {
        "init" => {
            let cfg = config::default_config();
            config::init_workspace(&cfg).map_err(|e| e.to_string())
        }
        "up" => {
            let cfg = config::load_config(".devcloud/config.yaml").map_err(|e| e.to_string())?;
            let cfg =
                config::apply_service_selection(&cfg, &args[1..]).map_err(|e| e.to_string())?;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("build tokio runtime: {e}"))?;
            rt.block_on(supervisor::run(cfg))
        }
        "reset" => {
            let cfg = config::load_config(".devcloud/config.yaml").map_err(|e| e.to_string())?;
            config::reset_workspace(&cfg).map_err(|e| e.to_string())
        }
        "dashboard" => {
            let cfg = config::load_config(".devcloud/config.yaml").map_err(|e| e.to_string())?;
            println!("Dashboard: http://localhost:{}", cfg.server.dashboard_port);
            Ok(())
        }
        "help" | "-h" | "--help" | "" => {
            print!("{}", usage_text());
            Ok(())
        }
        other => Err(format!("unknown command {other:?}\n\n{}", usage_text())),
    }
}

fn usage_text() -> String {
    format!(
        "Usage:\n  \
devcloud init\n  \
devcloud up [service ...]\n  \
devcloud reset\n  \
devcloud dashboard\n\n\
When one or more service names are passed to \"up\", only those services are\n\
started (overriding services.*.enabled in .devcloud/config.yaml). The\n\
dashboard always starts.\n\n\
Known services: {}\n",
        config::service_names().join(", ")
    )
}
