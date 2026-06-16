//! Embedded React SPA serving, replicating the legacy `internal/dashboard/assets.rs`
//! behavior EXACTLY:
//!   - `/dashboard` (bare)             -> 301 to `/dashboard/`
//!   - `/dashboard/`                   -> `index.html`, `Cache-Control: no-cache`
//!   - `/dashboard/assets/<hashed>`    -> file with
//!                                        `Cache-Control: public, max-age=31536000, immutable`
//!   - `/dashboard/<existing-file>`    -> the file (other assets)
//!   - `/dashboard/<client-route>`     -> `index.html` fallback (no-cache)
//!   - `/dashboard/assets/<missing>`   -> 404 (never falls back to index)
//!
//! The bundle is embedded at compile time via `include_dir!` from this crate's
//! Vite output directory (`assets/react/`).

use include_dir::{include_dir, Dir};

use crate::http::{Request, Response};

/// The built React SPA, embedded from the dashboard crate's asset directory.
static REACT_ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets/react");

/// The plain service-index HTML served at `/` — byte-identical to legacy
/// `serviceIndexHTML` in `internal/dashboard/static.rs`.
pub const SERVICE_INDEX_HTML: &str = include_str!("service_index.html");

const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";

/// Serves a `/dashboard` or `/dashboard/...` request. Mirrors legacy
/// `handleReactDashboardAssets`.
pub fn serve(req: &Request) -> Response {
    if req.method != "GET" && req.method != "HEAD" {
        return Response::method_not_allowed("GET, HEAD");
    }
    if req.path == "/dashboard" {
        return Response::redirect(301, "/dashboard/");
    }

    // Path relative to the embedded root, e.g. "assets/index-CTX.js" or "" for
    // the dashboard root. legacy strips "/dashboard/" then resolves against the
    // embedded FS; we do the same.
    let asset_path = req.path.trim_start_matches("/dashboard/");
    if asset_path.is_empty() {
        return serve_index(req);
    }

    if let Some(file) = REACT_ASSETS.get_file(asset_path) {
        let mut resp = Response::new(
            200,
            content_type_for(asset_path),
            file_bytes(req, file.contents()),
        );
        // Hashed build assets live under "assets/" and are content-addressed, so
        // they are safe to cache immutably — exactly legacy branch.
        if asset_path.starts_with("assets/") {
            resp = resp.header("Cache-Control", IMMUTABLE_CACHE);
        }
        return resp;
    }

    // Missing file: a request under "assets/" is a hard 404 (legacy never falls back
    // there); anything else is a client-side route → serve index.html.
    if asset_path.starts_with("assets/") {
        return Response::text_error(404, "404 page not found");
    }
    serve_index(req)
}

fn serve_index(req: &Request) -> Response {
    let Some(index) = REACT_ASSETS.get_file("index.html") else {
        return Response::text_error(500, "dashboard index unavailable");
    };
    Response::new(
        200,
        "text/html; charset=utf-8",
        file_bytes(req, index.contents()),
    )
    .header("Cache-Control", "no-cache")
}

/// HEAD must emit headers + Content-Length but no body, mirroring legacy
/// `serveReactDashboardIndex` HEAD short-circuit and `http.FileServer`.
fn file_bytes(req: &Request, contents: &[u8]) -> Vec<u8> {
    if req.method == "HEAD" {
        Vec::new()
    } else {
        contents.to_vec()
    }
}

/// Content-Type by extension, covering the file kinds a Vite SPA emits. Matches
/// what legacy `http.FileServer` (via `mime.TypeByExtension`) returns for these.
fn content_type_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "map" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}
