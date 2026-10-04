use include_dir::{include_dir, Dir};

use crate::http::Response;

static UI: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets/ui");

pub fn valid_path(path: &str) -> bool {
    if !path.starts_with('/') || path.contains(['\\', '\0']) {
        return false;
    }
    path == "/"
        || path[1..]
            .split('/')
            .all(|s| !s.is_empty() && s != ".." && s != ".")
}

pub fn asset(path: &str) -> Option<&'static [u8]> {
    UI.get_file(path.trim_start_matches('/'))
        .map(|f| f.contents())
}

pub fn serve(path: &str) -> Response {
    if !valid_path(path) {
        return Response::text(404, "Not found");
    }
    let name = if path == "/" { "/index.html" } else { path };
    let bytes = asset(name);
    if let Some(bytes) = bytes {
        let content_type = match name.rsplit('.').next().unwrap_or("") {
            "html" => "text/html; charset=utf-8",
            "js" => "application/javascript; charset=utf-8",
            "css" => "text/css; charset=utf-8",
            "svg" => "image/svg+xml",
            "png" => "image/png",
            "ico" => "image/x-icon",
            "woff2" => "font/woff2",
            "json" => "application/json",
            _ => "application/octet-stream",
        };
        return Response::new(200, content_type, bytes.to_vec());
    }
    if !path.starts_with("/assets/")
        && !path.starts_with("/api/")
        && path.rsplit('/').next().is_some_and(|p| !p.contains('.'))
    {
        if let Some(bytes) = asset("/index.html") {
            return Response::new(200, "text/html; charset=utf-8", bytes.to_vec());
        }
    }
    Response::text(404, "Not found")
}
