package dashboard

import (
	"embed"
	"errors"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:assets/react
var reactAssets embed.FS

func (s *Server) handleReactDashboardAssets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, "GET, HEAD")
		return
	}
	if r.URL.Path == "/dashboard" {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
		return
	}

	assetFS, err := fs.Sub(reactAssets, "assets/react")
	if err != nil {
		http.Error(w, "dashboard assets unavailable", http.StatusInternalServerError)
		return
	}

	assetPath := strings.TrimPrefix(r.URL.Path, "/dashboard/")
	if assetPath == "" {
		serveReactDashboardIndex(w, r, assetFS)
		return
	}

	file, err := assetFS.Open(assetPath)
	if err == nil {
		file.Close()
		if strings.HasPrefix(assetPath, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		http.StripPrefix("/dashboard/", http.FileServer(http.FS(assetFS))).ServeHTTP(w, r)
		return
	}
	if !errors.Is(err, fs.ErrNotExist) {
		http.Error(w, "dashboard asset unavailable", http.StatusInternalServerError)
		return
	}
	if strings.HasPrefix(assetPath, "assets/") {
		http.NotFound(w, r)
		return
	}

	serveReactDashboardIndex(w, r, assetFS)
}

func serveReactDashboardIndex(w http.ResponseWriter, r *http.Request, assetFS fs.FS) {
	index, err := fs.ReadFile(assetFS, "index.html")
	if err != nil {
		http.Error(w, "dashboard index unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}
	w.Write(index)
}

