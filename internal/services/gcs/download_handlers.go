package gcs

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, "GET, HEAD")
		return
	}
	bucket, suffix, ok := bucketAndSuffixFromPath(r.URL.EscapedPath(), "/download/storage/v1/b/")
	if !ok || !strings.HasPrefix(suffix, "o/") {
		writeError(w, http.StatusNotFound, "notFound", "not found")
		return
	}
	key, err := url.PathUnescape(strings.TrimPrefix(suffix, "o/"))
	if err != nil || key == "" {
		writeError(w, http.StatusBadRequest, "invalid", "invalid object name")
		return
	}
	s.serveObjectMedia(w, r, bucket, key)
}

func (s *Server) serveObjectMedia(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	object, body, found, err := s.store.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", "bucket not found")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := requestedGenerationMatches(r, object); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, key); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	start, end, partial, err := parseHTTPRange(r.Header.Get("Range"), int64(len(body)))
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "requestedRangeNotSatisfiable", "requested range is not satisfiable")
		return
	}
	payload := body[start : end+1]
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	s.setObjectHeaders(w, object)
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(payload)
}
