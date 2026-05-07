package gcs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type resumableContentRange struct {
	start int64
	end   int64
	total int64
}

func parseResumableContentRange(header string, payloadSize int64) (resumableContentRange, error) {
	if !strings.HasPrefix(header, "bytes ") {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	span, totalValue, ok := strings.Cut(strings.TrimPrefix(header, "bytes "), "/")
	if !ok || totalValue == "*" {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	left, right, ok := strings.Cut(span, "-")
	if !ok {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	end, err := strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	total, err := strconv.ParseInt(totalValue, 10, 64)
	if err != nil || total <= 0 || end >= total {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	if got, want := payloadSize, end-start+1; got != want {
		return resumableContentRange{}, fmt.Errorf("Content-Range does not match payload size")
	}
	return resumableContentRange{start: start, end: end, total: total}, nil
}

func isResumableStatusQuery(contentRange string) bool {
	contentRange = strings.TrimSpace(contentRange)
	return strings.HasPrefix(contentRange, "bytes */")
}

func newUploadID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (s *Server) loadResumableSessions() {
	root := s.config.UploadSessionPath
	if root == "" {
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		data, err := os.ReadFile(filepath.Join(root, id, "session.json"))
		if err != nil {
			continue
		}
		var session resumableSession
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}
		s.sessions[id] = session
	}
}

func (s *Server) saveResumableSession(id string, session resumableSession) error {
	root := s.config.UploadSessionPath
	if root == "" {
		return nil
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create upload session: %w", err)
	}
	data, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return fmt.Errorf("encode upload session: %w", err)
	}
	return os.WriteFile(filepath.Join(dir, "session.json"), append(data, '\n'), 0o644)
}

func (s *Server) appendResumableChunk(id string, payload []byte) error {
	root := s.config.UploadSessionPath
	if root == "" {
		return fmt.Errorf("upload session storage is not configured")
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create upload session: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "body.part"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open upload body: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(payload); err != nil {
		return fmt.Errorf("write upload body: %w", err)
	}
	return nil
}

func (s *Server) readResumableBody(id string) ([]byte, error) {
	if s.config.UploadSessionPath == "" {
		return nil, fmt.Errorf("upload session storage is not configured")
	}
	return os.ReadFile(filepath.Join(s.config.UploadSessionPath, id, "body.part"))
}

func (s *Server) deleteResumableSession(id string) error {
	if s.config.UploadSessionPath == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(s.config.UploadSessionPath, id))
}
