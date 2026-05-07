package gcs

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) checkObjectPreconditions(r *http.Request, bucket string, key string) (bool, error) {
	return s.checkStoredObjectPreconditions(r.Context(), bucket, key, preconditionsFromRequest(r))
}

func preconditionsFromRequest(r *http.Request) objectPreconditions {
	query := r.URL.Query()
	return objectPreconditions{
		IfGenerationMatch:        query.Get("ifGenerationMatch"),
		IfGenerationNotMatch:     query.Get("ifGenerationNotMatch"),
		IfMetagenerationMatch:    query.Get("ifMetagenerationMatch"),
		IfMetagenerationNotMatch: query.Get("ifMetagenerationNotMatch"),
	}
}

func sourcePreconditionsFromRequest(r *http.Request) objectPreconditions {
	query := r.URL.Query()
	return objectPreconditions{
		IfGenerationMatch:        query.Get("ifSourceGenerationMatch"),
		IfGenerationNotMatch:     query.Get("ifSourceGenerationNotMatch"),
		IfMetagenerationMatch:    query.Get("ifSourceMetagenerationMatch"),
		IfMetagenerationNotMatch: query.Get("ifSourceMetagenerationNotMatch"),
	}
}

func (s *Server) checkStoredObjectPreconditions(ctx context.Context, bucket string, key string, preconditions objectPreconditions) (bool, error) {
	object, _, found, err := s.store.GetObject(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	generation := int64(0)
	metageneration := int64(0)
	if found {
		generation = object.LastModified.UTC().UnixNano()
		metageneration = object.Metageneration
		if metageneration < 1 {
			metageneration = 1
		}
	}
	if match := preconditions.IfGenerationMatch; match != "" {
		value, err := strconv.ParseInt(match, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifGenerationMatch")
		}
		if value != generation {
			return false, nil
		}
	}
	if notMatch := preconditions.IfGenerationNotMatch; notMatch != "" {
		value, err := strconv.ParseInt(notMatch, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifGenerationNotMatch")
		}
		if value == generation {
			return false, nil
		}
	}
	if match := preconditions.IfMetagenerationMatch; match != "" {
		value, err := strconv.ParseInt(match, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifMetagenerationMatch")
		}
		if value != metageneration {
			return false, nil
		}
	}
	if notMatch := preconditions.IfMetagenerationNotMatch; notMatch != "" {
		value, err := strconv.ParseInt(notMatch, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifMetagenerationNotMatch")
		}
		if value == metageneration {
			return false, nil
		}
	}
	return true, nil
}

func writePreconditionError(w http.ResponseWriter, err error) {
	if strings.HasPrefix(err.Error(), "invalid if") {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeError(w, http.StatusNotFound, "notFound", err.Error())
}
