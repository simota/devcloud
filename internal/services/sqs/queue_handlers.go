package sqs

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	prefix, err := requestString(r, protocol, "QueueNamePrefix")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	urls := s.listQueueURLs(prefix)
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, listQueuesXMLResponse{
			Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: listQueuesXMLResult{
				QueueURLs: urls,
			},
			Meta: responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"QueueUrls": urls})
}

func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseQueueRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	queue, err := s.createQueue(input.QueueName, input.Attributes, input.Tags)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, createQueueXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: queueURLXMLResult{QueueURL: queue.URL},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"QueueUrl": queue.URL})
}

func (s *Server) handleGetQueueURL(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	name, err := requestString(r, protocol, "QueueName")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	queue, ok := s.queueByName(name)
	if !ok {
		writeProtocolError(w, protocol, "QueueDoesNotExist", "queue does not exist", http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, getQueueURLXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: queueURLXMLResult{QueueURL: queue.URL},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"QueueUrl": queue.URL})
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	queueURL, err := requestString(r, protocol, "QueueUrl")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if !s.deleteQueue(queueURL) {
		writeProtocolError(w, protocol, "QueueDoesNotExist", "queue does not exist", http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "DeleteQueue")
}

func (s *Server) handlePurgeQueue(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	queueURL, err := requestString(r, protocol, "QueueUrl")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.purgeQueue(queueURL); err != nil {
		writeProtocolError(w, protocol, "QueueDoesNotExist", err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "PurgeQueue")
}

func (s *Server) createQueue(name string, attrs map[string]string, tags map[string]string) (*queueState, error) {
	if name == "" {
		return nil, errors.New("QueueName is required")
	}
	if !queueNamePattern.MatchString(name) {
		return nil, errors.New("QueueName must contain only alphanumeric characters, hyphens, underscores, and optional .fifo suffix")
	}
	normalized := s.defaultQueueAttributes()
	for key, value := range attrs {
		normalized[key] = value
	}
	if normalized["FifoQueue"] == "true" && !strings.HasSuffix(name, ".fifo") {
		return nil, errors.New("FIFO queues must use a .fifo suffix")
	}
	if strings.HasSuffix(name, ".fifo") && normalized["FifoQueue"] != "true" {
		return nil, errors.New("queues with .fifo suffix must set FifoQueue to true")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.queues[name]; ok {
		if !sameQueueAttributes(existing.Attributes, normalized) {
			return nil, errors.New("queue name exists with different attributes")
		}
		return cloneQueue(existing), nil
	}
	if maxQueues := s.maxQueues(); maxQueues > 0 && len(s.queues) >= maxQueues {
		return nil, errors.New("queue limit exceeded")
	}
	now := time.Now().UTC()
	queue := &queueState{
		Name:       name,
		URL:        s.queueURL(name),
		ARN:        s.queueARN(name),
		Attributes: normalized,
		Tags:       copyAttributes(tags),
		CreatedAt:  now,
		ModifiedAt: now,
		Dedup:      map[string]deduplicationState{},
	}
	if err := s.validateQueueAttributesLocked(queue, attrs); err != nil {
		return nil, err
	}
	s.queues[name] = queue
	if err := s.persistLocked(); err != nil {
		delete(s.queues, name)
		return nil, err
	}
	return cloneQueue(queue), nil
}

func (s *Server) listQueueURLs(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		if prefix == "" || strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	urls := make([]string, 0, len(names))
	for _, name := range names {
		urls = append(urls, s.queues[name].URL)
	}
	return urls
}

func (s *Server) queueByName(name string) (*queueState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return nil, false
	}
	return cloneQueue(queue), true
}

func (s *Server) queueByURL(queueURL string) (*queueState, bool) {
	name := queueNameFromURL(queueURL)
	if name == "" {
		return nil, false
	}
	return s.queueByName(name)
}

func (s *Server) queueFromRequest(r *http.Request, protocol protocolKind) (*queueState, error) {
	queueURL, err := requestString(r, protocol, "QueueUrl")
	if err != nil {
		return nil, err
	}
	queue, ok := s.queueByURL(queueURL)
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	return queue, nil
}

func (s *Server) purgeQueue(queueURL string) error {
	name := queueNameFromURL(queueURL)
	if name == "" {
		return errors.New("queue does not exist")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return errors.New("queue does not exist")
	}
	queue.Messages = nil
	return s.persistLocked()
}

func (s *Server) deleteQueue(queueURL string) bool {
	name := queueNameFromURL(queueURL)
	if name == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return false
	}
	delete(s.queues, name)
	if err := s.persistLocked(); err != nil {
		s.queues[name] = queue
		return false
	}
	return true
}

func (s *Server) queueURL(name string) string {
	host := s.config.QueueURLHost
	if host == "" {
		host = "127.0.0.1"
	}
	if !strings.Contains(host, ":") {
		if _, port, ok := strings.Cut(s.config.Addr, ":"); ok && port != "" {
			host = host + ":" + port
		}
	}
	accountID := defaultString(s.config.AccountID, "000000000000")
	return fmt.Sprintf("http://%s/%s/%s", host, accountID, name)
}

func (s *Server) queueARN(name string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", defaultString(s.config.Region, "us-east-1"), defaultString(s.config.AccountID, "000000000000"), name)
}

func (s *Server) queueByARNLocked(arn string) *queueState {
	for _, queue := range s.queues {
		if queue.ARN == arn {
			return queue
		}
	}
	return nil
}

func queueNameFromURL(queueURL string) string {
	parsed, err := url.Parse(queueURL)
	if err != nil {
		return ""
	}
	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-1]
}
