package sqs

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

func (s *Server) handleTagQueue(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseTagQueueRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.tagQueue(input.QueueURL, input.Tags); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "TagQueue")
}

func (s *Server) handleUntagQueue(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseUntagQueueRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.untagQueue(input.QueueURL, input.TagKeys); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "UntagQueue")
}

func (s *Server) handleListQueueTags(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	queueURL, err := requestString(r, protocol, "QueueUrl")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	tags, err := s.listQueueTags(queueURL)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, listQueueTagsXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: listQueueTagsXMLResult{Tags: tagXMLList(tags)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]map[string]string{"Tags": tags})
}

func (s *Server) handleAddPermission(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseAddPermissionRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.addPermission(input); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "AddPermission")
}

func (s *Server) handleRemovePermission(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseRemovePermissionRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.removePermission(input.QueueURL, input.Label); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "RemovePermission")
}

func (s *Server) tagQueue(queueURL string, tags map[string]string) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if len(tags) == 0 {
		return errors.New("Tags is required")
	}
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
	if queue.Tags == nil {
		queue.Tags = map[string]string{}
	}
	for key, value := range tags {
		if key == "" {
			return errors.New("tag key is required")
		}
		queue.Tags[key] = value
	}
	return s.persistLocked()
}

func (s *Server) untagQueue(queueURL string, tagKeys []string) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if len(tagKeys) == 0 {
		return errors.New("TagKeys is required")
	}
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
	for _, key := range tagKeys {
		delete(queue.Tags, key)
	}
	return s.persistLocked()
}

func (s *Server) listQueueTags(queueURL string) (map[string]string, error) {
	if queueURL == "" {
		return nil, errors.New("QueueUrl is required")
	}
	name := queueNameFromURL(queueURL)
	if name == "" {
		return nil, errors.New("queue does not exist")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	return copyAttributes(queue.Tags), nil
}

func (s *Server) addPermission(input permissionRequest) error {
	if input.QueueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if input.Label == "" {
		return errors.New("Label is required")
	}
	if len(input.AWSAccountIDs) == 0 {
		return errors.New("AWSAccountIds is required")
	}
	if len(input.Actions) == 0 {
		return errors.New("Actions is required")
	}
	name := queueNameFromURL(input.QueueURL)
	if name == "" {
		return errors.New("queue does not exist")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return errors.New("queue does not exist")
	}
	policy, err := queuePolicyFromAttribute(queue)
	if err != nil {
		return err
	}
	statement := queuePolicyStatement{
		Sid:    input.Label,
		Effect: "Allow",
		Principal: queuePolicyPrincipal{
			AWS: append([]string(nil), input.AWSAccountIDs...),
		},
		Action:   normalizedPermissionActions(input.Actions),
		Resource: queue.ARN,
	}
	replaced := false
	for i := range policy.Statement {
		if policy.Statement[i].Sid == input.Label {
			policy.Statement[i] = statement
			replaced = true
			break
		}
	}
	if !replaced {
		policy.Statement = append(policy.Statement, statement)
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	queue.Attributes["Policy"] = string(encoded)
	return s.persistLocked()
}

func (s *Server) removePermission(queueURL string, label string) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if label == "" {
		return errors.New("Label is required")
	}
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
	policy, err := queuePolicyFromAttribute(queue)
	if err != nil {
		return err
	}
	statements := policy.Statement[:0]
	for _, statement := range policy.Statement {
		if statement.Sid != label {
			statements = append(statements, statement)
		}
	}
	policy.Statement = statements
	if len(policy.Statement) == 0 {
		delete(queue.Attributes, "Policy")
		return s.persistLocked()
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	queue.Attributes["Policy"] = string(encoded)
	return s.persistLocked()
}

func queuePolicyFromAttribute(queue *queueState) (queuePolicy, error) {
	raw := strings.TrimSpace(queue.Attributes["Policy"])
	if raw == "" {
		return queuePolicy{
			Version:   "2012-10-17",
			ID:        queue.ARN + "/SQSDefaultPolicy",
			Statement: []queuePolicyStatement{},
		}, nil
	}
	var policy queuePolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return queuePolicy{}, errors.New("Policy must be valid JSON")
	}
	if policy.Version == "" {
		policy.Version = "2012-10-17"
	}
	if policy.ID == "" {
		policy.ID = queue.ARN + "/SQSDefaultPolicy"
	}
	if policy.Statement == nil {
		policy.Statement = []queuePolicyStatement{}
	}
	return policy, nil
}

func normalizedPermissionActions(actions []string) []string {
	normalized := make([]string, 0, len(actions))
	for _, action := range actions {
		if action == "" {
			continue
		}
		if action == "*" || strings.Contains(action, ":") {
			normalized = append(normalized, action)
			continue
		}
		normalized = append(normalized, "SQS:"+action)
	}
	return normalized
}
