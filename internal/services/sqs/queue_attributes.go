package sqs

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) handleGetQueueAttributes(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseGetQueueAttributesRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	attrs, err := s.getQueueAttributes(input.QueueURL, input.AttributeNames)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, getQueueAttributesXMLResponse{
			Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: getQueueAttributesXMLResult{
				Attributes: attributeXMLList(attrs),
			},
			Meta: responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]map[string]string{"Attributes": attrs})
}

func (s *Server) handleSetQueueAttributes(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseQueueRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.updateQueueAttributes(input.QueueURL, input.Attributes); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "SetQueueAttributes")
}

func (s *Server) getQueueAttributes(queueURL string, names []string) (map[string]string, error) {
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
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	attrs := queueAttributesWithComputedLocked(queue, now)
	return filterQueueAttributes(attrs, names)
}

func (s *Server) updateQueueAttributes(queueURL string, attrs map[string]string) (*queueState, error) {
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
	if err := s.validateQueueAttributesLocked(queue, attrs); err != nil {
		return nil, err
	}
	for key, value := range attrs {
		queue.Attributes[key] = value
	}
	queue.ModifiedAt = time.Now().UTC()
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return cloneQueue(queue), nil
}

func (s *Server) validateQueueAttributesLocked(queue *queueState, attrs map[string]string) error {
	for name := range attrs {
		if !isSettableQueueAttribute(name) {
			return fmt.Errorf("unknown queue attribute name %q", name)
		}
	}
	if err := validateQueueAttributeValues(attrs); err != nil {
		return err
	}
	if allowPolicyRaw := attrs["RedriveAllowPolicy"]; allowPolicyRaw != "" {
		if _, err := parseRedriveAllowPolicy(allowPolicyRaw); err != nil {
			return err
		}
	}
	policyRaw := attrs["RedrivePolicy"]
	if policyRaw == "" {
		return nil
	}
	policy, err := parseRedrivePolicy(policyRaw)
	if err != nil {
		return err
	}
	if policy.MaxReceiveCount < 1 {
		return errors.New("RedrivePolicy maxReceiveCount must be greater than zero")
	}
	dlq := s.queueByARNLocked(policy.DeadLetterTargetARN)
	if dlq == nil {
		return errors.New("RedrivePolicy deadLetterTargetArn queue does not exist")
	}
	if isFIFOQueue(queue) != isFIFOQueue(dlq) {
		return errors.New("RedrivePolicy source queue and dead-letter queue must use the same queue type")
	}
	if err := validateRedriveAllowPolicy(dlq, queue.ARN); err != nil {
		return err
	}
	return nil
}

func (s *Server) defaultQueueAttributes() map[string]string {
	return map[string]string{
		"DelaySeconds":                  strconv.Itoa(s.defaultDelaySeconds()),
		"MaximumMessageSize":            strconv.FormatInt(s.maxMessageBytes(), 10),
		"MessageRetentionPeriod":        strconv.Itoa(s.defaultMessageRetentionSeconds()),
		"ReceiveMessageWaitTimeSeconds": strconv.Itoa(s.defaultReceiveWaitTimeSeconds()),
		"VisibilityTimeout":             strconv.Itoa(s.defaultVisibilityTimeoutSeconds()),
	}
}

func queueAttributesWithComputedLocked(queue *queueState, now time.Time) map[string]string {
	attrs := copyAttributes(queue.Attributes)
	attrs["QueueArn"] = queue.ARN
	attrs["CreatedTimestamp"] = strconv.FormatInt(queue.CreatedAt.Unix(), 10)
	attrs["LastModifiedTimestamp"] = strconv.FormatInt(queueLastModifiedAt(queue).Unix(), 10)
	visible, notVisible, delayed := approximateMessageCountsLocked(queue, now)
	attrs["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	attrs["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(notVisible)
	attrs["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	return attrs
}

func approximateMessageCountsLocked(queue *queueState, now time.Time) (visible int, notVisible int, delayed int) {
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		switch {
		case now.Before(message.AvailableAt):
			delayed++
		case now.Before(message.InvisibleUntil):
			notVisible++
		default:
			visible++
		}
	}
	return visible, notVisible, delayed
}

func filterQueueAttributes(attrs map[string]string, names []string) (map[string]string, error) {
	if len(names) == 0 || wantsAll(names) {
		return attrs, nil
	}
	filtered := map[string]string{}
	for _, name := range names {
		if !isReadableQueueAttribute(name) {
			return nil, fmt.Errorf("unknown queue attribute name %q", name)
		}
		if value, ok := attrs[name]; ok {
			filtered[name] = value
		}
	}
	return filtered, nil
}

func sameQueueAttributes(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValue := range left {
		if right[name] != leftValue {
			return false
		}
	}
	return true
}

func isReadableQueueAttribute(name string) bool {
	return name == "All" || queueAttributeNames[name]
}

func isSettableQueueAttribute(name string) bool {
	if strings.HasPrefix(name, "ApproximateNumberOfMessages") || name == "QueueArn" || name == "CreatedTimestamp" || name == "LastModifiedTimestamp" {
		return false
	}
	return queueAttributeNames[name]
}

func validateQueueAttributeValues(attrs map[string]string) error {
	bounds := map[string]struct {
		min int
		max int
	}{
		"DelaySeconds":                  {min: 0, max: maxDelaySeconds},
		"MaximumMessageSize":            {min: 1024, max: 1048576},
		"MessageRetentionPeriod":        {min: 60, max: 1209600},
		"ReceiveMessageWaitTimeSeconds": {min: 0, max: 20},
		"VisibilityTimeout":             {min: 0, max: maxVisibilityTimeoutSeconds},
		"KmsDataKeyReusePeriodSeconds":  {min: 60, max: 86400},
	}
	for name, bound := range bounds {
		value, ok := attrs[name]
		if !ok {
			continue
		}
		parsed, err := parseNonNegativeIntAttribute(name, value)
		if err != nil {
			return err
		}
		if parsed < bound.min || parsed > bound.max {
			return fmt.Errorf("invalid attribute value for %s", name)
		}
	}
	for _, name := range []string{
		"ContentBasedDeduplication",
		"FifoQueue",
		"SqsManagedSseEnabled",
	} {
		if value, ok := attrs[name]; ok {
			if !isSQSBool(value) {
				return fmt.Errorf("invalid attribute value for %s", name)
			}
		}
	}
	return nil
}

func parseNonNegativeIntAttribute(name string, value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid attribute value for %s", name)
	}
	return parsed, nil
}

func isSQSBool(value string) bool {
	return strings.EqualFold(value, "true") || strings.EqualFold(value, "false")
}

var queueAttributeNames = map[string]bool{
	"ApproximateNumberOfMessages":           true,
	"ApproximateNumberOfMessagesDelayed":    true,
	"ApproximateNumberOfMessagesNotVisible": true,
	"ContentBasedDeduplication":             true,
	"CreatedTimestamp":                      true,
	"DeduplicationScope":                    true,
	"DelaySeconds":                          true,
	"FifoQueue":                             true,
	"FifoThroughputLimit":                   true,
	"KmsDataKeyReusePeriodSeconds":          true,
	"KmsMasterKeyId":                        true,
	"LastModifiedTimestamp":                 true,
	"MaximumMessageSize":                    true,
	"MessageRetentionPeriod":                true,
	"Policy":                                true,
	"QueueArn":                              true,
	"ReceiveMessageWaitTimeSeconds":         true,
	"RedriveAllowPolicy":                    true,
	"RedrivePolicy":                         true,
	"SqsManagedSseEnabled":                  true,
	"VisibilityTimeout":                     true,
}

func isFIFOQueue(queue *queueState) bool {
	return strings.EqualFold(queue.Attributes["FifoQueue"], "true") || strings.HasSuffix(queue.Name, ".fifo")
}

func fifoDeduplicationID(queue *queueState, input sendMessageRequest) (string, error) {
	if input.MessageGroupID == "" {
		return "", errors.New("MessageGroupId is required for FIFO queues")
	}
	if input.MessageDeduplicationID != "" {
		return input.MessageDeduplicationID, nil
	}
	if strings.EqualFold(queue.Attributes["ContentBasedDeduplication"], "true") {
		sum := sha256.Sum256([]byte(input.MessageBody))
		return fmt.Sprintf("%x", sum[:]), nil
	}
	return "", errors.New("MessageDeduplicationId is required for FIFO queues unless ContentBasedDeduplication is enabled")
}

func parseRedrivePolicy(raw string) (redrivePolicy, error) {
	var values map[string]any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return redrivePolicy{}, errors.New("RedrivePolicy must be valid JSON")
	}
	policy := redrivePolicy{}
	if value, ok := values["deadLetterTargetArn"].(string); ok {
		policy.DeadLetterTargetARN = value
	}
	switch value := values["maxReceiveCount"].(type) {
	case string:
		count, err := strconv.Atoi(value)
		if err != nil {
			return redrivePolicy{}, errors.New("RedrivePolicy maxReceiveCount must be an integer")
		}
		policy.MaxReceiveCount = count
	case float64:
		if value != float64(int(value)) {
			return redrivePolicy{}, errors.New("RedrivePolicy maxReceiveCount must be an integer")
		}
		policy.MaxReceiveCount = int(value)
	}
	if policy.DeadLetterTargetARN == "" {
		return redrivePolicy{}, errors.New("RedrivePolicy deadLetterTargetArn is required")
	}
	return policy, nil
}

func redrivePolicyFromQueue(queue *queueState) (redrivePolicy, bool) {
	raw := queue.Attributes["RedrivePolicy"]
	if raw == "" {
		return redrivePolicy{}, false
	}
	policy, err := parseRedrivePolicy(raw)
	if err != nil || policy.MaxReceiveCount < 1 || policy.DeadLetterTargetARN == "" {
		return redrivePolicy{}, false
	}
	return policy, true
}

func parseRedriveAllowPolicy(raw string) (redriveAllowPolicy, error) {
	var values map[string]any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return redriveAllowPolicy{}, errors.New("RedriveAllowPolicy must be valid JSON")
	}
	policy := redriveAllowPolicy{Permission: "allowAll"}
	if value, ok := values["redrivePermission"].(string); ok && value != "" {
		policy.Permission = value
	}
	switch policy.Permission {
	case "allowAll", "denyAll":
	case "byQueue":
		rawARNs, ok := values["sourceQueueArns"].([]any)
		if !ok || len(rawARNs) == 0 {
			return redriveAllowPolicy{}, errors.New("RedriveAllowPolicy sourceQueueArns is required for byQueue")
		}
		if len(rawARNs) > 10 {
			return redriveAllowPolicy{}, errors.New("RedriveAllowPolicy sourceQueueArns must contain no more than 10 queues")
		}
		for _, rawARN := range rawARNs {
			arn, ok := rawARN.(string)
			if !ok || arn == "" {
				return redriveAllowPolicy{}, errors.New("RedriveAllowPolicy sourceQueueArns must contain queue ARNs")
			}
			policy.SourceQueueARNs = append(policy.SourceQueueARNs, arn)
		}
	default:
		return redriveAllowPolicy{}, errors.New("RedriveAllowPolicy redrivePermission must be allowAll, denyAll, or byQueue")
	}
	return policy, nil
}

func validateRedriveAllowPolicy(dlq *queueState, sourceARN string) error {
	raw := dlq.Attributes["RedriveAllowPolicy"]
	if raw == "" {
		return nil
	}
	policy, err := parseRedriveAllowPolicy(raw)
	if err != nil {
		return err
	}
	switch policy.Permission {
	case "allowAll":
		return nil
	case "denyAll":
		return errors.New("RedriveAllowPolicy does not allow this dead-letter queue")
	case "byQueue":
		for _, arn := range policy.SourceQueueARNs {
			if arn == sourceARN {
				return nil
			}
		}
		return errors.New("RedriveAllowPolicy does not allow this source queue")
	default:
		return errors.New("RedriveAllowPolicy redrivePermission must be allowAll, denyAll, or byQueue")
	}
}
