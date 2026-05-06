package pubsub

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var resourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+%-]{0,254}$`)

var attributeComparisonFilterPattern = regexp.MustCompile(`^attributes\.([A-Za-z0-9_.-]+)\s*(!=|=)\s*"([^"]*)"$`)

var attributePrefixFilterPattern = regexp.MustCompile(`^hasPrefix\(\s*attributes\.([A-Za-z0-9_.-]+)\s*,\s*"([^"]*)"\s*\)$`)

func validResourceID(id string) bool {
	return resourceIDPattern.MatchString(id)
}

func validProjectID(id string) bool {
	return resourceIDPattern.MatchString(id)
}

func validFullTopicName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "topics" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSubscriptionName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "subscriptions" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSnapshotName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "snapshots" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSchemaName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "schemas" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validSchemaType(schemaType string) bool {
	switch schemaType {
	case "", "TYPE_UNSPECIFIED", "PROTOCOL_BUFFER", "AVRO":
		return true
	default:
		return false
	}
}

func validSchemaEncoding(encoding string) bool {
	switch encoding {
	case "", "ENCODING_UNSPECIFIED", "JSON", "BINARY":
		return true
	default:
		return false
	}
}

func validSchemaMessageData(message []byte, encoding string) bool {
	if len(message) == 0 {
		return true
	}
	switch encoding {
	case "JSON":
		return json.Valid(message)
	default:
		return true
	}
}

func validateSchemaDefinition(schemaType string, definition string) error {
	if strings.TrimSpace(definition) == "" {
		return nil
	}
	switch schemaType {
	case "AVRO":
		var decoded any
		if err := json.Unmarshal([]byte(definition), &decoded); err != nil {
			return errors.New("avro schema definition must be valid json")
		}
		if _, ok := decoded.(map[string]any); !ok {
			return errors.New("avro schema definition must be a json object")
		}
	}
	return nil
}

func emptySchemaResource(schema schemaResource) bool {
	return schema.Name == "" &&
		schema.Type == "" &&
		schema.Definition == "" &&
		schema.RevisionID == "" &&
		schema.RevisionCreateTime == "" &&
		len(schema.Revisions) == 0
}

func validateDeadLetterPolicy(policy map[string]any) error {
	if len(policy) == 0 {
		return nil
	}
	rawTopic, ok := policy["deadLetterTopic"]
	if !ok {
		return fmt.Errorf("deadLetterPolicy.deadLetterTopic is required")
	}
	topic, ok := rawTopic.(string)
	if !ok || !validFullTopicName(topic) {
		return fmt.Errorf("invalid deadLetterPolicy.deadLetterTopic")
	}
	maxAttempts, ok := deadLetterMaxDeliveryAttempts(policy)
	if !ok {
		return fmt.Errorf("deadLetterPolicy.maxDeliveryAttempts is required")
	}
	if maxAttempts < 5 || maxAttempts > 100 {
		return fmt.Errorf("deadLetterPolicy.maxDeliveryAttempts must be between 5 and 100")
	}
	return nil
}

func (s *Server) deadLetterTopicExistsLocked(policy map[string]any) bool {
	if len(policy) == 0 {
		return true
	}
	_, found := s.topics[deadLetterTopic(policy)]
	return found
}

func validateTopicMetadata(topic topicResource) error {
	if strings.TrimSpace(topic.MessageRetentionDuration) != "" {
		if _, err := parseGoogleDuration(topic.MessageRetentionDuration); err != nil {
			return fmt.Errorf("messageRetentionDuration must be a non-negative duration")
		}
	}
	if len(topic.SchemaSettings) > 0 {
		rawSchema, ok := topic.SchemaSettings["schema"]
		if !ok {
			return fmt.Errorf("schemaSettings.schema is required")
		}
		schema, ok := rawSchema.(string)
		if !ok || !validFullSchemaName(schema) {
			return fmt.Errorf("invalid schemaSettings.schema")
		}
		if rawEncoding, ok := topic.SchemaSettings["encoding"]; ok {
			encoding, ok := rawEncoding.(string)
			if !ok || !validSchemaEncoding(encoding) {
				return fmt.Errorf("invalid schemaSettings.encoding")
			}
		}
	}
	return nil
}

func validateSubscriptionMetadata(subscription subscriptionResource) error {
	if strings.TrimSpace(subscription.MessageRetentionDuration) != "" {
		if _, err := parseGoogleDuration(subscription.MessageRetentionDuration); err != nil {
			return fmt.Errorf("messageRetentionDuration must be a non-negative duration")
		}
	}
	if len(subscription.ExpirationPolicy) > 0 {
		rawTTL, ok := subscription.ExpirationPolicy["ttl"]
		if !ok {
			return fmt.Errorf("expirationPolicy.ttl is required")
		}
		ttl, ok := rawTTL.(string)
		if !ok || strings.TrimSpace(ttl) == "" {
			return fmt.Errorf("expirationPolicy.ttl must be a duration string")
		}
		if _, err := parseGoogleDuration(ttl); err != nil {
			return fmt.Errorf("expirationPolicy.ttl must be a non-negative duration")
		}
	}
	return nil
}

func parseGoogleDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty duration")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("invalid duration")
	}
	return duration, nil
}

func validateSubscriptionFilter(filter string) error {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return nil
	}
	if !attributeComparisonFilterPattern.MatchString(filter) && !attributePrefixFilterPattern.MatchString(filter) {
		return fmt.Errorf("unsupported subscription filter")
	}
	return nil
}

func validateRetryPolicy(policy map[string]any) error {
	if len(policy) == 0 {
		return nil
	}
	minimum, hasMinimum, err := retryPolicyDuration(policy, "minimumBackoff")
	if err != nil {
		return err
	}
	maximum, hasMaximum, err := retryPolicyDuration(policy, "maximumBackoff")
	if err != nil {
		return err
	}
	if hasMinimum && hasMaximum && minimum > maximum {
		return fmt.Errorf("retryPolicy.minimumBackoff must be less than or equal to retryPolicy.maximumBackoff")
	}
	return nil
}

func validatePushConfig(config map[string]any) error {
	if len(config) == 0 {
		return nil
	}
	rawEndpoint, ok := config["pushEndpoint"]
	if !ok {
		return nil
	}
	endpoint, ok := rawEndpoint.(string)
	if !ok || strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("pushConfig.pushEndpoint must not include user info")
	}
	return nil
}

func validatePublishMessage(data string, attributes map[string]string) error {
	if data == "" && len(attributes) == 0 {
		return fmt.Errorf("message data or attributes are required")
	}
	if data != "" {
		if _, err := decodeBase64Bytes(data); err != nil {
			return fmt.Errorf("message data must be base64 encoded")
		}
	}
	for key := range attributes {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("message attributes must not contain empty keys")
		}
	}
	return nil
}

func validateMessageAgainstTopicSchemaSettings(data string, schemaSettings map[string]any) error {
	if len(schemaSettings) == 0 {
		return nil
	}
	encoding, _ := schemaSettings["encoding"].(string)
	if encoding == "" {
		return nil
	}
	message, err := decodeBase64Bytes(data)
	if err != nil {
		return fmt.Errorf("message data must be base64 encoded")
	}
	if !validSchemaMessageData(message, encoding) {
		return fmt.Errorf("message is invalid for topic schema encoding")
	}
	return nil
}

func decodeBase64Bytes(value string) ([]byte, error) {
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
	} {
		decoded, err := encoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid base64")
}

func retryPolicyDuration(policy map[string]any, field string) (time.Duration, bool, error) {
	raw, ok := policy[field]
	if !ok {
		return 0, false, nil
	}
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return 0, false, fmt.Errorf("retryPolicy.%s must be a duration string", field)
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 0, false, fmt.Errorf("retryPolicy.%s must be a non-negative duration", field)
	}
	return duration, true, nil
}

func subscriptionMatchesMessage(subscription subscriptionResource, message pubsubMessage) bool {
	filter := strings.TrimSpace(subscription.Filter)
	if filter == "" {
		return true
	}
	if match := attributeComparisonFilterPattern.FindStringSubmatch(filter); len(match) == 4 {
		value := message.Attributes[match[1]]
		if match[2] == "!=" {
			return value != match[3]
		}
		return value == match[3]
	}
	match := attributePrefixFilterPattern.FindStringSubmatch(filter)
	if len(match) != 3 {
		return false
	}
	return strings.HasPrefix(message.Attributes[match[1]], match[2])
}

func deadLetterMaxDeliveryAttempts(policy map[string]any) (int, bool) {
	raw, ok := policy["maxDeliveryAttempts"]
	if !ok {
		return 0, false
	}
	switch value := raw.(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		if value != float64(int(value)) {
			return 0, false
		}
		return int(value), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func deadLetterTopic(policy map[string]any) string {
	if len(policy) == 0 {
		return ""
	}
	topic, _ := policy["deadLetterTopic"].(string)
	return topic
}

func resourceProject(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}
