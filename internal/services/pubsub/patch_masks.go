package pubsub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func decodeTopicPatch(r *http.Request) (topicResource, map[string]struct{}, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return topicResource{}, nil, err
	}
	topicBody := body
	if raw, ok := body["topic"]; ok {
		if err := json.Unmarshal(raw, &topicBody); err != nil {
			return topicResource{}, nil, err
		}
	}
	var patch topicResource
	data, err := json.Marshal(topicBody)
	if err != nil {
		return topicResource{}, nil, err
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return topicResource{}, nil, err
	}

	fields, err := topicUpdateMaskFields(r, body, topicBody)
	if err != nil {
		return topicResource{}, nil, err
	}
	return patch, fields, nil
}

func topicUpdateMaskFields(r *http.Request, body map[string]json.RawMessage, topicBody map[string]json.RawMessage) (map[string]struct{}, error) {
	if raw := r.URL.Query().Get("updateMask"); raw != "" {
		return parseTopicUpdateMask(raw)
	}
	if raw, ok := body["updateMask"]; ok {
		var mask string
		if err := json.Unmarshal(raw, &mask); err == nil {
			return parseTopicUpdateMask(mask)
		}
		var structured struct {
			Paths []string `json:"paths"`
		}
		if err := json.Unmarshal(raw, &structured); err != nil {
			return nil, err
		}
		return parseTopicUpdateMask(strings.Join(structured.Paths, ","))
	}
	fields := map[string]struct{}{}
	for field := range topicBody {
		normalized, ok := normalizeTopicPatchField(field)
		if ok {
			fields[normalized] = struct{}{}
		}
	}
	return fields, nil
}

func parseTopicUpdateMask(mask string) (map[string]struct{}, error) {
	fields := map[string]struct{}{}
	for _, raw := range strings.Split(mask, ",") {
		field := strings.TrimSpace(raw)
		if field == "" {
			continue
		}
		normalized, ok := normalizeTopicPatchField(field)
		if !ok {
			return nil, fmt.Errorf("unsupported topic update field %q", field)
		}
		fields[normalized] = struct{}{}
	}
	return fields, nil
}

func normalizeTopicPatchField(field string) (string, bool) {
	field = strings.TrimPrefix(field, "topic.")
	switch field {
	case "name":
		return "name", true
	case "labels":
		return "labels", true
	case "messageRetentionDuration", "message_retention_duration":
		return "messageRetentionDuration", true
	case "schemaSettings", "schema_settings":
		return "schemaSettings", true
	case "kmsKeyName", "kms_key_name":
		return "kmsKeyName", true
	default:
		return "", false
	}
}

func decodeSubscriptionPatch(r *http.Request) (subscriptionResource, map[string]struct{}, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return subscriptionResource{}, nil, err
	}
	subscriptionBody := body
	if raw, ok := body["subscription"]; ok {
		if err := json.Unmarshal(raw, &subscriptionBody); err != nil {
			return subscriptionResource{}, nil, err
		}
	}
	var patch subscriptionResource
	data, err := json.Marshal(subscriptionBody)
	if err != nil {
		return subscriptionResource{}, nil, err
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return subscriptionResource{}, nil, err
	}

	fields, err := subscriptionUpdateMaskFields(r, body, subscriptionBody)
	if err != nil {
		return subscriptionResource{}, nil, err
	}
	return patch, fields, nil
}

func subscriptionUpdateMaskFields(r *http.Request, body map[string]json.RawMessage, subscriptionBody map[string]json.RawMessage) (map[string]struct{}, error) {
	if raw := r.URL.Query().Get("updateMask"); raw != "" {
		return parseSubscriptionUpdateMask(raw)
	}
	if raw, ok := body["updateMask"]; ok {
		var mask string
		if err := json.Unmarshal(raw, &mask); err == nil {
			return parseSubscriptionUpdateMask(mask)
		}
		var structured struct {
			Paths []string `json:"paths"`
		}
		if err := json.Unmarshal(raw, &structured); err != nil {
			return nil, err
		}
		return parseSubscriptionUpdateMask(strings.Join(structured.Paths, ","))
	}
	fields := map[string]struct{}{}
	for field := range subscriptionBody {
		normalized, ok := normalizeSubscriptionPatchField(field)
		if ok {
			fields[normalized] = struct{}{}
		}
	}
	return fields, nil
}

func parseSubscriptionUpdateMask(mask string) (map[string]struct{}, error) {
	fields := map[string]struct{}{}
	for _, raw := range strings.Split(mask, ",") {
		field := strings.TrimSpace(raw)
		if field == "" {
			continue
		}
		normalized, ok := normalizeSubscriptionPatchField(field)
		if !ok {
			return nil, fmt.Errorf("unsupported subscription update field %q", field)
		}
		fields[normalized] = struct{}{}
	}
	return fields, nil
}

func normalizeSubscriptionPatchField(field string) (string, bool) {
	field = strings.TrimPrefix(field, "subscription.")
	switch field {
	case "name":
		return "name", true
	case "topic":
		return "topic", true
	case "labels":
		return "labels", true
	case "ackDeadlineSeconds", "ack_deadline_seconds":
		return "ackDeadlineSeconds", true
	case "enableMessageOrdering", "enable_message_ordering":
		return "enableMessageOrdering", true
	case "enableExactlyOnceDelivery", "enable_exactly_once_delivery":
		return "enableExactlyOnceDelivery", true
	case "retainAckedMessages", "retain_acked_messages":
		return "retainAckedMessages", true
	case "messageRetentionDuration", "message_retention_duration":
		return "messageRetentionDuration", true
	case "expirationPolicy", "expiration_policy":
		return "expirationPolicy", true
	case "filter":
		return "filter", true
	case "deadLetterPolicy", "dead_letter_policy":
		return "deadLetterPolicy", true
	case "retryPolicy", "retry_policy":
		return "retryPolicy", true
	case "pushConfig", "push_config":
		return "pushConfig", true
	default:
		return "", false
	}
}

func hasPatchField(fields map[string]struct{}, field string) bool {
	_, ok := fields[field]
	return ok
}
