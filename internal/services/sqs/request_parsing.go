package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func parseQueueRequest(r *http.Request, protocol protocolKind) (queueRequest, error) {
	if protocol == protocolJSON {
		var input queueRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return queueRequest{}, err
		}
		if input.Attributes == nil {
			input.Attributes = map[string]string{}
		}
		if input.Tags == nil {
			input.Tags = input.TagsLower
		}
		if input.Tags == nil {
			input.Tags = map[string]string{}
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return queueRequest{}, err
	}
	return queueRequest{
		QueueName:  r.Form.Get("QueueName"),
		QueueURL:   requestQueueURL(r),
		Attributes: queryAttributes(r.Form),
		Tags:       queryTags(r.Form),
	}, nil
}

func parseGetQueueAttributesRequest(r *http.Request, protocol protocolKind) (getQueueAttributesRequest, error) {
	if protocol == protocolJSON {
		var input getQueueAttributesRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return getQueueAttributesRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return getQueueAttributesRequest{}, err
	}
	return getQueueAttributesRequest{
		QueueURL:       requestQueueURL(r),
		AttributeNames: queryListValues(r.Form, "AttributeName"),
	}, nil
}

func parseTagQueueRequest(r *http.Request, protocol protocolKind) (tagQueueRequest, error) {
	if protocol == protocolJSON {
		var input tagQueueRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return tagQueueRequest{}, err
		}
		if input.Tags == nil {
			input.Tags = map[string]string{}
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return tagQueueRequest{}, err
	}
	return tagQueueRequest{
		QueueURL: requestQueueURL(r),
		Tags:     queryTags(r.Form),
	}, nil
}

func parseUntagQueueRequest(r *http.Request, protocol protocolKind) (untagQueueRequest, error) {
	if protocol == protocolJSON {
		var input untagQueueRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return untagQueueRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return untagQueueRequest{}, err
	}
	return untagQueueRequest{
		QueueURL: requestQueueURL(r),
		TagKeys:  queryListValues(r.Form, "TagKey"),
	}, nil
}

func parseAddPermissionRequest(r *http.Request, protocol protocolKind) (permissionRequest, error) {
	if protocol == protocolJSON {
		var input permissionRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return permissionRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return permissionRequest{}, err
	}
	return permissionRequest{
		QueueURL:      requestQueueURL(r),
		Label:         r.Form.Get("Label"),
		AWSAccountIDs: queryListValues(r.Form, "AWSAccountId"),
		Actions:       queryListValues(r.Form, "ActionName"),
	}, nil
}

func parseRemovePermissionRequest(r *http.Request, protocol protocolKind) (permissionRequest, error) {
	if protocol == protocolJSON {
		var input permissionRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return permissionRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return permissionRequest{}, err
	}
	return permissionRequest{
		QueueURL: requestQueueURL(r),
		Label:    r.Form.Get("Label"),
	}, nil
}

func parseStartMessageMoveTaskRequest(r *http.Request, protocol protocolKind) (startMessageMoveTaskRequest, error) {
	if protocol == protocolJSON {
		var input startMessageMoveTaskRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return startMessageMoveTaskRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return startMessageMoveTaskRequest{}, err
	}
	return startMessageMoveTaskRequest{
		SourceARN:                    r.Form.Get("SourceArn"),
		DestinationARN:               r.Form.Get("DestinationArn"),
		MaxNumberOfMessagesPerSecond: formInt(r.Form, "MaxNumberOfMessagesPerSecond", 0),
	}, nil
}

func parseListMessageMoveTasksRequest(r *http.Request, protocol protocolKind) (listMessageMoveTasksRequest, error) {
	if protocol == protocolJSON {
		var input listMessageMoveTasksRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return listMessageMoveTasksRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return listMessageMoveTasksRequest{}, err
	}
	return listMessageMoveTasksRequest{
		SourceARN:  r.Form.Get("SourceArn"),
		MaxResults: formInt(r.Form, "MaxResults", 0),
	}, nil
}

func parseSendMessageRequest(r *http.Request, protocol protocolKind) (sendMessageRequest, error) {
	if protocol == protocolJSON {
		var input sendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return sendMessageRequest{}, err
		}
		if input.MessageAttributes == nil {
			input.MessageAttributes = map[string]messageAttributeValue{}
		}
		if input.MessageSystemAttributes == nil {
			input.MessageSystemAttributes = map[string]messageAttributeValue{}
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return sendMessageRequest{}, err
	}
	return sendMessageRequest{
		QueueURL:                requestQueueURL(r),
		MessageBody:             r.Form.Get("MessageBody"),
		DelaySeconds:            optionalFormInt(r.Form, "DelaySeconds"),
		MessageAttributes:       queryMessageAttributes(r.Form),
		MessageSystemAttributes: queryMessageAttributesWithPrefix(r.Form, "MessageSystemAttribute"),
		MessageGroupID:          r.Form.Get("MessageGroupId"),
		MessageDeduplicationID:  r.Form.Get("MessageDeduplicationId"),
	}, nil
}

func parseSendMessageBatchRequest(r *http.Request, protocol protocolKind) (sendMessageBatchRequest, error) {
	if protocol == protocolJSON {
		var input sendMessageBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return sendMessageBatchRequest{}, err
		}
		for i := range input.Entries {
			if input.Entries[i].MessageAttributes == nil {
				input.Entries[i].MessageAttributes = map[string]messageAttributeValue{}
			}
			if input.Entries[i].MessageSystemAttributes == nil {
				input.Entries[i].MessageSystemAttributes = map[string]messageAttributeValue{}
			}
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return sendMessageBatchRequest{}, err
	}
	entries := []sendMessageBatchEntry{}
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("SendMessageBatchRequestEntry.%d.", i)
		id := r.Form.Get(prefix + "Id")
		if id == "" {
			break
		}
		entries = append(entries, sendMessageBatchEntry{
			ID:                      id,
			MessageBody:             r.Form.Get(prefix + "MessageBody"),
			DelaySeconds:            optionalFormInt(r.Form, prefix+"DelaySeconds"),
			MessageAttributes:       queryMessageAttributesWithPrefix(r.Form, prefix+"MessageAttribute"),
			MessageSystemAttributes: queryMessageAttributesWithPrefix(r.Form, prefix+"MessageSystemAttribute"),
			MessageGroupID:          r.Form.Get(prefix + "MessageGroupId"),
			MessageDeduplicationID:  r.Form.Get(prefix + "MessageDeduplicationId"),
		})
	}
	return sendMessageBatchRequest{
		QueueURL: requestQueueURL(r),
		Entries:  entries,
	}, nil
}

func parseDeleteMessageBatchRequest(r *http.Request, protocol protocolKind) (deleteMessageBatchRequest, error) {
	if protocol == protocolJSON {
		var input deleteMessageBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return deleteMessageBatchRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return deleteMessageBatchRequest{}, err
	}
	entries := []deleteMessageBatchEntry{}
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.", i)
		id := r.Form.Get(prefix + "Id")
		if id == "" {
			break
		}
		entries = append(entries, deleteMessageBatchEntry{
			ID:            id,
			ReceiptHandle: r.Form.Get(prefix + "ReceiptHandle"),
		})
	}
	return deleteMessageBatchRequest{
		QueueURL: requestQueueURL(r),
		Entries:  entries,
	}, nil
}

func parseChangeMessageVisibilityBatchRequest(r *http.Request, protocol protocolKind) (changeMessageVisibilityBatchRequest, error) {
	if protocol == protocolJSON {
		var input changeMessageVisibilityBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return changeMessageVisibilityBatchRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return changeMessageVisibilityBatchRequest{}, err
	}
	entries := []changeMessageVisibilityBatchEntry{}
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.", i)
		id := r.Form.Get(prefix + "Id")
		if id == "" {
			break
		}
		entries = append(entries, changeMessageVisibilityBatchEntry{
			ID:                id,
			ReceiptHandle:     r.Form.Get(prefix + "ReceiptHandle"),
			VisibilityTimeout: formInt(r.Form, prefix+"VisibilityTimeout", 0),
		})
	}
	return changeMessageVisibilityBatchRequest{
		QueueURL: requestQueueURL(r),
		Entries:  entries,
	}, nil
}

func parseReceiveMessageRequest(r *http.Request, protocol protocolKind) (receiveMessageRequest, error) {
	if protocol == protocolJSON {
		var input receiveMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return receiveMessageRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return receiveMessageRequest{}, err
	}
	return receiveMessageRequest{
		QueueURL:                    requestQueueURL(r),
		MaxNumberOfMessages:         optionalFormInt(r.Form, "MaxNumberOfMessages"),
		VisibilityTimeout:           optionalFormInt(r.Form, "VisibilityTimeout"),
		WaitTimeSeconds:             optionalFormInt(r.Form, "WaitTimeSeconds"),
		AttributeNames:              queryListValues(r.Form, "AttributeName"),
		MessageAttributeNames:       queryListValues(r.Form, "MessageAttributeName"),
		MessageSystemAttributeNames: queryListValues(r.Form, "MessageSystemAttributeName"),
		ReceiveRequestAttemptID:     r.Form.Get("ReceiveRequestAttemptId"),
	}, nil
}

func parseReceiptRequest(r *http.Request, protocol protocolKind) (receiptRequest, error) {
	if protocol == protocolJSON {
		var input receiptRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return receiptRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return receiptRequest{}, err
	}
	return receiptRequest{
		QueueURL:      requestQueueURL(r),
		ReceiptHandle: r.Form.Get("ReceiptHandle"),
	}, nil
}

func parseVisibilityRequest(r *http.Request, protocol protocolKind) (visibilityRequest, error) {
	if protocol == protocolJSON {
		var input visibilityRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return visibilityRequest{}, err
		}
		return input, nil
	}
	if err := r.ParseForm(); err != nil {
		return visibilityRequest{}, err
	}
	return visibilityRequest{
		QueueURL:          requestQueueURL(r),
		ReceiptHandle:     r.Form.Get("ReceiptHandle"),
		VisibilityTimeout: formInt(r.Form, "VisibilityTimeout", 0),
	}, nil
}

func requestString(r *http.Request, protocol protocolKind, key string) (string, error) {
	if protocol == protocolJSON {
		var values map[string]any
		if err := json.NewDecoder(r.Body).Decode(&values); err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		value, _ := values[key].(string)
		return value, nil
	}
	if err := r.ParseForm(); err != nil {
		return "", err
	}
	if key == "QueueUrl" {
		return requestQueueURL(r), nil
	}
	return r.Form.Get(key), nil
}

func requestQueueURL(r *http.Request) string {
	if value := r.Form.Get("QueueUrl"); value != "" {
		return value
	}
	if isRootPath(r.URL.Path) || queueNameFromURL(r.URL.Path) == "" {
		return ""
	}
	return r.URL.Path
}

func queryAttributes(values url.Values) map[string]string {
	attrs := map[string]string{}
	for key, value := range values {
		if strings.HasPrefix(key, "Attribute.") && len(value) > 0 {
			name := strings.TrimPrefix(key, "Attribute.")
			if !strings.Contains(name, ".") {
				attrs[name] = value[0]
			}
		}
	}
	for i := 1; ; i++ {
		name := values.Get(fmt.Sprintf("Attribute.%d.Name", i))
		value := values.Get(fmt.Sprintf("Attribute.%d.Value", i))
		if name == "" {
			break
		}
		attrs[name] = value
	}
	return attrs
}

func queryTags(values url.Values) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		key := values.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}
		tags[key] = values.Get(fmt.Sprintf("Tag.%d.Value", i))
	}
	return tags
}

func queryMessageAttributes(values url.Values) map[string]messageAttributeValue {
	return queryMessageAttributesWithPrefix(values, "MessageAttribute")
}

func queryMessageAttributesWithPrefix(values url.Values, attributePrefix string) map[string]messageAttributeValue {
	attrs := map[string]messageAttributeValue{}
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("%s.%d.", attributePrefix, i)
		name := values.Get(prefix + "Name")
		if name == "" {
			break
		}
		attrs[name] = messageAttributeValue{
			DataType:    values.Get(prefix + "Value.DataType"),
			StringValue: values.Get(prefix + "Value.StringValue"),
			BinaryValue: values.Get(prefix + "Value.BinaryValue"),
		}
	}
	return attrs
}

func queryListValues(values url.Values, prefix string) []string {
	result := []string{}
	for _, value := range values[prefix] {
		if value != "" {
			result = append(result, value)
		}
	}
	for i := 1; ; i++ {
		value := values.Get(fmt.Sprintf("%s.%d", prefix, i))
		if value == "" {
			break
		}
		result = append(result, value)
	}
	for i := 1; ; i++ {
		value := values.Get(fmt.Sprintf("%s.member.%d", prefix, i))
		if value == "" {
			break
		}
		result = append(result, value)
	}
	return result
}

func optionalFormInt(values url.Values, key string) *int {
	if values.Get(key) == "" {
		return nil
	}
	value := formInt(values, key, 0)
	return &value
}

func formInt(values url.Values, key string, fallback int) int {
	raw := values.Get(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}
