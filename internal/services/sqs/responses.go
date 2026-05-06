package sqs

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"sort"
	"strings"
)

type listQueuesXMLResponse struct {
	XMLName xml.Name            `xml:"ListQueuesResponse"`
	Xmlns   string              `xml:"xmlns,attr,omitempty"`
	Result  listQueuesXMLResult `xml:"ListQueuesResult"`
	Meta    responseMetadataXML `xml:"ResponseMetadata"`
}

type listQueuesXMLResult struct {
	QueueURLs []string `xml:"QueueUrl,omitempty"`
}

type responseMetadataXML struct {
	RequestID string `xml:"RequestId"`
}

type createQueueXMLResponse struct {
	XMLName xml.Name            `xml:"CreateQueueResponse"`
	Xmlns   string              `xml:"xmlns,attr,omitempty"`
	Result  queueURLXMLResult   `xml:"CreateQueueResult"`
	Meta    responseMetadataXML `xml:"ResponseMetadata"`
}

type getQueueURLXMLResponse struct {
	XMLName xml.Name            `xml:"GetQueueUrlResponse"`
	Xmlns   string              `xml:"xmlns,attr,omitempty"`
	Result  queueURLXMLResult   `xml:"GetQueueUrlResult"`
	Meta    responseMetadataXML `xml:"ResponseMetadata"`
}

type queueURLXMLResult struct {
	QueueURL string `xml:"QueueUrl"`
}

type getQueueAttributesXMLResponse struct {
	XMLName xml.Name                    `xml:"GetQueueAttributesResponse"`
	Xmlns   string                      `xml:"xmlns,attr,omitempty"`
	Result  getQueueAttributesXMLResult `xml:"GetQueueAttributesResult"`
	Meta    responseMetadataXML         `xml:"ResponseMetadata"`
}

type getQueueAttributesXMLResult struct {
	Attributes []attributeXML `xml:"Attribute,omitempty"`
}

type listQueueTagsXMLResponse struct {
	XMLName xml.Name               `xml:"ListQueueTagsResponse"`
	Xmlns   string                 `xml:"xmlns,attr,omitempty"`
	Result  listQueueTagsXMLResult `xml:"ListQueueTagsResult"`
	Meta    responseMetadataXML    `xml:"ResponseMetadata"`
}

type listQueueTagsXMLResult struct {
	Tags []tagXML `xml:"Tag,omitempty"`
}

type listDeadLetterSourceQueuesXMLResponse struct {
	XMLName xml.Name                            `xml:"ListDeadLetterSourceQueuesResponse"`
	Xmlns   string                              `xml:"xmlns,attr,omitempty"`
	Result  listDeadLetterSourceQueuesXMLResult `xml:"ListDeadLetterSourceQueuesResult"`
	Meta    responseMetadataXML                 `xml:"ResponseMetadata"`
}

type listDeadLetterSourceQueuesXMLResult struct {
	QueueURLs []string `xml:"QueueUrl,omitempty"`
}

type startMessageMoveTaskXMLResponse struct {
	XMLName xml.Name                      `xml:"StartMessageMoveTaskResponse"`
	Xmlns   string                        `xml:"xmlns,attr,omitempty"`
	Result  startMessageMoveTaskXMLResult `xml:"StartMessageMoveTaskResult"`
	Meta    responseMetadataXML           `xml:"ResponseMetadata"`
}

type startMessageMoveTaskXMLResult struct {
	TaskHandle string `xml:"TaskHandle"`
}

type listMessageMoveTasksXMLResponse struct {
	XMLName xml.Name                      `xml:"ListMessageMoveTasksResponse"`
	Xmlns   string                        `xml:"xmlns,attr,omitempty"`
	Result  listMessageMoveTasksXMLResult `xml:"ListMessageMoveTasksResult"`
	Meta    responseMetadataXML           `xml:"ResponseMetadata"`
}

type listMessageMoveTasksXMLResult struct {
	Results []messageMoveTaskResultXML `xml:"Result,omitempty"`
}

type messageMoveTaskResultXML struct {
	TaskHandle                       string `xml:"TaskHandle"`
	Status                           string `xml:"Status"`
	SourceARN                        string `xml:"SourceArn"`
	DestinationARN                   string `xml:"DestinationArn,omitempty"`
	ApproximateNumberOfMessagesMoved int    `xml:"ApproximateNumberOfMessagesMoved"`
}

type cancelMessageMoveTaskXMLResponse struct {
	XMLName xml.Name                       `xml:"CancelMessageMoveTaskResponse"`
	Xmlns   string                         `xml:"xmlns,attr,omitempty"`
	Result  cancelMessageMoveTaskXMLResult `xml:"CancelMessageMoveTaskResult"`
	Meta    responseMetadataXML            `xml:"ResponseMetadata"`
}

type cancelMessageMoveTaskXMLResult struct {
	ApproximateNumberOfMessagesMoved int `xml:"ApproximateNumberOfMessagesMoved"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type sendMessageXMLResponse struct {
	XMLName xml.Name             `xml:"SendMessageResponse"`
	Xmlns   string               `xml:"xmlns,attr,omitempty"`
	Result  sendMessageXMLResult `xml:"SendMessageResult"`
	Meta    responseMetadataXML  `xml:"ResponseMetadata"`
}

type sendMessageXMLResult struct {
	MessageID                    string `xml:"MessageId"`
	MD5OfMessageBody             string `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string `xml:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `xml:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `xml:"SequenceNumber,omitempty"`
}

type sendMessageBatchXMLResponse struct {
	XMLName xml.Name                  `xml:"SendMessageBatchResponse"`
	Xmlns   string                    `xml:"xmlns,attr,omitempty"`
	Result  sendMessageBatchXMLResult `xml:"SendMessageBatchResult"`
	Meta    responseMetadataXML       `xml:"ResponseMetadata"`
}

type sendMessageBatchXMLResult struct {
	Successful []sendMessageBatchResultEntryXML `xml:"SendMessageBatchResultEntry,omitempty"`
	Failed     []batchResultErrorEntryXML       `xml:"BatchResultErrorEntry,omitempty"`
}

type sendMessageBatchResultEntryXML struct {
	ID                           string `xml:"Id"`
	MessageID                    string `xml:"MessageId"`
	MD5OfMessageBody             string `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string `xml:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `xml:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `xml:"SequenceNumber,omitempty"`
}

type batchResultErrorEntryXML struct {
	ID          string `xml:"Id"`
	SenderFault bool   `xml:"SenderFault"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
}

type deleteMessageBatchXMLResponse struct {
	XMLName xml.Name                    `xml:"DeleteMessageBatchResponse"`
	Xmlns   string                      `xml:"xmlns,attr,omitempty"`
	Result  deleteMessageBatchXMLResult `xml:"DeleteMessageBatchResult"`
	Meta    responseMetadataXML         `xml:"ResponseMetadata"`
}

type deleteMessageBatchXMLResult struct {
	Successful []deleteMessageBatchResultEntryXML `xml:"DeleteMessageBatchResultEntry,omitempty"`
	Failed     []batchResultErrorEntryXML         `xml:"BatchResultErrorEntry,omitempty"`
}

type deleteMessageBatchResultEntryXML struct {
	ID string `xml:"Id"`
}

type changeMessageVisibilityBatchXMLResponse struct {
	XMLName xml.Name                              `xml:"ChangeMessageVisibilityBatchResponse"`
	Xmlns   string                                `xml:"xmlns,attr,omitempty"`
	Result  changeMessageVisibilityBatchXMLResult `xml:"ChangeMessageVisibilityBatchResult"`
	Meta    responseMetadataXML                   `xml:"ResponseMetadata"`
}

type changeMessageVisibilityBatchXMLResult struct {
	Successful []changeMessageVisibilityBatchResultEntryXML `xml:"ChangeMessageVisibilityBatchResultEntry,omitempty"`
	Failed     []batchResultErrorEntryXML                   `xml:"BatchResultErrorEntry,omitempty"`
}

type changeMessageVisibilityBatchResultEntryXML struct {
	ID string `xml:"Id"`
}

type receiveMessageXMLResponse struct {
	XMLName xml.Name                `xml:"ReceiveMessageResponse"`
	Xmlns   string                  `xml:"xmlns,attr,omitempty"`
	Result  receiveMessageXMLResult `xml:"ReceiveMessageResult"`
	Meta    responseMetadataXML     `xml:"ResponseMetadata"`
}

type receiveMessageXMLResult struct {
	Messages []receivedMessageXML `xml:"Message,omitempty"`
}

type receivedMessageXML struct {
	MessageID                    string                `xml:"MessageId"`
	ReceiptHandle                string                `xml:"ReceiptHandle"`
	MD5OfMessageBody             string                `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string                `xml:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string                `xml:"MD5OfMessageSystemAttributes,omitempty"`
	Body                         string                `xml:"Body"`
	Attributes                   []attributeXML        `xml:"Attribute,omitempty"`
	MessageAttributes            []messageAttributeXML `xml:"MessageAttribute,omitempty"`
}

type attributeXML struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type messageAttributeXML struct {
	Name  string                   `xml:"Name"`
	Value messageAttributeValueXML `xml:"Value"`
}

type messageAttributeValueXML struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue,omitempty"`
	BinaryValue string `xml:"BinaryValue,omitempty"`
}

type emptyXMLResponse struct {
	XMLName xml.Name
	Xmlns   string              `xml:"xmlns,attr,omitempty"`
	Meta    responseMetadataXML `xml:"ResponseMetadata"`
}

func attributeXMLList(attrs map[string]string) []attributeXML {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]attributeXML, 0, len(names))
	for _, name := range names {
		result = append(result, attributeXML{Name: name, Value: attrs[name]})
	}
	return result
}

func tagXMLList(tags map[string]string) []tagXML {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]tagXML, 0, len(keys))
	for _, key := range keys {
		result = append(result, tagXML{Key: key, Value: tags[key]})
	}
	return result
}

func messagesToXML(messages []receivedMessage) []receivedMessageXML {
	result := make([]receivedMessageXML, 0, len(messages))
	for _, message := range messages {
		result = append(result, receivedMessageXML{
			MessageID:                    message.MessageID,
			ReceiptHandle:                message.ReceiptHandle,
			MD5OfMessageBody:             message.MD5OfMessageBody,
			MD5OfMessageAttributes:       message.MD5OfMessageAttributes,
			MD5OfMessageSystemAttributes: message.MD5OfMessageSystemAttributes,
			Body:                         message.Body,
			Attributes:                   attributeXMLList(message.Attributes),
			MessageAttributes:            messageAttributeXMLList(message.MessageAttributes),
		})
	}
	return result
}

func messageAttributeXMLList(attrs map[string]messageAttributeValue) []messageAttributeXML {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]messageAttributeXML, 0, len(names))
	for _, name := range names {
		value := attrs[name]
		result = append(result, messageAttributeXML{
			Name: name,
			Value: messageAttributeValueXML{
				DataType:    value.DataType,
				StringValue: value.StringValue,
				BinaryValue: value.BinaryValue,
			},
		})
	}
	return result
}

func batchSuccessfulToXML(entries []sendMessageBatchResultEntry) []sendMessageBatchResultEntryXML {
	result := make([]sendMessageBatchResultEntryXML, 0, len(entries))
	for _, entry := range entries {
		result = append(result, sendMessageBatchResultEntryXML{
			ID:                           entry.ID,
			MessageID:                    entry.MessageID,
			MD5OfMessageBody:             entry.MD5OfMessageBody,
			MD5OfMessageAttributes:       entry.MD5OfMessageAttributes,
			MD5OfMessageSystemAttributes: entry.MD5OfMessageSystemAttributes,
			SequenceNumber:               entry.SequenceNumber,
		})
	}
	return result
}

func batchFailedToXML(entries []batchResultErrorEntry) []batchResultErrorEntryXML {
	result := make([]batchResultErrorEntryXML, 0, len(entries))
	for _, entry := range entries {
		result = append(result, batchResultErrorEntryXML{
			ID:          entry.ID,
			SenderFault: entry.SenderFault,
			Code:        entry.Code,
			Message:     entry.Message,
		})
	}
	return result
}

func deleteBatchSuccessfulToXML(entries []deleteMessageBatchResultEntry) []deleteMessageBatchResultEntryXML {
	result := make([]deleteMessageBatchResultEntryXML, 0, len(entries))
	for _, entry := range entries {
		result = append(result, deleteMessageBatchResultEntryXML{ID: entry.ID})
	}
	return result
}

func visibilityBatchSuccessfulToXML(entries []changeMessageVisibilityBatchResultEntry) []changeMessageVisibilityBatchResultEntryXML {
	result := make([]changeMessageVisibilityBatchResultEntryXML, 0, len(entries))
	for _, entry := range entries {
		result = append(result, changeMessageVisibilityBatchResultEntryXML{ID: entry.ID})
	}
	return result
}

func errorCode(err error) string {
	if err == nil {
		return "InvalidParameterValue"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "queue name exists"):
		return "QueueNameExists"
	case strings.Contains(message, "queue does not exist"):
		return "QueueDoesNotExist"
	case strings.Contains(message, "receipt handle is invalid"):
		return "ReceiptHandleIsInvalid"
	case strings.Contains(message, "batch entry id must be unique"):
		return "BatchEntryIdsNotDistinct"
	case strings.Contains(message, "batch entry id"):
		return "InvalidBatchEntryId"
	case strings.Contains(message, "attribute name"):
		return "InvalidAttributeName"
	case strings.Contains(message, "attribute value"):
		return "InvalidAttributeValue"
	case strings.Contains(message, "invalid characters"):
		return "InvalidMessageContents"
	default:
		return "InvalidParameterValue"
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-RequestId", "devcloud-sqs")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, code string, message string, status int) {
	writeJSON(w, status, map[string]string{
		"__type":  code,
		"message": message,
	})
}

func writeQueryXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Header().Set("x-amzn-RequestId", "devcloud-sqs")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	xml.NewEncoder(w).Encode(value)
}

func writeQueryError(w http.ResponseWriter, code string, message string, status int) {
	writeQueryXML(w, status, errorXMLResponse{
		Error: errorXML{
			Type:    "Sender",
			Code:    code,
			Message: message,
		},
		RequestID: "devcloud-sqs",
	})
}

func writeEmptySuccess(w http.ResponseWriter, protocol protocolKind, operation string) {
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, emptyXMLResponse{
			XMLName: xml.Name{Local: operation + "Response"},
			Xmlns:   "http://queue.amazonaws.com/doc/2012-11-05/",
			Meta:    responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func writeProtocolError(w http.ResponseWriter, protocol protocolKind, code string, message string, status int) {
	if protocol == protocolQuery {
		writeQueryError(w, code, message, status)
		return
	}
	writeJSONError(w, code, message, status)
}

func protocolFromRequest(r *http.Request) protocolKind {
	if strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.") || strings.Contains(r.Header.Get("Content-Type"), "application/x-amz-json-1.0") {
		return protocolJSON
	}
	return protocolQuery
}

type errorXMLResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     errorXML `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

type errorXML struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
