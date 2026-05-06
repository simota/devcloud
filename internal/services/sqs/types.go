package sqs

import (
	"regexp"
	"time"
)

type queueState struct {
	Name       string
	URL        string
	ARN        string
	Attributes map[string]string
	Tags       map[string]string
	CreatedAt  time.Time
	ModifiedAt time.Time
	Messages   []*messageState
	Sequence   uint64
	Dedup      map[string]deduplicationState
}

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}(\.fifo)?$`)

var batchEntryIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

const fifoDeduplicationWindow = 5 * time.Minute

const (
	maxDelaySeconds             = 900
	maxVisibilityTimeoutSeconds = 43200
)

type deduplicationState struct {
	ExpiresAt time.Time
	Message   *messageState
}

type messageState struct {
	ID                  string
	Body                string
	BodyMD5             string
	Attributes          map[string]messageAttributeValue
	SystemAttributes    map[string]messageAttributeValue
	SentAt              time.Time
	AvailableAt         time.Time
	InvisibleUntil      time.Time
	ReceiveCount        int
	FirstReceiveAt      time.Time
	ReceiptHandle       string
	Deleted             bool
	MessageGroupID      string
	DeduplicationID     string
	SequenceNumber      string
	DeadLetterSourceARN string
}

type moveTaskState struct {
	TaskHandle                       string
	SourceARN                        string
	DestinationARN                   string
	Status                           string
	StartedAt                        time.Time
	ApproximateNumberOfMessagesMoved int
}

type messageAttributeValue struct {
	DataType         string   `json:"DataType"`
	StringValue      string   `json:"StringValue,omitempty"`
	BinaryValue      string   `json:"BinaryValue,omitempty"`
	StringListValues []string `json:"StringListValues,omitempty"`
	BinaryListValues []string `json:"BinaryListValues,omitempty"`
}

type protocolKind string

const (
	protocolJSON  protocolKind = "json"
	protocolQuery protocolKind = "query"
	sqsAPIVersion              = "2012-11-05"
)

type sqsRequestError struct {
	Code    string
	Message string
	Status  int
}

func (e sqsRequestError) Error() string {
	return e.Message
}

type queueRequest struct {
	QueueName  string            `json:"QueueName"`
	QueueURL   string            `json:"QueueUrl"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"Tags"`
	TagsLower  map[string]string `json:"tags"`
}

type getQueueAttributesRequest struct {
	QueueURL       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

type tagQueueRequest struct {
	QueueURL string            `json:"QueueUrl"`
	Tags     map[string]string `json:"Tags"`
}

type untagQueueRequest struct {
	QueueURL string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

type permissionRequest struct {
	QueueURL      string   `json:"QueueUrl"`
	Label         string   `json:"Label"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
	Actions       []string `json:"Actions"`
}

type startMessageMoveTaskRequest struct {
	SourceARN                    string `json:"SourceArn"`
	DestinationARN               string `json:"DestinationArn"`
	MaxNumberOfMessagesPerSecond int    `json:"MaxNumberOfMessagesPerSecond"`
}

type listMessageMoveTasksRequest struct {
	SourceARN  string `json:"SourceArn"`
	MaxResults int    `json:"MaxResults"`
}

type messageMoveTaskResult struct {
	TaskHandle                       string `json:"TaskHandle"`
	Status                           string `json:"Status"`
	SourceARN                        string `json:"SourceArn"`
	DestinationARN                   string `json:"DestinationArn,omitempty"`
	ApproximateNumberOfMessagesMoved int    `json:"ApproximateNumberOfMessagesMoved"`
}

type sendMessageRequest struct {
	QueueURL                string                           `json:"QueueUrl"`
	MessageBody             string                           `json:"MessageBody"`
	DelaySeconds            *int                             `json:"DelaySeconds"`
	MessageAttributes       map[string]messageAttributeValue `json:"MessageAttributes"`
	MessageSystemAttributes map[string]messageAttributeValue `json:"MessageSystemAttributes"`
	MessageGroupID          string                           `json:"MessageGroupId"`
	MessageDeduplicationID  string                           `json:"MessageDeduplicationId"`
}

type sendMessageBatchRequest struct {
	QueueURL string                  `json:"QueueUrl"`
	Entries  []sendMessageBatchEntry `json:"Entries"`
}

type sendMessageBatchEntry struct {
	ID                      string                           `json:"Id"`
	MessageBody             string                           `json:"MessageBody"`
	DelaySeconds            *int                             `json:"DelaySeconds"`
	MessageAttributes       map[string]messageAttributeValue `json:"MessageAttributes"`
	MessageSystemAttributes map[string]messageAttributeValue `json:"MessageSystemAttributes"`
	MessageGroupID          string                           `json:"MessageGroupId"`
	MessageDeduplicationID  string                           `json:"MessageDeduplicationId"`
}

type sendMessageBatchResult struct {
	Successful []sendMessageBatchResultEntry `json:"Successful"`
	Failed     []batchResultErrorEntry       `json:"Failed"`
}

type sendMessageBatchResultEntry struct {
	ID                           string `json:"Id"`
	MessageID                    string `json:"MessageId"`
	MD5OfMessageBody             string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `json:"SequenceNumber,omitempty"`
}

type batchResultErrorEntry struct {
	ID          string `json:"Id"`
	SenderFault bool   `json:"SenderFault"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
}

type deleteMessageBatchRequest struct {
	QueueURL string                    `json:"QueueUrl"`
	Entries  []deleteMessageBatchEntry `json:"Entries"`
}

type deleteMessageBatchEntry struct {
	ID            string `json:"Id"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type deleteMessageBatchResult struct {
	Successful []deleteMessageBatchResultEntry `json:"Successful"`
	Failed     []batchResultErrorEntry         `json:"Failed"`
}

type deleteMessageBatchResultEntry struct {
	ID string `json:"Id"`
}

type changeMessageVisibilityBatchRequest struct {
	QueueURL string                              `json:"QueueUrl"`
	Entries  []changeMessageVisibilityBatchEntry `json:"Entries"`
}

type changeMessageVisibilityBatchEntry struct {
	ID                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type changeMessageVisibilityBatchResult struct {
	Successful []changeMessageVisibilityBatchResultEntry `json:"Successful"`
	Failed     []batchResultErrorEntry                   `json:"Failed"`
}

type changeMessageVisibilityBatchResultEntry struct {
	ID string `json:"Id"`
}

type receiveMessageRequest struct {
	QueueURL                    string   `json:"QueueUrl"`
	MaxNumberOfMessages         *int     `json:"MaxNumberOfMessages"`
	VisibilityTimeout           *int     `json:"VisibilityTimeout"`
	WaitTimeSeconds             *int     `json:"WaitTimeSeconds"`
	AttributeNames              []string `json:"AttributeNames"`
	MessageAttributeNames       []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	ReceiveRequestAttemptID     string   `json:"ReceiveRequestAttemptId"`
}

type receiptRequest struct {
	QueueURL      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type visibilityRequest struct {
	QueueURL          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type receivedMessage struct {
	MessageID                    string                           `json:"MessageId"`
	ReceiptHandle                string                           `json:"ReceiptHandle"`
	MD5OfMessageBody             string                           `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string                           `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string                           `json:"MD5OfMessageSystemAttributes,omitempty"`
	Body                         string                           `json:"Body"`
	Attributes                   map[string]string                `json:"Attributes,omitempty"`
	MessageAttributes            map[string]messageAttributeValue `json:"MessageAttributes,omitempty"`
}

type redrivePolicy struct {
	DeadLetterTargetARN string
	MaxReceiveCount     int
}

type redriveAllowPolicy struct {
	Permission      string
	SourceQueueARNs []string
}

type queuePolicy struct {
	Version   string                 `json:"Version"`
	ID        string                 `json:"Id"`
	Statement []queuePolicyStatement `json:"Statement"`
}

type queuePolicyStatement struct {
	Sid       string               `json:"Sid"`
	Effect    string               `json:"Effect"`
	Principal queuePolicyPrincipal `json:"Principal"`
	Action    []string             `json:"Action"`
	Resource  string               `json:"Resource"`
}

type queuePolicyPrincipal struct {
	AWS []string `json:"AWS"`
}
