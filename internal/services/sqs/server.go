package sqs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Addr                            string
	Region                          string
	AccountID                       string
	QueueURLHost                    string
	AuthMode                        string
	AccessKeyID                     string
	SecretAccessKey                 string
	StoragePath                     string
	MaxQueues                       int
	MaxMessageBytes                 int64
	MaxReceiveBatchSize             int
	DefaultVisibilityTimeoutSeconds int
	DefaultDelaySeconds             int
	DefaultMessageRetentionSeconds  int
	DefaultReceiveWaitTimeSeconds   int
}

type Server struct {
	config    Config
	mu        sync.Mutex
	queues    map[string]*queueState
	moveTasks map[string]moveTaskState
	waitCh    chan struct{}
	loadErr   error
}

func NewServer(cfg Config) *Server {
	server := &Server{
		config:    cfg,
		queues:    map[string]*queueState{},
		moveTasks: map[string]moveTaskState{},
		waitCh:    make(chan struct{}),
	}
	if cfg.StoragePath != "" {
		server.loadErr = server.load()
	}
	return server
}

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

type Snapshot struct {
	Status  string          `json:"status"`
	Running bool            `json:"running"`
	Region  string          `json:"region"`
	Queues  []QueueSnapshot `json:"queues"`
}

type QueueSnapshot struct {
	Name                  string            `json:"name"`
	URL                   string            `json:"url"`
	ARN                   string            `json:"arn"`
	Attributes            map[string]string `json:"attributes"`
	Tags                  map[string]string `json:"tags,omitempty"`
	CreatedAt             time.Time         `json:"createdAt"`
	VisibleMessages       int               `json:"visibleMessages"`
	NotVisibleMessages    int               `json:"notVisibleMessages"`
	DelayedMessages       int               `json:"delayedMessages"`
	TotalRetainedMessages int               `json:"totalRetainedMessages"`
}

type QueueDetailSnapshot struct {
	Queue    QueueSnapshot     `json:"queue"`
	Messages []MessageSnapshot `json:"messages"`
	Leases   []LeaseSnapshot   `json:"leases"`
}

type DeadLetterSnapshot struct {
	DeadLetterQueue        *QueueSnapshot  `json:"deadLetterQueue,omitempty"`
	DeadLetterSourceQueues []QueueSnapshot `json:"deadLetterSourceQueues"`
}

type MessageSnapshot struct {
	MessageID        string                           `json:"messageId"`
	Body             string                           `json:"body"`
	MD5OfMessageBody string                           `json:"md5OfMessageBody"`
	Attributes       map[string]messageAttributeValue `json:"attributes,omitempty"`
	SystemAttributes map[string]messageAttributeValue `json:"systemAttributes,omitempty"`
	SentAt           time.Time                        `json:"sentAt"`
	AvailableAt      time.Time                        `json:"availableAt"`
	InvisibleUntil   time.Time                        `json:"invisibleUntil,omitempty"`
	ReceiveCount     int                              `json:"receiveCount"`
	FirstReceiveAt   *time.Time                       `json:"firstReceiveAt,omitempty"`
	State            string                           `json:"state"`
	MessageGroupID   string                           `json:"messageGroupId,omitempty"`
	DeduplicationID  string                           `json:"deduplicationId,omitempty"`
	SequenceNumber   string                           `json:"sequenceNumber,omitempty"`
}

type LeaseSnapshot struct {
	MessageID            string    `json:"messageId"`
	VisibleAfter         time.Time `json:"visibleAfter"`
	ReceiveCount         int       `json:"receiveCount"`
	ReceiptHandlePresent bool      `json:"receiptHandlePresent"`
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		names = append(names, name)
	}
	sort.Strings(names)

	queues := make([]QueueSnapshot, 0, len(names))
	for _, name := range names {
		queue := s.queues[name]
		cleanupExpiredMessagesLocked(queue, now)
		queues = append(queues, queueSnapshotLocked(queue, now))
	}

	return Snapshot{
		Status:  "running",
		Running: true,
		Region:  defaultString(s.config.Region, "us-east-1"),
		Queues:  queues,
	}
}

func queueSnapshotLocked(queue *queueState, now time.Time) QueueSnapshot {
	snapshot := QueueSnapshot{
		Name:       queue.Name,
		URL:        queue.URL,
		ARN:        queue.ARN,
		Attributes: copyAttributes(queue.Attributes),
		Tags:       copyAttributes(queue.Tags),
		CreatedAt:  queue.CreatedAt,
	}
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		snapshot.TotalRetainedMessages++
		switch {
		case now.Before(message.AvailableAt):
			snapshot.DelayedMessages++
		case now.Before(message.InvisibleUntil):
			snapshot.NotVisibleMessages++
		default:
			snapshot.VisibleMessages++
		}
	}
	return snapshot
}

func messageSnapshotLocked(message *messageState, now time.Time) MessageSnapshot {
	var firstReceiveAt *time.Time
	if !message.FirstReceiveAt.IsZero() {
		value := message.FirstReceiveAt
		firstReceiveAt = &value
	}
	return MessageSnapshot{
		MessageID:        message.ID,
		Body:             message.Body,
		MD5OfMessageBody: message.BodyMD5,
		Attributes:       copyMessageAttributes(message.Attributes),
		SystemAttributes: copyMessageAttributes(message.SystemAttributes),
		SentAt:           message.SentAt,
		AvailableAt:      message.AvailableAt,
		InvisibleUntil:   message.InvisibleUntil,
		ReceiveCount:     message.ReceiveCount,
		FirstReceiveAt:   firstReceiveAt,
		State:            messageStateName(message, now),
		MessageGroupID:   message.MessageGroupID,
		DeduplicationID:  message.DeduplicationID,
		SequenceNumber:   message.SequenceNumber,
	}
}

func messageStateName(message *messageState, now time.Time) string {
	switch {
	case message.Deleted:
		return "deleted"
	case now.Before(message.AvailableAt):
		return "delayed"
	case now.Before(message.InvisibleUntil):
		return "in_flight"
	default:
		return "visible"
	}
}

func (s *Server) QueueDetailSnapshot(name string) (QueueDetailSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[name]
	if !ok {
		return QueueDetailSnapshot{}, false
	}
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	detail := QueueDetailSnapshot{
		Queue:    queueSnapshotLocked(queue, now),
		Messages: make([]MessageSnapshot, 0, len(queue.Messages)),
		Leases:   []LeaseSnapshot{},
	}
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		detail.Messages = append(detail.Messages, messageSnapshotLocked(message, now))
		if message.ReceiptHandle != "" && now.Before(message.InvisibleUntil) {
			detail.Leases = append(detail.Leases, LeaseSnapshot{
				MessageID:            message.ID,
				VisibleAfter:         message.InvisibleUntil,
				ReceiveCount:         message.ReceiveCount,
				ReceiptHandlePresent: true,
			})
		}
	}
	return detail, true
}

func (s *Server) DeadLetterSnapshot(name string) (DeadLetterSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[name]
	if !ok {
		return DeadLetterSnapshot{}, false
	}
	now := time.Now().UTC()
	var deadLetterQueue *QueueSnapshot
	if policy, ok := redrivePolicyFromQueue(queue); ok {
		if dlq := s.queueByARNLocked(policy.DeadLetterTargetARN); dlq != nil {
			snapshot := queueSnapshotLocked(dlq, now)
			deadLetterQueue = &snapshot
		}
	}
	sourceNames := make([]string, 0)
	for sourceName, source := range s.queues {
		policy, ok := redrivePolicyFromQueue(source)
		if ok && policy.DeadLetterTargetARN == queue.ARN {
			sourceNames = append(sourceNames, sourceName)
		}
	}
	sort.Strings(sourceNames)
	sources := make([]QueueSnapshot, 0, len(sourceNames))
	for _, sourceName := range sourceNames {
		sources = append(sources, queueSnapshotLocked(s.queues[sourceName], now))
	}
	return DeadLetterSnapshot{
		DeadLetterQueue:        deadLetterQueue,
		DeadLetterSourceQueues: sources,
	}, true
}

func (s *Server) PurgeQueueByName(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return false
	}
	previous := queue.Messages
	queue.Messages = nil
	if err := s.persistLocked(); err != nil {
		queue.Messages = previous
		return false
	}
	return true
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *Server) routes() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-sqs")
	if s.loadErr != nil {
		writeJSONError(w, "InternalError", "failed to load sqs state", http.StatusInternalServerError)
		return
	}
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	if !isRootPath(r.URL.Path) && queueNameFromURL(r.URL.Path) == "" {
		writeProtocolError(w, protocolFromRequest(r), "InvalidAddress", "SQS endpoint path is invalid", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		writeJSONError(w, "InvalidAction", "method is not supported", http.StatusMethodNotAllowed)
		return
	}
	authProtocol := protocolFromRequest(r)
	if err := s.verifySignature(r); err != nil {
		code, status := signatureErrorDetails(err)
		writeProtocolError(w, authProtocol, code, err.Error(), status)
		return
	}

	protocol, operation, err := s.detectOperation(r)
	if err != nil {
		code := "InvalidAction"
		status := http.StatusBadRequest
		var requestErr sqsRequestError
		if errors.As(err, &requestErr) {
			code = requestErr.Code
			status = requestErr.Status
		}
		writeProtocolError(w, protocol, code, err.Error(), status)
		return
	}
	switch operation {
	case "ListQueues":
		s.handleListQueues(w, r, protocol)
	case "CreateQueue":
		s.handleCreateQueue(w, r, protocol)
	case "GetQueueUrl":
		s.handleGetQueueURL(w, r, protocol)
	case "GetQueueAttributes":
		s.handleGetQueueAttributes(w, r, protocol)
	case "SetQueueAttributes":
		s.handleSetQueueAttributes(w, r, protocol)
	case "DeleteQueue":
		s.handleDeleteQueue(w, r, protocol)
	case "PurgeQueue":
		s.handlePurgeQueue(w, r, protocol)
	case "TagQueue":
		s.handleTagQueue(w, r, protocol)
	case "UntagQueue":
		s.handleUntagQueue(w, r, protocol)
	case "ListQueueTags":
		s.handleListQueueTags(w, r, protocol)
	case "ListDeadLetterSourceQueues":
		s.handleListDeadLetterSourceQueues(w, r, protocol)
	case "StartMessageMoveTask":
		s.handleStartMessageMoveTask(w, r, protocol)
	case "ListMessageMoveTasks":
		s.handleListMessageMoveTasks(w, r, protocol)
	case "CancelMessageMoveTask":
		s.handleCancelMessageMoveTask(w, r, protocol)
	case "AddPermission":
		s.handleAddPermission(w, r, protocol)
	case "RemovePermission":
		s.handleRemovePermission(w, r, protocol)
	case "SendMessage":
		s.handleSendMessage(w, r, protocol)
	case "SendMessageBatch":
		s.handleSendMessageBatch(w, r, protocol)
	case "ReceiveMessage":
		s.handleReceiveMessage(w, r, protocol)
	case "DeleteMessage":
		s.handleDeleteMessage(w, r, protocol)
	case "DeleteMessageBatch":
		s.handleDeleteMessageBatch(w, r, protocol)
	case "ChangeMessageVisibility":
		s.handleChangeMessageVisibility(w, r, protocol)
	case "ChangeMessageVisibilityBatch":
		s.handleChangeMessageVisibilityBatch(w, r, protocol)
	default:
		writeProtocolError(w, protocol, "InvalidAction", "operation is not implemented", http.StatusBadRequest)
	}
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

func (s *Server) detectOperation(r *http.Request) (protocolKind, string, error) {
	target := r.Header.Get("X-Amz-Target")
	if strings.HasPrefix(target, "AmazonSQS.") {
		return protocolJSON, strings.TrimPrefix(target, "AmazonSQS."), nil
	}
	if r.Method == http.MethodGet {
		if err := validateQueryAPIVersion(r.URL.Query().Get("Version")); err != nil {
			return protocolQuery, "", err
		}
		return protocolQuery, r.URL.Query().Get("Action"), nil
	}
	if strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return protocolQuery, "", err
		}
		if err := validateQueryAPIVersion(r.Form.Get("Version")); err != nil {
			return protocolQuery, "", err
		}
		return protocolQuery, r.Form.Get("Action"), nil
	}
	if strings.Contains(r.Header.Get("Content-Type"), "application/x-amz-json-1.0") {
		return protocolJSON, "", errors.New("missing X-Amz-Target")
	}
	return protocolQuery, "", errors.New("missing SQS action")
}

func validateQueryAPIVersion(version string) error {
	if version == "" {
		return sqsRequestError{
			Code:    "MissingParameter",
			Message: "Version is required for SQS Query protocol",
			Status:  http.StatusBadRequest,
		}
	}
	if version != sqsAPIVersion {
		return sqsRequestError{
			Code:    "InvalidParameterValue",
			Message: "Version must be " + sqsAPIVersion,
			Status:  http.StatusBadRequest,
		}
	}
	return nil
}

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

func (s *Server) handleListDeadLetterSourceQueues(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	queueURL, err := requestString(r, protocol, "QueueUrl")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	urls, err := s.listDeadLetterSourceQueueURLs(queueURL)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, listDeadLetterSourceQueuesXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: listDeadLetterSourceQueuesXMLResult{QueueURLs: urls},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"QueueUrls": urls})
}

func (s *Server) handleStartMessageMoveTask(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseStartMessageMoveTaskRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	task, err := s.startMessageMoveTask(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, startMessageMoveTaskXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: startMessageMoveTaskXMLResult{TaskHandle: task.TaskHandle},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"TaskHandle": task.TaskHandle})
}

func (s *Server) handleListMessageMoveTasks(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseListMessageMoveTasksRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	tasks, err := s.listMessageMoveTasks(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, listMessageMoveTasksXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: listMessageMoveTasksXMLResult{Results: moveTasksToXML(tasks)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string][]messageMoveTaskResult{"Results": moveTasksToResults(tasks)})
}

func (s *Server) handleCancelMessageMoveTask(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	taskHandle, err := requestString(r, protocol, "TaskHandle")
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	moved, err := s.cancelMessageMoveTask(taskHandle)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, cancelMessageMoveTaskXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: cancelMessageMoveTaskXMLResult{ApproximateNumberOfMessagesMoved: moved},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"ApproximateNumberOfMessagesMoved": moved})
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

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseSendMessageRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	message, err := s.sendMessage(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, sendMessageXMLResponse{
			Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: sendMessageXMLResult{
				MessageID:                    message.ID,
				MD5OfMessageBody:             message.BodyMD5,
				MD5OfMessageAttributes:       md5OfMessageAttributes(message.Attributes),
				MD5OfMessageSystemAttributes: md5OfMessageAttributes(message.SystemAttributes),
				SequenceNumber:               message.SequenceNumber,
			},
			Meta: responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	response := map[string]string{
		"MessageId":        message.ID,
		"MD5OfMessageBody": message.BodyMD5,
	}
	if message.SequenceNumber != "" {
		response["SequenceNumber"] = message.SequenceNumber
	}
	if attrMD5 := md5OfMessageAttributes(message.Attributes); attrMD5 != "" {
		response["MD5OfMessageAttributes"] = attrMD5
	}
	if attrMD5 := md5OfMessageAttributes(message.SystemAttributes); attrMD5 != "" {
		response["MD5OfMessageSystemAttributes"] = attrMD5
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleSendMessageBatch(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseSendMessageBatchRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.sendMessageBatch(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, sendMessageBatchXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: sendMessageBatchXMLResult{Successful: batchSuccessfulToXML(result.Successful), Failed: batchFailedToXML(result.Failed)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleReceiveMessage(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseReceiveMessageRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	messages, err := s.receiveMessages(r.Context(), input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, receiveMessageXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: receiveMessageXMLResult{Messages: messagesToXML(messages)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string][]receivedMessage{"Messages": messages})
}

func (s *Server) handleDeleteMessage(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseReceiptRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.deleteMessage(input.QueueURL, input.ReceiptHandle); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "DeleteMessage")
}

func (s *Server) handleDeleteMessageBatch(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseDeleteMessageBatchRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.deleteMessageBatch(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, deleteMessageBatchXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: deleteMessageBatchXMLResult{Successful: deleteBatchSuccessfulToXML(result.Successful), Failed: batchFailedToXML(result.Failed)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleChangeMessageVisibility(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseVisibilityRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.changeMessageVisibility(input.QueueURL, input.ReceiptHandle, input.VisibilityTimeout); err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	writeEmptySuccess(w, protocol, "ChangeMessageVisibility")
}

func (s *Server) handleChangeMessageVisibilityBatch(w http.ResponseWriter, r *http.Request, protocol protocolKind) {
	input, err := parseChangeMessageVisibilityBatchRequest(r, protocol)
	if err != nil {
		writeProtocolError(w, protocol, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.changeMessageVisibilityBatch(input)
	if err != nil {
		writeProtocolError(w, protocol, errorCode(err), err.Error(), http.StatusBadRequest)
		return
	}
	if protocol == protocolQuery {
		writeQueryXML(w, http.StatusOK, changeMessageVisibilityBatchXMLResponse{
			Xmlns:  "http://queue.amazonaws.com/doc/2012-11-05/",
			Result: changeMessageVisibilityBatchXMLResult{Successful: visibilityBatchSuccessfulToXML(result.Successful), Failed: batchFailedToXML(result.Failed)},
			Meta:   responseMetadataXML{RequestID: "devcloud-sqs"},
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
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

func (s *Server) listDeadLetterSourceQueueURLs(queueURL string) ([]string, error) {
	name := queueNameFromURL(queueURL)
	if name == "" {
		return nil, errors.New("queue does not exist")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dlq, ok := s.queues[name]
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	names := make([]string, 0)
	for sourceName, source := range s.queues {
		policy, ok := redrivePolicyFromQueue(source)
		if ok && policy.DeadLetterTargetARN == dlq.ARN {
			names = append(names, sourceName)
		}
	}
	sort.Strings(names)
	urls := make([]string, 0, len(names))
	for _, sourceName := range names {
		urls = append(urls, s.queues[sourceName].URL)
	}
	return urls, nil
}

func (s *Server) startMessageMoveTask(input startMessageMoveTaskRequest) (moveTaskState, error) {
	if input.SourceARN == "" {
		return moveTaskState{}, errors.New("SourceArn is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	source := s.queueByARNLocked(input.SourceARN)
	if source == nil {
		return moveTaskState{}, errors.New("queue does not exist")
	}
	now := time.Now().UTC()
	destinationARN := input.DestinationARN
	if destinationARN != "" && s.queueByARNLocked(destinationARN) == nil {
		return moveTaskState{}, errors.New("destination queue does not exist")
	}

	moved := 0
	for _, message := range source.Messages {
		if message.Deleted {
			continue
		}
		targetARN := destinationARN
		if targetARN == "" {
			targetARN = message.DeadLetterSourceARN
		}
		if targetARN == "" {
			continue
		}
		destination := s.queueByARNLocked(targetARN)
		if destination == nil {
			continue
		}
		redriven := cloneMessage(message)
		redriven.AvailableAt = now
		redriven.InvisibleUntil = time.Time{}
		redriven.ReceiptHandle = ""
		redriven.ReceiveCount = 0
		redriven.FirstReceiveAt = time.Time{}
		redriven.Deleted = false
		redriven.DeadLetterSourceARN = ""
		destination.Messages = append(destination.Messages, redriven)
		message.Deleted = true
		message.ReceiptHandle = ""
		moved++
	}
	task := moveTaskState{
		TaskHandle:                       newOpaqueID("mvt"),
		SourceARN:                        input.SourceARN,
		DestinationARN:                   input.DestinationARN,
		Status:                           "COMPLETED",
		StartedAt:                        now,
		ApproximateNumberOfMessagesMoved: moved,
	}
	s.moveTasks[task.TaskHandle] = task
	if err := s.persistLocked(); err != nil {
		return moveTaskState{}, err
	}
	return task, nil
}

func (s *Server) listMessageMoveTasks(input listMessageMoveTasksRequest) ([]moveTaskState, error) {
	if input.SourceARN == "" {
		return nil, errors.New("SourceArn is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueByARNLocked(input.SourceARN) == nil {
		return nil, errors.New("queue does not exist")
	}
	tasks := make([]moveTaskState, 0)
	for _, task := range s.moveTasks {
		if task.SourceARN == input.SourceARN {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].StartedAt.After(tasks[j].StartedAt)
	})
	maxResults := input.MaxResults
	if maxResults <= 0 || maxResults > 10 {
		maxResults = 10
	}
	if len(tasks) > maxResults {
		tasks = tasks[:maxResults]
	}
	return tasks, nil
}

func (s *Server) cancelMessageMoveTask(taskHandle string) (int, error) {
	if taskHandle == "" {
		return 0, errors.New("TaskHandle is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.moveTasks[taskHandle]
	if !ok {
		return 0, errors.New("message move task does not exist")
	}
	return task.ApproximateNumberOfMessagesMoved, nil
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

func (s *Server) sendMessage(input sendMessageRequest) (*messageState, error) {
	if input.QueueURL == "" {
		return nil, errors.New("QueueUrl is required")
	}
	if input.MessageBody == "" {
		return nil, errors.New("MessageBody is required")
	}
	if maxBytes := s.maxMessageBytes(); maxBytes > 0 && int64(len([]byte(input.MessageBody))) > maxBytes {
		return nil, errors.New("MessageBody exceeds maximum message size")
	}
	if !validMessageBody(input.MessageBody) {
		return nil, errors.New("MessageBody contains invalid characters")
	}
	if err := validateMessageAttributes(input.MessageAttributes); err != nil {
		return nil, err
	}
	if err := validateMessageSystemAttributes(input.MessageSystemAttributes); err != nil {
		return nil, err
	}
	name := queueNameFromURL(input.QueueURL)
	if name == "" {
		return nil, errors.New("queue does not exist")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	if isFIFOQueue(queue) && input.DelaySeconds != nil {
		return nil, errors.New("DelaySeconds is not supported for FIFO queue messages")
	}
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	delaySeconds := intAttribute(queue.Attributes, "DelaySeconds", 0)
	if input.DelaySeconds != nil {
		delaySeconds = *input.DelaySeconds
	}
	if delaySeconds < 0 {
		return nil, errors.New("DelaySeconds must be non-negative")
	}
	if delaySeconds > maxDelaySeconds {
		return nil, errors.New("DelaySeconds must be no greater than 900")
	}
	message := &messageState{
		ID:               newOpaqueID("msg"),
		Body:             input.MessageBody,
		BodyMD5:          md5Hex(input.MessageBody),
		Attributes:       copyMessageAttributes(input.MessageAttributes),
		SystemAttributes: copyMessageAttributes(input.MessageSystemAttributes),
		SentAt:           now,
		AvailableAt:      now.Add(time.Duration(delaySeconds) * time.Second),
		MessageGroupID:   input.MessageGroupID,
	}
	if isFIFOQueue(queue) {
		deduplicationID, err := fifoDeduplicationID(queue, input)
		if err != nil {
			return nil, err
		}
		cleanupExpiredDeduplicationLocked(queue, now)
		if deduped, ok := queue.Dedup[deduplicationID]; ok && now.Before(deduped.ExpiresAt) {
			return cloneMessage(deduped.Message), nil
		}
		queue.Sequence++
		message.DeduplicationID = deduplicationID
		message.SequenceNumber = strconv.FormatUint(queue.Sequence, 10)
		if queue.Dedup == nil {
			queue.Dedup = map[string]deduplicationState{}
		}
		queue.Dedup[deduplicationID] = deduplicationState{
			ExpiresAt: now.Add(fifoDeduplicationWindow),
			Message:   cloneMessage(message),
		}
	}
	queue.Messages = append(queue.Messages, message)
	if err := s.persistLocked(); err != nil {
		queue.Messages = queue.Messages[:len(queue.Messages)-1]
		return nil, err
	}
	if !now.Before(message.AvailableAt) {
		s.notifyWaitersLocked()
	}
	return cloneMessage(message), nil
}

func (s *Server) sendMessageBatch(input sendMessageBatchRequest) (sendMessageBatchResult, error) {
	if input.QueueURL == "" {
		return sendMessageBatchResult{}, errors.New("QueueUrl is required")
	}
	if len(input.Entries) == 0 {
		return sendMessageBatchResult{}, errors.New("Entries is required")
	}
	if len(input.Entries) > 10 {
		return sendMessageBatchResult{}, errors.New("Entries must contain no more than 10 entries")
	}
	seenIDs := map[string]struct{}{}
	result := sendMessageBatchResult{
		Successful: []sendMessageBatchResultEntry{},
		Failed:     []batchResultErrorEntry{},
	}
	for _, entry := range input.Entries {
		if entry.ID == "" {
			return sendMessageBatchResult{}, errors.New("batch entry Id is required")
		}
		if !validBatchEntryID(entry.ID) {
			return sendMessageBatchResult{}, errors.New("batch entry Id may contain only alphanumeric characters, hyphens, and underscores, and must be no longer than 80 characters")
		}
		if _, exists := seenIDs[entry.ID]; exists {
			return sendMessageBatchResult{}, errors.New("batch entry Id must be unique")
		}
		seenIDs[entry.ID] = struct{}{}

		message, err := s.sendMessage(sendMessageRequest{
			QueueURL:                input.QueueURL,
			MessageBody:             entry.MessageBody,
			DelaySeconds:            entry.DelaySeconds,
			MessageAttributes:       entry.MessageAttributes,
			MessageSystemAttributes: entry.MessageSystemAttributes,
			MessageGroupID:          entry.MessageGroupID,
			MessageDeduplicationID:  entry.MessageDeduplicationID,
		})
		if err != nil {
			result.Failed = append(result.Failed, batchResultErrorEntry{
				ID:          entry.ID,
				SenderFault: true,
				Code:        errorCode(err),
				Message:     err.Error(),
			})
			continue
		}
		result.Successful = append(result.Successful, sendMessageBatchResultEntry{
			ID:                           entry.ID,
			MessageID:                    message.ID,
			MD5OfMessageBody:             message.BodyMD5,
			MD5OfMessageAttributes:       md5OfMessageAttributes(message.Attributes),
			MD5OfMessageSystemAttributes: md5OfMessageAttributes(message.SystemAttributes),
			SequenceNumber:               message.SequenceNumber,
		})
	}
	return result, nil
}

func (s *Server) deleteMessageBatch(input deleteMessageBatchRequest) (deleteMessageBatchResult, error) {
	if input.QueueURL == "" {
		return deleteMessageBatchResult{}, errors.New("QueueUrl is required")
	}
	if len(input.Entries) == 0 {
		return deleteMessageBatchResult{}, errors.New("Entries is required")
	}
	if len(input.Entries) > 10 {
		return deleteMessageBatchResult{}, errors.New("Entries must contain no more than 10 entries")
	}
	seenIDs := map[string]struct{}{}
	result := deleteMessageBatchResult{
		Successful: []deleteMessageBatchResultEntry{},
		Failed:     []batchResultErrorEntry{},
	}
	for _, entry := range input.Entries {
		if entry.ID == "" {
			return deleteMessageBatchResult{}, errors.New("batch entry Id is required")
		}
		if !validBatchEntryID(entry.ID) {
			return deleteMessageBatchResult{}, errors.New("batch entry Id may contain only alphanumeric characters, hyphens, and underscores, and must be no longer than 80 characters")
		}
		if _, exists := seenIDs[entry.ID]; exists {
			return deleteMessageBatchResult{}, errors.New("batch entry Id must be unique")
		}
		seenIDs[entry.ID] = struct{}{}

		if err := s.deleteMessage(input.QueueURL, entry.ReceiptHandle); err != nil {
			result.Failed = append(result.Failed, batchResultErrorEntry{
				ID:          entry.ID,
				SenderFault: true,
				Code:        errorCode(err),
				Message:     err.Error(),
			})
			continue
		}
		result.Successful = append(result.Successful, deleteMessageBatchResultEntry{ID: entry.ID})
	}
	return result, nil
}

func (s *Server) changeMessageVisibilityBatch(input changeMessageVisibilityBatchRequest) (changeMessageVisibilityBatchResult, error) {
	if input.QueueURL == "" {
		return changeMessageVisibilityBatchResult{}, errors.New("QueueUrl is required")
	}
	if len(input.Entries) == 0 {
		return changeMessageVisibilityBatchResult{}, errors.New("Entries is required")
	}
	if len(input.Entries) > 10 {
		return changeMessageVisibilityBatchResult{}, errors.New("Entries must contain no more than 10 entries")
	}
	seenIDs := map[string]struct{}{}
	result := changeMessageVisibilityBatchResult{
		Successful: []changeMessageVisibilityBatchResultEntry{},
		Failed:     []batchResultErrorEntry{},
	}
	for _, entry := range input.Entries {
		if entry.ID == "" {
			return changeMessageVisibilityBatchResult{}, errors.New("batch entry Id is required")
		}
		if !validBatchEntryID(entry.ID) {
			return changeMessageVisibilityBatchResult{}, errors.New("batch entry Id may contain only alphanumeric characters, hyphens, and underscores, and must be no longer than 80 characters")
		}
		if _, exists := seenIDs[entry.ID]; exists {
			return changeMessageVisibilityBatchResult{}, errors.New("batch entry Id must be unique")
		}
		seenIDs[entry.ID] = struct{}{}

		if err := s.changeMessageVisibility(input.QueueURL, entry.ReceiptHandle, entry.VisibilityTimeout); err != nil {
			result.Failed = append(result.Failed, batchResultErrorEntry{
				ID:          entry.ID,
				SenderFault: true,
				Code:        errorCode(err),
				Message:     err.Error(),
			})
			continue
		}
		result.Successful = append(result.Successful, changeMessageVisibilityBatchResultEntry{ID: entry.ID})
	}
	return result, nil
}

func (s *Server) receiveMessages(ctx context.Context, input receiveMessageRequest) ([]receivedMessage, error) {
	if input.QueueURL == "" {
		return nil, errors.New("QueueUrl is required")
	}
	name := queueNameFromURL(input.QueueURL)
	if name == "" {
		return nil, errors.New("queue does not exist")
	}
	maxMessages := 1
	if input.MaxNumberOfMessages != nil {
		maxMessages = *input.MaxNumberOfMessages
	}
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > s.maxReceiveBatchSize() {
		maxMessages = s.maxReceiveBatchSize()
	}
	waitSeconds := s.defaultReceiveWaitTimeSeconds()
	if input.WaitTimeSeconds != nil {
		waitSeconds = *input.WaitTimeSeconds
	}
	if waitSeconds < 0 {
		return nil, errors.New("WaitTimeSeconds must be non-negative")
	}
	if waitSeconds > 20 {
		return nil, errors.New("WaitTimeSeconds must be no greater than 20")
	}

	deadline := time.Now().UTC().Add(time.Duration(waitSeconds) * time.Second)
	for {
		messages, err := s.receiveAvailableMessages(name, maxMessages, input.VisibilityTimeout, input.MessageAttributeNames, requestedSystemAttributeNames(input))
		if err != nil || len(messages) > 0 || time.Now().UTC().After(deadline) {
			return messages, err
		}
		waitCh := s.currentWaitChannel()
		sleepFor := time.Until(deadline)
		if sleepFor > 100*time.Millisecond {
			sleepFor = 100 * time.Millisecond
		}
		timer := time.NewTimer(sleepFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *Server) currentWaitChannel() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waitCh
}

func (s *Server) notifyWaitersLocked() {
	close(s.waitCh)
	s.waitCh = make(chan struct{})
}

func (s *Server) receiveAvailableMessages(name string, maxMessages int, visibilityOverride *int, messageAttributeNames []string, systemAttributeNames []string) ([]receivedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	visibilitySeconds := intAttribute(queue.Attributes, "VisibilityTimeout", s.defaultVisibilityTimeoutSeconds())
	if visibilityOverride != nil {
		visibilitySeconds = *visibilityOverride
	}
	if visibilitySeconds < 0 {
		return nil, errors.New("VisibilityTimeout must be non-negative")
	}
	if visibilitySeconds > maxVisibilityTimeoutSeconds {
		return nil, errors.New("VisibilityTimeout must be no greater than 43200")
	}
	messages := make([]receivedMessage, 0, maxMessages)
	blockedFIFOMessageGroups := map[string]struct{}{}
	deliveredFIFOMessageGroups := map[string]struct{}{}
	fifoQueue := isFIFOQueue(queue)
	changed := false
	for _, message := range queue.Messages {
		if len(messages) >= maxMessages {
			break
		}
		if message.Deleted {
			continue
		}
		if fifoQueue && message.MessageGroupID != "" {
			if _, blocked := blockedFIFOMessageGroups[message.MessageGroupID]; blocked {
				continue
			}
			if _, delivered := deliveredFIFOMessageGroups[message.MessageGroupID]; delivered {
				continue
			}
		}
		if now.Before(message.AvailableAt) || now.Before(message.InvisibleUntil) {
			if fifoQueue && message.MessageGroupID != "" {
				blockedFIFOMessageGroups[message.MessageGroupID] = struct{}{}
			}
			continue
		}
		if s.moveToDeadLetterQueueIfNeededLocked(queue, message, now) {
			changed = true
			if fifoQueue && message.MessageGroupID != "" {
				blockedFIFOMessageGroups[message.MessageGroupID] = struct{}{}
			}
			continue
		}
		message.ReceiveCount++
		if message.FirstReceiveAt.IsZero() {
			message.FirstReceiveAt = now
		}
		message.ReceiptHandle = newOpaqueID("rct")
		message.InvisibleUntil = now.Add(time.Duration(visibilitySeconds) * time.Second)
		messages = append(messages, receivedMessageFromState(message, messageAttributeNames, systemAttributeNames))
		changed = true
		if fifoQueue && message.MessageGroupID != "" {
			deliveredFIFOMessageGroups[message.MessageGroupID] = struct{}{}
		}
	}
	if changed {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func (s *Server) moveToDeadLetterQueueIfNeededLocked(queue *queueState, message *messageState, now time.Time) bool {
	policy, ok := redrivePolicyFromQueue(queue)
	if !ok || message.ReceiveCount < policy.MaxReceiveCount {
		return false
	}
	dlq := s.queueByARNLocked(policy.DeadLetterTargetARN)
	if dlq == nil {
		return false
	}
	moved := cloneMessage(message)
	moved.AvailableAt = now
	moved.InvisibleUntil = time.Time{}
	moved.ReceiptHandle = ""
	moved.ReceiveCount = 0
	moved.FirstReceiveAt = time.Time{}
	moved.Deleted = false
	moved.DeadLetterSourceARN = queue.ARN
	dlq.Messages = append(dlq.Messages, moved)
	message.Deleted = true
	message.ReceiptHandle = ""
	return true
}

func (s *Server) deleteMessage(queueURL string, receiptHandle string) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if receiptHandle == "" {
		return errors.New("ReceiptHandle is required")
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
	now := time.Now().UTC()
	for _, message := range queue.Messages {
		if !message.Deleted && message.ReceiptHandle == receiptHandle {
			if !now.Before(message.InvisibleUntil) {
				message.ReceiptHandle = ""
				if err := s.persistLocked(); err != nil {
					return err
				}
				return errors.New("receipt handle is invalid")
			}
			message.Deleted = true
			message.ReceiptHandle = ""
			return s.persistLocked()
		}
	}
	return errors.New("receipt handle is invalid")
}

func (s *Server) changeMessageVisibility(queueURL string, receiptHandle string, visibilitySeconds int) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if receiptHandle == "" {
		return errors.New("ReceiptHandle is required")
	}
	if visibilitySeconds < 0 {
		return errors.New("VisibilityTimeout must be non-negative")
	}
	if visibilitySeconds > maxVisibilityTimeoutSeconds {
		return errors.New("VisibilityTimeout must be no greater than 43200")
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
	now := time.Now().UTC()
	for _, message := range queue.Messages {
		if !message.Deleted && message.ReceiptHandle == receiptHandle {
			if !now.Before(message.InvisibleUntil) {
				message.ReceiptHandle = ""
				if err := s.persistLocked(); err != nil {
					return err
				}
				return errors.New("receipt handle is invalid")
			}
			message.InvisibleUntil = now.Add(time.Duration(visibilitySeconds) * time.Second)
			if err := s.persistLocked(); err != nil {
				return err
			}
			if visibilitySeconds == 0 {
				s.notifyWaitersLocked()
			}
			return nil
		}
	}
	return errors.New("receipt handle is invalid")
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

type persistedState struct {
	Queues    map[string]persistedQueue `json:"queues"`
	MoveTasks map[string]moveTaskState  `json:"moveTasks,omitempty"`
}

type persistedQueue struct {
	Name       string                        `json:"name"`
	URL        string                        `json:"url"`
	ARN        string                        `json:"arn"`
	Attributes map[string]string             `json:"attributes"`
	Tags       map[string]string             `json:"tags,omitempty"`
	CreatedAt  time.Time                     `json:"createdAt"`
	ModifiedAt time.Time                     `json:"modifiedAt,omitempty"`
	Messages   []*messageState               `json:"messages,omitempty"`
	Sequence   uint64                        `json:"sequence,omitempty"`
	Dedup      map[string]deduplicationState `json:"dedup,omitempty"`
}

func (s *Server) load() error {
	path := filepath.Join(s.config.StoragePath, "state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	if persisted.Queues == nil {
		persisted.Queues = map[string]persistedQueue{}
	}
	if persisted.MoveTasks == nil {
		persisted.MoveTasks = map[string]moveTaskState{}
	}
	s.moveTasks = map[string]moveTaskState{}
	for taskHandle, task := range persisted.MoveTasks {
		if taskHandle == "" {
			taskHandle = task.TaskHandle
		}
		if taskHandle == "" {
			continue
		}
		if task.TaskHandle == "" {
			task.TaskHandle = taskHandle
		}
		s.moveTasks[taskHandle] = task
	}
	now := time.Now().UTC()
	for name, persistedQueue := range persisted.Queues {
		if name == "" {
			name = persistedQueue.Name
		}
		if name == "" {
			continue
		}
		queue := &queueState{
			Name:       name,
			URL:        persistedQueue.URL,
			ARN:        persistedQueue.ARN,
			Attributes: copyAttributes(persistedQueue.Attributes),
			Tags:       copyAttributes(persistedQueue.Tags),
			CreatedAt:  persistedQueue.CreatedAt,
			ModifiedAt: persistedQueue.ModifiedAt,
			Messages:   cloneMessages(persistedQueue.Messages),
			Sequence:   persistedQueue.Sequence,
			Dedup:      cloneDeduplication(persistedQueue.Dedup),
		}
		if queue.URL == "" {
			queue.URL = s.queueURL(name)
		}
		if queue.ARN == "" {
			queue.ARN = s.queueARN(name)
		}
		if len(queue.Attributes) == 0 {
			queue.Attributes = s.defaultQueueAttributes()
		}
		if queue.Tags == nil {
			queue.Tags = map[string]string{}
		}
		if queue.CreatedAt.IsZero() {
			queue.CreatedAt = now
		}
		if queue.ModifiedAt.IsZero() {
			queue.ModifiedAt = queue.CreatedAt
		}
		if queue.Dedup == nil {
			queue.Dedup = map[string]deduplicationState{}
		}
		cleanupExpiredMessagesLocked(queue, now)
		s.queues[name] = queue
	}
	return nil
}

func (s *Server) persistLocked() error {
	if strings.TrimSpace(s.config.StoragePath) == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
		return err
	}
	persisted := persistedState{
		Queues:    map[string]persistedQueue{},
		MoveTasks: map[string]moveTaskState{},
	}
	for name, queue := range s.queues {
		persisted.Queues[name] = persistedQueue{
			Name:       queue.Name,
			URL:        queue.URL,
			ARN:        queue.ARN,
			Attributes: copyAttributes(queue.Attributes),
			Tags:       copyAttributes(queue.Tags),
			CreatedAt:  queue.CreatedAt,
			ModifiedAt: queueLastModifiedAt(queue),
			Messages:   cloneMessages(queue.Messages),
			Sequence:   queue.Sequence,
			Dedup:      cloneDeduplication(queue.Dedup),
		}
	}
	for taskHandle, task := range s.moveTasks {
		persisted.MoveTasks[taskHandle] = task
	}
	path := filepath.Join(s.config.StoragePath, "state.json")
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(persisted)
	closeErr := file.Close()
	if encodeErr != nil {
		os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, path)
}

func (s *Server) maxQueues() int {
	if s.config.MaxQueues <= 0 {
		return 256
	}
	return s.config.MaxQueues
}

func (s *Server) maxMessageBytes() int64 {
	if s.config.MaxMessageBytes <= 0 {
		return 1024 * 1024
	}
	return s.config.MaxMessageBytes
}

func (s *Server) maxReceiveBatchSize() int {
	if s.config.MaxReceiveBatchSize <= 0 {
		return 10
	}
	if s.config.MaxReceiveBatchSize > 10 {
		return 10
	}
	return s.config.MaxReceiveBatchSize
}

func (s *Server) defaultVisibilityTimeoutSeconds() int {
	if s.config.DefaultVisibilityTimeoutSeconds < 0 {
		return 30
	}
	if s.config.DefaultVisibilityTimeoutSeconds == 0 {
		return 30
	}
	return s.config.DefaultVisibilityTimeoutSeconds
}

func (s *Server) defaultDelaySeconds() int {
	if s.config.DefaultDelaySeconds < 0 {
		return 0
	}
	return s.config.DefaultDelaySeconds
}

func (s *Server) defaultMessageRetentionSeconds() int {
	if s.config.DefaultMessageRetentionSeconds <= 0 {
		return 345600
	}
	return s.config.DefaultMessageRetentionSeconds
}

func (s *Server) defaultReceiveWaitTimeSeconds() int {
	if s.config.DefaultReceiveWaitTimeSeconds < 0 {
		return 0
	}
	if s.config.DefaultReceiveWaitTimeSeconds > 20 {
		return 20
	}
	return s.config.DefaultReceiveWaitTimeSeconds
}

func cloneQueue(queue *queueState) *queueState {
	return &queueState{
		Name:       queue.Name,
		URL:        queue.URL,
		ARN:        queue.ARN,
		Attributes: copyAttributes(queue.Attributes),
		Tags:       copyAttributes(queue.Tags),
		CreatedAt:  queue.CreatedAt,
		ModifiedAt: queueLastModifiedAt(queue),
	}
}

func queueLastModifiedAt(queue *queueState) time.Time {
	if queue.ModifiedAt.IsZero() {
		return queue.CreatedAt
	}
	return queue.ModifiedAt
}

func copyAttributes(attrs map[string]string) map[string]string {
	copied := make(map[string]string, len(attrs))
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
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

func isRootPath(path string) bool {
	return path == "" || path == "/"
}

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

func moveTasksToResults(tasks []moveTaskState) []messageMoveTaskResult {
	result := make([]messageMoveTaskResult, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, messageMoveTaskResult{
			TaskHandle:                       task.TaskHandle,
			Status:                           task.Status,
			SourceARN:                        task.SourceARN,
			DestinationARN:                   task.DestinationARN,
			ApproximateNumberOfMessagesMoved: task.ApproximateNumberOfMessagesMoved,
		})
	}
	return result
}

func moveTasksToXML(tasks []moveTaskState) []messageMoveTaskResultXML {
	result := make([]messageMoveTaskResultXML, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, messageMoveTaskResultXML{
			TaskHandle:                       task.TaskHandle,
			Status:                           task.Status,
			SourceARN:                        task.SourceARN,
			DestinationARN:                   task.DestinationARN,
			ApproximateNumberOfMessagesMoved: task.ApproximateNumberOfMessagesMoved,
		})
	}
	return result
}

func receivedMessageFromState(message *messageState, messageAttributeNames []string, systemAttributeNames []string) receivedMessage {
	attrs := map[string]string{
		"ApproximateReceiveCount": strconv.Itoa(message.ReceiveCount),
		"SentTimestamp":           strconv.FormatInt(message.SentAt.UnixMilli(), 10),
	}
	if !message.FirstReceiveAt.IsZero() {
		attrs["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(message.FirstReceiveAt.UnixMilli(), 10)
	}
	if wantsAny(systemAttributeNames, "AWSTraceHeader") {
		if traceHeader := message.SystemAttributes["AWSTraceHeader"].StringValue; traceHeader != "" {
			attrs["AWSTraceHeader"] = traceHeader
		}
	}
	response := receivedMessage{
		MessageID:        message.ID,
		ReceiptHandle:    message.ReceiptHandle,
		MD5OfMessageBody: message.BodyMD5,
		Body:             message.Body,
		Attributes:       attrs,
	}
	if wantsAll(messageAttributeNames) {
		response.MessageAttributes = copyMessageAttributes(message.Attributes)
		response.MD5OfMessageAttributes = md5OfMessageAttributes(response.MessageAttributes)
	} else if filtered := filterMessageAttributes(message.Attributes, messageAttributeNames); len(filtered) > 0 {
		response.MessageAttributes = filtered
		response.MD5OfMessageAttributes = md5OfMessageAttributes(filtered)
	}
	if wantsAny(systemAttributeNames, "AWSTraceHeader") {
		response.MD5OfMessageSystemAttributes = md5OfMessageAttributes(message.SystemAttributes)
	}
	return response
}

func wantsAll(names []string) bool {
	for _, name := range names {
		if name == "All" || name == ".*" {
			return true
		}
	}
	return false
}

func wantsAny(names []string, target string) bool {
	if wantsAll(names) {
		return true
	}
	for _, name := range names {
		if name == target {
			return true
		}
	}
	return false
}

func filterMessageAttributes(attrs map[string]messageAttributeValue, names []string) map[string]messageAttributeValue {
	if len(attrs) == 0 || len(names) == 0 {
		return nil
	}
	filtered := map[string]messageAttributeValue{}
	for _, requested := range names {
		if requested == "" {
			continue
		}
		if strings.HasSuffix(requested, ".*") {
			prefix := strings.TrimSuffix(requested, ".*")
			for name, value := range attrs {
				if strings.HasPrefix(name, prefix) {
					filtered[name] = value
				}
			}
			continue
		}
		if value, ok := attrs[requested]; ok {
			filtered[requested] = value
		}
	}
	return filtered
}

func validateMessageAttributes(attrs map[string]messageAttributeValue) error {
	for name, attr := range attrs {
		if err := validateMessageAttributeName(name); err != nil {
			return err
		}
		if err := validateMessageAttributeValue(name, attr); err != nil {
			return err
		}
	}
	return nil
}

func validateMessageAttributeName(name string) error {
	if name == "" {
		return errors.New("invalid attribute name: message attribute name is required")
	}
	if len(name) > 256 {
		return errors.New("invalid attribute name: message attribute name must be no longer than 256 characters")
	}
	if strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return errors.New("invalid attribute name: message attribute name must not start with AWS. or Amazon.")
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return errors.New("invalid attribute name: message attribute name must not start or end with a period or contain consecutive periods")
	}
	for _, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return errors.New("invalid attribute name: message attribute name contains unsupported characters")
	}
	return nil
}

func validateMessageSystemAttributes(attrs map[string]messageAttributeValue) error {
	for name, attr := range attrs {
		if name != "AWSTraceHeader" {
			return fmt.Errorf("invalid attribute value: unsupported message system attribute %s", name)
		}
		if err := validateMessageAttributeValue(name, attr); err != nil {
			return err
		}
	}
	return nil
}

func validateMessageAttributeValue(name string, attr messageAttributeValue) error {
	if strings.TrimSpace(attr.DataType) == "" {
		return fmt.Errorf("invalid attribute value for %s: DataType is required", name)
	}
	dataType := strings.ToLower(attr.DataType)
	if isUnsupportedMessageAttributeListType(dataType) {
		return fmt.Errorf("invalid attribute value for %s: list DataType is not supported", name)
	}
	switch {
	case strings.HasPrefix(dataType, "string"):
		return nil
	case strings.HasPrefix(dataType, "number"):
		if _, err := strconv.ParseFloat(attr.StringValue, 64); err != nil {
			return fmt.Errorf("invalid attribute value for %s: Number attributes must be numeric", name)
		}
		return nil
	case strings.HasPrefix(dataType, "binary"):
		if attr.BinaryValue == "" {
			return fmt.Errorf("invalid attribute value for %s: BinaryValue is required", name)
		}
		if _, err := base64.StdEncoding.DecodeString(attr.BinaryValue); err != nil {
			return fmt.Errorf("invalid attribute value for %s: BinaryValue must be base64", name)
		}
		return nil
	default:
		return fmt.Errorf("invalid attribute value for %s: unsupported DataType", name)
	}
}

func isUnsupportedMessageAttributeListType(dataType string) bool {
	return dataType == "string.list" || strings.HasPrefix(dataType, "string.list.") ||
		dataType == "binary.list" || strings.HasPrefix(dataType, "binary.list.")
}

func requestedSystemAttributeNames(input receiveMessageRequest) []string {
	if len(input.MessageSystemAttributeNames) > 0 {
		return input.MessageSystemAttributeNames
	}
	return input.AttributeNames
}

func cleanupExpiredMessagesLocked(queue *queueState, now time.Time) {
	retentionSeconds := intAttribute(queue.Attributes, "MessageRetentionPeriod", 345600)
	retained := queue.Messages[:0]
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		if retentionSeconds > 0 && now.Sub(message.SentAt) > time.Duration(retentionSeconds)*time.Second {
			continue
		}
		retained = append(retained, message)
	}
	queue.Messages = retained
	cleanupExpiredDeduplicationLocked(queue, now)
}

func cleanupExpiredDeduplicationLocked(queue *queueState, now time.Time) {
	for id, state := range queue.Dedup {
		if !now.Before(state.ExpiresAt) {
			delete(queue.Dedup, id)
		}
	}
}

func intAttribute(attrs map[string]string, key string, fallback int) int {
	value, err := strconv.Atoi(attrs[key])
	if err != nil {
		return fallback
	}
	return value
}

func copyMessageAttributes(attrs map[string]messageAttributeValue) map[string]messageAttributeValue {
	copied := make(map[string]messageAttributeValue, len(attrs))
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}

func cloneMessage(message *messageState) *messageState {
	if message == nil {
		return nil
	}
	cloned := *message
	cloned.Attributes = copyMessageAttributes(message.Attributes)
	cloned.SystemAttributes = copyMessageAttributes(message.SystemAttributes)
	return &cloned
}

func cloneMessages(messages []*messageState) []*messageState {
	cloned := make([]*messageState, 0, len(messages))
	for _, message := range messages {
		if message != nil {
			cloned = append(cloned, cloneMessage(message))
		}
	}
	return cloned
}

func cloneDeduplication(values map[string]deduplicationState) map[string]deduplicationState {
	cloned := make(map[string]deduplicationState, len(values))
	for key, value := range values {
		cloned[key] = deduplicationState{
			ExpiresAt: value.ExpiresAt,
			Message:   cloneMessage(value.Message),
		}
	}
	return cloned
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

func md5Hex(value string) string {
	sum := md5.Sum([]byte(value))
	return fmt.Sprintf("%x", sum)
}

func validMessageBody(body string) bool {
	for _, r := range body {
		if r == '\uFFFD' {
			return false
		}
		if r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		if r >= 0x20 && r <= 0xD7FF {
			continue
		}
		if r >= 0xE000 && r <= 0xFFFD {
			continue
		}
		if r >= 0x10000 && r <= 0x10FFFF {
			continue
		}
		return false
	}
	return true
}

func validBatchEntryID(id string) bool {
	return batchEntryIDPattern.MatchString(id)
}

func md5OfMessageAttributes(attrs map[string]messageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	var payload bytes.Buffer
	for _, name := range names {
		attr := attrs[name]
		writeMD5AttributeString(&payload, name)
		writeMD5AttributeString(&payload, attr.DataType)
		if strings.HasPrefix(strings.ToLower(attr.DataType), "binary") {
			payload.WriteByte(2)
			writeMD5AttributeBytes(&payload, decodeBinaryAttribute(attr.BinaryValue))
			continue
		}
		payload.WriteByte(1)
		writeMD5AttributeString(&payload, attr.StringValue)
	}
	sum := md5.Sum(payload.Bytes())
	return fmt.Sprintf("%x", sum)
}

func writeMD5AttributeString(buf *bytes.Buffer, value string) {
	writeMD5AttributeBytes(buf, []byte(value))
}

func writeMD5AttributeBytes(buf *bytes.Buffer, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	buf.Write(length[:])
	buf.Write(value)
}

func decodeBinaryAttribute(value string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		return decoded
	}
	return []byte(value)
}

func newOpaqueID(prefix string) string {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(raw[:])
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

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
