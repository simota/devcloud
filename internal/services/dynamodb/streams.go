package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"
)

func (s *Server) handleListStreams(w http.ResponseWriter, r *http.Request) {
	var request listStreamsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	if request.TableName != "" {
		if _, ok := s.tables[request.TableName]; !ok {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
	}
	streams := make([]streamSummary, 0, len(s.tables))
	for _, state := range s.tables {
		description := state.description
		if request.TableName != "" && description.TableName != request.TableName {
			continue
		}
		if description.LatestStreamArn == "" || description.StreamSpecification == nil || !description.StreamSpecification.StreamEnabled {
			continue
		}
		streams = append(streams, streamSummary{
			StreamArn:   description.LatestStreamArn,
			StreamLabel: description.LatestStreamLabel,
			TableName:   description.TableName,
		})
	}
	s.mu.Unlock()
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].TableName == streams[j].TableName {
			return streams[i].StreamArn < streams[j].StreamArn
		}
		return streams[i].TableName < streams[j].TableName
	})

	start := 0
	if request.ExclusiveStartStreamArn != "" {
		start = -1
		for i, stream := range streams {
			if stream.StreamArn == request.ExclusiveStartStreamArn {
				start = i + 1
				break
			}
		}
		if start == -1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start stream arn does not exist")
			return
		}
	}
	end := len(streams)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"Streams": streams[start:end]}
	if end < len(streams) {
		response["LastEvaluatedStreamArn"] = streams[end-1].StreamArn
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDescribeStream(w http.ResponseWriter, r *http.Request) {
	var request describeStreamRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.StreamArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "stream arn is required")
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}
	if request.ExclusiveStartShardID != "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start shard id does not exist")
		return
	}

	s.mu.Lock()
	var description tableDescription
	var ok bool
	for _, state := range s.tables {
		if state.description.LatestStreamArn == request.StreamArn && state.description.StreamSpecification != nil && state.description.StreamSpecification.StreamEnabled {
			description = state.description
			ok = true
			break
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"StreamDescription": streamDescriptionForTable(description)})
}

func (s *Server) handleGetShardIterator(w http.ResponseWriter, r *http.Request) {
	var request getShardIteratorRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.StreamArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "stream arn is required")
		return
	}
	if request.ShardID == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "shard id is required")
		return
	}
	switch request.ShardIteratorType {
	case "TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
	case "":
		writeError(w, http.StatusBadRequest, "ValidationException", "shard iterator type is required")
		return
	default:
		writeError(w, http.StatusBadRequest, "ValidationException", "unsupported shard iterator type")
		return
	}
	if (request.ShardIteratorType == "AT_SEQUENCE_NUMBER" || request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER") && request.SequenceNumber == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "sequence number is required")
		return
	}
	if !s.streamShardExists(request.StreamArn, request.ShardID) {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream shard not found")
		return
	}

	position := 0
	if request.ShardIteratorType == "LATEST" {
		position = s.streamRecordCount(request.StreamArn)
	}
	if request.ShardIteratorType == "AT_SEQUENCE_NUMBER" || request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" {
		var ok bool
		position, ok = s.streamPositionForSequence(request.StreamArn, request.SequenceNumber, request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER")
		if !ok {
			writeError(w, http.StatusBadRequest, "TrimmedDataAccessException", "sequence number is invalid")
			return
		}
	}

	iterator, err := encodeStreamIterator(streamIterator{
		StreamArn: request.StreamArn,
		ShardID:   request.ShardID,
		Position:  position,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to create stream iterator")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ShardIterator": iterator})
}

func (s *Server) handleGetRecords(w http.ResponseWriter, r *http.Request) {
	var request getRecordsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ShardIterator == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "shard iterator is required")
		return
	}
	if request.Limit < 0 || request.Limit > 1000 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 1000")
		return
	}
	iterator, err := decodeStreamIterator(request.ShardIterator)
	if err != nil {
		writeError(w, http.StatusBadRequest, "TrimmedDataAccessException", "shard iterator is invalid")
		return
	}
	if !s.streamShardExists(iterator.StreamArn, iterator.ShardID) {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream shard not found")
		return
	}

	records := s.streamRecords(iterator.StreamArn, iterator.Position, request.Limit)
	nextIterator, err := encodeStreamIterator(streamIterator{
		StreamArn: iterator.StreamArn,
		ShardID:   iterator.ShardID,
		Position:  iterator.Position + len(records),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to create stream iterator")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"NextShardIterator": nextIterator,
		"Records":           records,
	})
}
func validateStreamSpecification(specification streamSpecification) error {
	if !specification.StreamEnabled {
		return nil
	}
	switch specification.StreamViewType {
	case "KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES":
		return nil
	case "":
		return errors.New("stream view type is required when stream is enabled")
	default:
		return errors.New("stream view type must be KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, or NEW_AND_OLD_IMAGES")
	}
}

func enableStreamDescription(description *tableDescription, region string, specification streamSpecification) {
	label := description.LatestStreamLabel
	if label == "" {
		label = time.Now().UTC().Format("2006-01-02T15:04:05.000")
	}
	description.LatestStreamLabel = label
	description.LatestStreamArn = description.TableArn + "/stream/" + label
	if description.LatestStreamArn == "/stream/"+label {
		description.LatestStreamArn = "arn:aws:dynamodb:" + region + ":000000000000:table/" + description.TableName + "/stream/" + label
	}
	specification.StreamEnabled = true
	description.StreamSpecification = &streamSpecification{
		StreamEnabled:  true,
		StreamViewType: specification.StreamViewType,
	}
}

func streamDescriptionForTable(description tableDescription) streamDescription {
	streamViewType := ""
	if description.StreamSpecification != nil {
		streamViewType = description.StreamSpecification.StreamViewType
	}
	return streamDescription{
		CreationRequestDateTime: description.CreationDateTime,
		KeySchema:               append([]keySchemaElement(nil), description.KeySchema...),
		Shards: []shardDescription{{
			ShardID: "shardId-000000000000",
			SequenceNumberRange: sequenceNumberRange{
				StartingSequenceNumber: "0",
			},
		}},
		StreamArn:      description.LatestStreamArn,
		StreamLabel:    description.LatestStreamLabel,
		StreamStatus:   "ENABLED",
		StreamViewType: streamViewType,
		TableName:      description.TableName,
	}
}

func (s *Server) streamShardExists(streamArn string, shardID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.tables {
		description := state.description
		if description.LatestStreamArn != streamArn || description.StreamSpecification == nil || !description.StreamSpecification.StreamEnabled {
			continue
		}
		for _, shard := range streamDescriptionForTable(description).Shards {
			if shard.ShardID == shardID {
				return true
			}
		}
	}
	return false
}

func (s *Server) streamRecordCount(streamArn string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok {
		return 0
	}
	return len(state.streamRecords)
}

func (s *Server) streamRecords(streamArn string, position int, limit int) []streamRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok || position >= len(state.streamRecords) {
		return []streamRecord{}
	}
	if position < 0 {
		position = 0
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	end := position + limit
	if end > len(state.streamRecords) {
		end = len(state.streamRecords)
	}
	return cloneStreamRecords(state.streamRecords[position:end])
}

func (s *Server) streamPositionForSequence(streamArn string, sequenceNumber string, after bool) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok {
		return 0, false
	}
	for i, record := range state.streamRecords {
		if record.DynamoDB.SequenceNumber == sequenceNumber {
			if after {
				return i + 1, true
			}
			return i, true
		}
	}
	return 0, false
}

func (s *Server) tableStateForStreamARNLocked(streamArn string) (*tableState, bool) {
	for _, state := range s.tables {
		description := state.description
		if description.LatestStreamArn == streamArn && description.StreamSpecification != nil && description.StreamSpecification.StreamEnabled {
			return state, true
		}
	}
	return nil, false
}

func (s *Server) appendStreamRecordLocked(state *tableState, eventName string, oldItem item, newItem item, oldExisted bool) {
	if state.description.StreamSpecification == nil || !state.description.StreamSpecification.StreamEnabled {
		return
	}
	if eventName == "REMOVE" && !oldExisted {
		return
	}
	source := newItem
	if eventName == "REMOVE" {
		source = oldItem
	}
	keys, err := extractKey(state.description, source)
	if err != nil {
		return
	}
	sequence := fmt.Sprintf("%d", len(state.streamRecords)+1)
	image := streamRecordImage{
		ApproximateCreationDateTime: time.Now().Unix(),
		Keys:                        cloneItem(keys),
		SequenceNumber:              sequence,
		StreamViewType:              state.description.StreamSpecification.StreamViewType,
	}
	switch image.StreamViewType {
	case "NEW_IMAGE":
		if eventName != "REMOVE" {
			image.NewImage = cloneItem(newItem)
		}
	case "OLD_IMAGE":
		if oldExisted {
			image.OldImage = cloneItem(oldItem)
		}
	case "NEW_AND_OLD_IMAGES":
		if eventName != "REMOVE" {
			image.NewImage = cloneItem(newItem)
		}
		if oldExisted {
			image.OldImage = cloneItem(oldItem)
		}
	}
	if encoded, err := json.Marshal(image); err == nil {
		image.SizeBytes = len(encoded)
	}
	state.streamRecords = append(state.streamRecords, streamRecord{
		EventID:      state.description.TableName + ":" + sequence,
		EventName:    eventName,
		EventSource:  "aws:dynamodb",
		EventVersion: "1.1",
		AWSRegion:    defaultString(s.config.Region, "us-east-1"),
		DynamoDB:     image,
	})
}

func streamEventName(existed bool, delete bool) string {
	if delete {
		return "REMOVE"
	}
	if existed {
		return "MODIFY"
	}
	return "INSERT"
}

func encodeStreamIterator(iterator streamIterator) (string, error) {
	payload, err := json.Marshal(iterator)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeStreamIterator(value string) (streamIterator, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return streamIterator{}, err
	}
	var iterator streamIterator
	if err := json.Unmarshal(payload, &iterator); err != nil {
		return streamIterator{}, err
	}
	if iterator.StreamArn == "" || iterator.ShardID == "" {
		return streamIterator{}, errors.New("invalid stream iterator")
	}
	return iterator, nil
}

func cloneStreamSpecification(value *streamSpecification) *streamSpecification {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
func cloneStreamRecords(values []streamRecord) []streamRecord {
	clone := make([]streamRecord, len(values))
	for i, value := range values {
		clone[i] = value
		clone[i].DynamoDB.Keys = cloneItem(value.DynamoDB.Keys)
		clone[i].DynamoDB.NewImage = cloneItem(value.DynamoDB.NewImage)
		clone[i].DynamoDB.OldImage = cloneItem(value.DynamoDB.OldImage)
	}
	return clone
}
