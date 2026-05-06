package sqs

import (
	"errors"
	"net/http"
	"strconv"
	"time"
)

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
