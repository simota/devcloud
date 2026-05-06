package sqs

import (
	"errors"
	"net/http"
	"sort"
	"time"
)

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
