package pubsub

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func (s *Server) loadResources() error {
	if s.config.StoragePath != "" {
		data, err := os.ReadFile(s.resourceFilePath())
		if errors.Is(err, os.ErrNotExist) {
			if s.config.MessageStoragePath == "" {
				return nil
			}
		} else if err != nil {
			return err
		} else {
			var file resourceFile
			if err := json.Unmarshal(data, &file); err != nil {
				return err
			}
			s.loadResourceFile(file)
		}
	}

	if s.config.MessageStoragePath == "" {
		return nil
	}
	messageData, err := os.ReadFile(s.messageStateFilePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var messageFile messageStateFile
	if err := json.Unmarshal(messageData, &messageFile); err != nil {
		return err
	}
	s.loadMessageStateFile(messageFile)
	return nil
}

func (s *Server) loadResourceFile(file resourceFile) {
	for _, topic := range file.Topics {
		if topic.Name != "" {
			s.topics[topic.Name] = topic
		}
	}
	for _, subscription := range file.Subscriptions {
		if subscription.Name != "" {
			s.subscriptions[subscription.Name] = subscription
		}
	}
	for _, snapshot := range file.Snapshots {
		if snapshot.Name != "" {
			s.snapshots[snapshot.Name] = snapshot
		}
	}
	for _, schema := range file.Schemas {
		if schema.Name != "" {
			s.schemas[schema.Name] = schema
		}
	}
	for _, message := range file.Messages {
		if message.MessageID != "" {
			s.messages[message.MessageID] = message
			if id, err := strconv.ParseUint(message.MessageID, 10, 64); err == nil && id > s.nextMessageID {
				s.nextMessageID = id
			}
		}
	}
	for subscription, deliveries := range file.Deliveries {
		if subscription != "" {
			s.deliveries[subscription] = deliveries
		}
	}
	if file.NextMessageID > s.nextMessageID {
		s.nextMessageID = file.NextMessageID
	}
	s.nextAckID = file.NextAckID
}

func (s *Server) loadMessageStateFile(file messageStateFile) {
	s.messages = map[string]pubsubMessage{}
	s.deliveries = map[string][]deliveryRecord{}
	s.nextMessageID = 0
	for _, message := range file.Messages {
		if message.MessageID != "" {
			s.messages[message.MessageID] = message
			if id, err := strconv.ParseUint(message.MessageID, 10, 64); err == nil && id > s.nextMessageID {
				s.nextMessageID = id
			}
		}
	}
	for subscription, deliveries := range file.Deliveries {
		if subscription != "" {
			s.deliveries[subscription] = deliveries
		}
	}
	if file.NextMessageID > s.nextMessageID {
		s.nextMessageID = file.NextMessageID
	}
	s.nextAckID = file.NextAckID
}

func (s *Server) saveResourcesLocked() error {
	if s.config.StoragePath == "" && s.config.MessageStoragePath == "" {
		s.cleanupUnreferencedMessagesLocked()
		return nil
	}
	s.cleanupRetainedMessagesLocked(s.now().UTC())
	s.cleanupUnreferencedMessagesLocked()
	if s.config.StoragePath != "" {
		if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
			return err
		}
	}
	topics := make([]topicResource, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	subscriptions := make([]subscriptionResource, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Name < subscriptions[j].Name })
	snapshots := make([]snapshotResource, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	schemas := make([]schemaResource, 0, len(s.schemas))
	for _, schema := range s.schemas {
		schemas = append(schemas, schema)
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Name < schemas[j].Name })
	messages := make([]pubsubMessage, 0, len(s.messages))
	for _, message := range s.messages {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].MessageID < messages[j].MessageID })
	deliveries := make(map[string][]deliveryRecord, len(s.deliveries))
	for subscription, records := range s.deliveries {
		if len(records) > 0 {
			deliveries[subscription] = append([]deliveryRecord(nil), records...)
		}
	}
	includeMessageState := s.config.MessageStoragePath == ""
	data, err := json.MarshalIndent(s.resourceFileForSave(topics, subscriptions, snapshots, schemas, messages, deliveries, includeMessageState), "", "  ")
	if err != nil {
		return err
	}
	if s.config.StoragePath != "" {
		if err := writeJSONFileAtomically(s.resourceFilePath(), data); err != nil {
			return err
		}
	}
	if s.config.MessageStoragePath == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.MessageStoragePath, 0o755); err != nil {
		return err
	}
	messageData, err := json.MarshalIndent(messageStateFile{
		Messages:      messages,
		Deliveries:    deliveries,
		NextMessageID: s.nextMessageID,
		NextAckID:     s.nextAckID,
	}, "", "  ")
	if err != nil {
		return err
	}
	return writeJSONFileAtomically(s.messageStateFilePath(), messageData)
}

func (s *Server) resourceFileForSave(topics []topicResource, subscriptions []subscriptionResource, snapshots []snapshotResource, schemas []schemaResource, messages []pubsubMessage, deliveries map[string][]deliveryRecord, includeMessageState bool) resourceFile {
	file := resourceFile{
		Topics:        topics,
		Subscriptions: subscriptions,
		Snapshots:     snapshots,
		Schemas:       schemas,
	}
	if includeMessageState {
		file.Messages = messages
		file.Deliveries = deliveries
		file.NextMessageID = s.nextMessageID
		file.NextAckID = s.nextAckID
	}
	return file
}

func writeJSONFileAtomically(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) resourceFilePath() string {
	return filepath.Join(s.config.StoragePath, "resources.json")
}

func (s *Server) messageStateFilePath() string {
	return filepath.Join(s.config.MessageStoragePath, "pubsub.json")
}
