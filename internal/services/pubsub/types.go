package pubsub

import (
	"time"
)

type topicResource struct {
	Name                     string            `json:"name"`
	Labels                   map[string]string `json:"labels,omitempty"`
	CreatedAt                string            `json:"createdAt,omitempty"`
	UpdatedAt                string            `json:"updatedAt,omitempty"`
	MessageRetentionDuration string            `json:"messageRetentionDuration,omitempty"`
	SchemaSettings           map[string]any    `json:"schemaSettings,omitempty"`
	KMSKeyName               string            `json:"kmsKeyName,omitempty"`
}

type subscriptionResource struct {
	Name                      string            `json:"name"`
	Topic                     string            `json:"topic"`
	Detached                  bool              `json:"detached,omitempty"`
	Labels                    map[string]string `json:"labels,omitempty"`
	CreatedAt                 string            `json:"createdAt,omitempty"`
	UpdatedAt                 string            `json:"updatedAt,omitempty"`
	AckDeadlineSeconds        int               `json:"ackDeadlineSeconds,omitempty"`
	EnableMessageOrdering     bool              `json:"enableMessageOrdering,omitempty"`
	EnableExactlyOnceDelivery bool              `json:"enableExactlyOnceDelivery,omitempty"`
	RetainAckedMessages       bool              `json:"retainAckedMessages,omitempty"`
	MessageRetentionDuration  string            `json:"messageRetentionDuration,omitempty"`
	ExpirationPolicy          map[string]any    `json:"expirationPolicy,omitempty"`
	Filter                    string            `json:"filter,omitempty"`
	DeadLetterPolicy          map[string]any    `json:"deadLetterPolicy,omitempty"`
	RetryPolicy               map[string]any    `json:"retryPolicy,omitempty"`
	PushConfig                map[string]any    `json:"pushConfig,omitempty"`
}

type snapshotResource struct {
	Name         string            `json:"name"`
	Topic        string            `json:"topic,omitempty"`
	Subscription string            `json:"subscription,omitempty"`
	ExpireTime   string            `json:"expireTime,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Deliveries   []deliveryRecord  `json:"deliveries,omitempty"`
}

type schemaResource struct {
	Name               string                   `json:"name"`
	Type               string                   `json:"type,omitempty"`
	Definition         string                   `json:"definition,omitempty"`
	RevisionID         string                   `json:"revisionId,omitempty"`
	RevisionCreateTime string                   `json:"revisionCreateTime,omitempty"`
	Revisions          []schemaRevisionResource `json:"revisions,omitempty"`
}

type schemaRevisionResource struct {
	Type               string `json:"type,omitempty"`
	Definition         string `json:"definition,omitempty"`
	RevisionID         string `json:"revisionId,omitempty"`
	RevisionCreateTime string `json:"revisionCreateTime,omitempty"`
}

type pubsubMessage struct {
	Data        string            `json:"data,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type deliveryRecord struct {
	MessageID        string
	AckID            string
	LeaseDeadline    time.Time
	NextDeliveryTime time.Time
	DeliveryAttempt  int
	Acked            bool
}

type resourceFile struct {
	Topics        []topicResource             `json:"topics"`
	Subscriptions []subscriptionResource      `json:"subscriptions"`
	Snapshots     []snapshotResource          `json:"snapshots,omitempty"`
	Schemas       []schemaResource            `json:"schemas,omitempty"`
	Messages      []pubsubMessage             `json:"messages,omitempty"`
	Deliveries    map[string][]deliveryRecord `json:"deliveries,omitempty"`
	NextMessageID uint64                      `json:"nextMessageId,omitempty"`
	NextAckID     uint64                      `json:"nextAckId,omitempty"`
}

type messageStateFile struct {
	Messages      []pubsubMessage             `json:"messages,omitempty"`
	Deliveries    map[string][]deliveryRecord `json:"deliveries,omitempty"`
	NextMessageID uint64                      `json:"nextMessageId,omitempty"`
	NextAckID     uint64                      `json:"nextAckId,omitempty"`
}
