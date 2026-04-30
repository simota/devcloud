package mail

import (
	"time"

	"devcloud/internal/storage/blob"
)

type Message struct {
	ID          string              `json:"id"`
	From        string              `json:"from"`
	To          []string            `json:"to"`
	Subject     string              `json:"subject"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Raw         blob.ID             `json:"raw"`
	TextBody    string              `json:"textBody,omitempty"`
	HTMLBody    string              `json:"htmlBody,omitempty"`
	Attachments []Attachment        `json:"attachments,omitempty"`
	ReceivedAt  time.Time           `json:"receivedAt"`
	DeletedAt   *time.Time          `json:"deletedAt,omitempty"`
	ParseError  string              `json:"parseError,omitempty"`
}

type Attachment struct {
	ID          string  `json:"id"`
	FileName    string  `json:"fileName"`
	ContentType string  `json:"contentType"`
	Size        int64   `json:"size"`
	Blob        blob.ID `json:"blob"`
}

type Envelope struct {
	From string
	To   []string
}

type ListMessagesInput struct {
	Limit  int
	Cursor string
}

type ListMessagesResult struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"nextCursor"`
}
