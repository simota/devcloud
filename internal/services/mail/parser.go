package mail

import (
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
)

func ParseMessage(raw []byte, envelope Envelope) Message {
	msg := Message{
		From:    envelope.From,
		To:      append([]string(nil), envelope.To...),
		Headers: map[string][]string{},
	}

	parsed, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		msg.ParseError = err.Error()
		return msg
	}

	for key, values := range parsed.Header {
		msg.Headers[key] = append([]string(nil), values...)
	}
	msg.Subject = parsed.Header.Get("Subject")
	if from := parsed.Header.Get("From"); from != "" {
		msg.From = from
	}
	if to := parsed.Header.Get("To"); to != "" && len(msg.To) == 0 {
		msg.To = []string{to}
	}
	fillMessageBody(&msg, parsed.Header.Get("Content-Type"), parsed.Body)
	return msg
}

func fillMessageBody(msg *Message, contentType string, body io.Reader) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		readMultipartBody(msg, params["boundary"], body)
		return
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return
	}
	switch strings.ToLower(mediaType) {
	case "text/html":
		msg.HTMLBody = string(data)
	default:
		msg.TextBody = string(data)
	}
}

func readMultipartBody(msg *Message, boundary string, body io.Reader) {
	if boundary == "" {
		return
	}
	reader := multipart.NewReader(body, boundary)
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		fillMessageBodyPart(msg, part)
		part.Close()
	}
}

func fillMessageBodyPart(msg *Message, part *multipart.Part) {
	contentType := part.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		readMultipartBody(msg, params["boundary"], part)
		return
	}

	data, err := io.ReadAll(part)
	if err != nil {
		return
	}
	switch strings.ToLower(mediaType) {
	case "text/html":
		if msg.HTMLBody == "" {
			msg.HTMLBody = string(data)
		}
	case "text/plain", "":
		if msg.TextBody == "" {
			msg.TextBody = string(data)
		}
	}
}
