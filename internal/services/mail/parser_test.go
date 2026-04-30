package mail

import (
	"strings"
	"testing"
)

func TestParseMessageExtractsHeadersAndBody(t *testing.T) {
	message := ParseMessage([]byte("From: header@example.com\r\nTo: user@example.com\r\nSubject: Hello\r\n\r\nbody\r\n"), Envelope{})

	if message.From != "header@example.com" {
		t.Fatalf("From = %q", message.From)
	}
	if len(message.To) != 1 || message.To[0] != "user@example.com" {
		t.Fatalf("To = %#v", message.To)
	}
	if message.Subject != "Hello" {
		t.Fatalf("Subject = %q", message.Subject)
	}
	if message.TextBody != "body\r\n" {
		t.Fatalf("TextBody = %q", message.TextBody)
	}
	if message.ParseError != "" {
		t.Fatalf("ParseError = %q", message.ParseError)
	}
}

func TestParseMessageKeepsEnvelopeOnParseFailure(t *testing.T) {
	envelope := Envelope{
		From: "sender@example.com",
		To:   []string{"user@example.com"},
	}

	message := ParseMessage([]byte("Subject: broken\r\nnot-a-header\r\n\r\nbody"), envelope)

	if message.ParseError == "" {
		t.Fatal("ParseError is empty")
	}
	if message.From != envelope.From {
		t.Fatalf("From = %q, want %q", message.From, envelope.From)
	}
	if len(message.To) != 1 || message.To[0] != envelope.To[0] {
		t.Fatalf("To = %#v, want %#v", message.To, envelope.To)
	}
	if message.Subject != "" {
		t.Fatalf("Subject = %q, want empty on parse failure", message.Subject)
	}
}

func TestParseMessageExtractsMultipartTextAndHTMLBodies(t *testing.T) {
	raw := []byte(strings.Join([]string{
		"From: sender@example.com",
		"To: user@example.com",
		"Subject: Multipart",
		`Content-Type: multipart/alternative; boundary="devcloud-boundary"`,
		"",
		"--devcloud-boundary",
		"Content-Type: text/plain; charset=utf-8",
		"",
		"plain body",
		"--devcloud-boundary",
		"Content-Type: text/html; charset=utf-8",
		"",
		"<p>html body</p>",
		"--devcloud-boundary--",
		"",
	}, "\r\n"))

	message := ParseMessage(raw, Envelope{})

	if message.TextBody != "plain body" {
		t.Fatalf("TextBody = %q", message.TextBody)
	}
	if message.HTMLBody != "<p>html body</p>" {
		t.Fatalf("HTMLBody = %q", message.HTMLBody)
	}
}
