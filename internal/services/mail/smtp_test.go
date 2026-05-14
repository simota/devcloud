package mail

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"testing"
)

type recordingStore struct {
	mu       sync.Mutex
	messages []Message
	raw      []string
}

func (s *recordingStore) Append(_ context.Context, message Message, raw io.Reader) (Message, error) {
	data, err := io.ReadAll(raw)
	if err != nil {
		return Message{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, message)
	s.raw = append(s.raw, string(data))
	return message, nil
}

func (s *recordingStore) List(context.Context, ListMessagesInput) (ListMessagesResult, error) {
	return ListMessagesResult{}, nil
}

func (s *recordingStore) Get(context.Context, string) (Message, bool, error) {
	return Message{}, false, nil
}

func (s *recordingStore) GetRaw(context.Context, string) (io.ReadCloser, bool, error) {
	return nil, false, nil
}

func (s *recordingStore) Delete(context.Context, string) error {
	return nil
}

func (s *recordingStore) DeleteAll(context.Context) error {
	return nil
}

func (s *recordingStore) snapshot() ([]Message, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.messages...), append([]string(nil), s.raw...)
}

func TestSMTPSessionAcceptsAndPersistsMessage(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Hello")
	client.sendLine("")
	client.sendLine("hello")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, raw := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if messages[0].From != "sender@example.com" {
		t.Fatalf("From = %q", messages[0].From)
	}
	if len(messages[0].To) != 1 || messages[0].To[0] != "user@example.com" {
		t.Fatalf("To = %#v", messages[0].To)
	}
	if messages[0].Subject != "Hello" {
		t.Fatalf("Subject = %q", messages[0].Subject)
	}
	if len(raw) != 1 || raw[0] != "Subject: Hello\r\n\r\nhello\r\n" {
		t.Fatalf("raw = %#v", raw)
	}
}

func TestSMTPSessionAcceptsHELOAndMultipleRecipients(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("HELO localhost")
	client.expectReply("250")
	client.sendLine("NOOP")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<first@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<second@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Team")
	client.sendLine("")
	client.sendLine("hello team")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if got, want := messages[0].To, []string{"first@example.com", "second@example.com"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("To = %#v, want %#v", got, want)
	}
}

func TestSMTPSessionEHLOAdvertisesSizeLimit(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 2048}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReplyContaining("250", "SIZE 2048")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPSessionRejectsBadSequence(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("503")
	client.sendLine("DATA")
	client.expectReply("503")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionRejectsOversizeMessage(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 8}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("0123456789")
	client.sendLine(".")
	client.expectReply("552")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionRejectsAdvertisedOversizeMessage(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 8}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com> SIZE=9")
	client.expectReply("552")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("503")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionRejectsMalformedMailSizeParameter(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 8}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>SIZE=100")
	client.expectReply("500")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("503")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionAcceptsAdvertisedSizeWithinLimit(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 64}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com> SIZE=16")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Sized")
	client.sendLine("")
	client.sendLine("ok")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if messages[0].Subject != "Sized" {
		t.Fatalf("Subject = %q", messages[0].Subject)
	}
}

func TestSMTPSessionRejectsMalformedRecipientPath(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com> extra")
	client.expectReply("500")
	client.sendLine("DATA")
	client.expectReply("503")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionAcceptsNullReversePath(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Bounce")
	client.sendLine("")
	client.sendLine("delivery status")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if messages[0].From != "" {
		t.Fatalf("From = %q, want null reverse-path", messages[0].From)
	}
	if messages[0].Subject != "Bounce" {
		t.Fatalf("Subject = %q", messages[0].Subject)
	}
}

func TestSMTPSessionRSETClearsEnvelope(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<discarded@example.com>")
	client.expectReply("250")
	client.sendLine("RSET")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("503")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<kept@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: After reset")
	client.sendLine("")
	client.sendLine("body")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if len(messages[0].To) != 1 || messages[0].To[0] != "kept@example.com" {
		t.Fatalf("To = %#v", messages[0].To)
	}
}

func TestSMTPSessionRSETClearsNullReversePathState(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<>")
	client.expectReply("250")
	client.sendLine("RSET")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("503")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 0 {
		t.Fatalf("stored messages = %d, want 0", len(messages))
	}
}

func TestSMTPSessionRejectsRSETArgumentWithoutClearingEnvelope(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<kept@example.com>")
	client.expectReply("250")
	client.sendLine("RSET now")
	client.expectReply("500")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Kept")
	client.sendLine("")
	client.sendLine("body")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 1 {
		t.Fatalf("stored messages = %d, want 1", len(messages))
	}
	if len(messages[0].To) != 1 || messages[0].To[0] != "kept@example.com" {
		t.Fatalf("To = %#v", messages[0].To)
	}
}

func TestSMTPSessionDATAResetsEnvelope(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<first@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: First")
	client.sendLine("")
	client.sendLine("body")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("503")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<second@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Second")
	client.sendLine("")
	client.sendLine("body")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	messages, _ := store.snapshot()
	if len(messages) != 2 {
		t.Fatalf("stored messages = %d, want 2", len(messages))
	}
	if messages[0].Subject != "First" || messages[1].Subject != "Second" {
		t.Fatalf("subjects = %q, %q", messages[0].Subject, messages[1].Subject)
	}
	if len(messages[1].To) != 1 || messages[1].To[0] != "second@example.com" {
		t.Fatalf("second To = %#v", messages[1].To)
	}
}

func TestSMTPSessionUnstuffsDataLines(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("MAIL FROM:<sender@example.com>")
	client.expectReply("250")
	client.sendLine("RCPT TO:<user@example.com>")
	client.expectReply("250")
	client.sendLine("DATA")
	client.expectReply("354")
	client.sendLine("Subject: Dot")
	client.sendLine("")
	client.sendLine("..starts with dot")
	client.sendLine(".")
	client.expectReply("250")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done

	_, raw := store.snapshot()
	if len(raw) != 1 || !strings.Contains(raw[0], "\r\n.starts with dot\r\n") {
		t.Fatalf("raw = %#v", raw)
	}
}

func TestSMTPEHLOAdvertisesAuthWhenEnabled(t *testing.T) {
	store := &recordingStore{}
	cfg := SMTPConfig{MaxMessageBytes: 1024, AuthMode: SMTPAuthRelaxed, Username: "dev", Password: "dev"}
	client, done := startSMTPSession(t, cfg, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReplyContaining("250", "AUTH PLAIN LOGIN")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPEHLODoesNotAdvertiseAuthWhenOff(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	reply := client.readReply()
	if !strings.HasPrefix(reply, "250") {
		t.Fatalf("EHLO reply = %q", reply)
	}
	if strings.Contains(reply, "AUTH") {
		t.Fatalf("EHLO advertised AUTH when mode is off: %q", reply)
	}
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPAuthOffRejectsAuthCommand(t *testing.T) {
	store := &recordingStore{}
	client, done := startSMTPSession(t, SMTPConfig{MaxMessageBytes: 1024}, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("AUTH PLAIN " + base64.StdEncoding.EncodeToString([]byte("\x00dev\x00dev")))
	client.expectReply("502")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPAuthRelaxedAcceptsAnyCredentials(t *testing.T) {
	store := &recordingStore{}
	cfg := SMTPConfig{MaxMessageBytes: 1024, AuthMode: SMTPAuthRelaxed}
	client, done := startSMTPSession(t, cfg, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	plain := base64.StdEncoding.EncodeToString([]byte("\x00anyone@example.com\x00anything-goes"))
	client.sendLine("AUTH PLAIN " + plain)
	client.expectReply("235")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPAuthStrictAcceptsConfiguredCredentials(t *testing.T) {
	store := &recordingStore{}
	cfg := SMTPConfig{MaxMessageBytes: 1024, AuthMode: SMTPAuthStrict, Username: "configured", Password: "secret"}
	client, done := startSMTPSession(t, cfg, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	plain := base64.StdEncoding.EncodeToString([]byte("\x00configured\x00secret"))
	client.sendLine("AUTH PLAIN " + plain)
	client.expectReply("235")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPAuthStrictRejectsWrongCredentials(t *testing.T) {
	store := &recordingStore{}
	cfg := SMTPConfig{MaxMessageBytes: 1024, AuthMode: SMTPAuthStrict, Username: "configured", Password: "secret"}
	client, done := startSMTPSession(t, cfg, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	plain := base64.StdEncoding.EncodeToString([]byte("\x00configured\x00wrong"))
	client.sendLine("AUTH PLAIN " + plain)
	client.expectReply("535")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

func TestSMTPAuthLoginChallengeResponseFlow(t *testing.T) {
	store := &recordingStore{}
	cfg := SMTPConfig{MaxMessageBytes: 1024, AuthMode: SMTPAuthStrict, Username: "alice", Password: "pa55"}
	client, done := startSMTPSession(t, cfg, NewService(store))
	defer client.conn.Close()

	client.expectReply("220")
	client.sendLine("EHLO localhost")
	client.expectReply("250")
	client.sendLine("AUTH LOGIN")
	client.expectReplyContaining("334", "VXNlcm5hbWU6") // base64("Username:")
	client.sendLine(base64.StdEncoding.EncodeToString([]byte("alice")))
	client.expectReplyContaining("334", "UGFzc3dvcmQ6") // base64("Password:")
	client.sendLine(base64.StdEncoding.EncodeToString([]byte("pa55")))
	client.expectReply("235")
	client.sendLine("QUIT")
	client.expectReply("221")
	client.conn.Close()
	<-done
}

type smtpTestClient struct {
	t      *testing.T
	conn   net.Conn
	reader *textproto.Reader
}

func startSMTPSession(t *testing.T, cfg SMTPConfig, service *Service) (*smtpTestClient, <-chan struct{}) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	server := NewSMTPServer(cfg, service)
	go func() {
		defer close(done)
		server.handleConn(context.Background(), serverConn)
	}()
	return &smtpTestClient{
		t:      t,
		conn:   clientConn,
		reader: textproto.NewReader(bufio.NewReader(clientConn)),
	}, done
}

func (c *smtpTestClient) sendLine(line string) {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, line+"\r\n"); err != nil {
		c.t.Fatalf("write %q: %v", line, err)
	}
}

func (c *smtpTestClient) expectReply(prefix string) {
	c.t.Helper()
	line := c.readReply()
	if !strings.HasPrefix(line, prefix) {
		c.t.Fatalf("reply = %q, want prefix %q", line, prefix)
	}
}

func (c *smtpTestClient) expectReplyContaining(prefix string, want string) {
	c.t.Helper()
	line := c.readReply()
	if !strings.HasPrefix(line, prefix) || !strings.Contains(line, want) {
		c.t.Fatalf("reply = %q, want prefix %q and substring %q", line, prefix, want)
	}
}

func (c *smtpTestClient) readReply() string {
	c.t.Helper()
	line, err := c.reader.ReadLine()
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	lines := []string{line}
	for len(line) >= 4 && line[3] == '-' {
		line, err = c.reader.ReadLine()
		if err != nil {
			c.t.Fatalf("read multiline reply: %v", err)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}
