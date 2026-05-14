package mail

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"strconv"
	"strings"
)

const (
	SMTPAuthOff     = "off"
	SMTPAuthRelaxed = "relaxed"
	SMTPAuthStrict  = "strict"
)

type SMTPConfig struct {
	Addr            string
	MaxMessageBytes int64
	AuthMode        string
	Username        string
	Password        string
}

func (c SMTPConfig) authMode() string {
	mode := strings.ToLower(strings.TrimSpace(c.AuthMode))
	if mode == "" {
		return SMTPAuthOff
	}
	return mode
}

type SMTPServer struct {
	config  SMTPConfig
	service *Service
}

func NewSMTPServer(cfg SMTPConfig, service *Service) *SMTPServer {
	return &SMTPServer{config: cfg, service: service}
}

func (s *SMTPServer) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(ctx, conn)
	}
}

type smtpSession struct {
	server        *SMTPServer
	reader        *textproto.Reader
	writer        *bufio.Writer
	greeted       bool
	hasMailFrom   bool
	authenticated bool
	envelope      Envelope
}

func (s *SMTPServer) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	session := &smtpSession{
		server: s,
		reader: textproto.NewReader(bufio.NewReader(conn)),
		writer: bufio.NewWriter(conn),
	}
	if !session.reply(220, "devcloud ESMTP ready") {
		return
	}

	for {
		line, err := session.reader.ReadLine()
		if err != nil {
			return
		}
		if !session.handleLine(ctx, line) {
			return
		}
	}
}

func (s *smtpSession) handleLine(ctx context.Context, line string) bool {
	command, arg := splitSMTPCommand(line)
	switch command {
	case "HELO", "EHLO":
		if strings.TrimSpace(arg) == "" {
			return s.reply(500, "syntax error")
		}
		s.greeted = true
		s.resetEnvelope()
		if command == "EHLO" {
			return s.replyEHLO()
		}
		return s.reply(250, "OK")
	case "MAIL":
		if !s.greeted {
			return s.reply(503, "bad sequence of commands")
		}
		from, size, hasSize, ok := parseMailFromArg(arg)
		if !ok {
			return s.reply(500, "syntax error")
		}
		if hasSize && s.server.config.MaxMessageBytes > 0 && size > s.server.config.MaxMessageBytes {
			s.resetEnvelope()
			return s.reply(552, "message size exceeds limit")
		}
		s.envelope = Envelope{From: from}
		s.hasMailFrom = true
		return s.reply(250, "OK")
	case "RCPT":
		if !s.hasMailFrom {
			return s.reply(503, "bad sequence of commands")
		}
		to, ok := parseAddressArg(arg, "TO:")
		if !ok {
			return s.reply(500, "syntax error")
		}
		s.envelope.To = append(s.envelope.To, to)
		return s.reply(250, "OK")
	case "DATA":
		if !s.hasMailFrom || len(s.envelope.To) == 0 {
			return s.reply(503, "bad sequence of commands")
		}
		if strings.TrimSpace(arg) != "" {
			return s.reply(500, "syntax error")
		}
		return s.handleData(ctx)
	case "AUTH":
		if !s.greeted {
			return s.reply(503, "bad sequence of commands")
		}
		if s.server.config.authMode() == SMTPAuthOff {
			return s.reply(502, "command not implemented")
		}
		if s.authenticated {
			return s.reply(503, "bad sequence of commands")
		}
		return s.handleAuth(arg)
	case "RSET":
		if strings.TrimSpace(arg) != "" {
			return s.reply(500, "syntax error")
		}
		s.resetEnvelope()
		return s.reply(250, "OK")
	case "NOOP":
		return s.reply(250, "OK")
	case "QUIT":
		s.reply(221, "bye")
		return false
	case "":
		return s.reply(500, "syntax error")
	default:
		return s.reply(502, "command not implemented")
	}
}

func (s *smtpSession) handleData(ctx context.Context) bool {
	if !s.reply(354, "End data with <CR><LF>.<CR><LF>") {
		return false
	}

	var raw bytes.Buffer
	oversized := false
	maxBytes := s.server.config.MaxMessageBytes
	for {
		line, err := s.reader.ReadLine()
		if err != nil {
			return false
		}
		if line == "." {
			break
		}
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		if !oversized {
			nextLen := int64(raw.Len() + len(line) + len("\r\n"))
			if maxBytes > 0 && nextLen > maxBytes {
				oversized = true
			} else {
				raw.WriteString(line)
				raw.WriteString("\r\n")
			}
		}
	}
	if oversized {
		s.resetEnvelope()
		return s.reply(552, "message size exceeds limit")
	}

	if _, err := s.server.service.Receive(ctx, s.envelope, bytes.NewReader(raw.Bytes())); err != nil {
		return s.reply(451, "requested action aborted: local error in processing")
	}
	s.resetEnvelope()
	return s.reply(250, "OK")
}

func (s *smtpSession) resetEnvelope() {
	s.envelope = Envelope{}
	s.hasMailFrom = false
}

func (s *smtpSession) reply(code int, message string) bool {
	if _, err := fmt.Fprintf(s.writer, "%d %s\r\n", code, message); err != nil {
		return false
	}
	return s.writer.Flush() == nil
}

func (s *smtpSession) replyEHLO() bool {
	maxBytes := s.server.config.MaxMessageBytes
	authEnabled := s.server.config.authMode() != SMTPAuthOff

	lines := []string{"devcloud"}
	if maxBytes > 0 {
		lines = append(lines, fmt.Sprintf("SIZE %d", maxBytes))
	}
	if authEnabled {
		lines = append(lines, "AUTH PLAIN LOGIN")
	}

	for i, payload := range lines {
		separator := "-"
		if i == len(lines)-1 {
			separator = " "
		}
		if _, err := fmt.Fprintf(s.writer, "250%s%s\r\n", separator, payload); err != nil {
			return false
		}
	}
	return s.writer.Flush() == nil
}

func (s *smtpSession) handleAuth(arg string) bool {
	mechanism, initial := splitAuthArg(arg)
	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		return s.handleAuthPlain(initial)
	case "LOGIN":
		return s.handleAuthLogin(initial)
	case "":
		return s.reply(501, "syntax error in AUTH")
	default:
		return s.reply(504, "unrecognized authentication type")
	}
}

func (s *smtpSession) handleAuthPlain(initial string) bool {
	encoded := initial
	if encoded == "" {
		if !s.reply(334, "") {
			return false
		}
		line, err := s.reader.ReadLine()
		if err != nil {
			return false
		}
		encoded = line
	}
	if encoded == "*" {
		return s.reply(501, "authentication cancelled")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return s.reply(501, "invalid base64")
	}
	parts := strings.SplitN(string(decoded), "\x00", 3)
	if len(parts) != 3 {
		return s.reply(501, "malformed PLAIN credentials")
	}
	username := parts[1]
	password := parts[2]
	return s.completeAuth(username, password)
}

func (s *smtpSession) handleAuthLogin(initial string) bool {
	username, ok := s.readAuthLoginField(initial, "VXNlcm5hbWU6")
	if !ok {
		return false
	}
	password, ok := s.readAuthLoginField("", "UGFzc3dvcmQ6")
	if !ok {
		return false
	}
	return s.completeAuth(username, password)
}

func (s *smtpSession) readAuthLoginField(initial string, prompt string) (string, bool) {
	encoded := initial
	if encoded == "" {
		if !s.reply(334, prompt) {
			return "", false
		}
		line, err := s.reader.ReadLine()
		if err != nil {
			return "", false
		}
		encoded = line
	}
	if encoded == "*" {
		s.reply(501, "authentication cancelled")
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		s.reply(501, "invalid base64")
		return "", false
	}
	return string(decoded), true
}

func (s *smtpSession) completeAuth(username string, password string) bool {
	if s.server.config.authMode() == SMTPAuthStrict {
		if username != s.server.config.Username || password != s.server.config.Password {
			return s.reply(535, "authentication failed")
		}
	}
	s.authenticated = true
	return s.reply(235, "authentication succeeded")
}

func splitAuthArg(arg string) (mechanism string, initial string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", ""
	}
	mechanism, rest, found := strings.Cut(arg, " ")
	if !found {
		return mechanism, ""
	}
	return mechanism, strings.TrimSpace(rest)
}

func splitSMTPCommand(line string) (string, string) {
	line = strings.TrimRight(line, " \t")
	if line == "" {
		return "", ""
	}
	command, arg, found := strings.Cut(line, " ")
	if !found {
		return strings.ToUpper(command), ""
	}
	return strings.ToUpper(command), strings.TrimLeft(arg, " \t")
}

func parseAddressArg(arg string, prefix string) (string, bool) {
	address, rest, ok := parsePathArg(arg, prefix, false)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", false
	}
	return address, true
}

func parsePathArg(arg string, prefix string, allowEmpty bool) (string, string, bool) {
	arg = strings.TrimSpace(arg)
	if len(arg) < len(prefix) || !strings.EqualFold(arg[:len(prefix)], prefix) {
		return "", "", false
	}
	rest := strings.TrimSpace(arg[len(prefix):])
	if rest == "" {
		return "", "", false
	}
	if strings.HasPrefix(rest, "<") {
		end := strings.Index(rest, ">")
		if end < 0 || (!allowEmpty && end == 1) {
			return "", "", false
		}
		if len(rest) > end+1 && rest[end+1] != ' ' && rest[end+1] != '\t' {
			return "", "", false
		}
		return rest[1:end], strings.TrimSpace(rest[end+1:]), true
	}
	split := strings.IndexFunc(rest, func(r rune) bool {
		return r == ' ' || r == '\t'
	})
	if split < 0 {
		return rest, "", true
	}
	address, remaining := rest[:split], rest[split+1:]
	if address == "" {
		return "", "", false
	}
	return address, strings.TrimSpace(remaining), true
}

func parseMailFromArg(arg string) (address string, size int64, hasSize bool, ok bool) {
	var rest string
	address, rest, ok = parsePathArg(arg, "FROM:", true)
	if !ok {
		return "", 0, false, false
	}

	for _, field := range strings.Fields(rest) {
		name, value, found := strings.Cut(field, "=")
		if !found {
			return "", 0, false, false
		}
		if !strings.EqualFold(name, "SIZE") {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return "", 0, false, false
		}
		return address, parsed, true, true
	}
	return address, 0, false, true
}
