package mail

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"strconv"
	"strings"
)

type SMTPConfig struct {
	Addr            string
	MaxMessageBytes int64
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
	server      *SMTPServer
	reader      *textproto.Reader
	writer      *bufio.Writer
	greeted     bool
	hasMailFrom bool
	envelope    Envelope
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
	if maxBytes <= 0 {
		return s.reply(250, "OK")
	}
	if _, err := fmt.Fprintf(s.writer, "250-devcloud\r\n250 SIZE %d\r\n", maxBytes); err != nil {
		return false
	}
	return s.writer.Flush() == nil
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
