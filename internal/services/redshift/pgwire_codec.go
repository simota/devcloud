package redshift

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

func readMessagePayload(r io.Reader) ([]byte, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lengthBytes))
	if length < 4 {
		return nil, errors.New("invalid PostgreSQL message length")
	}
	payload := make([]byte, length-4)
	_, err := io.ReadFull(r, payload)
	return payload, err
}

func readPasswordMessage(r io.Reader) (string, error) {
	messageType := []byte{0}
	if _, err := io.ReadFull(r, messageType); err != nil {
		return "", err
	}
	if messageType[0] != 'p' {
		return "", errors.New("expected password message")
	}
	payload, err := readMessagePayload(r)
	if err != nil {
		return "", err
	}
	return readCString(payload), nil
}

func parseStartupParameters(payload []byte) map[string]string {
	params := make(map[string]string)
	parts := bytes.Split(payload, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		key := string(parts[i])
		if key == "" {
			break
		}
		params[key] = string(parts[i+1])
	}
	return params
}

func readCString(payload []byte) string {
	if idx := bytes.IndexByte(payload, 0); idx >= 0 {
		return string(payload[:idx])
	}
	return string(payload)
}

func readCStringFromReader(reader *bytes.Reader) (string, bool) {
	var builder strings.Builder
	for {
		ch, err := reader.ReadByte()
		if err != nil {
			return "", false
		}
		if ch == 0 {
			return builder.String(), true
		}
		builder.WriteByte(ch)
	}
}

func readInt16FromReader(reader *bytes.Reader) (int16, bool) {
	var value int16
	if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
		return 0, false
	}
	return value, true
}

func readInt32FromReader(reader *bytes.Reader) (int32, bool) {
	var value int32
	if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
		return 0, false
	}
	return value, true
}

func discardInt16Values(reader *bytes.Reader, count int) bool {
	_, ok := readInt16Values(reader, count)
	return ok
}

func readInt16Values(reader *bytes.Reader, count int) ([]int16, bool) {
	if count < 0 {
		return nil, false
	}
	values := make([]int16, 0, count)
	for i := 0; i < count; i++ {
		value, ok := readInt16FromReader(reader)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func parseDescribeOrClosePayload(payload []byte) (byte, string, bool) {
	if len(payload) == 0 {
		return 0, "", false
	}
	reader := bytes.NewReader(payload[1:])
	name, ok := readCStringFromReader(reader)
	if !ok {
		return 0, "", false
	}
	if payload[0] != 'S' && payload[0] != 'P' {
		return 0, "", false
	}
	return payload[0], name, true
}

func writeAuthCleartextPassword(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgAuthCleartext)
	return writeMessage(w, 'R', body.Bytes())
}

func writeAuthenticationOK(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgAuthOK)
	return writeMessage(w, 'R', body.Bytes())
}

func writeParameterStatuses(w io.Writer, startupParams map[string]string) error {
	clientEncoding := defaultString(startupParams["client_encoding"], "UTF8")
	statuses := map[string]string{
		"server_version":              "8.0.2",
		"server_encoding":             "UTF8",
		"client_encoding":             clientEncoding,
		"DateStyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
		"application_name":            startupParams["application_name"],
		"is_superuser":                "on",
		"session_authorization":       defaultString(startupParams["user"], "dev"),
	}
	for key, value := range statuses {
		if value == "" {
			continue
		}
		var body bytes.Buffer
		writeCString(&body, key)
		writeCString(&body, value)
		if err := writeMessage(w, 'S', body.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func writeBackendKeyData(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgDefaultBackendPID)
	binary.Write(&body, binary.BigEndian, pgDefaultSecretKey)
	return writeMessage(w, 'K', body.Bytes())
}

func writeReadyForQuery(w io.Writer) error {
	return writeMessage(w, 'Z', []byte{pgTransactionIdle})
}

type pgField struct {
	Name     string
	TypeOID  int32
	TypeSize int16
}

func writeRowDescription(w io.Writer, fields []pgField) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(fields)))
	for _, field := range fields {
		writeCString(&body, field.Name)
		binary.Write(&body, binary.BigEndian, int32(0))
		binary.Write(&body, binary.BigEndian, int16(0))
		binary.Write(&body, binary.BigEndian, field.TypeOID)
		binary.Write(&body, binary.BigEndian, field.TypeSize)
		binary.Write(&body, binary.BigEndian, int32(-1))
		binary.Write(&body, binary.BigEndian, int16(0))
	}
	return writeMessage(w, 'T', body.Bytes())
}

func writeParameterDescription(w io.Writer, parameterOIDs []int32) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(parameterOIDs)))
	for _, oid := range parameterOIDs {
		binary.Write(&body, binary.BigEndian, oid)
	}
	return writeMessage(w, 't', body.Bytes())
}

func writeDataRow(w io.Writer, values []string) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(values)))
	for _, value := range values {
		binary.Write(&body, binary.BigEndian, int32(len(value)))
		body.WriteString(value)
	}
	return writeMessage(w, 'D', body.Bytes())
}

func writeCommandComplete(w io.Writer, tag string) error {
	var body bytes.Buffer
	writeCString(&body, tag)
	return writeMessage(w, 'C', body.Bytes())
}

func writeErrorResponse(w io.Writer, sqlState string, message string) error {
	var body bytes.Buffer
	body.WriteByte('S')
	writeCString(&body, "ERROR")
	body.WriteByte('C')
	writeCString(&body, sqlState)
	body.WriteByte('M')
	writeCString(&body, message)
	body.WriteByte(0)
	return writeMessage(w, 'E', body.Bytes())
}

func writeMessage(w io.Writer, messageType byte, body []byte) error {
	if messageType != 0 {
		if _, err := w.Write([]byte{messageType}); err != nil {
			return err
		}
	}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(body)+4))
	if _, err := w.Write(length); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func writeCString(w io.Writer, value string) {
	io.WriteString(w, value)
	w.Write([]byte{0})
}
