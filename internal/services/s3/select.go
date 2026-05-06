package s3

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) handleSelectObjectContent(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	var request selectObjectContentRequest
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.ExpressionType) != "SQL" {
		writeXMLError(w, "InvalidExpressionType", "only SQL expressions are supported", http.StatusBadRequest)
		return
	}
	if !isSupportedSelectExpression(request.Expression) {
		writeXMLError(w, "NotImplemented", "only SELECT * FROM S3Object is supported", http.StatusNotImplemented)
		return
	}
	object, body, ok, err := s.store.GetObjectVersion(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok || object.DeleteMarker {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	output, err := evaluateSelectObjectContent(request, body)
	if err != nil {
		writeXMLError(w, "NotImplemented", err.Error(), http.StatusNotImplemented)
		return
	}
	payload := append(encodeEventStreamMessage(map[string]string{
		":message-type": "event",
		":event-type":   "Records",
		":content-type": "application/octet-stream",
	}, output), encodeEventStreamMessage(map[string]string{
		":message-type": "event",
		":event-type":   "End",
	}, nil)...)
	w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}
func isSupportedSelectExpression(expression string) bool {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(expression)), " ")
	return strings.EqualFold(normalized, "SELECT * FROM S3Object") ||
		strings.EqualFold(normalized, "SELECT * FROM S3Object s")
}

func evaluateSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	switch {
	case request.InputSerialization.CSV != nil:
		return evaluateCSVSelectObjectContent(request, body)
	case request.InputSerialization.JSON != nil:
		return evaluateJSONSelectObjectContent(request, body)
	default:
		return nil, fmt.Errorf("input serialization is not supported")
	}
}

func evaluateCSVSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	if request.OutputSerialization.CSV == nil {
		return nil, fmt.Errorf("only CSV output is supported for CSV input")
	}
	input := request.InputSerialization.CSV.withDefaults()
	output := request.OutputSerialization.CSV.withDefaults()
	if len(input.fieldDelimiter()) != 1 || len(output.fieldDelimiter()) != 1 {
		return nil, fmt.Errorf("only single-byte CSV field delimiters are supported")
	}
	reader := csv.NewReader(bytes.NewReader(body))
	reader.Comma = rune(input.fieldDelimiter()[0])
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV input is malformed")
	}
	if strings.EqualFold(input.FileHeaderInfo, "USE") && len(records) > 0 {
		records = records[1:]
	}
	var out bytes.Buffer
	writer := csv.NewWriter(&out)
	writer.Comma = rune(output.fieldDelimiter()[0])
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	if delimiter := output.recordDelimiter(); delimiter != "\n" {
		return []byte(strings.ReplaceAll(out.String(), "\n", delimiter)), nil
	}
	return out.Bytes(), nil
}

func evaluateJSONSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	input := request.InputSerialization.JSON
	if request.OutputSerialization.JSON == nil {
		return nil, fmt.Errorf("only JSON output is supported for JSON input")
	}
	if input == nil || (input.Type != "" && input.Type != "LINES") {
		return nil, fmt.Errorf("only JSON LINES input is supported")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var out bytes.Buffer
	for {
		var value any
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("JSON input is malformed")
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		out.Write(encoded)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func encodeEventStreamMessage(headers map[string]string, payload []byte) []byte {
	var headerBytes bytes.Buffer
	for name, value := range headers {
		headerBytes.WriteByte(byte(len(name)))
		headerBytes.WriteString(name)
		headerBytes.WriteByte(7)
		_ = binary.Write(&headerBytes, binary.BigEndian, uint16(len(value)))
		headerBytes.WriteString(value)
	}
	totalLength := uint32(16 + headerBytes.Len() + len(payload))
	headersLength := uint32(headerBytes.Len())
	message := make([]byte, 0, totalLength)
	prelude := make([]byte, 8)
	binary.BigEndian.PutUint32(prelude[0:4], totalLength)
	binary.BigEndian.PutUint32(prelude[4:8], headersLength)
	preludeCRC := make([]byte, 4)
	binary.BigEndian.PutUint32(preludeCRC, crc32.ChecksumIEEE(prelude))
	message = append(message, prelude...)
	message = append(message, preludeCRC...)
	message = append(message, headerBytes.Bytes()...)
	message = append(message, payload...)
	messageCRC := make([]byte, 4)
	binary.BigEndian.PutUint32(messageCRC, crc32.ChecksumIEEE(message))
	message = append(message, messageCRC...)
	return message
}

type selectObjectContentRequest struct {
	XMLName             xml.Name                  `xml:"SelectObjectContentRequest"`
	Expression          string                    `xml:"Expression"`
	ExpressionType      string                    `xml:"ExpressionType"`
	InputSerialization  selectInputSerialization  `xml:"InputSerialization"`
	OutputSerialization selectOutputSerialization `xml:"OutputSerialization"`
	RequestProgress     struct{}                  `xml:"RequestProgress"`
	ScanRange           struct{}                  `xml:"ScanRange"`
}

type selectInputSerialization struct {
	CSV  *selectCSVSerialization  `xml:"CSV"`
	JSON *selectJSONSerialization `xml:"JSON"`
}

type selectOutputSerialization struct {
	CSV  *selectCSVSerialization  `xml:"CSV"`
	JSON *selectJSONSerialization `xml:"JSON"`
}

type selectCSVSerialization struct {
	FileHeaderInfo  string `xml:"FileHeaderInfo"`
	RecordDelimiter string `xml:"RecordDelimiter"`
	FieldDelimiter  string `xml:"FieldDelimiter"`
}

type selectJSONSerialization struct {
	Type string `xml:"Type"`
}

func (s *selectCSVSerialization) withDefaults() selectCSVSerialization {
	if s == nil {
		return selectCSVSerialization{}
	}
	return *s
}

func (s selectCSVSerialization) fieldDelimiter() string {
	if s.FieldDelimiter == "" {
		return ","
	}
	return s.FieldDelimiter
}

func (s selectCSVSerialization) recordDelimiter() string {
	if s.RecordDelimiter == "" {
		return "\n"
	}
	return s.RecordDelimiter
}
