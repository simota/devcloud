package s3

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

func TestSelectObjectContentSupportsNarrowCSVAndJSONQueries(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.csv", strings.NewReader("name,age\nalice,31\nbob,28\n")); put.Code != http.StatusOK {
		t.Fatalf("put csv status = %d; body=%s", put.Code, put.Body.String())
	}
	csvRequest := `<SelectObjectContentRequest>
<Expression>SELECT * FROM S3Object</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV /></OutputSerialization>
</SelectObjectContentRequest>`
	csvSelect := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.csv?select&select-type=2", strings.NewReader(csvRequest))
	if csvSelect.Code != http.StatusOK {
		t.Fatalf("csv select status = %d, want %d; body=%s", csvSelect.Code, http.StatusOK, csvSelect.Body.String())
	}
	if got := csvSelect.Header().Get("Content-Type"); got != "application/vnd.amazon.eventstream" {
		t.Fatalf("csv select content type = %q", got)
	}
	records := eventStreamRecords(t, csvSelect.Body.Bytes())
	if got := string(records); got != "alice,31\nbob,28\n" {
		t.Fatalf("csv select records = %q", got)
	}

	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.jsonl", strings.NewReader(`{"name":"alice","age":31}`+"\n"+`{"name":"bob","age":28}`+"\n")); put.Code != http.StatusOK {
		t.Fatalf("put json status = %d; body=%s", put.Code, put.Body.String())
	}
	jsonRequest := `<SelectObjectContentRequest>
<Expression>SELECT * FROM S3Object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
<OutputSerialization><JSON /></OutputSerialization>
</SelectObjectContentRequest>`
	jsonSelect := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.jsonl?select&select-type=2", strings.NewReader(jsonRequest))
	if jsonSelect.Code != http.StatusOK {
		t.Fatalf("json select status = %d, want %d; body=%s", jsonSelect.Code, http.StatusOK, jsonSelect.Body.String())
	}
	if got := string(eventStreamRecords(t, jsonSelect.Body.Bytes())); got != "{\"age\":31,\"name\":\"alice\"}\n{\"age\":28,\"name\":\"bob\"}\n" {
		t.Fatalf("json select records = %q", got)
	}
}

func TestSelectObjectContentRejectsUnsupportedSQL(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.csv", strings.NewReader("name,age\nalice,31\n")); put.Code != http.StatusOK {
		t.Fatalf("put csv status = %d; body=%s", put.Code, put.Body.String())
	}
	request := `<SelectObjectContentRequest>
<Expression>SELECT name FROM S3Object WHERE age &gt; 30</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV /></InputSerialization>
<OutputSerialization><CSV /></OutputSerialization>
</SelectObjectContentRequest>`
	rec := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.csv?select&select-type=2", strings.NewReader(request))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported select status = %d, want %d; body=%s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported select error: %v", err)
	}
	if parsed.Code != "NotImplemented" {
		t.Fatalf("unsupported select error code = %q, want NotImplemented", parsed.Code)
	}
}

