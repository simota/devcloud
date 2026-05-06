package bigquery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) insertRows(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	table, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	request, err := s.decodeInsertAllRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	existingRows, err := s.readRows(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	seenInsertIDs := make(map[string]struct{}, len(existingRows)+len(request.Rows))
	for _, row := range existingRows {
		if row.InsertID != "" {
			seenInsertIDs[row.InsertID] = struct{}{}
		}
	}

	now := time.Now().UTC()
	accepted := make([]storedRow, 0, len(request.Rows))
	insertErrors := make([]insertError, 0)
	for i, row := range request.Rows {
		values, rowErrors := validateRowJSON(row.JSON, table.Schema, request.IgnoreUnknownValues)
		if len(rowErrors) > 0 {
			insertErrors = append(insertErrors, insertError{
				Index:  i,
				Errors: rowErrors,
			})
			continue
		}
		if row.InsertID != "" {
			if _, duplicate := seenInsertIDs[row.InsertID]; duplicate {
				continue
			}
			seenInsertIDs[row.InsertID] = struct{}{}
		}
		accepted = append(accepted, storedRow{
			InsertID:   row.InsertID,
			JSON:       values,
			InsertedAt: now.Format(time.RFC3339Nano),
		})
	}
	if len(insertErrors) > 0 && !request.SkipInvalidRows {
		accepted = nil
	}
	if len(accepted) > 0 {
		if int64(len(existingRows)+len(accepted)) > s.maxRowsPerTable() {
			writeError(w, http.StatusBadRequest, "quotaExceeded", "table row limit exceeded")
			return
		}
		if err := s.appendRows(projectID, datasetID, tableID, accepted); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
		if err := s.refreshTableRowStats(table); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
	}

	writeJSON(w, http.StatusOK, insertAllResponse{
		Kind:         "bigquery#tableDataInsertAllResponse",
		InsertErrors: insertErrors,
	})
}

func (s *Server) listRows(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	table, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	rows, err := s.readRows(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if offset > len(rows) {
		offset = len(rows)
	}
	end := offset + maxResults
	if end > len(rows) {
		end = len(rows)
	}
	fields := selectedRowFields(table.Schema, r.URL.Query().Get("selectedFields"))
	responseRows := make([]tableDataRow, 0, end-offset)
	for _, row := range rows[offset:end] {
		responseRows = append(responseRows, tableDataRow{F: formatRowValues(row.JSON, fields)})
	}
	response := tableDataListResponse{
		Kind:      "bigquery#tableDataList",
		ETag:      datasetETag(time.Now().UTC()),
		TotalRows: strconv.Itoa(len(rows)),
		Rows:      responseRows,
	}
	if end < len(rows) {
		response.PageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) queryRows(w http.ResponseWriter, r *http.Request, projectID string) {
	request, err := s.decodeQueryRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	job, err := s.createQueryJob(projectID, jobReference{Location: request.Location}, queryJobConfiguration{
		Query:           request.Query,
		UseLegacySQL:    request.UseLegacySQL,
		QueryParameters: request.QueryParameters,
	}, request.MaxResults, false, request.DryRun, s.effectiveUseLegacySQL(request.UseLegacySQL))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalidQuery", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job.Response)
}

func validateTableSchema(schema tableSchema) error {
	for _, field := range schema.Fields {
		if err := validateTableField(field); err != nil {
			return err
		}
	}
	return nil
}

func validateTableField(field tableFieldSchema) error {
	if err := validateResourceID(field.Name, "field"); err != nil {
		return err
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON", "RECORD", "STRUCT":
	default:
		return fmt.Errorf("unsupported field type %q", field.Type)
	}
	mode := strings.ToUpper(defaultString(field.Mode, "NULLABLE"))
	switch mode {
	case "NULLABLE", "REQUIRED", "REPEATED":
	default:
		return fmt.Errorf("unsupported field mode %q", field.Mode)
	}
	if fieldType == "RECORD" || fieldType == "STRUCT" {
		if len(field.Fields) == 0 {
			return fmt.Errorf("record field %q requires nested fields", field.Name)
		}
		for _, nested := range field.Fields {
			if err := validateTableField(nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRowJSON(row map[string]json.RawMessage, schema tableSchema, ignoreUnknownValues bool) (map[string]json.RawMessage, []insertErrorItem) {
	errors := make([]insertErrorItem, 0)
	values := make(map[string]json.RawMessage)
	fieldsByName := make(map[string]tableFieldSchema, len(schema.Fields))
	for _, field := range schema.Fields {
		fieldsByName[field.Name] = field
	}
	for key, raw := range row {
		field, ok := fieldsByName[key]
		if !ok {
			if ignoreUnknownValues {
				continue
			}
			errors = append(errors, insertErrorItem{
				Reason:   "invalid",
				Location: key,
				Message:  fmt.Sprintf("no such field: %s", key),
			})
			continue
		}
		if err := validateFieldValue(raw, field); err != nil {
			errors = append(errors, insertErrorItem{
				Reason:   "invalid",
				Location: key,
				Message:  err.Error(),
			})
			continue
		}
		values[key] = raw
	}
	for _, field := range schema.Fields {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			raw, ok := values[field.Name]
			if !ok || isJSONNull(raw) {
				errors = append(errors, insertErrorItem{
					Reason:   "invalid",
					Location: field.Name,
					Message:  fmt.Sprintf("required field %q is missing", field.Name),
				})
			}
		}
	}
	return values, errors
}

func validateFieldValue(raw json.RawMessage, field tableFieldSchema) error {
	if isJSONNull(raw) {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			return fmt.Errorf("required field %q cannot be null", field.Name)
		}
		return nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return fmt.Errorf("field %q must be an array", field.Name)
		}
		itemField := field
		itemField.Mode = "NULLABLE"
		for _, value := range values {
			if err := validateFieldValue(value, itemField); err != nil {
				return err
			}
		}
		return nil
	}

	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("field %q must be a string", field.Name)
		}
	case "INTEGER", "INT64":
		if !isIntegerJSON(raw) {
			return fmt.Errorf("field %q must be an integer", field.Name)
		}
	case "FLOAT", "FLOAT64":
		if !isFloatJSON(raw) {
			return fmt.Errorf("field %q must be a number", field.Name)
		}
	case "BOOLEAN", "BOOL":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("field %q must be a boolean", field.Name)
		}
	case "JSON":
		return nil
	case "RECORD", "STRUCT":
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return fmt.Errorf("field %q must be an object", field.Name)
		}
		_, errors := validateRowJSON(object, tableSchema{Fields: field.Fields}, false)
		if len(errors) > 0 {
			return fmt.Errorf("field %q has invalid nested values", field.Name)
		}
	default:
		return fmt.Errorf("unsupported field type %q", field.Type)
	}
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.EqualFold(strings.TrimSpace(string(raw)), "null")
}

func isIntegerJSON(raw json.RawMessage) bool {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		_, err := strconv.ParseInt(asString, 10, 64)
		return err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return false
	}
	_, err := strconv.ParseInt(number.String(), 10, 64)
	return err == nil
}

func isFloatJSON(raw json.RawMessage) bool {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		_, err := strconv.ParseFloat(asString, 64)
		return err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return false
	}
	_, err := strconv.ParseFloat(number.String(), 64)
	return err == nil
}

func rowOffsetFromRequest(r *http.Request) (int, error) {
	token := strings.TrimSpace(r.URL.Query().Get("pageToken"))
	if token == "" {
		token = strings.TrimSpace(r.URL.Query().Get("startIndex"))
	}
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid page token")
	}
	return offset, nil
}

func maxResultsFromRequest(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("maxResults"))
	if raw == "" {
		return 100, nil
	}
	maxResults, err := strconv.Atoi(raw)
	if err != nil || maxResults < 0 {
		return 0, fmt.Errorf("maxResults must be positive")
	}
	if maxResults == 0 {
		return 100, nil
	}
	if maxResults > 10000 {
		return 10000, nil
	}
	return maxResults, nil
}

func selectedRowFields(schema tableSchema, selectedFields string) []tableFieldSchema {
	if strings.TrimSpace(selectedFields) == "" {
		return schema.Fields
	}
	allowed := make(map[string]bool)
	for _, field := range strings.Split(selectedFields, ",") {
		name := strings.TrimSpace(field)
		if name != "" {
			allowed[name] = true
		}
	}
	fields := make([]tableFieldSchema, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if allowed[field.Name] {
			fields = append(fields, field)
		}
	}
	return fields
}

func formatRowValues(row map[string]json.RawMessage, fields []tableFieldSchema) []tableCell {
	values := make([]tableCell, 0, len(fields))
	for _, field := range fields {
		raw, ok := row[field.Name]
		if !ok || isJSONNull(raw) {
			values = append(values, tableCell{V: nil})
			continue
		}
		values = append(values, tableCell{V: rawValueForFieldResponse(raw, field)})
	}
	return values
}

func rawValueForFieldResponse(raw json.RawMessage, field tableFieldSchema) any {
	if strings.EqualFold(field.Mode, "REPEATED") {
		return rawValueForResponse(raw)
	}
	switch strings.ToUpper(defaultString(field.Type, "STRING")) {
	case "INTEGER", "INT64":
		if value, ok := rawInt(raw); ok {
			return strconv.FormatInt(value, 10)
		}
	case "BOOLEAN", "BOOL":
		var value bool
		if err := json.Unmarshal(raw, &value); err == nil {
			return strconv.FormatBool(value)
		}
	case "NUMERIC", "BIGNUMERIC":
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			return value
		}
		var number json.Number
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&number); err == nil {
			return number.String()
		}
	}
	return rawValueForResponse(raw)
}

func rawValueForResponse(raw json.RawMessage) any {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return string(raw)
	}
	return value
}
