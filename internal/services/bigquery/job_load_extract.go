package bigquery

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	s3svc "devcloud/internal/services/s3"
)

func (s *Server) createCopyJob(requestProjectID string, requestedRef jobReference, config copyJobConfiguration) (queryJobRecord, error) {
	sourceTableRefs, err := copySourceTables(config)
	if err != nil {
		return queryJobRecord{}, err
	}
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	if _, found, err := s.readDataset(destinationRef.ProjectID, destinationRef.DatasetID); err != nil {
		return queryJobRecord{}, err
	} else if !found {
		return queryJobRecord{}, fmt.Errorf("not found: dataset %s:%s", destinationRef.ProjectID, destinationRef.DatasetID)
	}

	var source tableResource
	sourceRows := make([]storedRow, 0)
	for index, sourceTableRef := range sourceTableRefs {
		sourceTableRef = normalizeTableReference(sourceTableRef, requestProjectID)
		if err := validateTableReference(sourceTableRef); err != nil {
			return queryJobRecord{}, err
		}
		sourceTable, found, err := s.readTable(sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if !found {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		}
		if index == 0 {
			source = sourceTable
		} else if !reflect.DeepEqual(source.Schema, sourceTable.Schema) {
			return queryJobRecord{}, fmt.Errorf("source tables must have matching schemas")
		}
		rows, err := s.readRows(sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		sourceRows = append(sourceRows, rows...)
	}

	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_EMPTY")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	if writeDisposition != "WRITE_EMPTY" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_APPEND" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}

	destination, destinationFound, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !destinationFound && createDisposition == "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	}
	if destinationFound && writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	now := time.Now().UTC()
	if !destinationFound || writeDisposition == "WRITE_TRUNCATE" {
		destination = source
		destination.ID = destinationRef.ProjectID + ":" + destinationRef.DatasetID + "." + destinationRef.TableID
		destination.SelfLink = s.tableSelfLink(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		destination.TableReference = destinationRef
		destination.CreationTime = unixMillisString(now)
	}
	destination.ETag = datasetETag(now)
	destination.LastModifiedTime = unixMillisString(now)
	if err := s.writeTable(destination); err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(sourceRows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, sourceRows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(destination); err != nil {
		return queryJobRecord{}, err
	}

	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Copy: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createLoadJob(ctx context.Context, requestProjectID string, requestedRef jobReference, config loadJobConfiguration) (queryJobRecord, error) {
	if s.config.ObjectStore == nil {
		return queryJobRecord{}, fmt.Errorf("local GCS object store is not configured")
	}
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	sourceFormat := normalizeDataFormat(config.SourceFormat, "NEWLINE_DELIMITED_JSON")
	if sourceFormat != "NEWLINE_DELIMITED_JSON" && sourceFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported load sourceFormat %q", config.SourceFormat)
	}
	if len(config.SourceURIs) == 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.sourceUris is required")
	}
	table, found, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		if createDisposition == "CREATE_NEVER" {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		}
		var err error
		table, err = s.createLoadDestinationTable(destinationRef, config.Schema)
		if err != nil {
			return queryJobRecord{}, err
		}
	}
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_APPEND")
	if writeDisposition != "WRITE_APPEND" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_EMPTY" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}
	if writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	if config.SkipLeadingRows < 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.skipLeadingRows must be non-negative")
	}
	rows, err := s.loadRowsFromGCSObjects(ctx, config.SourceURIs, sourceFormat, table.Schema, config.SkipLeadingRows)
	if err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(rows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, rows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(table); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Load: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createUploadLoadJob(requestProjectID string, requestedRef jobReference, config loadJobConfiguration, media io.Reader) (queryJobRecord, error) {
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	sourceFormat := normalizeDataFormat(config.SourceFormat, "NEWLINE_DELIMITED_JSON")
	if sourceFormat != "NEWLINE_DELIMITED_JSON" && sourceFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported load sourceFormat %q", config.SourceFormat)
	}
	table, found, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		if createDisposition == "CREATE_NEVER" {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		}
		var err error
		table, err = s.createLoadDestinationTable(destinationRef, config.Schema)
		if err != nil {
			return queryJobRecord{}, err
		}
	}
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_APPEND")
	if writeDisposition != "WRITE_APPEND" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_EMPTY" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}
	if writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	if config.SkipLeadingRows < 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.skipLeadingRows must be non-negative")
	}
	rows, err := loadRows(media, sourceFormat, table.Schema, config.SkipLeadingRows)
	if err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(rows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, rows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(table); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Load: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createLoadDestinationTable(ref tableReference, schema tableSchema) (tableResource, error) {
	if len(schema.Fields) == 0 {
		return tableResource{}, fmt.Errorf("configuration.load.schema is required when creating a destination table")
	}
	if err := validateTableSchema(schema); err != nil {
		return tableResource{}, err
	}
	if _, found, err := s.readDataset(ref.ProjectID, ref.DatasetID); err != nil {
		return tableResource{}, err
	} else if !found {
		return tableResource{}, fmt.Errorf("not found: dataset %s:%s", ref.ProjectID, ref.DatasetID)
	}

	now := time.Now().UTC()
	table := tableResource{
		Kind:             "bigquery#table",
		ID:               ref.ProjectID + ":" + ref.DatasetID + "." + ref.TableID,
		SelfLink:         s.tableSelfLink(ref.ProjectID, ref.DatasetID, ref.TableID),
		TableReference:   ref,
		Type:             "TABLE",
		Schema:           schema,
		ETag:             datasetETag(now),
		CreationTime:     unixMillisString(now),
		LastModifiedTime: unixMillisString(now),
		NumRows:          "0",
		NumBytes:         "0",
		Location:         s.defaultLocation(),
	}
	if err := s.writeTable(table); err != nil {
		return tableResource{}, err
	}
	return table, nil
}

func (s *Server) createExtractJob(ctx context.Context, requestProjectID string, requestedRef jobReference, config extractJobConfiguration) (queryJobRecord, error) {
	if s.config.ObjectStore == nil {
		return queryJobRecord{}, fmt.Errorf("local GCS object store is not configured")
	}
	sourceRef := normalizeTableReference(config.SourceTable, requestProjectID)
	if err := validateTableReference(sourceRef); err != nil {
		return queryJobRecord{}, err
	}
	destinationFormat := normalizeDataFormat(config.DestinationFormat, "NEWLINE_DELIMITED_JSON")
	if destinationFormat != "NEWLINE_DELIMITED_JSON" && destinationFormat != "JSON" && destinationFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported extract destinationFormat %q", config.DestinationFormat)
	}
	if len(config.DestinationURIs) != 1 {
		return queryJobRecord{}, fmt.Errorf("configuration.extract.destinationUris must contain exactly one URI")
	}
	destinationBucket, destinationKey, err := parseGCSURI(config.DestinationURIs[0])
	if err != nil {
		return queryJobRecord{}, err
	}
	table, found, err := s.readTable(sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	}
	rows, err := s.readRows(sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	body, contentType, err := extractedRows(rows, destinationFormat, table.Schema)
	if err != nil {
		return queryJobRecord{}, err
	}
	if _, err := s.config.ObjectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      destinationBucket,
		Key:         destinationKey,
		Body:        bytes.NewReader(body),
		ContentType: contentType,
	}); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Extract: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) loadRowsFromGCSObjects(ctx context.Context, sourceURIs []string, sourceFormat string, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	rows := make([]storedRow, 0)
	for _, uri := range sourceURIs {
		bucket, key, err := parseGCSURI(uri)
		if err != nil {
			return nil, err
		}
		_, body, found, err := s.config.ObjectStore.GetObject(ctx, bucket, key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("source object not found")
		}
		loadedRows, err := loadRows(bytes.NewReader(body), sourceFormat, schema, skipLeadingRows)
		if err != nil {
			return nil, err
		}
		rows = append(rows, loadedRows...)
	}
	return rows, nil
}

func loadRows(body io.Reader, sourceFormat string, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	switch normalizeDataFormat(sourceFormat, "NEWLINE_DELIMITED_JSON") {
	case "NEWLINE_DELIMITED_JSON":
		return loadRowsFromNDJSON(body, schema)
	case "CSV":
		return loadRowsFromCSV(body, schema, skipLeadingRows)
	default:
		return nil, fmt.Errorf("unsupported load sourceFormat %q", sourceFormat)
	}
}

func loadRowsFromNDJSON(body io.Reader, schema tableSchema) ([]storedRow, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	decoder := json.NewDecoder(body)
	rows := make([]storedRow, 0)
	for {
		var row map[string]json.RawMessage
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("invalid newline-delimited JSON object")
		}
		values, rowErrors := validateRowJSON(row, schema, false)
		if len(rowErrors) > 0 {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		rows = append(rows, storedRow{JSON: values, InsertedAt: now})
	}
	return rows, nil
}

func loadRowsFromCSV(body io.Reader, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	reader := csv.NewReader(body)
	reader.FieldsPerRecord = len(schema.Fields)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("invalid CSV row")
	}
	if skipLeadingRows > len(records) {
		records = nil
	} else if skipLeadingRows > 0 {
		records = records[skipLeadingRows:]
	}
	rows := make([]storedRow, 0, len(records))
	for _, record := range records {
		row := make(map[string]json.RawMessage, len(schema.Fields))
		for i, field := range schema.Fields {
			raw, err := csvCellRawMessage(record[i], field)
			if err != nil {
				return nil, err
			}
			row[field.Name] = raw
		}
		values, rowErrors := validateRowJSON(row, schema, false)
		if len(rowErrors) > 0 {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		rows = append(rows, storedRow{JSON: values, InsertedAt: now})
	}
	return rows, nil
}

func csvCellRawMessage(value string, field tableFieldSchema) (json.RawMessage, error) {
	if value == "" {
		return json.RawMessage("null"), nil
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON":
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return encoded, nil
	case "INTEGER", "INT64":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		return json.RawMessage(value), nil
	case "FLOAT", "FLOAT64":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		return json.RawMessage(value), nil
	case "BOOLEAN", "BOOL":
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		if parsed {
			return json.RawMessage("true"), nil
		}
		return json.RawMessage("false"), nil
	case "RECORD", "STRUCT":
		return nil, fmt.Errorf("CSV load does not support RECORD fields")
	default:
		return nil, fmt.Errorf("unsupported field type %q", field.Type)
	}
}

func extractedRows(rows []storedRow, format string, schema tableSchema) ([]byte, string, error) {
	switch normalizeDataFormat(format, "NEWLINE_DELIMITED_JSON") {
	case "NEWLINE_DELIMITED_JSON", "JSON":
		body, err := extractedNDJSON(rows, schema)
		return body, "application/x-ndjson", err
	case "CSV":
		body, err := extractedCSV(rows, schema)
		return body, "text/csv", err
	default:
		return nil, "", fmt.Errorf("unsupported extract destinationFormat %q", format)
	}
}

func extractedNDJSON(rows []storedRow, schema tableSchema) ([]byte, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, row := range rows {
		value := make(map[string]any, len(schema.Fields))
		for _, field := range schema.Fields {
			raw, ok := row.JSON[field.Name]
			if !ok || isJSONNull(raw) {
				value[field.Name] = nil
				continue
			}
			value[field.Name] = rawValueForResponse(raw)
		}
		if err := encoder.Encode(value); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}

func extractedCSV(rows []storedRow, schema tableSchema) ([]byte, error) {
	var body bytes.Buffer
	writer := csv.NewWriter(&body)
	for _, row := range rows {
		record := make([]string, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			raw, ok := row.JSON[field.Name]
			if !ok || isJSONNull(raw) {
				record = append(record, "")
				continue
			}
			record = append(record, csvValueString(rawValueForResponse(raw)))
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func csvValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

func normalizeDataFormat(value string, fallback string) string {
	return strings.ToUpper(strings.TrimSpace(defaultString(value, fallback)))
}

func parseGCSURI(uri string) (string, string, error) {
	if strings.ContainsAny(uri, "*?[") {
		return "", "", fmt.Errorf("wildcard GCS URIs are not supported")
	}
	trimmed := strings.TrimSpace(uri)
	if !strings.HasPrefix(trimmed, "gs://") {
		return "", "", fmt.Errorf("only gs:// URIs are supported")
	}
	withoutScheme := strings.TrimPrefix(trimmed, "gs://")
	bucket, key, ok := strings.Cut(withoutScheme, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fmt.Errorf("gs:// URI must include bucket and object")
	}
	return bucket, key, nil
}

func copySourceTables(config copyJobConfiguration) ([]tableReference, error) {
	if len(config.SourceTables) > 0 {
		if config.SourceTable.TableID != "" {
			return nil, fmt.Errorf("copy job supports sourceTable or sourceTables, not both")
		}
		return config.SourceTables, nil
	}
	if config.SourceTable.TableID == "" {
		return nil, fmt.Errorf("configuration.copy.sourceTable is required")
	}
	return []tableReference{config.SourceTable}, nil
}

func normalizeTableReference(ref tableReference, defaultProjectID string) tableReference {
	if ref.ProjectID == "" {
		ref.ProjectID = defaultString(defaultProjectID, "devcloud")
	}
	return ref
}

func validateTableReference(ref tableReference) error {
	if err := validateResourceID(ref.ProjectID, "project"); err != nil {
		return err
	}
	if err := validateResourceID(ref.DatasetID, "dataset"); err != nil {
		return err
	}
	return validateResourceID(ref.TableID, "table")
}
