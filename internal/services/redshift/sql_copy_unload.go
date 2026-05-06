package redshift

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	s3svc "devcloud/internal/services/s3"
)

type copyCSVOptions struct {
	delimiter    rune
	format       string
	ignoreHeader int
	nullAs       string
	hasNullAs    bool
}

func (s *Server) copyFromLocalCSV(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return queryResult{}, errors.New("COPY requires FROM")
	}
	name := parseQualifiedName(statement[len("copy "):fromIndex])
	path, rest, err := parseLeadingSQLStringLiteral(strings.TrimSpace(statement[fromIndex+len(" from "):]))
	if err != nil {
		return queryResult{}, fmt.Errorf("COPY requires a local file path or s3 URI: %w", err)
	}
	options, err := parseCopyCSVOptions(rest)
	if err != nil {
		return queryResult{}, err
	}
	s.mu.Lock()
	table := s.lookupTableLocked(name)
	if table == nil {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("cannot copy into view %s.%s", name.schema, name.table)
	}
	columns := append([]column(nil), table.columns...)
	s.mu.Unlock()

	records, err := s.readCopyRecords(path, options, columns)
	if err != nil {
		return queryResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	table = s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot copy into view %s.%s", name.schema, name.table)
	}
	for line, record := range records {
		if len(record) != len(table.columns) {
			return queryResult{}, fmt.Errorf("COPY row %d has %d values for %d columns", line+1, len(record), len(table.columns))
		}
		copied := append([]string(nil), record...)
		table.rows = append(table.rows, copied)
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("COPY %d", len(records))}, nil
}

func (s *Server) unloadToLocalCSV(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("unload "):])
	if !strings.HasPrefix(rest, "(") {
		return queryResult{}, errors.New("UNLOAD requires a parenthesized SELECT")
	}
	close := matchingParen(rest, 0)
	if close < 0 {
		return queryResult{}, errors.New("UNLOAD has an unterminated SELECT")
	}
	selectSQL, _, err := parseLeadingSQLStringLiteral(strings.TrimSpace(rest[1:close]))
	if err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD requires SELECT SQL as a string literal: %w", err)
	}
	afterSelect := strings.TrimSpace(rest[close+1:])
	if !strings.HasPrefix(strings.ToLower(afterSelect), "to ") {
		return queryResult{}, errors.New("UNLOAD requires TO")
	}
	targetPrefix, _, err := parseLeadingSQLStringLiteral(strings.TrimSpace(afterSelect[len("to "):]))
	if err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD requires a local target prefix or s3 URI: %w", err)
	}

	result, err := s.executeSQL(selectSQL)
	if err != nil {
		return queryResult{}, err
	}
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	for _, row := range result.rows {
		if err := writer.Write(row); err != nil {
			return queryResult{}, fmt.Errorf("UNLOAD write CSV: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD flush CSV: %w", err)
	}
	if strings.HasPrefix(strings.ToLower(targetPrefix), "s3://") {
		if err := s.writeS3Object(targetPrefix+"000", bytes.NewReader(output.Bytes())); err != nil {
			return queryResult{}, err
		}
		return queryResult{tag: "UNLOAD"}, nil
	}

	outputPath := filepath.Clean(targetPrefix + "000")
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD create target directory: %w", err)
	}
	if err := os.WriteFile(outputPath, output.Bytes(), 0o644); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD write target file: %w", err)
	}
	return queryResult{tag: "UNLOAD"}, nil
}

func (s *Server) readCopyRecords(source string, options copyCSVOptions, columns []column) ([][]string, error) {
	if options.format == "json" {
		return s.readCopyJSONRecords(source, columns)
	}
	return s.readCopyCSVRecords(source, options)
}

func (s *Server) readCopyCSVRecords(source string, options copyCSVOptions) ([][]string, error) {
	var reader io.Reader
	if strings.HasPrefix(strings.ToLower(source), "s3://") {
		data, err := s.readS3Object(source)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	} else {
		file, err := os.Open(filepath.Clean(source))
		if err != nil {
			return nil, fmt.Errorf("COPY open source file: %w", err)
		}
		defer file.Close()
		reader = file
	}
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	if options.delimiter != 0 {
		csvReader.Comma = options.delimiter
	}
	records, err := s.readCSVRecordsWithLimit(csvReader, options.delimiter)
	if err != nil {
		return nil, err
	}
	if options.ignoreHeader > 0 {
		if options.ignoreHeader >= len(records) {
			records = nil
		} else {
			records = records[options.ignoreHeader:]
		}
	}
	if options.hasNullAs {
		for rowIndex := range records {
			for columnIndex, value := range records[rowIndex] {
				if value == options.nullAs {
					records[rowIndex][columnIndex] = ""
				}
			}
		}
	}
	return records, nil
}

func (s *Server) readCopyJSONRecords(source string, columns []column) ([][]string, error) {
	var reader io.Reader
	if strings.HasPrefix(strings.ToLower(source), "s3://") {
		data, err := s.readS3Object(source)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	} else {
		file, err := os.Open(filepath.Clean(source))
		if err != nil {
			return nil, fmt.Errorf("COPY open source file: %w", err)
		}
		defer file.Close()
		reader = file
	}

	scanner := bufio.NewScanner(reader)
	maxScanBytes := 4 * 1024 * 1024
	if s.config.MaxCopyInputBytes > int64(maxScanBytes) {
		maxScanBytes = int(s.config.MaxCopyInputBytes)
	}
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBytes)

	var records [][]string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := s.validateCopyJSONLineSize(line); err != nil {
			return nil, err
		}
		record, err := jsonLineToRecord(line, columns)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("COPY read JSON: %w", err)
	}
	return records, nil
}

func (s *Server) validateCopyJSONLineSize(line string) error {
	if s.config.MaxCopyInputBytes <= 0 {
		return nil
	}
	if int64(len(line)) > s.config.MaxCopyInputBytes {
		return fmt.Errorf("COPY input row exceeds maxCopyInputBytes")
	}
	return nil
}

func jsonLineToRecord(line string, columns []column) ([]string, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("COPY read JSON: %w", err)
	}
	lowerObject := make(map[string]any, len(object))
	for key, value := range object {
		lowerObject[strings.ToLower(key)] = value
	}
	record := make([]string, 0, len(columns))
	for _, column := range columns {
		value, ok := lowerObject[strings.ToLower(column.name)]
		if !ok || value == nil {
			record = append(record, "")
			continue
		}
		record = append(record, jsonCopyValueString(value))
	}
	return record, nil
}

func jsonCopyValueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(data)
	}
}

func (s *Server) readCSVRecordsWithLimit(csvReader *csv.Reader, delimiter rune) ([][]string, error) {
	var records [][]string
	for {
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("COPY read CSV: %w", err)
		}
		if err := s.validateCopyRecordSize(record, delimiter); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

func (s *Server) validateCopyRecordSize(record []string, delimiter rune) error {
	if s.config.MaxCopyInputBytes <= 0 {
		return nil
	}
	size := 0
	for index, value := range record {
		if index > 0 {
			size += len(string(delimiter))
		}
		size += len(value)
		if int64(size) > s.config.MaxCopyInputBytes {
			return fmt.Errorf("COPY input row exceeds maxCopyInputBytes")
		}
	}
	return nil
}

func (s *Server) readS3Object(uri string) ([]byte, error) {
	if s.config.ObjectStore == nil {
		return nil, errors.New("COPY from s3 URI requires local S3 service to be enabled")
	}
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}
	_, data, ok, err := s.config.ObjectStore.GetObject(context.Background(), bucket, key)
	if err != nil {
		return nil, fmt.Errorf("COPY read S3 object: %w", err)
	}
	if !ok {
		return nil, errors.New("COPY source S3 object does not exist")
	}
	return data, nil
}

func (s *Server) writeS3Object(uri string, body io.Reader) error {
	if s.config.ObjectStore == nil {
		return errors.New("UNLOAD to s3 URI requires local S3 service to be enabled")
	}
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return err
	}
	if _, err := s.config.ObjectStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      bucket,
		Key:         key,
		Body:        body,
		ContentType: "text/csv",
	}); err != nil {
		return fmt.Errorf("UNLOAD write S3 object: %w", err)
	}
	return nil
}

func parseS3URI(uri string) (string, string, error) {
	if !strings.HasPrefix(strings.ToLower(uri), "s3://") {
		return "", "", fmt.Errorf("expected s3 URI")
	}
	rest := uri[len("s3://"):]
	bucket, key, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fmt.Errorf("s3 URI requires bucket and key")
	}
	return bucket, key, nil
}

func parseCopyCSVOptions(value string) (copyCSVOptions, error) {
	tokens, err := tokenizeSQLOptions(value)
	if err != nil {
		return copyCSVOptions{}, err
	}
	options := copyCSVOptions{delimiter: ',', format: "csv"}
	for i := 0; i < len(tokens); i++ {
		token := strings.ToLower(tokens[i].value)
		switch token {
		case "", "csv":
			options.format = "csv"
			continue
		case "json":
			options.format = "json"
			if i+1 < len(tokens) && (strings.EqualFold(tokens[i+1].value, "auto") || strings.EqualFold(tokens[i+1].value, "noshred")) {
				i++
			}
		case "iam_role", "credentials", "region":
			if i+1 < len(tokens) {
				i++
			}
		case "delimiter":
			next := i + 1
			if next < len(tokens) && strings.EqualFold(tokens[next].value, "as") {
				next++
			}
			if next >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY DELIMITER requires a value")
			}
			delimiter, err := parseCSVDelimiter(tokens[next].value)
			if err != nil {
				return copyCSVOptions{}, err
			}
			options.delimiter = delimiter
			i = next
		case "ignoreheader":
			if i+1 >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY IGNOREHEADER requires a row count")
			}
			count, err := strconv.Atoi(tokens[i+1].value)
			if err != nil || count < 0 {
				return copyCSVOptions{}, errors.New("COPY IGNOREHEADER requires a non-negative row count")
			}
			options.ignoreHeader = count
			i++
		case "null":
			next := i + 1
			if next < len(tokens) && strings.EqualFold(tokens[next].value, "as") {
				next++
			}
			if next >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY NULL AS requires a value")
			}
			options.nullAs = tokens[next].value
			options.hasNullAs = true
			i = next
		}
	}
	return options, nil
}

type sqlOptionToken struct {
	value string
}

func tokenizeSQLOptions(value string) ([]sqlOptionToken, error) {
	var tokens []sqlOptionToken
	for i := 0; i < len(value); {
		if unicode.IsSpace(rune(value[i])) || value[i] == ';' {
			i++
			continue
		}
		if value[i] == '\'' {
			parsed, rest, err := parseLeadingSQLStringLiteral(value[i:])
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, sqlOptionToken{value: parsed})
			i = len(value) - len(rest)
			continue
		}
		start := i
		for i < len(value) && !unicode.IsSpace(rune(value[i])) && value[i] != ';' {
			i++
		}
		tokens = append(tokens, sqlOptionToken{value: value[start:i]})
	}
	return tokens, nil
}

func parseCSVDelimiter(value string) (rune, error) {
	if value == `\t` {
		return '\t', nil
	}
	runes := []rune(value)
	if len(runes) != 1 {
		return 0, errors.New("COPY DELIMITER requires exactly one character")
	}
	if runes[0] == '\r' || runes[0] == '\n' || runes[0] == 0xfffd {
		return 0, errors.New("COPY DELIMITER contains an unsupported character")
	}
	return runes[0], nil
}
