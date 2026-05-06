package bigquery

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func (s *Server) createQueryJob(requestProjectID string, requestedRef jobReference, config queryJobConfiguration, maxResults int, includeConfiguration bool, dryRun bool, useLegacySQL bool) (queryJobRecord, error) {
	if useLegacySQL {
		return queryJobRecord{}, fmt.Errorf("legacy SQL is not supported; set useLegacySql to false")
	}
	effectiveQuery, err := bindQueryParameters(config.Query, config.QueryParameters)
	if err != nil {
		return queryJobRecord{}, err
	}
	result, err := s.executeQueryForJob(requestProjectID, effectiveQuery, dryRun)
	if err != nil {
		return queryJobRecord{}, err
	}
	if config.DestinationTable.TableID != "" && !dryRun {
		if err := s.writeQueryDestinationTable(requestProjectID, config, result); err != nil {
			return queryJobRecord{}, err
		}
	}
	now := time.Now().UTC()
	jobID := strings.TrimSpace(requestedRef.JobID)
	if jobID == "" {
		jobID = "devcloud_query_" + strconv.FormatInt(now.UnixNano(), 10)
	} else if err := validateResourceID(jobID, "job"); err != nil {
		return queryJobRecord{}, err
	} else if _, found, err := s.readQueryJob(requestProjectID, jobID); err != nil {
		return queryJobRecord{}, err
	} else if found {
		return queryJobRecord{}, fmt.Errorf("already exists: job %s:%s", requestProjectID, jobID)
	}
	jobRef := jobReference{
		ProjectID: requestProjectID,
		JobID:     jobID,
		Location:  defaultString(requestedRef.Location, s.defaultLocation()),
	}
	response := queryResponse{
		Kind:         "bigquery#queryResponse",
		Schema:       tableSchema{Fields: result.Fields},
		JobReference: jobRef,
		TotalRows:    strconv.Itoa(len(result.Rows)),
		Rows:         result.Rows,
		JobComplete:  true,
		CacheHit:     false,
	}
	resource := jobResource{
		Kind:         "bigquery#job",
		ID:           requestProjectID + ":" + jobID,
		SelfLink:     "/bigquery/v2/projects/" + url.PathEscape(requestProjectID) + "/jobs/" + url.PathEscape(jobID),
		JobReference: jobRef,
		Status: jobStatus{
			State: "DONE",
		},
		Statistics: jobStatistics{
			CreationTime: unixMillisString(now),
			StartTime:    unixMillisString(now),
			EndTime:      unixMillisString(now),
			Query: queryStatistics{
				TotalRows: strconv.Itoa(len(result.Rows)),
				CacheHit:  false,
				DryRun:    dryRun,
			},
		},
	}
	if includeConfiguration {
		config.UseLegacySQL = boolPtr(false)
		resource.Configuration = jobConfiguration{
			DryRun: dryRun,
			Query:  config,
		}
	}
	job := queryJobRecord{
		Job:      resource,
		Response: response,
	}
	if err := s.writeQueryJob(requestProjectID, jobID, job); err != nil {
		return queryJobRecord{}, err
	}
	job.Response = s.pageQueryResponse(response, 0, maxResults)
	return job, nil
}

func (s *Server) writeQueryDestinationTable(requestProjectID string, config queryJobConfiguration, result queryExecutionResult) error {
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return err
	}
	if _, found, err := s.readDataset(destinationRef.ProjectID, destinationRef.DatasetID); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("not found: dataset %s:%s", destinationRef.ProjectID, destinationRef.DatasetID)
	}
	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_EMPTY")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	if writeDisposition != "WRITE_EMPTY" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_APPEND" {
		return fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}

	destination, destinationFound, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return err
	}
	if !destinationFound && createDisposition == "CREATE_NEVER" {
		return fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	}
	if destinationFound && writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return err
		}
		if len(existingRows) > 0 {
			return fmt.Errorf("destination table is not empty")
		}
	}
	resultSchema := tableSchema{Fields: result.Fields}
	if destinationFound && writeDisposition == "WRITE_APPEND" && !reflect.DeepEqual(destination.Schema, resultSchema) {
		return fmt.Errorf("destination table schema does not match query result")
	}

	now := time.Now().UTC()
	if !destinationFound || writeDisposition == "WRITE_TRUNCATE" {
		destination = tableResource{
			Kind:             "bigquery#table",
			ID:               destinationRef.ProjectID + ":" + destinationRef.DatasetID + "." + destinationRef.TableID,
			SelfLink:         s.tableSelfLink(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID),
			TableReference:   destinationRef,
			Type:             "TABLE",
			Schema:           resultSchema,
			ETag:             datasetETag(now),
			CreationTime:     unixMillisString(now),
			LastModifiedTime: unixMillisString(now),
			NumRows:          "0",
			NumBytes:         "0",
			Location:         s.defaultLocation(),
		}
	}
	destination.Type = defaultString(destination.Type, "TABLE")
	destination.Schema = resultSchema
	destination.ETag = datasetETag(now)
	destination.LastModifiedTime = unixMillisString(now)
	if err := s.writeTable(destination); err != nil {
		return err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	rows := queryResultRowsToStoredRows(result)
	if len(rows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, rows); err != nil {
			return err
		}
	}
	return s.refreshTableRowStats(destination)
}

func (s *Server) executeQueryForJob(requestProjectID string, rawQuery string, dryRun bool) (queryExecutionResult, error) {
	if dryRun {
		return s.dryRunQueryWithDepth(requestProjectID, rawQuery, 0)
	}
	return s.executeQueryWithDepth(requestProjectID, rawQuery, 0)
}

func (s *Server) dryRunQuery(requestProjectID string, rawQuery string) (queryExecutionResult, error) {
	return s.dryRunQueryWithDepth(requestProjectID, rawQuery, 0)
}

func (s *Server) dryRunQueryWithDepth(requestProjectID string, rawQuery string, depth int) (queryExecutionResult, error) {
	query, err := parseSimpleSelect(rawQuery, requestProjectID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	schema, _, err := s.querySource(query, true, depth)
	if err != nil {
		return queryExecutionResult{}, err
	}
	if query.Aggregate.Function != "" {
		if query.GroupBy != "" {
			fields, err := groupedAggregateDryRunFields(schema, query)
			if err != nil {
				return queryExecutionResult{}, err
			}
			return queryExecutionResult{Fields: fields}, nil
		}
		fields, err := aggregateDryRunFields(schema, query.Aggregate)
		if err != nil {
			return queryExecutionResult{}, err
		}
		return queryExecutionResult{Fields: fields}, nil
	}
	fields, err := fieldsForQuery(schema, query.SelectedFields)
	if err != nil {
		return queryExecutionResult{}, err
	}
	return queryExecutionResult{Fields: fields}, nil
}

func (s *Server) executeQuery(requestProjectID string, rawQuery string) (queryExecutionResult, error) {
	return s.executeQueryWithDepth(requestProjectID, rawQuery, 0)
}

func (s *Server) executeQueryWithDepth(requestProjectID string, rawQuery string, depth int) (queryExecutionResult, error) {
	query, err := parseSimpleSelect(rawQuery, requestProjectID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	schema, rows, err := s.querySource(query, false, depth)
	if err != nil {
		return queryExecutionResult{}, err
	}
	return executeParsedQuery(schema, rows, query)
}

func (s *Server) querySource(query simpleSelectQuery, dryRun bool, depth int) (tableSchema, []storedRow, error) {
	table, found, err := s.readTable(query.ProjectID, query.DatasetID, query.TableID)
	if err != nil {
		return tableSchema{}, nil, err
	}
	if !found {
		return tableSchema{}, nil, fmt.Errorf("not found: table %s:%s.%s", query.ProjectID, query.DatasetID, query.TableID)
	}

	if strings.EqualFold(table.Type, "VIEW") {
		if table.View == nil || strings.TrimSpace(table.View.Query) == "" {
			return tableSchema{}, nil, fmt.Errorf("view %s:%s.%s has no query", query.ProjectID, query.DatasetID, query.TableID)
		}
		if table.View.UseLegacySQL {
			return tableSchema{}, nil, fmt.Errorf("legacy SQL views are not supported")
		}
		if depth >= 8 {
			return tableSchema{}, nil, fmt.Errorf("view reference depth exceeded")
		}
		result, err := s.executeQueryForView(query.ProjectID, table.View.Query, dryRun, depth+1)
		if err != nil {
			return tableSchema{}, nil, err
		}
		if dryRun {
			return tableSchema{Fields: result.Fields}, nil, nil
		}
		return tableSchema{Fields: result.Fields}, queryResultRowsToStoredRows(result), nil
	}

	if dryRun {
		return table.Schema, nil, nil
	}
	rows, err := s.readRows(query.ProjectID, query.DatasetID, query.TableID)
	if err != nil {
		return tableSchema{}, nil, err
	}
	return table.Schema, rows, nil
}

func (s *Server) executeQueryForView(requestProjectID string, rawQuery string, dryRun bool, depth int) (queryExecutionResult, error) {
	if dryRun {
		return s.dryRunQueryWithDepth(requestProjectID, rawQuery, depth)
	}
	return s.executeQueryWithDepth(requestProjectID, rawQuery, depth)
}
