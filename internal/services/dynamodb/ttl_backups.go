package dynamodb

import (
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"time"
)

func (s *Server) handleDescribeContinuousBackups(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	description := continuousBackupsDescriptionForState(state)
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ContinuousBackupsDescription": description,
	})
}

func (s *Server) handleUpdateContinuousBackups(w http.ResponseWriter, r *http.Request) {
	var request updateContinuousBackupsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	status := "DISABLED"
	if request.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled {
		status = "ENABLED"
	}
	description := continuousBackupsDescription{
		ContinuousBackupsStatus: "ENABLED",
		PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{
			PointInTimeRecoveryStatus: status,
		},
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous *continuousBackupsDescription
	if ok {
		previous = cloneContinuousBackupsDescription(state.continuousBackups)
		state.continuousBackups = cloneContinuousBackupsDescription(&description)
		if err := s.persistLocked(); err != nil {
			state.continuousBackups = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ContinuousBackupsDescription": description})
}

func (s *Server) handleCreateBackup(w http.ResponseWriter, r *http.Request) {
	var request createBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.BackupName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	createdAt := time.Now().Unix()
	description := backupDescriptionForTable(state.description, request.BackupName, createdAt)
	if _, exists := s.backups[description.BackupDetails.BackupArn]; exists {
		writeError(w, http.StatusBadRequest, "BackupInUseException", "backup already exists")
		return
	}
	s.backups[description.BackupDetails.BackupArn] = description
	s.backupTables[description.BackupDetails.BackupArn] = cloneTableDescription(state.description)
	s.backupItems[description.BackupDetails.BackupArn] = cloneItems(state.items)
	if err := s.persistLocked(); err != nil {
		delete(s.backups, description.BackupDetails.BackupArn)
		delete(s.backupTables, description.BackupDetails.BackupArn)
		delete(s.backupItems, description.BackupDetails.BackupArn)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDetails": description.BackupDetails})
}

func (s *Server) handleDescribeBackup(w http.ResponseWriter, r *http.Request) {
	var request describeBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}

	s.mu.Lock()
	description, ok := s.backups[request.BackupARN]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDescription": description})
}

func (s *Server) handleListBackups(w http.ResponseWriter, r *http.Request) {
	var request listBackupsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	backups := make([]backupSummary, 0, len(s.backups))
	for _, description := range s.backups {
		if request.TableName != "" && description.SourceTableDetails.TableName != request.TableName {
			continue
		}
		backups = append(backups, backupSummaryForDescription(description))
	}
	s.mu.Unlock()

	sort.Slice(backups, func(i, j int) bool {
		if backups[i].TableName == backups[j].TableName {
			return backups[i].BackupArn < backups[j].BackupArn
		}
		return backups[i].TableName < backups[j].TableName
	})
	start := 0
	if request.ExclusiveStartBackupARN != "" {
		start = -1
		for i, backup := range backups {
			if backup.BackupArn == request.ExclusiveStartBackupARN {
				start = i + 1
				break
			}
		}
		if start == -1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start backup arn does not exist")
			return
		}
	}
	end := len(backups)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"BackupSummaries": backups[start:end]}
	if end < len(backups) {
		response["LastEvaluatedBackupArn"] = backups[end-1].BackupArn
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteBackup(w http.ResponseWriter, r *http.Request) {
	var request deleteBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}

	s.mu.Lock()
	description, ok := s.backups[request.BackupARN]
	tableDescriptionBackup := s.backupTables[request.BackupARN]
	items := s.backupItems[request.BackupARN]
	if ok {
		delete(s.backups, request.BackupARN)
		delete(s.backupTables, request.BackupARN)
		delete(s.backupItems, request.BackupARN)
		if err := s.persistLocked(); err != nil {
			s.backups[request.BackupARN] = description
			if tableDescriptionBackup.TableName != "" {
				s.backupTables[request.BackupARN] = tableDescriptionBackup
			}
			if items != nil {
				s.backupItems[request.BackupARN] = items
			}
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDescription": description})
}

func (s *Server) handleRestoreTableFromBackup(w http.ResponseWriter, r *http.Request) {
	var request restoreTableFromBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}
	if request.TargetTableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "target table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	backup, ok := s.backups[request.BackupARN]
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	if _, exists := s.tables[request.TargetTableName]; exists {
		writeError(w, http.StatusBadRequest, "ResourceInUseException", "table already exists")
		return
	}
	if len(s.tables) >= s.maxTables() {
		writeError(w, http.StatusBadRequest, "LimitExceededException", "table limit exceeded")
		return
	}

	description := s.restoredTableDescription(request.TargetTableName, backup, time.Now().Unix())
	items := cloneItems(s.backupItems[request.BackupARN])
	state := &tableState{
		description: description,
		items:       items,
		tags:        map[string]string{},
	}
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.tables[request.TargetTableName] = state
	if err := s.persistLocked(); err != nil {
		delete(s.tables, request.TargetTableName)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": state.description})
}

func (s *Server) handleDescribeTimeToLive(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var description timeToLiveDescription
	if ok {
		description = ttlDescription(state.description)
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TimeToLiveDescription": description})
}

func (s *Server) handleUpdateTimeToLive(w http.ResponseWriter, r *http.Request) {
	var request timeToLiveRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.TimeToLiveSpecification.Enabled && request.TimeToLiveSpecification.AttributeName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ttl attribute name is required when ttl is enabled")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous *timeToLiveDescription
	if ok {
		previous = cloneTTLDescription(state.description.TimeToLiveDescription)
		state.description.TimeToLiveDescription = ttlDescriptionFromSpecification(request.TimeToLiveSpecification)
		if err := s.persistLocked(); err != nil {
			state.description.TimeToLiveDescription = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TimeToLiveSpecification": request.TimeToLiveSpecification})
}
func ttlDescription(description tableDescription) timeToLiveDescription {
	if description.TimeToLiveDescription == nil {
		return timeToLiveDescription{TimeToLiveStatus: "DISABLED"}
	}
	return *cloneTTLDescription(description.TimeToLiveDescription)
}

func ttlDescriptionFromSpecification(specification timeToLiveSpecification) *timeToLiveDescription {
	status := "DISABLED"
	if specification.Enabled {
		status = "ENABLED"
	}
	return &timeToLiveDescription{
		AttributeName:    specification.AttributeName,
		TimeToLiveStatus: status,
	}
}

func cloneTTLDescription(value *timeToLiveDescription) *timeToLiveDescription {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func (s *Server) expireTTLItems(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	backups, changed := s.expireTTLItemsLocked(now)
	if !changed {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		restoreBackups(backups)
		return err
	}
	return nil
}

func (s *Server) expireTTLItemsLocked(now time.Time) (map[*tableState]map[string]itemBackup, bool) {
	backups := map[*tableState]map[string]itemBackup{}
	changed := false
	for _, state := range s.tables {
		tableChanged := false
		ttl := ttlDescription(state.description)
		if ttl.TimeToLiveStatus != "ENABLED" || ttl.AttributeName == "" {
			continue
		}
		for key, candidate := range state.items {
			if !ttlItemExpired(candidate, ttl.AttributeName, now) {
				continue
			}
			rememberItemBackup(backups, state, key)
			delete(state.items, key)
			tableChanged = true
			changed = true
		}
		if tableChanged {
			state.description.ItemCount = len(state.items)
			updateIndexItemCounts(state)
		}
	}
	return backups, changed
}

func ttlItemExpired(value item, attributeName string, now time.Time) bool {
	attr, ok := value[attributeName]
	if !ok {
		return false
	}
	seconds, ok := attr["N"].(string)
	if !ok {
		return false
	}
	expiry, ok := new(big.Rat).SetString(seconds)
	if !ok {
		return false
	}
	return expiry.Cmp(big.NewRat(now.Unix(), 1)) <= 0
}
func continuousBackupsDescriptionForState(state *tableState) continuousBackupsDescription {
	if state != nil && state.continuousBackups != nil {
		return *cloneContinuousBackupsDescription(state.continuousBackups)
	}
	return continuousBackupsDescription{
		ContinuousBackupsStatus: "ENABLED",
		PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{
			PointInTimeRecoveryStatus: "DISABLED",
		},
	}
}

func backupDescriptionForTable(description tableDescription, backupName string, createdAt int64) backupDescription {
	backupARN := fmt.Sprintf("%s/backup/%d-%s", description.TableArn, createdAt, backupName)
	return backupDescription{
		BackupDetails: backupDetails{
			BackupArn:              backupARN,
			BackupCreationDateTime: createdAt,
			BackupName:             backupName,
			BackupSizeBytes:        description.TableSizeBytes,
			BackupStatus:           "AVAILABLE",
			BackupType:             "USER",
		},
		SourceTableDetails: sourceTableDetails{
			AttributeDefinitions:  append([]attributeDefinition(nil), description.AttributeDefinitions...),
			BillingMode:           billingModeFromDescription(description),
			ItemCount:             description.ItemCount,
			KeySchema:             append([]keySchemaElement(nil), description.KeySchema...),
			TableArn:              description.TableArn,
			TableCreationDateTime: description.CreationDateTime,
			TableID:               description.TableArn,
			TableName:             description.TableName,
			TableSizeBytes:        description.TableSizeBytes,
		},
	}
}

func (s *Server) restoredTableDescription(targetTableName string, backup backupDescription, createdAt int64) tableDescription {
	region := defaultString(s.config.Region, "us-east-1")
	description, ok := s.backupTables[backup.BackupDetails.BackupArn]
	if !ok {
		description = tableDescription{
			AttributeDefinitions: append([]attributeDefinition(nil), backup.SourceTableDetails.AttributeDefinitions...),
			BillingModeSummary:   &billingModeSummary{BillingMode: defaultString(backup.SourceTableDetails.BillingMode, "PAY_PER_REQUEST")},
			KeySchema:            append([]keySchemaElement(nil), backup.SourceTableDetails.KeySchema...),
		}
	} else {
		description = cloneTableDescription(description)
	}
	description.CreationDateTime = createdAt
	description.ItemCount = len(s.backupItems[backup.BackupDetails.BackupArn])
	description.LatestStreamArn = ""
	description.LatestStreamLabel = ""
	description.StreamSpecification = nil
	description.TableArn = "arn:aws:dynamodb:" + region + ":000000000000:table/" + targetTableName
	description.TableName = targetTableName
	description.TableSizeBytes = backup.BackupDetails.BackupSizeBytes
	description.TableStatus = "ACTIVE"
	for i := range description.GlobalSecondaryIndexes {
		description.GlobalSecondaryIndexes[i].IndexArn = description.TableArn + "/index/" + description.GlobalSecondaryIndexes[i].IndexName
	}
	for i := range description.LocalSecondaryIndexes {
		description.LocalSecondaryIndexes[i].IndexArn = description.TableArn + "/index/" + description.LocalSecondaryIndexes[i].IndexName
	}
	return description
}

func backupSummaryForDescription(description backupDescription) backupSummary {
	return backupSummary{
		BackupArn:              description.BackupDetails.BackupArn,
		BackupCreationDateTime: description.BackupDetails.BackupCreationDateTime,
		BackupName:             description.BackupDetails.BackupName,
		BackupSizeBytes:        description.BackupDetails.BackupSizeBytes,
		BackupStatus:           description.BackupDetails.BackupStatus,
		BackupType:             description.BackupDetails.BackupType,
		TableArn:               description.SourceTableDetails.TableArn,
		TableName:              description.SourceTableDetails.TableName,
	}
}

func billingModeFromDescription(description tableDescription) string {
	if description.BillingModeSummary == nil {
		return "PAY_PER_REQUEST"
	}
	return description.BillingModeSummary.BillingMode
}

func cloneContinuousBackupsDescription(value *continuousBackupsDescription) *continuousBackupsDescription {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
