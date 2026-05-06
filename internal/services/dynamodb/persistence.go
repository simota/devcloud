package dynamodb

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func (s *Server) table(name string) (tableDescription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[name]
	if !ok {
		return tableDescription{}, false
	}
	return state.description, true
}

func (s *Server) load() error {
	path := filepath.Join(s.config.StoragePath, "state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	if persisted.Tables == nil {
		persisted.Tables = map[string]persistedTable{}
	}
	for name, table := range persisted.Tables {
		items := table.Items
		if items == nil {
			items = map[string]item{}
		}
		state := &tableState{
			description:            table.Description,
			items:                  items,
			streamRecords:          cloneStreamRecords(table.StreamRecords),
			tags:                   cloneTags(table.Tags),
			continuousBackups:      cloneContinuousBackupsDescription(table.ContinuousBackups),
			resourcePolicy:         table.ResourcePolicy,
			resourcePolicyRevision: table.ResourcePolicyRevision,
		}
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		s.tables[name] = state
	}
	for arn, backup := range persisted.Backups {
		if arn == "" {
			continue
		}
		s.backups[arn] = backup
	}
	for arn, description := range persisted.BackupTables {
		if arn == "" {
			continue
		}
		s.backupTables[arn] = cloneTableDescription(description)
	}
	for arn, items := range persisted.BackupItems {
		if arn == "" {
			continue
		}
		s.backupItems[arn] = cloneItems(items)
	}
	return nil
}

func (s *Server) persistLocked() error {
	if s.config.StoragePath == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
		return err
	}
	persisted := persistedState{
		Tables:       map[string]persistedTable{},
		Backups:      map[string]backupDescription{},
		BackupTables: map[string]tableDescription{},
		BackupItems:  map[string]map[string]item{},
	}
	for name, state := range s.tables {
		items := make(map[string]item, len(state.items))
		for key, value := range state.items {
			items[key] = cloneItem(value)
		}
		persisted.Tables[name] = persistedTable{
			Description:            state.description,
			Items:                  items,
			StreamRecords:          cloneStreamRecords(state.streamRecords),
			Tags:                   cloneTags(state.tags),
			ContinuousBackups:      cloneContinuousBackupsDescription(state.continuousBackups),
			ResourcePolicy:         state.resourcePolicy,
			ResourcePolicyRevision: state.resourcePolicyRevision,
		}
	}
	for arn, backup := range s.backups {
		persisted.Backups[arn] = backup
	}
	for arn, description := range s.backupTables {
		persisted.BackupTables[arn] = cloneTableDescription(description)
	}
	for arn, items := range s.backupItems {
		persisted.BackupItems[arn] = cloneItems(items)
	}
	path := filepath.Join(s.config.StoragePath, "state.json")
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(persisted)
	closeErr := file.Close()
	if encodeErr != nil {
		os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, path)
}

func (s *Server) maxItemBytes() int64 {
	if s.config.MaxItemBytes > 0 {
		return s.config.MaxItemBytes
	}
	return 400000
}

func (s *Server) maxTables() int {
	if s.config.MaxTables > 0 {
		return s.config.MaxTables
	}
	return 256
}
