package dynamodb

import (
	"sort"
)

func (s *Server) Snapshot() DashboardSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := make([]DashboardTableSnapshot, 0, len(s.tables))
	for _, state := range s.tables {
		tables = append(tables, DashboardTableSnapshot{
			TableName:              state.description.TableName,
			TableStatus:            state.description.TableStatus,
			ItemCount:              state.description.ItemCount,
			KeySchema:              append([]keySchemaElement(nil), state.description.KeySchema...),
			GlobalSecondaryIndexes: append([]globalSecondaryIndexDescription(nil), state.description.GlobalSecondaryIndexes...),
			LocalSecondaryIndexes:  append([]localSecondaryIndexDescription(nil), state.description.LocalSecondaryIndexes...),
			LatestStreamArn:        state.description.LatestStreamArn,
			LatestStreamLabel:      state.description.LatestStreamLabel,
			StreamSpecification:    cloneStreamSpecification(state.description.StreamSpecification),
			TimeToLiveDescription:  ttlDescription(state.description),
		})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})
	return DashboardSnapshot{
		Running: true,
		Status:  "running",
		Region:  defaultString(s.config.Region, "us-east-1"),
		Tables:  tables,
	}
}

func (s *Server) TableSnapshot(tableName string) (DashboardTableSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[tableName]
	if !ok {
		return DashboardTableSnapshot{}, false
	}
	return DashboardTableSnapshot{
		TableName:              state.description.TableName,
		TableStatus:            state.description.TableStatus,
		ItemCount:              state.description.ItemCount,
		KeySchema:              append([]keySchemaElement(nil), state.description.KeySchema...),
		GlobalSecondaryIndexes: append([]globalSecondaryIndexDescription(nil), state.description.GlobalSecondaryIndexes...),
		LocalSecondaryIndexes:  append([]localSecondaryIndexDescription(nil), state.description.LocalSecondaryIndexes...),
		LatestStreamArn:        state.description.LatestStreamArn,
		LatestStreamLabel:      state.description.LatestStreamLabel,
		StreamSpecification:    cloneStreamSpecification(state.description.StreamSpecification),
		TimeToLiveDescription:  ttlDescription(state.description),
	}, true
}

func (s *Server) TableItems(tableName string, limit int) ([]DashboardItemSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[tableName]
	if !ok {
		return nil, false
	}
	source := sortedItems(state)
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	items := make([]DashboardItemSnapshot, 0, minInt(limit, len(source)))
	for _, candidate := range source {
		if len(items) == limit {
			break
		}
		key, err := extractKey(state.description, candidate.value)
		if err != nil {
			continue
		}
		items = append(items, DashboardItemSnapshot{
			Key:  dashboardItemPayload(key),
			Item: dashboardItemPayload(candidate.value),
		})
	}
	return items, true
}
func dashboardItemPayload(value item) map[string]any {
	payload := make(map[string]any, len(value))
	for name, attr := range value {
		payload[name] = cloneAttributeValue(attr)
	}
	return payload
}
