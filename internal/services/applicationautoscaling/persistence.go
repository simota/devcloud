package applicationautoscaling

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

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
	if persisted.ScalableTargets != nil {
		s.scalableTargets = persisted.ScalableTargets
	}
	if persisted.ScalingPolicies != nil {
		s.scalingPolicies = persisted.ScalingPolicies
	}
	if persisted.ScheduledActions != nil {
		s.scheduledActions = persisted.ScheduledActions
	}
	if persisted.Tags != nil {
		s.tags = persisted.Tags
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
		ScalableTargets:  map[string]scalableTarget{},
		ScalingPolicies:  map[string]scalingPolicy{},
		ScheduledActions: map[string]scheduledAction{},
		Tags:             map[string]map[string]string{},
	}
	for k, v := range s.scalableTargets {
		persisted.ScalableTargets[k] = v
	}
	for k, v := range s.scalingPolicies {
		persisted.ScalingPolicies[k] = v
	}
	for k, v := range s.scheduledActions {
		persisted.ScheduledActions[k] = v
	}
	for arn, kv := range s.tags {
		cloned := make(map[string]string, len(kv))
		for tk, tv := range kv {
			cloned[tk] = tv
		}
		persisted.Tags[arn] = cloned
	}
	path := filepath.Join(s.config.StoragePath, "state.json")
	tmpPath := path + ".tmp"
	data, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
