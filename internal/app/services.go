package app

import (
	"fmt"
	"sort"
	"strings"
)

// ServiceNames returns the canonical service identifiers accepted by
// ApplyServiceSelection, in alphabetical order.
func ServiceNames() []string {
	names := make([]string, 0, len(serviceToggles))
	for name := range serviceToggles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ApplyServiceSelection returns a copy of cfg with services.*.enabled set so
// that only the services named in selected are enabled. Names are matched
// case-insensitively and accept the aliases listed in serviceAliases. The
// dashboard is always started regardless of selection. An empty or nil
// selection leaves cfg unchanged.
func ApplyServiceSelection(cfg Config, selected []string) (Config, error) {
	if len(selected) == 0 {
		return cfg, nil
	}

	chosen := make(map[string]struct{}, len(selected))
	for _, raw := range selected {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		canonical, ok := serviceAliases[name]
		if !ok {
			return Config{}, fmt.Errorf("unknown service %q (known: %s)", raw, strings.Join(ServiceNames(), ", "))
		}
		chosen[canonical] = struct{}{}
	}

	if len(chosen) == 0 {
		return cfg, nil
	}

	out := cfg
	for name, toggle := range serviceToggles {
		_, enable := chosen[name]
		toggle(&out, enable)
	}
	return out, nil
}

// serviceToggles maps canonical service names to functions that set the
// corresponding services.*.Enabled flag on a Config.
var serviceToggles = map[string]func(*Config, bool){
	"mail":           func(c *Config, v bool) { c.Services.Mail.Enabled = v },
	"s3":             func(c *Config, v bool) { c.Services.S3.Enabled = v },
	"gcs":            func(c *Config, v bool) { c.Services.GCS.Enabled = v },
	"dynamodb":       func(c *Config, v bool) { c.Services.DynamoDB.Enabled = v },
	"bigquery":       func(c *Config, v bool) { c.Services.BigQuery.Enabled = v },
	"redshift":       func(c *Config, v bool) { c.Services.Redshift.Enabled = v },
	"redis":          func(c *Config, v bool) { c.Services.Redis.Enabled = v },
	"sqs":            func(c *Config, v bool) { c.Services.SQS.Enabled = v },
	"pubsub":         func(c *Config, v bool) { c.Services.PubSub.Enabled = v },
	"appautoscaling": func(c *Config, v bool) { c.Services.AppAutoScaling.Enabled = v },
}

// serviceAliases maps user-facing names (lowercase) to canonical keys in
// serviceToggles. Allows common variants like "smtp" for mail or
// "application-autoscaling" for appautoscaling.
var serviceAliases = map[string]string{
	"mail":                   "mail",
	"smtp":                   "mail",
	"s3":                     "s3",
	"gcs":                    "gcs",
	"dynamodb":               "dynamodb",
	"ddb":                    "dynamodb",
	"bigquery":               "bigquery",
	"bq":                     "bigquery",
	"redshift":               "redshift",
	"redis":                  "redis",
	"sqs":                    "sqs",
	"pubsub":                 "pubsub",
	"pub-sub":                "pubsub",
	"pub_sub":                "pubsub",
	"appautoscaling":         "appautoscaling",
	"app-autoscaling":        "appautoscaling",
	"app_autoscaling":        "appautoscaling",
	"applicationautoscaling": "appautoscaling",
}
