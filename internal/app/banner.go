package app

import (
	"fmt"
	"io"
)

type endpointRow struct {
	name      string
	endpoint  string
	dashboard string
}

var devcloudBanner = []string{
	"     _                _                 _ ",
	"  __| | _____   _____| | ___  _   _  __| |",
	" / _` |/ _ \\ \\ / / __| |/ _ \\| | | |/ _` |",
	"| (_| |  __/\\ V / (__| | (_) | |_| | (_| |",
	" \\__,_|\\___| \\_/ \\___|_|\\___/ \\__,_|\\__,_|",
}

func printEndpoints(w io.Writer, cfg Config) {
	dashURL := "http://" + loopbackAddr(cfg.Server.DashboardPort)
	rows := collectEndpointRows(cfg, dashURL)

	nameWidth := len("Dashboard")
	endpointWidth := len(dashURL + "/")
	for _, r := range rows {
		if n := len(r.name); n > nameWidth {
			nameWidth = n
		}
		if n := len(r.endpoint); n > endpointWidth {
			endpointWidth = n
		}
	}

	fmt.Fprintln(w)
	for _, line := range devcloudBanner {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Endpoints:")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %-*s  %s\n", nameWidth, "Dashboard", dashURL+"/")
	for _, r := range rows {
		if r.dashboard != "" {
			fmt.Fprintf(w, "  %-*s  %-*s  %s\n", nameWidth, r.name, endpointWidth, r.endpoint, r.dashboard)
		} else {
			fmt.Fprintf(w, "  %-*s  %s\n", nameWidth, r.name, r.endpoint)
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Press Ctrl-C to stop.")
}

func collectEndpointRows(cfg Config, dashURL string) []endpointRow {
	httpAddr := func(port int) string { return "http://" + loopbackAddr(port) }
	dashLink := func(svc string) string { return dashURL + "/dashboard/" + svc }

	var rows []endpointRow
	if cfg.Services.Mail.Enabled {
		rows = append(rows, endpointRow{"Mail (SMTP)", "smtp://" + loopbackAddr(cfg.Server.SMTPPort), dashLink("mail")})
	}
	if cfg.Services.S3.Enabled {
		rows = append(rows, endpointRow{"S3", httpAddr(cfg.Server.S3Port), dashLink("s3")})
	}
	if cfg.Services.GCS.Enabled {
		rows = append(rows, endpointRow{"GCS", httpAddr(cfg.Server.GCSPort), dashLink("gcs")})
	}
	if cfg.Services.DynamoDB.Enabled {
		rows = append(rows, endpointRow{"DynamoDB", httpAddr(cfg.Server.DynamoDBPort), dashLink("dynamodb")})
	}
	if cfg.Services.BigQuery.Enabled {
		rows = append(rows, endpointRow{"BigQuery", httpAddr(cfg.Server.BigQueryPort), dashLink("bigquery")})
	}
	if cfg.Services.SQS.Enabled {
		rows = append(rows, endpointRow{"SQS", httpAddr(cfg.Server.SQSPort), dashLink("sqs")})
	}
	if cfg.Services.PubSub.Enabled {
		rows = append(rows, endpointRow{"Pub/Sub (gRPC)", loopbackAddr(cfg.Server.PubSubGRPCPort), dashLink("pubsub")})
		if cfg.Services.PubSub.EnableREST {
			rows = append(rows, endpointRow{"Pub/Sub (REST)", httpAddr(cfg.Server.PubSubRESTPort), ""})
		}
	}
	if cfg.Services.Redshift.Enabled {
		rows = append(rows, endpointRow{"Redshift (SQL)", "postgres://" + loopbackAddr(cfg.Server.RedshiftPort), dashLink("redshift")})
		rows = append(rows, endpointRow{"Redshift (API)", httpAddr(cfg.Server.RedshiftAPIPort), ""})
	}
	if cfg.Services.Redis.Enabled {
		rows = append(rows, endpointRow{"Redis", redisEndpointForDisplay(cfg), dashLink("redis")})
	}
	return rows
}
