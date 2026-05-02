package dashboard

type DashboardService struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	Status      string `json:"status"`
	Endpoint    string `json:"endpoint,omitempty"`
	StoragePath string `json:"storagePath,omitempty"`
	Description string `json:"description"`
}

type dashboardServicesResponse struct {
	Services []DashboardService `json:"services"`
}

func (s *Server) dashboardServices() []DashboardService {
	services := []DashboardService{
		{
			ID:          "mail",
			Name:        "Mail",
			Path:        "/mail",
			Status:      mailServiceStatus(s.config.MailDisabled),
			Endpoint:    defaultString(s.config.MailEndpoint, "smtp://127.0.0.1:1025"),
			StoragePath: defaultString(s.config.MailStoragePath, ".devcloud/data/mail"),
			Description: "Inspect messages received by the local SMTP server.",
		},
	}

	services = append(services, DashboardService{
		ID:          "s3",
		Name:        "S3",
		Path:        "/s3",
		Status:      s3ServiceStatus(s.s3 != nil),
		Endpoint:    defaultString(s.config.S3Endpoint, "http://127.0.0.1:4566"),
		StoragePath: defaultString(s.config.S3StoragePath, ".devcloud/data/s3"),
		Description: "Browse buckets, objects, metadata, and local S3 activity.",
	})

	services = append(services, DashboardService{
		ID:          "gcs",
		Name:        "GCS",
		Path:        "/gcs",
		Status:      objectServiceStatus(s.gcs != nil),
		Endpoint:    defaultString(s.config.GCSEndpoint, "http://127.0.0.1:4443"),
		StoragePath: defaultString(s.config.GCSStoragePath, ".devcloud/data/s3"),
		Description: "Browse buckets, objects, metadata, and local GCS activity.",
	})

	services = append(services, DashboardService{
		ID:          "dynamodb",
		Name:        "DynamoDB",
		Path:        "/dynamodb",
		Status:      objectServiceStatus(s.dynamo != nil),
		Endpoint:    defaultString(s.config.DynamoDBEndpoint, "http://127.0.0.1:8000"),
		StoragePath: defaultString(s.config.DynamoDBStoragePath, ".devcloud/data/dynamodb"),
		Description: "Inspect local DynamoDB tables, indexes, and item counts.",
	})

	services = append(services, DashboardService{
		ID:          "bigquery",
		Name:        "BigQuery",
		Path:        "/bigquery",
		Status:      objectServiceStatus(s.bq != nil),
		Endpoint:    defaultString(s.config.BigQueryEndpoint, "http://127.0.0.1:9050"),
		StoragePath: defaultString(s.config.BigQueryStoragePath, ".devcloud/data/bigquery"),
		Description: "Inspect local BigQuery projects, datasets, tables, rows, and jobs.",
	})

	services = append(services, DashboardService{
		ID:          "sqs",
		Name:        "SQS",
		Path:        "/dashboard/sqs",
		Status:      objectServiceStatus(s.sqs != nil),
		Endpoint:    defaultString(s.config.SQSEndpoint, "http://127.0.0.1:9324"),
		StoragePath: defaultString(s.config.SQSStoragePath, ".devcloud/data/sqs"),
		Description: "Inspect local SQS queues, messages, leases, and attributes.",
	})

	return services
}

func mailServiceStatus(disabled bool) string {
	if disabled {
		return "disabled"
	}
	return "running"
}

func s3ServiceStatus(running bool) string {
	return objectServiceStatus(running)
}

func objectServiceStatus(running bool) string {
	if running {
		return "running"
	}
	return "disabled"
}
