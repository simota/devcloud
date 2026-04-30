package dashboard

type DashboardService struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	Status      string `json:"status"`
	Endpoint    string `json:"endpoint,omitempty"`
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
			Description: "Inspect messages received by the local SMTP server.",
		},
	}

	services = append(services, DashboardService{
		ID:          "s3",
		Name:        "S3",
		Path:        "/s3",
		Status:      s3ServiceStatus(s.s3 != nil),
		Endpoint:    defaultString(s.config.S3Endpoint, "http://127.0.0.1:4566"),
		Description: "Browse buckets, objects, metadata, and local S3 activity.",
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
	if running {
		return "running"
	}
	return "disabled"
}
