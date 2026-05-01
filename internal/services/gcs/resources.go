package gcs

type bucketResource struct {
	Kind          string `json:"kind"`
	ID            string `json:"id"`
	SelfLink      string `json:"selfLink"`
	ProjectNumber string `json:"projectNumber,omitempty"`
	Name          string `json:"name"`
	TimeCreated   string `json:"timeCreated"`
	Updated       string `json:"updated"`
	Location      string `json:"location"`
	StorageClass  string `json:"storageClass"`
}

type bucketsListResponse struct {
	Kind          string           `json:"kind"`
	Items         []bucketResource `json:"items,omitempty"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

type objectResource struct {
	Kind               string            `json:"kind"`
	ID                 string            `json:"id"`
	SelfLink           string            `json:"selfLink"`
	Name               string            `json:"name"`
	Bucket             string            `json:"bucket"`
	Generation         string            `json:"generation"`
	Metageneration     string            `json:"metageneration"`
	ContentType        string            `json:"contentType"`
	Size               string            `json:"size"`
	MD5Hash            string            `json:"md5Hash,omitempty"`
	CRC32C             string            `json:"crc32c,omitempty"`
	ETag               string            `json:"etag"`
	TimeCreated        string            `json:"timeCreated"`
	Updated            string            `json:"updated"`
	StorageClass       string            `json:"storageClass"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
}

type objectsListResponse struct {
	Kind          string           `json:"kind"`
	Items         []objectResource `json:"items,omitempty"`
	Prefixes      []string         `json:"prefixes,omitempty"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

type rewriteResponse struct {
	Kind                string         `json:"kind"`
	TotalBytesRewritten string         `json:"totalBytesRewritten"`
	ObjectSize          string         `json:"objectSize"`
	Done                bool           `json:"done"`
	Resource            objectResource `json:"resource"`
}

type composeRequest struct {
	SourceObjects []composeSourceObject `json:"sourceObjects"`
	Destination   composeDestination    `json:"destination"`
}

type composeSourceObject struct {
	Name                string `json:"name"`
	Generation          string `json:"generation,omitempty"`
	ObjectPreconditions struct {
		IfGenerationMatch string `json:"ifGenerationMatch,omitempty"`
	} `json:"objectPreconditions,omitempty"`
}

type composeDestination struct {
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Errors  []errorItem `json:"errors"`
}

type errorItem struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}
