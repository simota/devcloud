package s3

import "testing"

func TestParseVirtualHostStyle(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		path       string
		wantBucket string
		wantKey    string
		wantOK     bool
	}{
		{
			name:       "bucket on .localhost",
			host:       "demo-bucket.localhost",
			path:       "/",
			wantBucket: "demo-bucket",
			wantKey:    "",
			wantOK:     true,
		},
		{
			name:       "bucket on .localhost with port",
			host:       "demo-bucket.localhost:4566",
			path:       "/docs/readme.txt",
			wantBucket: "demo-bucket",
			wantKey:    "docs/readme.txt",
			wantOK:     true,
		},
		{
			name:       "bucket on .127.0.0.1",
			host:       "demo-bucket.127.0.0.1",
			path:       "/",
			wantBucket: "demo-bucket",
			wantKey:    "",
			wantOK:     true,
		},
		{
			name:       "bucket on .127.0.0.1 with port and key",
			host:       "demo-bucket.127.0.0.1:4566",
			path:       "/objects/key.bin",
			wantBucket: "demo-bucket",
			wantKey:    "objects/key.bin",
			wantOK:     true,
		},
		{
			name:       "bucket on .0.0.0.0",
			host:       "demo-bucket.0.0.0.0:4566",
			path:       "/",
			wantBucket: "demo-bucket",
			wantKey:    "",
			wantOK:     true,
		},
		{
			name:       "bare localhost falls through to path-style",
			host:       "localhost:4566",
			path:       "/demo-bucket",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "bare 127.0.0.1 falls through to path-style",
			host:       "127.0.0.1:4566",
			path:       "/demo-bucket",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "bare 0.0.0.0 falls through to path-style",
			host:       "0.0.0.0:4566",
			path:       "/demo-bucket",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "bucket name with dots is rejected",
			host:       "legacy.bucket.localhost",
			path:       "/",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "bucket name with dots is rejected on 127.0.0.1",
			host:       "legacy.bucket.127.0.0.1",
			path:       "/",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "non-local suffix is rejected",
			host:       "demo-bucket.s3.amazonaws.com",
			path:       "/",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "empty host returns false",
			host:       "",
			path:       "/",
			wantBucket: "",
			wantKey:    "",
			wantOK:     false,
		},
		{
			name:       "mixed-case host is accepted",
			host:       "Demo-Bucket.LOCALHOST",
			path:       "/Docs/Readme.TXT",
			wantBucket: "demo-bucket",
			wantKey:    "Docs/Readme.TXT",
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotKey, gotOK := parseVirtualHostStyle(tt.host, tt.path)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v (host=%q path=%q bucket=%q key=%q)", gotOK, tt.wantOK, tt.host, tt.path, gotBucket, gotKey)
			}
			if gotBucket != tt.wantBucket {
				t.Fatalf("bucket = %q, want %q", gotBucket, tt.wantBucket)
			}
			if gotKey != tt.wantKey {
				t.Fatalf("key = %q, want %q", gotKey, tt.wantKey)
			}
		})
	}
}
