import { Bucket, S3Object, ObjectDetail, MultipartUpload, ObjectVersion, ActivityEntry, S3Status } from "./types";

export const MOCK_STATUS: S3Status = {
  running: true,
  endpoint: "http://127.0.0.1:4566",
  region: "us-east-1",
  authMode: "relaxed",
  version: "3.0.4",
  storagePath: ".devcloud/data",
};

export const MOCK_BUCKETS: Bucket[] = [
  {
    name: "demo",
    region: "us-east-1",
    createdAt: "2026-04-30T10:00:00Z",
    objectCount: 12,
    totalBytes: 1048576,
    versioning: "Off",
  },
  {
    name: "website-assets",
    region: "us-east-1",
    createdAt: "2026-04-29T08:30:00Z",
    objectCount: 45,
    totalBytes: 52428800,
    versioning: "Enabled",
  },
  {
    name: "logs",
    region: "ap-northeast-1",
    createdAt: "2026-04-28T14:00:00Z",
    objectCount: 1823,
    totalBytes: 268435456,
    versioning: "Off",
  },
  {
    name: "backups",
    region: "us-east-1",
    createdAt: "2026-04-27T09:00:00Z",
    objectCount: 0,
    totalBytes: 0,
    versioning: "Suspended",
  },
];

type ObjectStore = Record<string, Record<string, { commonPrefixes: string[]; objects: S3Object[] }>>;

export const MOCK_OBJECT_STORE: ObjectStore = {
  demo: {
    "": {
      commonPrefixes: ["assets/", "config/"],
      objects: [
        {
          key: "README.md",
          size: 8192,
          etag: '"d41d8cd98f00b204e9800998ecf8427e"',
          contentType: "text/markdown",
          lastModified: "2026-04-30T10:00:00Z",
          storageClass: "STANDARD",
        },
        {
          key: "app.js",
          size: 43120,
          etag: '"abc123def456789012345678901234"',
          contentType: "application/javascript",
          lastModified: "2026-04-30T10:01:22Z",
          storageClass: "STANDARD",
        },
        {
          key: "package.json",
          size: 1234,
          etag: '"xyz789abc123def456789012345678"',
          contentType: "application/json",
          lastModified: "2026-04-30T09:55:10Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "assets/": {
      commonPrefixes: ["assets/images/", "assets/fonts/"],
      objects: [
        {
          key: "assets/styles.css",
          size: 15360,
          etag: '"css123abc456def789012345678901"',
          contentType: "text/css",
          lastModified: "2026-04-30T09:00:00Z",
          storageClass: "STANDARD",
        },
        {
          key: "assets/main.js",
          size: 128000,
          etag: '"js456abc789def012345678901234"',
          contentType: "application/javascript",
          lastModified: "2026-04-30T09:10:33Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "assets/images/": {
      commonPrefixes: [],
      objects: [
        {
          key: "assets/images/hero.png",
          size: 524288,
          etag: '"img123abc456def789012345678"',
          contentType: "image/png",
          lastModified: "2026-04-29T16:20:00Z",
          storageClass: "STANDARD",
        },
        {
          key: "assets/images/logo.svg",
          size: 4096,
          etag: '"svg456abc789def012345678901"',
          contentType: "image/svg+xml",
          lastModified: "2026-04-29T16:21:05Z",
          storageClass: "STANDARD",
        },
        {
          key: "assets/images/banner.jpg",
          size: 204800,
          etag: '"jpg789abc012def345678901234"',
          contentType: "image/jpeg",
          lastModified: "2026-04-29T17:00:00Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "assets/fonts/": {
      commonPrefixes: [],
      objects: [
        {
          key: "assets/fonts/inter-regular.woff2",
          size: 35840,
          etag: '"woff2abc123def456789012345678"',
          contentType: "font/woff2",
          lastModified: "2026-04-29T14:00:00Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "config/": {
      commonPrefixes: [],
      objects: [
        {
          key: "config/app.yaml",
          size: 512,
          etag: '"yaml123abc456def789012345678"',
          contentType: "application/yaml",
          lastModified: "2026-04-30T08:30:00Z",
          storageClass: "STANDARD",
        },
        {
          key: "config/example.env",
          size: 256,
          etag: '"env456abc789def012345678901"',
          contentType: "text/plain",
          lastModified: "2026-04-30T08:31:00Z",
          storageClass: "STANDARD",
        },
      ],
    },
  },
  "website-assets": {
    "": {
      commonPrefixes: ["static/", "media/", "uploads/"],
      objects: [
        {
          key: "index.html",
          size: 6144,
          etag: '"html123abc456def789012345"',
          contentType: "text/html",
          lastModified: "2026-04-29T12:00:00Z",
          storageClass: "STANDARD",
        },
        {
          key: "robots.txt",
          size: 128,
          etag: '"robots123abc456def789012"',
          contentType: "text/plain",
          lastModified: "2026-04-28T10:00:00Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "static/": {
      commonPrefixes: ["static/js/", "static/css/"],
      objects: [],
    },
    "media/": {
      commonPrefixes: [],
      objects: [
        {
          key: "media/video.mp4",
          size: 10485760,
          etag: '"mp4abc123def456789012345678"',
          contentType: "video/mp4",
          lastModified: "2026-04-28T18:00:00Z",
          storageClass: "STANDARD",
        },
      ],
    },
    "uploads/": {
      commonPrefixes: ["uploads/2026/"],
      objects: [],
    },
  },
  logs: {
    "": {
      commonPrefixes: ["2026/"],
      objects: [],
    },
    "2026/": {
      commonPrefixes: ["2026/04/"],
      objects: [],
    },
    "2026/04/": {
      commonPrefixes: [],
      objects: [
        {
          key: "2026/04/access-2026-04-30.log",
          size: 1048576,
          etag: '"log123abc456def789012345678"',
          contentType: "text/plain",
          lastModified: "2026-04-30T23:59:59Z",
          storageClass: "STANDARD",
        },
        {
          key: "2026/04/access-2026-04-29.log",
          size: 983040,
          etag: '"log456abc789def012345678901"',
          contentType: "text/plain",
          lastModified: "2026-04-29T23:59:59Z",
          storageClass: "STANDARD",
        },
        {
          key: "2026/04/error-2026-04-30.log",
          size: 4096,
          etag: '"log789abc012def345678901234"',
          contentType: "text/plain",
          lastModified: "2026-04-30T23:59:59Z",
          storageClass: "STANDARD",
        },
      ],
    },
  },
  backups: {
    "": {
      commonPrefixes: [],
      objects: [],
    },
  },
};

export const MOCK_OBJECT_DETAILS: Record<string, ObjectDetail> = {
  "demo::README.md": {
    key: "README.md",
    bucket: "demo",
    size: 8192,
    etag: '"d41d8cd98f00b204e9800998ecf8427e"',
    contentType: "text/markdown",
    lastModified: "2026-04-30T10:00:00Z",
    storageClass: "STANDARD",
    metadata: {
      "content-type": "text/markdown",
      "content-length": "8192",
      "x-amz-meta-source": "local",
      "x-amz-meta-author": "devcloud",
      "x-amz-storage-class": "STANDARD",
    },
    versionId: "null",
    s3Uri: "s3://demo/README.md",
    endpointUrl: "http://127.0.0.1:4566/demo/README.md",
    previewType: "text",
    previewText: `# Demo Project

This is a demo project for devcloud S3 testing.

## Setup

\`\`\`bash
aws --endpoint-url http://127.0.0.1:4566 configure
\`\`\`

## Usage

Upload a file:

\`\`\`bash
aws --endpoint-url http://127.0.0.1:4566 s3 cp ./myfile.txt s3://demo/myfile.txt
\`\`\`

List objects:

\`\`\`bash
aws --endpoint-url http://127.0.0.1:4566 s3 ls s3://demo/
\`\`\`
`,
  },
  "demo::package.json": {
    key: "package.json",
    bucket: "demo",
    size: 1234,
    etag: '"xyz789abc123def456789012345678"',
    contentType: "application/json",
    lastModified: "2026-04-30T09:55:10Z",
    storageClass: "STANDARD",
    metadata: {
      "content-type": "application/json",
      "content-length": "1234",
      "x-amz-meta-env": "development",
      "x-amz-storage-class": "STANDARD",
    },
    versionId: "null",
    s3Uri: "s3://demo/package.json",
    endpointUrl: "http://127.0.0.1:4566/demo/package.json",
    previewType: "json",
    previewText: JSON.stringify(
      {
        name: "demo",
        version: "1.0.0",
        description: "Demo project",
        scripts: {
          start: "node app.js",
          build: "webpack --mode production",
          test: "jest",
        },
        dependencies: {
          express: "^4.18.2",
          lodash: "^4.17.21",
        },
        devDependencies: {
          webpack: "^5.88.0",
          jest: "^29.0.0",
        },
      },
      null,
      2
    ),
  },
  "demo::app.js": {
    key: "app.js",
    bucket: "demo",
    size: 43120,
    etag: '"abc123def456789012345678901234"',
    contentType: "application/javascript",
    lastModified: "2026-04-30T10:01:22Z",
    storageClass: "STANDARD",
    metadata: {
      "content-type": "application/javascript",
      "content-length": "43120",
      "x-amz-storage-class": "STANDARD",
    },
    versionId: "null",
    s3Uri: "s3://demo/app.js",
    endpointUrl: "http://127.0.0.1:4566/demo/app.js",
    previewType: "text",
    previewText: `const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

app.get('/', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/health', (req, res) => {
  res.json({ healthy: true });
});

app.listen(port, () => {
  console.log(\`Server running on port \${port}\`);
});

module.exports = app;`,
  },
  "demo::assets/styles.css": {
    key: "assets/styles.css",
    bucket: "demo",
    size: 15360,
    etag: '"css123abc456def789012345678901"',
    contentType: "text/css",
    lastModified: "2026-04-30T09:00:00Z",
    storageClass: "STANDARD",
    metadata: { "content-type": "text/css", "content-length": "15360", "x-amz-storage-class": "STANDARD" },
    versionId: "null",
    s3Uri: "s3://demo/assets/styles.css",
    endpointUrl: "http://127.0.0.1:4566/demo/assets/styles.css",
    previewType: "text",
    previewText: `:root {
  --color-primary: #176B4D;
  --color-secondary: #245B8F;
  --font-size-base: 14px;
}

body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #F7F8F5;
  color: #1D211C;
}`,
  },
  "demo::config/app.yaml": {
    key: "config/app.yaml",
    bucket: "demo",
    size: 512,
    etag: '"yaml123abc456def789012345678"',
    contentType: "application/yaml",
    lastModified: "2026-04-30T08:30:00Z",
    storageClass: "STANDARD",
    metadata: { "content-type": "application/yaml", "content-length": "512", "x-amz-meta-env": "local", "x-amz-storage-class": "STANDARD" },
    versionId: "null",
    s3Uri: "s3://demo/config/app.yaml",
    endpointUrl: "http://127.0.0.1:4566/demo/config/app.yaml",
    previewType: "text",
    previewText: `app:
  name: demo-app
  port: 3000
  environment: local

database:
  host: localhost
  port: 5432
  name: demo_db

storage:
  provider: s3
  endpoint: http://127.0.0.1:4566
  bucket: demo
  region: us-east-1`,
  },
};

export const MOCK_VERSIONS: Record<string, ObjectVersion[]> = {
  "website-assets::index.html": [
    {
      versionId: "versionId-20260430-v3",
      isLatest: true,
      isDeleteMarker: false,
      size: 6144,
      etag: '"html123abc456def789012345"',
      lastModified: "2026-04-29T12:00:00Z",
    },
    {
      versionId: "versionId-20260428-v2",
      isLatest: false,
      isDeleteMarker: false,
      size: 5900,
      etag: '"html456abc789def012345678"',
      lastModified: "2026-04-28T10:30:00Z",
    },
    {
      versionId: "versionId-20260426-v1",
      isLatest: false,
      isDeleteMarker: false,
      size: 4096,
      etag: '"html789abc012def345678901"',
      lastModified: "2026-04-26T09:00:00Z",
    },
  ],
};

export const MOCK_MULTIPART: Record<string, MultipartUpload[]> = {
  demo: [
    {
      uploadId: "VXBsb2FkSWQtMTIzNDU2Nzg5MGFiY2RlZg",
      key: "assets/large-bundle.js",
      initiated: "2026-04-30T09:45:00Z",
      parts: 3,
      uploadedSize: 15728640,
    },
  ],
  "website-assets": [
    {
      uploadId: "VXBsb2FkSWQtYWJjZGVmMDEyMzQ1Njc4OQ",
      key: "media/background-video.mp4",
      initiated: "2026-04-30T08:00:00Z",
      parts: 7,
      uploadedSize: 73400320,
    },
    {
      uploadId: "VXBsb2FkSWQtOTg3NjU0MzIxMGZlZGNiYQ",
      key: "media/intro.mp4",
      initiated: "2026-04-29T22:30:00Z",
      parts: 2,
      uploadedSize: 20971520,
    },
  ],
};

export const INITIAL_ACTIVITY: ActivityEntry = {
  method: "PUT",
  path: "/demo/README.md",
  timestamp: "2026-04-30T10:00:00Z",
  statusCode: 200,
};

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString("ja-JP", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export function formatDateFull(iso: string): string {
  const d = new Date(iso);
  return d.toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

export function getDisplayName(key: string, prefix: string): string {
  return key.replace(prefix, "");
}

export function getObjectDetailKey(bucket: string, key: string): string {
  return `${bucket}::${key}`;
}

export function getMockObjectDetail(bucket: string, key: string): ObjectDetail {
  const detailKey = getObjectDetailKey(bucket, key);
  if (MOCK_OBJECT_DETAILS[detailKey]) return MOCK_OBJECT_DETAILS[detailKey];

  const store = MOCK_OBJECT_STORE[bucket];
  if (store) {
    for (const prefix of Object.keys(store)) {
      const found = store[prefix].objects.find((o) => o.key === key);
      if (found) {
        return {
          ...found,
          bucket,
          metadata: {
            "content-type": found.contentType,
            "content-length": String(found.size),
            "x-amz-storage-class": found.storageClass,
          },
          versionId: "null",
          s3Uri: `s3://${bucket}/${key}`,
          endpointUrl: `http://127.0.0.1:4566/${bucket}/${key}`,
          previewType: found.size > 262144 ? "binary" : "none",
        };
      }
    }
  }

  return {
    key,
    bucket,
    size: 0,
    etag: '""',
    contentType: "application/octet-stream",
    lastModified: new Date().toISOString(),
    storageClass: "STANDARD",
    metadata: {},
    versionId: "null",
    s3Uri: `s3://${bucket}/${key}`,
    endpointUrl: `http://127.0.0.1:4566/${bucket}/${key}`,
    previewType: "none",
  };
}
