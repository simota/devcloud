import React, { useState } from "react";
import {
  X,
  Download,
  Copy,
  Trash2,
  Check,
  FileText,
  Info,
  Tag,
  GitBranch,
  Layers,
  Eye,
  Terminal,
  Clock,
  XCircle,
  AlertTriangle,
} from "lucide-react";
import { ObjectDetail, ObjectVersion, MultipartUpload } from "./types";
import { formatBytes, formatDateFull } from "./mockData";
import { MOCK_VERSIONS, MOCK_MULTIPART } from "./mockData";

interface ObjectInspectorProps {
  object: ObjectDetail | null;
  bucketName: string | null;
  onClose?: () => void;
  onDelete: (obj: ObjectDetail) => void;
}

type InspectorTab = "overview" | "metadata" | "versions" | "multipart" | "preview";

const TAB_CONFIG: { id: InspectorTab; label: string; icon: React.ReactNode }[] = [
  { id: "overview", label: "Overview", icon: <Info size={12} /> },
  { id: "metadata", label: "Metadata", icon: <Tag size={12} /> },
  { id: "versions", label: "Versions", icon: <GitBranch size={12} /> },
  { id: "multipart", label: "Multipart", icon: <Layers size={12} /> },
  { id: "preview", label: "Preview", icon: <Eye size={12} /> },
];

export function ObjectInspector({ object, bucketName, onClose, onDelete }: ObjectInspectorProps) {
  const [activeTab, setActiveTab] = useState<InspectorTab>("overview");
  const [copied, setCopied] = useState<string | null>(null);

  function copy(text: string, key: string) {
    navigator.clipboard.writeText(text);
    setCopied(key);
    setTimeout(() => setCopied(null), 1500);
  }

  function CopyBtn({ text, label }: { text: string; label: string }) {
    const isCopied = copied === label;
    return (
      <button
        onClick={() => copy(text, label)}
        title={`Copy ${label}`}
        aria-label={`Copy ${label}`}
        style={{
          display: "flex",
          alignItems: "center",
          gap: "4px",
          fontSize: "11px",
          fontWeight: 500,
          color: isCopied ? "#059669" : "#737373",
          background: "none",
          border: "1px solid #E8E8E5",
          borderRadius: "10px",
          padding: "3px 8px",
          transition: "all 0.15s ease",
          cursor: "pointer",
          whiteSpace: "nowrap",
        }}
      >
        {isCopied ? <Check size={11} /> : <Copy size={11} />}
        {isCopied ? "Copied" : label}
      </button>
    );
  }

  if (!object) {
    return (
      <div
        style={{
          width: "360px",
          minWidth: "300px",
          background: "#FFFFFF",
          borderLeft: "1px solid #E8E8E5",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          padding: "32px 20px",
          gap: "10px",
          textAlign: "center",
          flexShrink: 0,
        }}
      >
        <FileText size={32} color="#E8E8E5" />
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A" }}>Object Inspector</div>
        <div style={{ fontSize: "12px", color: "#737373", lineHeight: "1.6" }}>
          Select an object to inspect metadata, versions, and download options.
        </div>
      </div>
    );
  }

  const displayName = object.key.split("/").pop() || object.key;
  const versions: ObjectVersion[] = MOCK_VERSIONS[`${object.bucket}::${object.key}`] || [];
  const multipartUploads: MultipartUpload[] = MOCK_MULTIPART[object.bucket] || [];

  const cliDownload = `aws --endpoint-url http://127.0.0.1:4566 s3 cp \\\n  s3://${object.bucket}/${object.key} \\\n  ./${displayName}`;
  const cliHead = `aws --endpoint-url http://127.0.0.1:4566 s3api \\\n  head-object \\\n  --bucket ${object.bucket} \\\n  --key ${object.key}`;

  return (
    <div
      style={{
        width: "360px",
        minWidth: "300px",
        background: "#FFFFFF",
        borderLeft: "1px solid #E8E8E5",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        overflow: "hidden",
      }}
    >
      {/* Inspector Header */}
      <div
        style={{
          padding: "10px 14px 8px",
          borderBottom: "1px solid #E8E8E5",
          flexShrink: 0,
        }}
      >
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "8px" }}>
          <div style={{ minWidth: 0, flex: 1 }}>
            <div
              style={{
                fontSize: "13px",
                fontWeight: 600,
                color: "#0A0A0A",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
              title={object.key}
            >
              {displayName}
            </div>
            <div style={{ fontSize: "11px", color: "#737373", marginTop: "2px" }}>
              {formatBytes(object.size)} · {object.contentType}
            </div>
          </div>
          {onClose && (
            <button
              onClick={onClose}
              aria-label="Close inspector"
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                padding: "3px",
                borderRadius: "8px",
                color: "#737373",
                display: "flex",
                flexShrink: 0,
              }}
            >
              <X size={14} />
            </button>
          )}
        </div>

        {/* Action Buttons */}
        <div style={{ display: "flex", gap: "6px", marginTop: "10px", flexWrap: "wrap" }}>
          <button
            onClick={() => window.open(object.endpointUrl, "_blank")}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              fontSize: "12px",
              fontWeight: 500,
              color: "#1E40AF",
              background: "#EFF6FF",
              border: "1px solid #DBEAFE",
              borderRadius: "10px",
              padding: "4px 10px",
              cursor: "pointer",
            }}
          >
            <Download size={12} />
            Download
          </button>
          <button
            onClick={() => onDelete(object)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              fontSize: "12px",
              fontWeight: 500,
              color: "#B42318",
              background: "#FEF2F2",
              border: "1px solid #FECACA",
              borderRadius: "10px",
              padding: "4px 10px",
              cursor: "pointer",
            }}
          >
            <Trash2 size={12} />
            Delete
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div
        style={{
          display: "flex",
          borderBottom: "1px solid #E8E8E5",
          background: "#FFFFFF",
          flexShrink: 0,
          overflowX: "auto",
        }}
      >
        {TAB_CONFIG.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              padding: "8px 10px",
              background: "none",
              border: "none",
              borderBottom: activeTab === tab.id ? "2px solid #10B981" : "2px solid transparent",
              cursor: "pointer",
              fontSize: "12px",
              fontWeight: activeTab === tab.id ? 600 : 400,
              color: activeTab === tab.id ? "#059669" : "#737373",
              whiteSpace: "nowrap",
              marginBottom: "-1px",
            }}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div style={{ flex: 1, overflowY: "auto", padding: "12px 14px" }}>
        {activeTab === "overview" && (
          <OverviewTab object={object} CopyBtn={CopyBtn} />
        )}
        {activeTab === "metadata" && (
          <MetadataTab metadata={object.metadata} />
        )}
        {activeTab === "versions" && (
          <VersionsTab versions={versions} />
        )}
        {activeTab === "multipart" && (
          <MultipartTab uploads={multipartUploads} />
        )}
        {activeTab === "preview" && (
          <PreviewTab object={object} />
        )}
      </div>

      {/* CLI Commands */}
      <div
        style={{
          borderTop: "1px solid #E8E8E5",
          padding: "10px 14px",
          flexShrink: 0,
          background: "#FAFAF9",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "6px",
            marginBottom: "8px",
          }}
        >
          <Terminal size={12} color="#737373" />
          <span style={{ fontSize: "11px", fontWeight: 600, color: "#737373" }}>AWS CLI</span>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
          <CodeBlock code={cliDownload} label="Download" CopyBtn={CopyBtn} />
          <CodeBlock code={cliHead} label="Head object" CopyBtn={CopyBtn} />
        </div>
      </div>
    </div>
  );
}

function OverviewTab({
  object,
  CopyBtn,
}: {
  object: ObjectDetail;
  CopyBtn: React.FC<{ text: string; label: string }>;
}) {
  const fields = [
    { label: "Key", value: object.key, mono: true, copyValue: object.key },
    { label: "Bucket", value: object.bucket },
    { label: "Size", value: formatBytes(object.size) },
    { label: "ETag", value: object.etag.replace(/"/g, ""), mono: true, copyValue: object.etag.replace(/"/g, "") },
    { label: "Content-Type", value: object.contentType, mono: true },
    { label: "Last Modified", value: formatDateFull(object.lastModified) },
    { label: "Storage Class", value: object.storageClass },
    { label: "Version ID", value: object.versionId, mono: true },
    { label: "S3 URI", value: object.s3Uri, mono: true, copyValue: object.s3Uri },
    { label: "Endpoint URL", value: object.endpointUrl, mono: true, copyValue: object.endpointUrl },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
      {fields.map((f) => (
        <div
          key={f.label}
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "2px",
            padding: "7px 0",
            borderBottom: "1px solid #F4F4F2",
          }}
        >
          <span style={{ fontSize: "11px", fontWeight: 600, color: "#737373" }}>{f.label}</span>
          <div style={{ display: "flex", alignItems: "flex-start", gap: "6px", justifyContent: "space-between" }}>
            <span
              style={{
                fontSize: "12px",
                color: "#0A0A0A",
                fontFamily: f.mono ? "'JetBrains Mono', 'SF Mono', Menlo, monospace" : "inherit",
                wordBreak: "break-all",
                flex: 1,
              }}
            >
              {f.value}
            </span>
            {f.copyValue && <CopyBtn text={f.copyValue} label={f.label} />}
          </div>
        </div>
      ))}
    </div>
  );
}

function MetadataTab({ metadata }: { metadata: Record<string, string> }) {
  const systemKeys = Object.keys(metadata).filter((k) => !k.startsWith("x-amz-meta-"));
  const userKeys = Object.keys(metadata).filter((k) => k.startsWith("x-amz-meta-"));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
      <MetadataSection title="System Metadata" keys={systemKeys} metadata={metadata} />
      {userKeys.length > 0 && (
        <MetadataSection title="User Metadata (x-amz-meta-*)" keys={userKeys} metadata={metadata} />
      )}
      {userKeys.length === 0 && (
        <div style={{ fontSize: "12px", color: "#737373" }}>No user metadata found.</div>
      )}
    </div>
  );
}

function MetadataSection({
  title,
  keys,
  metadata,
}: {
  title: string;
  keys: string[];
  metadata: Record<string, string>;
}) {
  return (
    <div>
      <div style={{ fontSize: "12px", fontWeight: 600, color: "#737373", marginBottom: "6px" }}>{title}</div>
      <div
        style={{
          background: "#FAFAF9",
          border: "1px solid #E8E8E5",
          borderRadius: "10px",
          overflow: "hidden",
        }}
      >
        {keys.map((k, i) => (
          <div
            key={k}
            style={{
              display: "flex",
              alignItems: "flex-start",
              gap: "8px",
              padding: "6px 10px",
              borderBottom: i < keys.length - 1 ? "1px solid #F4F4F2" : "none",
            }}
          >
            <span
              style={{
                fontSize: "11px",
                fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
                color: "#1E40AF",
                width: "140px",
                flexShrink: 0,
                wordBreak: "break-all",
              }}
            >
              {k}
            </span>
            <span
              style={{
                fontSize: "11px",
                fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
                color: "#0A0A0A",
                wordBreak: "break-all",
              }}
            >
              {metadata[k]}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function VersionsTab({ versions }: { versions: ObjectVersion[] }) {
  if (versions.length === 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", alignItems: "center", padding: "24px 0" }}>
        <GitBranch size={28} color="#E8E8E5" />
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A" }}>Versioning not enabled</div>
        <div style={{ fontSize: "12px", color: "#737373", textAlign: "center", lineHeight: "1.5" }}>
          Enable versioning on this bucket to track object versions.
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
      {versions.map((v) => (
        <div
          key={v.versionId}
          style={{
            padding: "8px 0",
            borderBottom: "1px solid #F4F4F2",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "3px" }}>
            <span
              style={{
                fontSize: "11px",
                fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
                color: "#1E40AF",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                flex: 1,
              }}
            >
              {v.versionId}
            </span>
            {v.isLatest && (
              <span
                style={{
                  fontSize: "10px",
                  fontWeight: 600,
                  color: "#059669",
                  background: "#D1FAE5",
                  borderRadius: "999px",
                  padding: "1px 5px",
                }}
              >
                LATEST
              </span>
            )}
            {v.isDeleteMarker && (
              <span
                style={{
                  fontSize: "10px",
                  fontWeight: 600,
                  color: "#B42318",
                  background: "#FEF2F2",
                  borderRadius: "999px",
                  padding: "1px 5px",
                }}
              >
                DELETE MARKER
              </span>
            )}
          </div>
          <div style={{ fontSize: "11px", color: "#737373", display: "flex", gap: "8px" }}>
            <span>{formatBytes(v.size)}</span>
            <span>·</span>
            <span style={{ fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace" }}>{v.etag.replace(/"/g, "").slice(0, 12)}…</span>
            <span>·</span>
            <span>{formatDateFull(v.lastModified).slice(0, 16)}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function MultipartTab({ uploads }: { uploads: MultipartUpload[] }) {
  const [aborted, setAborted] = useState<Set<string>>(new Set());

  if (uploads.length === 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", alignItems: "center", padding: "24px 0" }}>
        <Layers size={28} color="#E8E8E5" />
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A" }}>No multipart uploads</div>
        <div style={{ fontSize: "12px", color: "#737373" }}>No incomplete multipart uploads in this bucket.</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
      <div style={{ fontSize: "12px", color: "#9A5B13", display: "flex", alignItems: "center", gap: "5px" }}>
        <AlertTriangle size={12} />
        {uploads.filter((u) => !aborted.has(u.uploadId)).length} incomplete upload(s)
      </div>
      {uploads.map((u) => {
        if (aborted.has(u.uploadId)) return null;
        return (
          <div
            key={u.uploadId}
            style={{
              background: "#FAFAF9",
              border: "1px solid #E8E8E5",
              borderRadius: "10px",
              padding: "10px 12px",
            }}
          >
            <div
              style={{
                fontSize: "12px",
                fontWeight: 500,
                color: "#0A0A0A",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                marginBottom: "4px",
              }}
            >
              {u.key}
            </div>
            <div style={{ fontSize: "11px", color: "#737373", lineHeight: "1.6" }}>
              <div>
                Upload ID:{" "}
                <span style={{ fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace" }}>{u.uploadId.slice(0, 20)}…</span>
              </div>
              <div>Parts: {u.parts} · {formatBytes(u.uploadedSize)}</div>
              <div>Initiated: {formatDateFull(u.initiated).slice(0, 16)}</div>
            </div>
            <button
              onClick={() => setAborted(new Set([...aborted, u.uploadId]))}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "4px",
                marginTop: "8px",
                fontSize: "11px",
                fontWeight: 500,
                color: "#B42318",
                background: "#FEF2F2",
                border: "1px solid #FECACA",
                borderRadius: "8px",
                padding: "3px 8px",
                cursor: "pointer",
              }}
            >
              <XCircle size={11} />
              Abort
            </button>
          </div>
        );
      })}
    </div>
  );
}

function PreviewTab({ object }: { object: ObjectDetail }) {
  const sizeLimit = 256 * 1024;

  if (object.size > sizeLimit) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "10px", alignItems: "center", padding: "24px 0", textAlign: "center" }}>
        <Eye size={28} color="#E8E8E5" />
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A" }}>File too large to preview</div>
        <div style={{ fontSize: "12px", color: "#737373" }}>
          Preview is limited to 256 KiB. This file is {formatBytes(object.size)}.
        </div>
        <button
          onClick={() => window.open(object.endpointUrl, "_blank")}
          style={{
            display: "flex",
            alignItems: "center",
            gap: "5px",
            fontSize: "12px",
            fontWeight: 500,
            color: "#1E40AF",
            background: "#EFF6FF",
            border: "1px solid #DBEAFE",
            borderRadius: "10px",
            padding: "5px 12px",
            cursor: "pointer",
          }}
        >
          <Download size={12} />
          Download
        </button>
      </div>
    );
  }

  if (object.contentType.startsWith("image/")) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
        <div style={{ fontSize: "11px", fontWeight: 600, color: "#737373" }}>Image Preview</div>
        <div
          style={{
            background: "#F4F4F2",
            borderRadius: "10px",
            padding: "12px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "120px",
          }}
        >
          <img
            src={object.endpointUrl}
            alt={object.key}
            style={{ maxWidth: "100%", maxHeight: "200px", borderRadius: "8px" }}
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = "none";
            }}
          />
          <span style={{ fontSize: "12px", color: "#737373" }}>Image preview unavailable in mock mode</span>
        </div>
      </div>
    );
  }

  if (!object.previewText) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", alignItems: "center", padding: "24px 0", textAlign: "center" }}>
        <Clock size={28} color="#E8E8E5" />
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A" }}>No preview available</div>
        <div style={{ fontSize: "12px", color: "#737373" }}>Binary or unknown content type.</div>
        <button
          onClick={() => window.open(object.endpointUrl, "_blank")}
          style={{
            display: "flex",
            alignItems: "center",
            gap: "5px",
            fontSize: "12px",
            fontWeight: 500,
            color: "#1E40AF",
            background: "#EFF6FF",
            border: "1px solid #DBEAFE",
            borderRadius: "10px",
            padding: "5px 12px",
            cursor: "pointer",
          }}
        >
          <Download size={12} />
          Download
        </button>
      </div>
    );
  }

  const displayContent =
    object.previewType === "json"
      ? (() => {
          try {
            return JSON.stringify(JSON.parse(object.previewText), null, 2);
          } catch {
            return object.previewText;
          }
        })()
      : object.previewText;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: "11px", fontWeight: 600, color: "#737373" }}>
          {object.previewType === "json" ? "JSON Preview" : "Text Preview"}
        </span>
        <span style={{ fontSize: "11px", color: "#737373" }}>{formatBytes(object.size)}</span>
      </div>
      <pre
        style={{
          fontSize: "11px",
          color: "#E8EFE7",
          background: "#0A0A0A",
          borderRadius: "10px",
          padding: "12px",
          margin: 0,
          overflowX: "auto",
          overflowY: "auto",
          maxHeight: "400px",
          lineHeight: "1.6",
          fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {displayContent}
      </pre>
    </div>
  );
}

function CodeBlock({
  code,
  label,
  CopyBtn,
}: {
  code: string;
  label: string;
  CopyBtn: React.FC<{ text: string; label: string }>;
}) {
  return (
    <div
      style={{
        background: "#0A0A0A",
        borderRadius: "10px",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "4px 8px",
          borderBottom: "1px solid rgba(255,255,255,0.08)",
        }}
      >
        <span style={{ fontSize: "10px", color: "rgba(232,239,231,0.5)", fontWeight: 600 }}>{label}</span>
        <CopyBtn text={code.replace(/\\\n/g, " ")} label={label} />
      </div>
      <pre
        style={{
          fontSize: "11px",
          color: "#E8EFE7",
          margin: 0,
          padding: "7px 10px",
          fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
          lineHeight: "1.6",
          overflowX: "auto",
          whiteSpace: "pre-wrap",
        }}
      >
        {code}
      </pre>
    </div>
  );
}