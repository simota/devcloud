import React, { useState, useMemo } from "react";
import {
  ChevronRight,
  Home,
  RefreshCw,
  Search,
  X,
  Folder,
  File,
  FileText,
  FileJson,
  FileCode,
  Image,
  ChevronUp,
  ChevronDown,
  Minus,
} from "lucide-react";
import { S3Object, SortKey, SortDir } from "./types";
import { formatBytes, formatDate } from "./mockData";

interface ObjectBrowserProps {
  bucketName: string | null;
  prefix: string;
  objects: S3Object[];
  commonPrefixes: string[];
  onNavigate: (prefix: string) => void;
  onSelectObject: (obj: S3Object) => void;
  selectedObjectKey: string | null;
  onRefresh: () => void;
  onDeleteObject: (obj: S3Object) => void;
}

function getFileIcon(contentType: string, isPrefix?: boolean) {
  if (isPrefix) return <Folder size={14} color="#1E40AF" />;
  if (contentType.startsWith("image/")) return <Image size={14} color="#9A5B13" />;
  if (contentType === "application/json") return <FileJson size={14} color="#059669" />;
  if (contentType.includes("javascript") || contentType.includes("css") || contentType.includes("html"))
    return <FileCode size={14} color="#1E40AF" />;
  if (contentType.startsWith("text/")) return <FileText size={14} color="#737373" />;
  return <File size={14} color="#737373" />;
}

function buildBreadcrumbs(prefix: string) {
  if (!prefix) return [];
  const parts = prefix.split("/").filter(Boolean);
  return parts.map((part, i) => ({
    label: part,
    prefix: parts.slice(0, i + 1).join("/") + "/",
  }));
}

export function ObjectBrowser({
  bucketName,
  prefix,
  objects,
  commonPrefixes,
  onNavigate,
  onSelectObject,
  selectedObjectKey,
  onRefresh,
  onDeleteObject,
}: ObjectBrowserProps) {
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const breadcrumbs = buildBreadcrumbs(prefix);

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  }

  const allRows = useMemo(() => {
    const prefixRows: S3Object[] = commonPrefixes.map((p) => ({
      key: p,
      size: 0,
      etag: "",
      contentType: "folder",
      lastModified: "",
      storageClass: "",
      isPrefix: true,
    }));

    const filtered = [...prefixRows, ...objects].filter((o) => {
      if (!search) return true;
      const displayName = o.key.replace(prefix, "");
      return displayName.toLowerCase().includes(search.toLowerCase());
    });

    return filtered.sort((a, b) => {
      if (a.isPrefix && !b.isPrefix) return -1;
      if (!a.isPrefix && b.isPrefix) return 1;
      const aName = a.key.replace(prefix, "");
      const bName = b.key.replace(prefix, "");
      let cmp = 0;
      if (sortKey === "name") cmp = aName.localeCompare(bName);
      else if (sortKey === "size") cmp = a.size - b.size;
      else if (sortKey === "lastModified")
        cmp = new Date(a.lastModified || 0).getTime() - new Date(b.lastModified || 0).getTime();
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [objects, commonPrefixes, search, sortKey, sortDir, prefix]);

  function SortIcon({ col }: { col: SortKey }) {
    if (sortKey !== col) return <Minus size={11} color="#E8E8E5" />;
    return sortDir === "asc" ? (
      <ChevronUp size={11} color="#737373" />
    ) : (
      <ChevronDown size={11} color="#737373" />
    );
  }

  if (!bucketName) {
    return (
      <div
        style={{
          flex: 1,
          background: "transparent",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          gap: "12px",
          color: "#737373",
        }}
      >
        <Folder size={40} color="#E8E8E5" />
        <div style={{ fontSize: "14px", fontWeight: 600, color: "#0A0A0A", letterSpacing: "-0.01em" }}>No bucket selected</div>
        <div style={{ fontSize: "13px", color: "#737373" }}>Select a bucket from the sidebar to browse objects.</div>
      </div>
    );
  }

  return (
    <div
      style={{
        flex: 1,
        minWidth: 0,
        background: "transparent",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* Browser Header */}
      <div
        style={{
          background: "rgba(255,255,255,0.72)",
          backdropFilter: "blur(8px)",
          WebkitBackdropFilter: "blur(8px)",
          borderBottom: "1px solid #E8E8E5",
          padding: "10px 16px",
          display: "flex",
          alignItems: "center",
          gap: "10px",
          flexShrink: 0,
        }}
      >
        {/* Breadcrumb */}
        <nav
          aria-label="Object prefix navigation"
          style={{ display: "flex", alignItems: "center", gap: "2px", flex: 1, minWidth: 0, overflow: "hidden" }}
        >
          <button
            onClick={() => onNavigate("")}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              background: "none",
              border: "none",
              cursor: "pointer",
              padding: "4px 6px",
              borderRadius: "6px",
              color: "#1E40AF",
              fontSize: "13px",
              fontWeight: 500,
              whiteSpace: "nowrap",
              flexShrink: 0,
            }}
          >
            <Home size={12} />
            {bucketName}
          </button>
          {breadcrumbs.map((crumb) => (
            <span key={crumb.prefix} style={{ display: "flex", alignItems: "center", gap: "2px", flexShrink: 0 }}>
              <ChevronRight size={12} color="#E8E8E5" />
              <button
                onClick={() => onNavigate(crumb.prefix)}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  padding: "4px 6px",
                  borderRadius: "6px",
                  color: "#1E40AF",
                  fontSize: "13px",
                  fontWeight: 500,
                  whiteSpace: "nowrap",
                }}
              >
                {crumb.label}
              </button>
            </span>
          ))}
          {prefix && (
            <span style={{ display: "flex", alignItems: "center" }}>
              <ChevronRight size={12} color="#E8E8E5" />
              <span style={{ fontSize: "13px", color: "#737373", padding: "0 5px" }}>/</span>
            </span>
          )}
        </nav>

        {/* Search */}
        <div style={{ position: "relative", flexShrink: 0 }}>
          <Search
            size={13}
            color="#737373"
            style={{ position: "absolute", left: "10px", top: "50%", transform: "translateY(-50%)" }}
          />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter..."
            aria-label="Filter objects"
            style={{
              fontSize: "13px",
              color: "#0A0A0A",
              background: "#FAFAF9",
              border: "1px solid #E8E8E5",
              borderRadius: "10px",
              padding: "6px 28px 6px 30px",
              width: "160px",
              outline: "none",
              transition: "all 0.15s ease",
            }}
          />
          {search && (
            <button
              onClick={() => setSearch("")}
              style={{
                position: "absolute",
                right: "8px",
                top: "50%",
                transform: "translateY(-50%)",
                background: "none",
                border: "none",
                cursor: "pointer",
                padding: "0",
                color: "#737373",
                display: "flex",
                alignItems: "center",
              }}
            >
              <X size={12} />
            </button>
          )}
        </div>

        <button
          onClick={onRefresh}
          title="Refresh objects"
          style={{
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: "6px",
            borderRadius: "8px",
            color: "#737373",
            display: "flex",
            alignItems: "center",
            flexShrink: 0,
          }}
        >
          <RefreshCw size={13} />
        </button>
      </div>

      {/* Object Table */}
      <div style={{ flex: 1, overflowY: "auto" }}>
        {allRows.length === 0 ? (
          <EmptyObjects bucketName={bucketName} prefix={prefix} />
        ) : (
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              tableLayout: "fixed",
            }}
          >
            <thead>
              <tr
                style={{
                  background: "rgba(255,255,255,0.85)",
                  backdropFilter: "blur(8px)",
                  borderBottom: "1px solid #E8E8E5",
                  position: "sticky",
                  top: 0,
                  zIndex: 1,
                }}
              >
                <Th style={{ width: "40%" }} onClick={() => handleSort("name")}>
                  <span style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                    Name <SortIcon col="name" />
                  </span>
                </Th>
                <Th style={{ width: "10%" }} onClick={() => handleSort("size")}>
                  <span style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                    Size <SortIcon col="size" />
                  </span>
                </Th>
                <Th style={{ width: "15%" }} className="hidden-sm">Content-Type</Th>
                <Th style={{ width: "12%" }} onClick={() => handleSort("lastModified")} className="hidden-sm">
                  <span style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                    Modified <SortIcon col="lastModified" />
                  </span>
                </Th>
                <Th style={{ width: "20%" }} className="hidden-md">ETag</Th>
                <Th style={{ width: "8%" }} className="hidden-lg">Class</Th>
              </tr>
            </thead>
            <tbody>
              {allRows.map((obj) => {
                const displayName = obj.key.replace(prefix, "");
                const isSelected = !obj.isPrefix && obj.key === selectedObjectKey;
                return (
                  <tr
                    key={obj.key}
                    onClick={() => {
                      if (obj.isPrefix) {
                        onNavigate(obj.key);
                      } else {
                        onSelectObject(obj);
                      }
                    }}
                    style={{
                      background: isSelected ? "rgba(16, 185, 129, 0.08)" : "transparent",
                      borderBottom: "1px solid #E8E8E5",
                      borderLeft: isSelected ? "4px solid #10B981" : "4px solid transparent",
                      cursor: "pointer",
                      transition: "all 0.15s ease",
                    }}
                    className="object-row"
                    aria-selected={isSelected}
                    role="row"
                  >
                    <td
                      style={{
                        padding: "9px 12px 9px 14px",
                        overflow: "hidden",
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                        {getFileIcon(obj.contentType, obj.isPrefix)}
                        <span
                          style={{
                            fontSize: "13px",
                            color: obj.isPrefix ? "#1E40AF" : "#0A0A0A",
                            fontWeight: obj.isPrefix ? 500 : 400,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                          aria-label={`${obj.isPrefix ? "Folder" : "Object"}: ${displayName}`}
                        >
                          {displayName}
                        </span>
                      </div>
                    </td>
                    <td style={{ padding: "9px 8px", fontSize: "13px", color: "#737373" }}>
                      {obj.isPrefix ? <span style={{ color: "#E8E8E5" }}>—</span> : formatBytes(obj.size)}
                    </td>
                    <td style={{ padding: "9px 8px", fontSize: "12px", color: "#737373", overflow: "hidden" }} className="hidden-sm">
                      <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", display: "block" }}>
                        {obj.isPrefix ? "" : obj.contentType}
                      </span>
                    </td>
                    <td style={{ padding: "9px 8px", fontSize: "12px", color: "#737373" }} className="hidden-sm">
                      {obj.isPrefix ? "" : formatDate(obj.lastModified)}
                    </td>
                    <td style={{ padding: "9px 8px", fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace", fontSize: "11px", color: "#737373", overflow: "hidden" }} className="hidden-md">
                      <span
                        style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", display: "block" }}
                        title={obj.etag}
                      >
                        {obj.isPrefix ? "" : obj.etag.replace(/"/g, "")}
                      </span>
                    </td>
                    <td style={{ padding: "9px 8px", fontSize: "11px", color: "#737373" }} className="hidden-lg">
                      {obj.isPrefix ? "" : obj.storageClass}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <style>{`
        .object-row:hover { background: #FAFAFA !important; }
        @media (max-width: 900px) { .hidden-sm { display: none !important; } }
        @media (max-width: 1100px) { .hidden-md { display: none !important; } }
        @media (max-width: 1400px) { .hidden-lg { display: none !important; } }
      `}</style>
    </div>
  );
}

function Th({
  children,
  onClick,
  style,
  className,
}: {
  children: React.ReactNode;
  onClick?: () => void;
  style?: React.CSSProperties;
  className?: string;
}) {
  return (
    <th
      onClick={onClick}
      className={className}
      style={{
        padding: "8px 8px 8px 14px",
        textAlign: "left",
        fontSize: "11px",
        fontWeight: 600,
        color: "#737373",
        textTransform: "uppercase",
        letterSpacing: "0.04em",
        cursor: onClick ? "pointer" : "default",
        userSelect: "none",
        whiteSpace: "nowrap",
        ...style,
      }}
    >
      {children}
    </th>
  );
}

function EmptyObjects({ bucketName, prefix }: { bucketName: string; prefix: string }) {
  const cmd = prefix
    ? `aws --endpoint-url http://127.0.0.1:4566 s3 ls s3://${bucketName}/${prefix}`
    : `aws --endpoint-url http://127.0.0.1:4566 s3 cp README.md s3://${bucketName}/README.md`;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        padding: "48px 24px",
        gap: "12px",
        textAlign: "center",
      }}
    >
      <Folder size={40} color="#E8E8E5" />
      <div>
        <div style={{ fontSize: "14px", fontWeight: 600, color: "#0A0A0A", marginBottom: "4px", letterSpacing: "-0.01em" }}>
          {prefix ? "This prefix is empty" : "This bucket is empty"}
        </div>
        <div style={{ fontSize: "13px", color: "#737373" }}>Upload with:</div>
      </div>
      <pre
        style={{
          fontSize: "12px",
          color: "#E8EFE7",
          background: "#0A0A0A",
          borderRadius: "10px",
          padding: "12px 16px",
          fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
          margin: 0,
          maxWidth: "480px",
          overflowX: "auto",
          textAlign: "left",
          lineHeight: "1.6",
        }}
      >
        {cmd}
      </pre>
    </div>
  );
}