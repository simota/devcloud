import { useState } from "react";
import { Plus, Trash2, Copy, RefreshCw, HardDrive, MoreHorizontal, ChevronRight } from "lucide-react";
import { Bucket } from "./types";
import { formatBytes, formatDate } from "./mockData";

interface BucketSidebarProps {
  buckets: Bucket[];
  selectedBucket: string | null;
  onSelectBucket: (name: string) => void;
  onDeleteBucket: (bucket: Bucket) => void;
  onCreateBucket: () => void;
  onRefresh: () => void;
}

const VERSIONING_COLOR: Record<string, string> = {
  Off: "#737373",
  Enabled: "#0F5132",
  Suspended: "#9A5B13",
};

export function BucketSidebar({
  buckets,
  selectedBucket,
  onSelectBucket,
  onDeleteBucket,
  onCreateBucket,
  onRefresh,
}: BucketSidebarProps) {
  const [menuOpen, setMenuOpen] = useState<string | null>(null);
  const [copied, setCopied] = useState<string | null>(null);

  function handleCopyArn(name: string) {
    navigator.clipboard.writeText(`arn:aws:s3:::${name}`);
    setCopied(name);
    setMenuOpen(null);
    setTimeout(() => setCopied(null), 1500);
  }

  return (
    <aside
      style={{
        width: "260px",
        minWidth: "220px",
        maxWidth: "260px",
        background: "rgba(255,255,255,0.72)",
        backdropFilter: "blur(8px)",
        WebkitBackdropFilter: "blur(8px)",
        borderRight: "1px solid #E8E8E5",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        overflow: "hidden",
      }}
    >
      {/* Sidebar Header */}
      <div
        style={{
          padding: "12px 16px 10px",
          borderBottom: "1px solid #E8E8E5",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexShrink: 0,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
          <span style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A", letterSpacing: "-0.01em" }}>Buckets</span>
          {buckets.length > 0 && (
            <span
              style={{
                fontSize: "11px",
                fontWeight: 600,
                color: "#737373",
                background: "#F4F4F2",
                borderRadius: "999px",
                padding: "1px 8px",
              }}
            >
              {buckets.length}
            </span>
          )}
        </div>
        <div style={{ display: "flex", gap: "4px" }}>
          <button
            onClick={onRefresh}
            title="Refresh buckets"
            style={{ background: "none", border: "none", cursor: "pointer", padding: "4px", borderRadius: "6px", color: "#737373", display: "flex", alignItems: "center", transition: "all 0.15s ease" }}
          >
            <RefreshCw size={13} />
          </button>
          <button
            onClick={onCreateBucket}
            title="Create bucket"
            style={{ background: "none", border: "none", cursor: "pointer", padding: "4px", borderRadius: "6px", color: "#059669", display: "flex", alignItems: "center", transition: "all 0.15s ease" }}
          >
            <Plus size={14} />
          </button>
        </div>
      </div>

      {/* Bucket List */}
      <div style={{ flex: 1, overflowY: "auto" }}>
        {buckets.length === 0 ? (
          <EmptyBuckets onCreateBucket={onCreateBucket} />
        ) : (
          <ul style={{ listStyle: "none", margin: 0, padding: "6px 0" }}>
            {buckets.map((bucket) => (
              <li
                key={bucket.name}
                style={{ position: "relative" }}
                onClick={() => {
                  onSelectBucket(bucket.name);
                  setMenuOpen(null);
                }}
              >
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    padding: "9px 12px 9px 14px",
                    cursor: "pointer",
                    background: selectedBucket === bucket.name ? "rgba(16, 185, 129, 0.08)" : "transparent",
                    borderLeft: selectedBucket === bucket.name ? "4px solid #10B981" : "4px solid transparent",
                    transition: "all 0.15s ease",
                    gap: "8px",
                  }}
                >
                  <HardDrive
                    size={14}
                    color={selectedBucket === bucket.name ? "#059669" : "#737373"}
                    style={{ flexShrink: 0 }}
                  />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div
                      style={{
                        fontSize: "13px",
                        fontWeight: selectedBucket === bucket.name ? 600 : 500,
                        color: "#0A0A0A",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        letterSpacing: "-0.005em",
                      }}
                    >
                      {bucket.name}
                    </div>
                    <div
                      style={{
                        fontSize: "11px",
                        color: "#737373",
                        display: "flex",
                        gap: "6px",
                        alignItems: "center",
                        marginTop: "2px",
                      }}
                    >
                      <span>{bucket.objectCount.toLocaleString()} objects</span>
                      <span style={{ color: "#E8E8E5" }}>·</span>
                      <span>{formatBytes(bucket.totalBytes)}</span>
                    </div>
                    <div
                      style={{
                        fontSize: "11px",
                        color: "#737373",
                        display: "flex",
                        gap: "5px",
                        alignItems: "center",
                        marginTop: "1px",
                      }}
                    >
                      <span>{bucket.region}</span>
                      <span style={{ color: "#E8E8E5" }}>·</span>
                      <span style={{ color: VERSIONING_COLOR[bucket.versioning] }}>
                        v{bucket.versioning}
                      </span>
                    </div>
                  </div>

                  {selectedBucket === bucket.name ? (
                    <ChevronRight size={13} color="#059669" style={{ flexShrink: 0 }} />
                  ) : (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setMenuOpen(menuOpen === bucket.name ? null : bucket.name);
                      }}
                      title="Bucket actions"
                      style={{
                        background: "none",
                        border: "none",
                        cursor: "pointer",
                        padding: "3px",
                        borderRadius: "6px",
                        color: "#737373",
                        display: "flex",
                        alignItems: "center",
                        flexShrink: 0,
                        opacity: 0,
                        transition: "opacity 0.15s ease",
                      }}
                      className="bucket-menu-btn"
                    >
                      <MoreHorizontal size={13} />
                    </button>
                  )}
                </div>

                {/* Context menu */}
                {menuOpen === bucket.name && (
                  <div
                    style={{
                      position: "absolute",
                      right: "8px",
                      top: "100%",
                      background: "#FFFFFF",
                      border: "1px solid #E8E8E5",
                      borderRadius: "10px",
                      boxShadow: "0 20px 50px -10px rgba(0,0,0,0.15), 0 8px 20px -8px rgba(0,0,0,0.08)",
                      zIndex: 100,
                      minWidth: "168px",
                      padding: "5px 0",
                    }}
                    onClick={(e) => e.stopPropagation()}
                  >
                    <button
                      onClick={() => handleCopyArn(bucket.name)}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "8px",
                        width: "100%",
                        padding: "8px 12px",
                        background: "none",
                        border: "none",
                        cursor: "pointer",
                        fontSize: "13px",
                        color: "#0A0A0A",
                        textAlign: "left",
                        transition: "background 0.15s ease",
                      }}
                      onMouseEnter={(e) => (e.currentTarget.style.background = "#FAFAFA")}
                      onMouseLeave={(e) => (e.currentTarget.style.background = "none")}
                    >
                      <Copy size={13} />
                      {copied === bucket.name ? "Copied!" : "Copy ARN"}
                    </button>
                    <div style={{ height: "1px", background: "#E8E8E5", margin: "4px 0" }} />
                    <button
                      onClick={() => {
                        setMenuOpen(null);
                        onDeleteBucket(bucket);
                      }}
                      disabled={bucket.objectCount > 0}
                      title={bucket.objectCount > 0 ? "Delete objects first" : undefined}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "8px",
                        width: "100%",
                        padding: "8px 12px",
                        background: "none",
                        border: "none",
                        cursor: bucket.objectCount > 0 ? "not-allowed" : "pointer",
                        fontSize: "13px",
                        color: bucket.objectCount > 0 ? "#E8E8E5" : "#B42318",
                        textAlign: "left",
                        transition: "background 0.15s ease",
                      }}
                    >
                      <Trash2 size={13} />
                      Delete bucket
                    </button>
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>

      <style>{`
        li:hover .bucket-menu-btn { opacity: 1 !important; }
        li:hover { background-color: transparent; }
      `}</style>
    </aside>
  );
}

function EmptyBuckets({ onCreateBucket }: { onCreateBucket: () => void }) {
  return (
    <div
      style={{
        padding: "28px 16px",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: "12px",
        textAlign: "center",
      }}
    >
      <HardDrive size={28} color="#E8E8E5" />
      <div>
        <div style={{ fontSize: "13px", fontWeight: 600, color: "#0A0A0A", marginBottom: "4px", letterSpacing: "-0.01em" }}>
          No buckets yet
        </div>
        <div style={{ fontSize: "12px", color: "#737373", lineHeight: "1.5" }}>
          Create a bucket or run:
        </div>
      </div>
      <pre
        style={{
          fontSize: "11px",
          color: "#E8EFE7",
          background: "#0A0A0A",
          borderRadius: "10px",
          padding: "10px 12px",
          margin: 0,
          width: "100%",
          overflowX: "auto",
          textAlign: "left",
          lineHeight: "1.6",
          fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
        }}
      >
        {`aws --endpoint-url \\\n  http://127.0.0.1:4566 \\\n  s3api create-bucket \\\n  --bucket demo`}
      </pre>
      <button
        onClick={onCreateBucket}
        style={{
          display: "flex",
          alignItems: "center",
          gap: "5px",
          fontSize: "13px",
          fontWeight: 500,
          color: "#FFFFFF",
          background: "linear-gradient(135deg, #10B981 0%, #059669 100%)",
          border: "none",
          borderRadius: "10px",
          padding: "8px 14px",
          cursor: "pointer",
          width: "100%",
          justifyContent: "center",
          letterSpacing: "-0.005em",
          boxShadow: "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)",
          transition: "all 0.15s ease",
        }}
      >
        <Plus size={13} />
        Create bucket
      </button>
    </div>
  );
}
