import { ActivityEntry, S3Status } from "./types";
import { Circle, HardDrive, Activity } from "lucide-react";

interface FooterProps {
  lastActivity: ActivityEntry | null;
  status: S3Status;
}

const METHOD_COLORS: Record<string, string> = {
  GET: "#60A5FA",
  PUT: "#10B981",
  DELETE: "#F87171",
  POST: "#FBBF24",
  HEAD: "#A3A3A3",
};

const STATUS_COLORS: Record<number, string> = {
  200: "#10B981",
  204: "#10B981",
  403: "#FBBF24",
  404: "#FBBF24",
  500: "#F87171",
};

export function Footer({ lastActivity, status }: FooterProps) {
  return (
    <footer
      style={{
        background: "#0A0A0A",
        color: "#E5E5E5",
        height: "30px",
        display: "flex",
        alignItems: "center",
        padding: "0 16px",
        gap: "20px",
        flexShrink: 0,
        overflow: "hidden",
      }}
    >
      {/* Last request */}
      {lastActivity && (
        <div style={{ display: "flex", alignItems: "center", gap: "6px", flex: 1, minWidth: 0, overflow: "hidden" }}>
          <Activity size={11} color="rgba(232,239,231,0.5)" />
          <span style={{ fontSize: "11px", color: "rgba(229,229,229,0.55)", flexShrink: 0 }}>Last request</span>
          <span
            style={{
              fontSize: "11px",
              fontWeight: 600,
              color: METHOD_COLORS[lastActivity.method] || "#E8EFE7",
              flexShrink: 0,
            }}
          >
            {lastActivity.method}
          </span>
          <span
            style={{
              fontSize: "11px",
              fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
              color: "#E5E5E5",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {lastActivity.path}
          </span>
          <span
            style={{
              fontSize: "11px",
              fontWeight: 600,
              color: STATUS_COLORS[lastActivity.statusCode] || "#E8EFE7",
              flexShrink: 0,
            }}
          >
            {lastActivity.statusCode}
          </span>
        </div>
      )}

      {/* Storage path */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "5px",
          flexShrink: 0,
          overflow: "hidden",
        }}
        className="hidden-mobile"
      >
        <HardDrive size={11} color="rgba(232,239,231,0.5)" />
        <span style={{ fontSize: "11px", color: "rgba(229,229,229,0.55)" }}>Storage</span>
        <span style={{ fontSize: "11px", fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace", color: "#E5E5E5" }}>{status.storagePath}</span>
      </div>

      {/* API state */}
      <div style={{ display: "flex", alignItems: "center", gap: "5px", flexShrink: 0 }}>
        <Circle
          size={7}
          fill={status.running ? "#10B981" : "#F87171"}
          color={status.running ? "#10B981" : "#F87171"}
          style={{
            borderRadius: "50%",
            boxShadow: status.running
              ? "0 0 0 3px rgba(16,185,129,0.18)"
              : "0 0 0 3px rgba(248,113,113,0.18)",
          }}
        />
        <span style={{ fontSize: "11px", color: "rgba(229,229,229,0.75)" }}>
          S3 API {status.running ? "OK" : "DOWN"}
        </span>
      </div>

      <style>{`
        @media (max-width: 640px) { .hidden-mobile { display: none !important; } }
      `}</style>
    </footer>
  );
}
