import { RefreshCw, Plus, Circle, Database, Mail } from "lucide-react";
import { S3Status } from "./types";

interface AppHeaderProps {
  status: S3Status;
  onRefresh: () => void;
  onCreateBucket: () => void;
  isRefreshing: boolean;
}

const AUTH_COLORS: Record<string, string> = {
  relaxed: "#0F5132",
  strict: "#9A5B13",
  off: "#B42318",
};

export function AppHeader({ status, onRefresh, onCreateBucket, isRefreshing }: AppHeaderProps) {
  return (
    <header
      style={{
        background: "rgba(255,255,255,0.72)",
        backdropFilter: "blur(8px)",
        WebkitBackdropFilter: "blur(8px)",
        borderBottom: "1px solid #E8E8E5",
        padding: "0 16px",
        height: "52px",
        display: "flex",
        alignItems: "center",
        gap: "12px",
        minWidth: 0,
        flexShrink: 0,
        zIndex: 10,
      }}
    >
      {/* Service Identity */}
      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginRight: "4px" }}>
        <Database size={16} color="#059669" />
        <span
          style={{
            fontSize: "15px",
            fontWeight: 600,
            color: "#0A0A0A",
            letterSpacing: 0,
            whiteSpace: "nowrap",
          }}
        >
          devcloud S3
        </span>
      </div>

      {/* Service Switcher */}
      <div
        role="tablist"
        aria-label="Service switcher"
        style={{
          display: "flex",
          alignItems: "center",
          gap: "2px",
          background: "#F4F4F2",
          border: "1px solid #E8E8E5",
          borderRadius: "8px",
          padding: "2px",
          flexShrink: 0,
        }}
      >
        <button
          type="button"
          role="tab"
          aria-selected="false"
          title="Mail"
          style={{
            width: "30px",
            height: "26px",
            display: "grid",
            placeItems: "center",
            color: "#737373",
            background: "transparent",
            border: "none",
            borderRadius: "6px",
            cursor: "pointer",
          }}
        >
          <Mail size={14} />
        </button>
        <button
          type="button"
          role="tab"
          aria-selected="true"
          title="S3"
          style={{
            width: "30px",
            height: "26px",
            display: "grid",
            placeItems: "center",
            color: "#047857",
            background: "#FFFFFF",
            border: "1px solid #E8E8E5",
            borderRadius: "6px",
            cursor: "default",
            boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
          }}
        >
          <Database size={14} />
        </button>
      </div>

      {/* Chips */}
      <div style={{ display: "flex", alignItems: "center", gap: "6px", flex: 1, flexWrap: "wrap", overflow: "hidden" }}>
        {/* Endpoint */}
        <span
          style={{
            fontSize: "12px",
            fontWeight: 500,
            color: "#1E40AF",
            background: "#FAFAF9",
            border: "1px solid #E8E8E5",
            borderRadius: "999px",
            padding: "2px 9px",
            fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
            whiteSpace: "nowrap",
          }}
        >
          {status.endpoint}
        </span>

        {/* Region */}
        <span
          style={{
            fontSize: "12px",
            fontWeight: 500,
            color: "#737373",
            background: "#FAFAF9",
            border: "1px solid #E8E8E5",
            borderRadius: "999px",
            padding: "2px 9px",
            whiteSpace: "nowrap",
          }}
        >
          {status.region}
        </span>

        {/* Auth mode */}
        <span
          style={{
            fontSize: "12px",
            fontWeight: 500,
            color: AUTH_COLORS[status.authMode] || "#737373",
            background: "#FAFAF9",
            border: "1px solid #E8E8E5",
            borderRadius: "999px",
            padding: "2px 9px",
            whiteSpace: "nowrap",
          }}
        >
          {status.authMode}
        </span>

        {/* Running status */}
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: "6px",
            fontSize: "12px",
            fontWeight: 500,
            color: status.running ? "#0F5132" : "#B42318",
            whiteSpace: "nowrap",
          }}
        >
          <Circle
            size={7}
            fill={status.running ? "#10B981" : "#B42318"}
            color={status.running ? "#10B981" : "#B42318"}
            style={{
              borderRadius: "50%",
              boxShadow: status.running
                ? "0 0 0 3px rgba(16,185,129,0.15)"
                : "0 0 0 3px rgba(180,35,24,0.15)",
            }}
          />
          {status.running ? "Running" : "Stopped"}
        </span>
      </div>

      {/* Actions */}
      <div style={{ display: "flex", alignItems: "center", gap: "8px", flexShrink: 0 }}>
        <button
          onClick={onRefresh}
          aria-label="Refresh"
          style={{
            display: "flex",
            alignItems: "center",
            gap: "5px",
            fontSize: "13px",
            fontWeight: 500,
            color: "#0A0A0A",
            background: "transparent",
            border: "1px solid #E8E8E5",
            borderRadius: "10px",
            padding: "6px 12px",
            cursor: "pointer",
            whiteSpace: "nowrap",
            letterSpacing: 0,
            transition: "all 0.15s ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#F4F4F2";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "transparent";
          }}
        >
          <RefreshCw size={13} style={{ animation: isRefreshing ? "spin 1s linear infinite" : "none" }} />
          <span className="hidden sm:inline">Refresh</span>
        </button>

        <button
          onClick={onCreateBucket}
          aria-label="Create bucket"
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
            padding: "6px 14px",
            cursor: "pointer",
            whiteSpace: "nowrap",
            letterSpacing: 0,
            boxShadow: "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)",
            transition: "all 0.15s ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.transform = "translateY(-1px)";
            e.currentTarget.style.boxShadow = "0 4px 12px rgba(16,185,129,0.25)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = "translateY(0)";
            e.currentTarget.style.boxShadow = "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)";
          }}
        >
          <Plus size={13} />
          <span className="hidden sm:inline">Create bucket</span>
        </button>
      </div>

      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </header>
  );
}
