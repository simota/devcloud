import React, { useState } from "react";
import { X, AlertTriangle, Trash2, HardDrive } from "lucide-react";
import { Bucket, ObjectDetail } from "./types";

// ─── Create Bucket Dialog ───────────────────────────────────────────────────

interface CreateBucketDialogProps {
  onClose: () => void;
  onCreate: (name: string, region: string) => void;
}

const REGIONS = [
  "us-east-1",
  "us-east-2",
  "us-west-1",
  "us-west-2",
  "ap-northeast-1",
  "ap-southeast-1",
  "eu-west-1",
  "eu-central-1",
];

function validateBucketName(name: string): string | null {
  if (name.length < 3) return "Name must be at least 3 characters.";
  if (name.length > 63) return "Name must be at most 63 characters.";
  if (!/^[a-z0-9][a-z0-9.\-]*[a-z0-9]$/.test(name)) return "Only lowercase letters, numbers, dots, and hyphens allowed. Must start and end with letter or number.";
  if (/[A-Z]/.test(name)) return "No uppercase letters allowed.";
  if (/\.\./.test(name)) return "Adjacent dots not allowed.";
  if (/\d+\.\d+\.\d+\.\d+/.test(name)) return "Name cannot be formatted as an IP address.";
  return null;
}

export function CreateBucketDialog({ onClose, onCreate }: CreateBucketDialogProps) {
  const [name, setName] = useState("");
  const [region, setRegion] = useState("us-east-1");
  const [error, setError] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitted(true);
    const err = validateBucketName(name);
    if (err) {
      setError(err);
      return;
    }
    onCreate(name, region);
    onClose();
  }

  function handleNameChange(val: string) {
    setName(val);
    if (submitted) {
      setError(validateBucketName(val));
    }
  }

  return (
    <DialogOverlay onClose={onClose}>
      <div
        style={{
          background: "#FFFFFF",
          borderRadius: "16px",
          width: "420px",
          maxWidth: "calc(100vw - 32px)",
          boxShadow: "0 20px 50px -10px rgba(0,0,0,0.15), 0 8px 20px -8px rgba(0,0,0,0.08)",
          overflow: "hidden",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "14px 18px",
            borderBottom: "1px solid #E8E8E5",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <HardDrive size={15} color="#059669" />
            <span style={{ fontSize: "14px", fontWeight: 600, color: "#0A0A0A" }}>Create Bucket</span>
          </div>
          <button
            onClick={onClose}
            aria-label="Close dialog"
            style={{ background: "none", border: "none", cursor: "pointer", padding: "3px", color: "#737373", display: "flex", alignItems: "center", borderRadius: "8px" }}
          >
            <X size={15} />
          </button>
        </div>

        <form onSubmit={handleSubmit} style={{ padding: "18px" }}>
          <div style={{ marginBottom: "14px" }}>
            <label
              htmlFor="bucket-name"
              style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#0A0A0A", marginBottom: "5px" }}
            >
              Bucket name
            </label>
            <input
              id="bucket-name"
              type="text"
              value={name}
              onChange={(e) => handleNameChange(e.target.value)}
              placeholder="my-bucket"
              autoFocus
              aria-describedby="bucket-name-hint bucket-name-error"
              style={{
                width: "100%",
                fontSize: "13px",
                color: "#0A0A0A",
                background: "#FAFAF9",
                border: `1px solid ${error ? "#B42318" : "#E8E8E5"}`,
                borderRadius: "10px",
                padding: "7px 10px",
                outline: "none",
                boxSizing: "border-box",
              }}
            />
            {error && (
              <div id="bucket-name-error" role="alert" style={{ fontSize: "12px", color: "#B42318", marginTop: "4px" }}>
                {error}
              </div>
            )}
            <div id="bucket-name-hint" style={{ fontSize: "11px", color: "#737373", marginTop: "4px" }}>
              3–63 chars · lowercase, numbers, dots, hyphens · no IP-like names
            </div>
          </div>

          <div style={{ marginBottom: "18px" }}>
            <label
              htmlFor="bucket-region"
              style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#0A0A0A", marginBottom: "5px" }}
            >
              Region
            </label>
            <select
              id="bucket-region"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              style={{
                width: "100%",
                fontSize: "13px",
                color: "#0A0A0A",
                background: "#FAFAF9",
                border: "1px solid #E8E8E5",
                borderRadius: "10px",
                padding: "7px 10px",
                outline: "none",
                cursor: "pointer",
              }}
            >
              {REGIONS.map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>

          <div style={{ display: "flex", gap: "8px", justifyContent: "flex-end" }}>
            <button
              type="button"
              onClick={onClose}
              style={{
                fontSize: "13px",
                fontWeight: 500,
                color: "#0A0A0A",
                background: "transparent",
                border: "1px solid #E8E8E5",
                borderRadius: "10px",
                padding: "8px 16px",
                cursor: "pointer",
                letterSpacing: "-0.005em",
                transition: "all 0.15s ease",
              }}
            >
              Cancel
            </button>
            <button
              type="submit"
              style={{
                fontSize: "13px",
                fontWeight: 500,
                color: "#FFFFFF",
                background: "linear-gradient(135deg, #10B981 0%, #059669 100%)",
                border: "none",
                borderRadius: "10px",
                padding: "7px 16px",
                cursor: "pointer",
              }}
            >
              Create bucket
            </button>
          </div>
        </form>
      </div>
    </DialogOverlay>
  );
}

// ─── Delete Bucket Dialog ────────────────────────────────────────────────────

interface DeleteBucketDialogProps {
  bucket: Bucket;
  onClose: () => void;
  onConfirm: () => void;
}

export function DeleteBucketDialog({ bucket, onClose, onConfirm }: DeleteBucketDialogProps) {
  return (
    <DialogOverlay onClose={onClose}>
      <ConfirmDialog
        icon={<AlertTriangle size={20} color="#B42318" />}
        title="Delete bucket"
        message={
          <span>
            Are you sure you want to delete bucket{" "}
            <strong style={{ fontFamily: "monospace" }}>{bucket.name}</strong>?
            {bucket.objectCount > 0 && (
              <span style={{ display: "block", marginTop: "6px", color: "#B42318" }}>
                This bucket contains {bucket.objectCount.toLocaleString()} objects. Delete all objects first.
              </span>
            )}
          </span>
        }
        disabled={bucket.objectCount > 0}
        confirmLabel="Delete bucket"
        confirmDanger
        onClose={onClose}
        onConfirm={onConfirm}
      />
    </DialogOverlay>
  );
}

// ─── Delete Object Dialog ────────────────────────────────────────────────────

interface DeleteObjectDialogProps {
  object: ObjectDetail;
  onClose: () => void;
  onConfirm: () => void;
}

export function DeleteObjectDialog({ object, onClose, onConfirm }: DeleteObjectDialogProps) {
  return (
    <DialogOverlay onClose={onClose}>
      <ConfirmDialog
        icon={<Trash2 size={20} color="#B42318" />}
        title="Delete object"
        message={
          <span>
            Delete object{" "}
            <strong style={{ fontFamily: "monospace" }}>{object.key}</strong> from bucket{" "}
            <strong style={{ fontFamily: "monospace" }}>{object.bucket}</strong>?
            <span style={{ display: "block", marginTop: "6px", color: "#737373", fontSize: "12px" }}>
              This action cannot be undone.
            </span>
          </span>
        }
        confirmLabel="Delete object"
        confirmDanger
        onClose={onClose}
        onConfirm={onConfirm}
      />
    </DialogOverlay>
  );
}

// ─── Shared Components ───────────────────────────────────────────────────────

function DialogOverlay({ children, onClose }: { children: React.ReactNode; onClose: () => void }) {
  return (
    <div
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(10,10,10,0.5)",
        backdropFilter: "blur(8px)",
        WebkitBackdropFilter: "blur(8px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
        padding: "16px",
      }}
    >
      {children}
    </div>
  );
}

function ConfirmDialog({
  icon,
  title,
  message,
  confirmLabel,
  confirmDanger,
  disabled,
  onClose,
  onConfirm,
}: {
  icon: React.ReactNode;
  title: string;
  message: React.ReactNode;
  confirmLabel: string;
  confirmDanger?: boolean;
  disabled?: boolean;
  onClose: () => void;
  onConfirm: () => void;
}) {
  return (
    <div
      style={{
        background: "#FFFFFF",
        borderRadius: "16px",
        width: "400px",
        maxWidth: "calc(100vw - 32px)",
        boxShadow: "0 20px 50px -10px rgba(0,0,0,0.15), 0 8px 20px -8px rgba(0,0,0,0.08)",
        overflow: "hidden",
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <div style={{ padding: "20px 20px 0" }}>
        <div style={{ display: "flex", gap: "12px", alignItems: "flex-start" }}>
          <div
            style={{
              width: "36px",
              height: "36px",
              borderRadius: "50%",
              background: "#FEF2F2",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              flexShrink: 0,
            }}
          >
            {icon}
          </div>
          <div>
            <div style={{ fontSize: "14px", fontWeight: 600, color: "#0A0A0A", marginBottom: "6px" }}>{title}</div>
            <div style={{ fontSize: "13px", color: "#737373", lineHeight: "1.6" }}>{message}</div>
          </div>
        </div>
      </div>

      <div
        style={{
          display: "flex",
          gap: "8px",
          justifyContent: "flex-end",
          padding: "16px 20px",
        }}
      >
        <button
          onClick={onClose}
          style={{
            fontSize: "13px",
            fontWeight: 500,
            color: "#0A0A0A",
            background: "transparent",
            border: "1px solid #E8E8E5",
            borderRadius: "10px",
            padding: "8px 16px",
            cursor: "pointer",
            letterSpacing: "-0.005em",
            transition: "all 0.15s ease",
          }}
        >
          Cancel
        </button>
        <button
          onClick={() => { onConfirm(); onClose(); }}
          disabled={disabled}
          style={{
            fontSize: "13px",
            fontWeight: 500,
            color: disabled ? "#E8E8E5" : "#FFFFFF",
            background: disabled
              ? "#F4F4F2"
              : confirmDanger
              ? "#B42318"
              : "linear-gradient(135deg, #10B981 0%, #059669 100%)",
            border: "none",
            borderRadius: "10px",
            padding: "8px 16px",
            cursor: disabled ? "not-allowed" : "pointer",
            letterSpacing: "-0.005em",
            transition: "all 0.15s ease",
            boxShadow: disabled ? "none" : "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)",
          }}
        >
          {confirmLabel}
        </button>
      </div>
    </div>
  );
}