import { useEffect, useRef } from "react";

type Props = {
  open: boolean;
  title: string;
  description: string;
  cancelLabel?: string;
  confirmLabel: string;
  onCancel: () => void;
  onConfirm: () => void;
};

export function ConfirmDialog({ open, title, description, cancelLabel = "Cancel", confirmLabel, onCancel, onConfirm }: Props) {
  const confirmRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    confirmRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onCancel]);

  if (!open) return null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="confirm-title"
      aria-describedby="confirm-desc"
      className="fixed inset-0 z-50 grid place-items-center bg-black/30 p-4 animate-[fadeIn_120ms_ease-out]"
      onClick={onCancel}
    >
      <div
        className="w-full max-w-sm rounded-lg border border-[#D9DED5] bg-white p-5 shadow-lg animate-[scaleIn_120ms_ease-out]"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="confirm-title" className="text-[#1D211C]" style={{ fontSize: 18, lineHeight: "24px", fontWeight: 600 }}>
          {title}
        </h2>
        <p id="confirm-desc" className="mt-1.5 text-[#5F675D]" style={{ fontSize: 14, lineHeight: "20px" }}>
          {description}
        </p>
        <div className="mt-5 flex items-center justify-end gap-2">
          <button
            onClick={onCancel}
            className="rounded-md border border-[#D9DED5] bg-white px-3 py-1.5 text-[#1D211C] hover:bg-[#EEF1EC] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
            style={{ fontSize: 14, lineHeight: "20px", fontWeight: 500 }}
          >
            {cancelLabel}
          </button>
          <button
            ref={confirmRef}
            onClick={onConfirm}
            className="rounded-md bg-[#B42318] px-3 py-1.5 text-white hover:bg-[#9b1f15] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#B42318] focus-visible:ring-offset-1"
            style={{ fontSize: 14, lineHeight: "20px", fontWeight: 600 }}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
      <style>{`
        @keyframes fadeIn { from { opacity: 0 } to { opacity: 1 } }
        @keyframes scaleIn { from { opacity: 0; transform: scale(0.98) } to { opacity: 1; transform: scale(1) } }
      `}</style>
    </div>
  );
}
