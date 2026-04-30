type Props = {
  lastReceived: string | null;
  storagePath: string;
  apiOk: boolean;
  updatedLabel: string;
};

export function MailFooter({ lastReceived, storagePath, apiOk, updatedLabel }: Props) {
  return (
    <footer className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 border-t border-black/5 bg-white/60 px-4 py-2 backdrop-blur-xl sm:px-6">
      <div className="flex items-center gap-4">
        <span className="text-[#5F675D]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 500 }}>
          Last received{" "}
          <span
            className="text-[#1D211C] tabular-nums"
            style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}
          >
            {lastReceived || "—"}
          </span>
        </span>
        <span className="hidden sm:inline text-[#5F675D]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 500 }}>
          Storage{" "}
          <span
            className="text-[#1D211C]"
            style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}
          >
            {storagePath}
          </span>
        </span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-[#5F675D]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 500 }}>
          {updatedLabel}
        </span>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-black/[0.03] px-2 py-0.5 ring-1 ring-inset ring-black/5">
          <span className={`size-1.5 rounded-full ${apiOk ? "bg-[#176B4D]" : "bg-[#B42318]"}`} aria-hidden />
          <span className="text-[#1D211C]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 600 }}>
            API {apiOk ? "OK" : "Down"}
          </span>
        </span>
      </div>
    </footer>
  );
}
