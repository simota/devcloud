import { Check, Command, Copy, RefreshCw, Trash2 } from "lucide-react";
import { useState } from "react";

type Props = {
  endpoint: string;
  running: boolean;
  onRefresh: () => void;
  onClearAll: () => void;
  refreshing: boolean;
  messageCount: number;
};

export function MailHeader({ endpoint, running, onRefresh, onClearAll, refreshing, messageCount }: Props) {
  const [copied, setCopied] = useState(false);

  const copyEndpoint = async () => {
    try {
      await navigator.clipboard.writeText(endpoint);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {}
  };

  return (
    <header className="flex items-center justify-between gap-3 border-b border-black/5 bg-white/70 px-4 py-2.5 backdrop-blur-xl sm:px-6">
      <div className="flex items-center gap-3 min-w-0">
        <div className="flex items-baseline gap-2 min-w-0">
          <span className="tracking-tight text-[#1D211C]" style={{ fontSize: 15, lineHeight: "20px", fontWeight: 600 }}>
            Mail
          </span>
          <span className="text-[#5F675D]" style={{ fontSize: 12, lineHeight: "16px", fontWeight: 500 }}>
            {messageCount} {messageCount === 1 ? "message" : "messages"}
          </span>
        </div>

        <span className="hidden sm:inline-flex items-center gap-1.5 rounded-full bg-[#DDEFE7]/70 px-2.5 py-1 text-[#176B4D] ring-1 ring-inset ring-[#176B4D]/15">
          <span className="relative grid place-items-center">
            {running && <span className="absolute inset-0 -m-1 rounded-full bg-[#176B4D]/30 animate-ping" />}
            <span className={`relative size-1.5 rounded-full ${running ? "bg-[#176B4D]" : "bg-[#B42318]"}`} />
          </span>
          <span style={{ fontSize: 12, lineHeight: "16px", fontWeight: 600 }}>
            {running ? "Live" : "Stopped"}
          </span>
        </span>
      </div>

      <div className="flex items-center gap-2">
        <button
          onClick={copyEndpoint}
          className="hidden sm:inline-flex items-center gap-1.5 rounded-full border border-black/5 bg-white/70 px-3 py-1.5 text-[#1D211C] shadow-[0_1px_0_rgba(0,0,0,0.02)] hover:bg-white focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D] focus-visible:ring-offset-1"
          style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12, lineHeight: "16px", fontWeight: 500 }}
          aria-label={`SMTP ${endpoint}, click to copy`}
          title="Copy SMTP endpoint"
        >
          <span className="text-[#5F675D]">smtp://</span>
          <span>{endpoint}</span>
          {copied ? <Check className="size-3.5 text-[#176B4D]" /> : <Copy className="size-3.5 text-[#5F675D]" />}
        </button>

        <button
          className="hidden lg:inline-flex items-center gap-1.5 rounded-full border border-black/5 bg-white/70 px-3 py-1.5 text-[#5F675D] hover:bg-white focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
          style={{ fontSize: 12, lineHeight: "16px", fontWeight: 500 }}
          aria-label="Open command palette"
        >
          <Command className="size-3.5" />
          <span>Search</span>
          <span className="ml-2 inline-flex items-center gap-0.5 rounded bg-black/[0.05] px-1.5 py-0.5 text-[#5F675D]" style={{ fontSize: 11, lineHeight: "14px" }}>
            ⌘K
          </span>
        </button>

        <button
          onClick={onRefresh}
          className="inline-flex items-center gap-1.5 rounded-full border border-black/5 bg-white/70 px-3 py-1.5 text-[#1D211C] hover:bg-white focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
          style={{ fontSize: 13, lineHeight: "18px", fontWeight: 500 }}
          aria-label="Refresh inbox"
        >
          <RefreshCw className={`size-3.5 text-[#5F675D] ${refreshing ? "animate-spin" : ""}`} />
          <span className="hidden sm:inline">Refresh</span>
        </button>
        <button
          onClick={onClearAll}
          className="inline-flex items-center gap-1.5 rounded-full border border-black/5 bg-white/70 px-3 py-1.5 text-[#5F675D] hover:bg-white hover:text-[#B42318] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#B42318]"
          style={{ fontSize: 13, lineHeight: "18px", fontWeight: 500 }}
          aria-label="Clear all messages"
        >
          <Trash2 className="size-3.5" />
          <span className="hidden sm:inline">Clear</span>
        </button>
      </div>
    </header>
  );
}
