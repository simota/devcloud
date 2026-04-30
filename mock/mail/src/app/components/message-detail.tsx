import { useEffect, useState } from "react";
import { ArrowLeft, Check, Copy, MoreHorizontal, Trash2 } from "lucide-react";
import { type Message, formatFull, middleTruncate } from "./mail-data";

type Tab = "preview" | "headers" | "raw";

type Props = {
  message: Message | null;
  onDelete: (id: string) => void;
  onBack?: () => void;
  showBack?: boolean;
};

const ALLOWED_HTML_TAGS = new Set([
  "A",
  "B",
  "BLOCKQUOTE",
  "BR",
  "CODE",
  "DIV",
  "EM",
  "H1",
  "H2",
  "H3",
  "LI",
  "OL",
  "P",
  "PRE",
  "SPAN",
  "STRONG",
  "UL",
]);

function sanitizeEmailHTML(html: string) {
  const doc = new DOMParser().parseFromString(html, "text/html");

  doc.body.querySelectorAll("*").forEach((node) => {
    if (!ALLOWED_HTML_TAGS.has(node.tagName)) {
      node.remove();
      return;
    }

    Array.from(node.attributes).forEach((attr) => {
      const name = attr.name.toLowerCase();
      const value = attr.value.trim().toLowerCase();
      const safeHref = name === "href" && !value.startsWith("javascript:");

      if (name !== "title" && name !== "aria-label" && !safeHref) {
        node.removeAttribute(attr.name);
      }
    });
  });

  return doc.body.innerHTML;
}

function gradientFor(seed: string) {
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
  const palettes: [string, string][] = [
    ["#1f8a64", "#176B4D"],
    ["#c47a2c", "#9A5B13"],
    ["#5872b8", "#3a4f8a"],
    ["#a45cb8", "#7a3a8a"],
    ["#7e8a7c", "#5F675D"],
    ["#2da37a", "#176B4D"],
  ];
  return palettes[h % palettes.length];
}

export function MessageDetail({ message, onDelete, onBack, showBack }: Props) {
  const [tab, setTab] = useState<Tab>("preview");
  const [copiedId, setCopiedId] = useState(false);

  useEffect(() => {
    setTab("preview");
  }, [message?.id]);

  if (!message) {
    return <EmptyState />;
  }

  const copyId = async () => {
    try {
      await navigator.clipboard.writeText(message.id);
      setCopiedId(true);
      setTimeout(() => setCopiedId(false), 1200);
    } catch {}
  };

  const [c1, c2] = gradientFor(message.from);
  const safeHTML = message.html ? sanitizeEmailHTML(message.html) : "";

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="relative">
        <div
          className="pointer-events-none absolute inset-x-0 top-0 h-32 opacity-[0.07]"
          style={{ background: `radial-gradient(60% 100% at 50% 0%, ${c1} 0%, transparent 70%)` }}
          aria-hidden
        />
        <div className="relative px-5 pb-4 pt-5 sm:px-8 sm:pt-7">
          <div className="flex items-start justify-between gap-3">
            <div className="flex items-center gap-3 min-w-0">
              {showBack && (
                <button
                  onClick={onBack}
                  className="md:hidden -ml-1 grid place-items-center size-9 shrink-0 rounded-full bg-white/70 ring-1 ring-black/5 hover:bg-white focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
                  aria-label="Back to inbox"
                >
                  <ArrowLeft className="size-4 text-[#1D211C]" />
                </button>
              )}
              <div
                className="grid size-10 shrink-0 place-items-center rounded-2xl text-white shadow-[0_4px_14px_-4px_rgba(0,0,0,0.25)]"
                style={{ background: `linear-gradient(135deg, ${c1}, ${c2})`, fontSize: 14, fontWeight: 600 }}
                aria-hidden
              >
                {(message.from.split("@")[0] || "?").charAt(0).toUpperCase()}
              </div>
              <div className="min-w-0">
                <h1
                  className="truncate text-[#1D211C]"
                  style={{ fontSize: 20, lineHeight: "26px", fontWeight: 600, letterSpacing: "-0.01em" }}
                >
                  {message.subject || <span className="text-[#5F675D] italic">(No subject)</span>}
                </h1>
                <p
                  className="mt-0.5 truncate text-[#5F675D]"
                  style={{ fontSize: 13, lineHeight: "18px", fontWeight: 500 }}
                  title={message.from}
                >
                  {message.from}
                  <span className="mx-1.5 text-[#D9DED5]">·</span>
                  <span className="tabular-nums">{formatFull(message.receivedAt)}</span>
                </p>
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-1.5">
              <button
                onClick={copyId}
                className="grid size-9 place-items-center rounded-full bg-white/70 text-[#5F675D] ring-1 ring-black/5 hover:bg-white hover:text-[#1D211C] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
                aria-label="Copy message ID"
                title="Copy message ID"
              >
                {copiedId ? <Check className="size-4 text-[#176B4D]" /> : <Copy className="size-4" />}
              </button>
              <button
                onClick={() => onDelete(message.id)}
                className="grid size-9 place-items-center rounded-full bg-white/70 text-[#5F675D] ring-1 ring-black/5 hover:bg-[#FBEFEE] hover:text-[#B42318] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#B42318]"
                aria-label="Delete message"
                title="Delete"
              >
                <Trash2 className="size-4" />
              </button>
              <button
                className="grid size-9 place-items-center rounded-full bg-white/70 text-[#5F675D] ring-1 ring-black/5 hover:bg-white hover:text-[#1D211C] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
                aria-label="More"
                title="More"
              >
                <MoreHorizontal className="size-4" />
              </button>
            </div>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-1.5">
            <Pill label="To" value={message.to.map((a) => middleTruncate(a, 32)).join(", ")} title={message.to.join(", ")} />
            <Pill label="ID" value={middleTruncate(message.id, 22)} mono subtle title={message.id} />
            {message.html && <Tag>HTML</Tag>}
            {message.parseWarning && <Tag tone="warn">Parse warning</Tag>}
          </div>
        </div>
      </div>

      <div className="px-5 sm:px-8">
        <div className="inline-flex rounded-full bg-black/[0.04] p-0.5" role="tablist" aria-label="Message views">
          {(["preview", "headers", "raw"] as Tab[]).map((t) => {
            const active = tab === t;
            return (
              <button
                key={t}
                role="tab"
                aria-selected={active}
                aria-controls={`tabpanel-${t}`}
                id={`tab-${t}`}
                onClick={() => setTab(t)}
                className={[
                  "rounded-full px-3 py-1 capitalize transition-colors duration-150",
                  "focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D] focus-visible:ring-offset-1",
                  active ? "bg-white text-[#1D211C] shadow-[0_1px_2px_rgba(0,0,0,0.06)]" : "text-[#5F675D] hover:text-[#1D211C]",
                ].join(" ")}
                style={{ fontSize: 12, lineHeight: "18px", fontWeight: 600 }}
              >
                {t}
              </button>
            );
          })}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-5 py-5 sm:px-8 sm:py-6">
        {tab === "preview" && (
          <div role="tabpanel" id="tabpanel-preview" aria-labelledby="tab-preview">
            <div className="rounded-2xl border border-black/5 bg-white p-6 shadow-[0_1px_2px_rgba(0,0,0,0.03),0_24px_48px_-32px_rgba(0,0,0,0.18)]">
              {message.html ? (
                <div
                  className="max-w-2xl text-[#1D211C]"
                  style={{ fontSize: 14, lineHeight: "22px" }}
                  dangerouslySetInnerHTML={{ __html: safeHTML }}
                />
              ) : (
                <pre
                  className="whitespace-pre-wrap break-words text-[#1D211C]"
                  style={{ fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", fontSize: 14, lineHeight: "22px" }}
                >
                  {message.body}
                </pre>
              )}
            </div>
          </div>
        )}

        {tab === "headers" && (
          <div role="tabpanel" id="tabpanel-headers" aria-labelledby="tab-headers">
            <div className="overflow-hidden rounded-2xl border border-black/5 bg-white shadow-[0_1px_2px_rgba(0,0,0,0.03)]">
              <table className="w-full border-collapse">
                <tbody>
                  {Object.entries(message.headers).map(([k, v], i) => (
                    <tr key={k} className={`align-top ${i !== 0 ? "border-t border-black/[0.05]" : ""}`}>
                      <th
                        scope="row"
                        className="w-44 px-4 py-2.5 text-left text-[#5F675D]"
                        style={{ fontSize: 12, lineHeight: "16px", fontWeight: 500 }}
                      >
                        {k}
                      </th>
                      <td
                        className="px-4 py-2.5 text-[#1D211C] break-all"
                        style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12, lineHeight: "18px" }}
                      >
                        {v}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {tab === "raw" && (
          <div role="tabpanel" id="tabpanel-raw" aria-labelledby="tab-raw">
            <div className="overflow-hidden rounded-2xl border border-white/5 bg-[#101511] shadow-[0_8px_32px_-12px_rgba(0,0,0,0.4)]">
              <div className="flex items-center justify-between border-b border-white/5 px-4 py-2">
                <div className="flex items-center gap-1.5">
                  <span className="size-2.5 rounded-full bg-[#ff5f57]/80" aria-hidden />
                  <span className="size-2.5 rounded-full bg-[#febc2e]/80" aria-hidden />
                  <span className="size-2.5 rounded-full bg-[#28c840]/80" aria-hidden />
                </div>
                <span className="text-[#9aa39a]" style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 11, lineHeight: "16px", fontWeight: 500 }}>
                  raw source · {message.raw.split("\n").length} lines
                </span>
                <span className="size-2.5" aria-hidden />
              </div>
              <pre
                tabIndex={0}
                className="max-h-[60vh] overflow-auto px-5 py-4 text-[#E8EFE7] focus:outline-none"
                style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12, lineHeight: "20px", whiteSpace: "pre" }}
              >
                {message.raw}
              </pre>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function Pill({ label, value, mono, subtle, title }: { label: string; value: string; mono?: boolean; subtle?: boolean; title?: string }) {
  return (
    <span
      className={`inline-flex max-w-full items-center gap-1.5 rounded-full bg-white/70 px-2.5 py-1 ring-1 ring-inset ring-black/5 ${subtle ? "text-[#5F675D]" : "text-[#1D211C]"}`}
      title={title || value}
    >
      <span className="text-[#5F675D]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 600, letterSpacing: "0.04em", textTransform: "uppercase" }}>
        {label}
      </span>
      <span
        className="truncate"
        style={{
          fontFamily: mono ? "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" : undefined,
          fontSize: 12,
          lineHeight: "16px",
          fontWeight: 500,
        }}
      >
        {value}
      </span>
    </span>
  );
}

function Tag({ children, tone = "default" }: { children: React.ReactNode; tone?: "default" | "warn" }) {
  const styles =
    tone === "warn"
      ? "bg-[#FCEFD8] text-[#9A5B13] ring-[#9A5B13]/15"
      : "bg-[#DDEFE7] text-[#176B4D] ring-[#176B4D]/15";
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 ring-1 ring-inset ${styles}`} style={{ fontSize: 11, lineHeight: "16px", fontWeight: 600, letterSpacing: "0.02em" }}>
      {children}
    </span>
  );
}

function EmptyState() {
  return (
    <div className="grid h-full place-items-center p-6">
      <div className="max-w-md text-center">
        <div className="mx-auto grid size-12 place-items-center rounded-2xl bg-gradient-to-br from-[#DDEFE7] to-[#EEF1EC] text-[#176B4D] shadow-[0_4px_16px_-6px_rgba(23,107,77,0.25)] ring-1 ring-[#176B4D]/10">
          <svg viewBox="0 0 24 24" className="size-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
            <rect x="3" y="5" width="18" height="14" rx="3" />
            <path d="m3 7 9 6 9-6" />
          </svg>
        </div>
        <h2 className="mt-4 text-[#1D211C]" style={{ fontSize: 20, lineHeight: "26px", fontWeight: 600, letterSpacing: "-0.01em" }}>
          Inbox is empty
        </h2>
        <p className="mt-1 text-[#5F675D]" style={{ fontSize: 14, lineHeight: "20px" }}>
          Send mail to <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}>localhost:1025</span> and it will appear here in real time.
        </p>
        <div className="mt-5 overflow-hidden rounded-2xl border border-white/5 bg-[#101511] text-left shadow-[0_8px_32px_-12px_rgba(0,0,0,0.4)]">
          <div className="flex items-center gap-1.5 border-b border-white/5 px-3 py-2">
            <span className="size-2.5 rounded-full bg-[#ff5f57]/80" aria-hidden />
            <span className="size-2.5 rounded-full bg-[#febc2e]/80" aria-hidden />
            <span className="size-2.5 rounded-full bg-[#28c840]/80" aria-hidden />
          </div>
          <pre
            className="overflow-auto px-4 py-3 text-[#E8EFE7]"
            style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12, lineHeight: "20px" }}
          >
{`host  localhost
port  1025
TLS   off
auth  none`}
          </pre>
        </div>
      </div>
    </div>
  );
}
