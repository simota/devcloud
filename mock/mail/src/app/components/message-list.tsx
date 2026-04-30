import { AlertTriangle, Search, Users } from "lucide-react";
import { type Message, formatTime, middleTruncate } from "./mail-data";

type Props = {
  messages: Message[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  query: string;
  onQueryChange: (q: string) => void;
  totalCount: number;
};

const PALETTES = [
  ["#176B4D", "#1f8a64"],
  ["#9A5B13", "#c47a2c"],
  ["#3a4f8a", "#5872b8"],
  ["#7a3a8a", "#a45cb8"],
  ["#5F675D", "#7e8a7c"],
  ["#176B4D", "#2da37a"],
];

function avatarPalette(seed: string) {
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
  return PALETTES[h % PALETTES.length];
}

function initial(from: string) {
  const name = from.split("@")[0] || from;
  return name.charAt(0).toUpperCase();
}

export function MessageList({ messages, selectedId, onSelect, query, onQueryChange, totalCount }: Props) {
  return (
    <div className="flex h-full flex-col">
      <div className="px-4 pb-2 pt-3">
        <label className="relative flex items-center">
          <Search className="pointer-events-none absolute left-3 size-3.5 text-[#5F675D]" />
          <input
            type="text"
            value={query}
            onChange={(e) => onQueryChange(e.target.value)}
            placeholder="Filter by subject, from, to"
            className="w-full rounded-xl border border-black/5 bg-white/70 py-2 pl-9 pr-3 text-[#1D211C] shadow-[0_1px_0_rgba(0,0,0,0.02)] placeholder:text-[#5F675D] focus:outline-none focus:ring-2 focus:ring-[#176B4D] focus:border-transparent backdrop-blur-sm"
            style={{ fontSize: 13, lineHeight: "18px" }}
            aria-label="Search messages"
          />
        </label>
        {query && (
          <p className="mt-1.5 px-1 text-[#5F675D]" style={{ fontSize: 12, lineHeight: "16px", fontWeight: 500 }}>
            {messages.length} of {totalCount} match
          </p>
        )}
      </div>

      <ul className="flex-1 overflow-y-auto px-2 pb-3" role="listbox" aria-label="Messages">
        {messages.length === 0 ? (
          <li className="px-4 py-10 text-center text-[#5F675D]" style={{ fontSize: 13, lineHeight: "18px" }}>
            No messages match.
          </li>
        ) : (
          messages.map((m) => {
            const selected = m.id === selectedId;
            const [c1, c2] = avatarPalette(m.from);
            return (
              <li key={m.id} role="option" aria-selected={selected} className="px-1">
                <button
                  onClick={() => onSelect(m.id)}
                  className={[
                    "group relative my-0.5 w-full rounded-xl px-3 py-2.5 text-left transition-all duration-150",
                    "focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D] focus-visible:ring-offset-1",
                    selected
                      ? "bg-white shadow-[0_1px_2px_rgba(0,0,0,0.04),0_8px_24px_-12px_rgba(23,107,77,0.25)] ring-1 ring-[#176B4D]/15"
                      : "hover:bg-white/70",
                  ].join(" ")}
                >
                  <div className="flex items-start gap-3">
                    <div
                      className="mt-0.5 grid size-8 shrink-0 place-items-center rounded-full text-white shadow-[0_1px_2px_rgba(0,0,0,0.08)]"
                      style={{ background: `linear-gradient(135deg, ${c1}, ${c2})`, fontSize: 12, fontWeight: 600 }}
                      aria-hidden
                    >
                      {initial(m.from)}
                    </div>

                    <div className="min-w-0 flex-1">
                      <div className="flex items-baseline justify-between gap-3">
                        <span
                          className="truncate text-[#1D211C]"
                          style={{ fontSize: 13, lineHeight: "18px", fontWeight: 600 }}
                          title={m.from}
                        >
                          {middleTruncate(m.from.split("@")[0], 22)}
                          <span className="text-[#5F675D]" style={{ fontWeight: 400 }}>
                            @{middleTruncate(m.from.split("@")[1] ?? "", 18)}
                          </span>
                        </span>
                        <span
                          className="shrink-0 text-[#5F675D] tabular-nums"
                          style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 11, lineHeight: "16px", fontWeight: 500 }}
                        >
                          {formatTime(m.receivedAt)}
                        </span>
                      </div>

                      <div className="mt-0.5 flex items-center gap-1.5">
                        {m.parseWarning && (
                          <AlertTriangle className="size-3 shrink-0 text-[#9A5B13]" aria-label="Parse warning" />
                        )}
                        <span
                          className="truncate text-[#1D211C]"
                          style={{ fontSize: 13, lineHeight: "18px", fontWeight: 500 }}
                        >
                          {m.subject || <span className="text-[#5F675D] italic font-normal">(No subject)</span>}
                        </span>
                        {m.to.length > 1 && (
                          <span className="inline-flex shrink-0 items-center gap-0.5 rounded-full bg-black/[0.04] px-1.5 text-[#5F675D]" style={{ fontSize: 11, lineHeight: "16px", fontWeight: 500 }}>
                            <Users className="size-2.5" />
                            {m.to.length}
                          </span>
                        )}
                      </div>

                      <p className="mt-0.5 line-clamp-1 text-[#5F675D]" style={{ fontSize: 12, lineHeight: "16px", fontWeight: 400 }}>
                        {m.snippet}
                      </p>
                    </div>
                  </div>

                  {m.isNew && (
                    <span className="absolute right-3 top-2.5 inline-flex items-center rounded-full bg-gradient-to-br from-[#1f8a64] to-[#176B4D] px-1.5 text-white shadow-[0_2px_6px_-2px_rgba(23,107,77,0.6)]" style={{ fontSize: 10, lineHeight: "14px", fontWeight: 600 }}>
                      new
                    </span>
                  )}
                </button>
              </li>
            );
          })
        )}
      </ul>
    </div>
  );
}
