import { useEffect, useMemo, useRef, useState } from "react";
import { MailHeader } from "./components/mail-header";
import { MessageList } from "./components/message-list";
import { MessageDetail } from "./components/message-detail";
import { MailFooter } from "./components/mail-footer";
import { ConfirmDialog } from "./components/confirm-dialog";
import { SidebarRail } from "./components/sidebar-rail";
import { initialMessages, formatTime, type Message } from "./components/mail-data";

export default function App() {
  const [messages, setMessages] = useState<Message[]>(initialMessages);
  const [selectedId, setSelectedId] = useState<string | null>(initialMessages[0]?.id ?? null);
  const [query, setQuery] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<Date>(new Date());
  const [confirmOpen, setConfirmOpen] = useState<null | { kind: "all" } | { kind: "one"; id: string }>(null);
  const [mobileView, setMobileView] = useState<"list" | "detail">("list");
  const [tick, setTick] = useState(0);
  const newMsgTimers = useRef<Record<string, number>>({});

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return messages;
    return messages.filter(
      (m) =>
        m.subject.toLowerCase().includes(q) ||
        m.from.toLowerCase().includes(q) ||
        m.to.some((t) => t.toLowerCase().includes(q)),
    );
  }, [messages, query]);

  const selected = useMemo(
    () => messages.find((m) => m.id === selectedId) ?? null,
    [messages, selectedId],
  );

  useEffect(() => {
    if (!selected && filtered.length > 0) {
      setSelectedId(filtered[0].id);
    }
  }, [selected, filtered]);

  useEffect(() => {
    const id = window.setInterval(() => setTick((t) => t + 1), 30000);
    return () => window.clearInterval(id);
  }, []);

  const lastReceived = messages[0]?.receivedAt ?? null;

  const refresh = () => {
    setRefreshing(true);
    window.setTimeout(() => {
      setRefreshing(false);
      setUpdatedAt(new Date());
    }, 500);
  };

  const handleDelete = (id: string) => {
    setMessages((prev) => {
      const idx = prev.findIndex((m) => m.id === id);
      const next = prev.filter((m) => m.id !== id);
      if (id === selectedId) {
        const newSel = next[idx] ?? next[idx - 1] ?? next[0] ?? null;
        setSelectedId(newSel ? newSel.id : null);
      }
      return next;
    });
    setUpdatedAt(new Date());
  };

  const handleClearAll = () => {
    setMessages([]);
    setSelectedId(null);
    setUpdatedAt(new Date());
    setConfirmOpen(null);
  };

  const onSelect = (id: string) => {
    setSelectedId(id);
    setMobileView("detail");
  };

  const updatedLabel = useMemo(() => {
    void tick;
    const diff = Math.max(0, Date.now() - updatedAt.getTime());
    const secs = Math.floor(diff / 1000);
    if (secs < 5) return "Updated just now";
    if (secs < 60) return `Updated ${secs}s ago`;
    const mins = Math.floor(secs / 60);
    return `Updated ${mins}m ago`;
  }, [updatedAt, tick]);

  // Cleanup new-flag timers
  useEffect(() => {
    return () => {
      Object.values(newMsgTimers.current).forEach((t) => window.clearTimeout(t));
    };
  }, []);

  return (
    <div
      className="relative flex h-dvh w-full text-[#1D211C]"
      style={{ fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" }}
    >
      {/* Ambient background */}
      <div
        className="pointer-events-none absolute inset-0 -z-10"
        style={{
          background:
            "radial-gradient(900px 500px at -10% -10%, rgba(23,107,77,0.10), transparent 60%)," +
            "radial-gradient(700px 400px at 110% 0%, rgba(154,91,19,0.06), transparent 60%)," +
            "linear-gradient(180deg, #F7F8F5 0%, #F2F4EE 100%)",
        }}
        aria-hidden
      />

      <SidebarRail />

      <div className="flex min-w-0 flex-1 flex-col">
        <MailHeader
          endpoint="localhost:1025"
          running
          onRefresh={refresh}
          onClearAll={() => setConfirmOpen({ kind: "all" })}
          refreshing={refreshing}
          messageCount={messages.length}
        />

        <main className="flex min-h-0 flex-1 gap-3 p-3 sm:gap-4 sm:p-4">
          {messages.length === 0 ? (
            <div className="flex-1 overflow-hidden rounded-2xl border border-black/5 bg-white/70 backdrop-blur-xl shadow-[0_1px_2px_rgba(0,0,0,0.03),0_24px_48px_-32px_rgba(0,0,0,0.18)]">
              <MessageDetail message={null} onDelete={() => {}} />
            </div>
          ) : (
            <>
              <section
                className={[
                  "min-h-0 overflow-hidden rounded-2xl border border-black/5 bg-white/60 backdrop-blur-xl shadow-[0_1px_2px_rgba(0,0,0,0.03)]",
                  "md:w-[40%] md:max-w-[420px] md:min-w-[320px] xl:w-[380px] xl:max-w-[380px] xl:min-w-[380px]",
                  mobileView === "list" ? "flex w-full" : "hidden md:flex",
                  "flex-col",
                ].join(" ")}
                aria-label="Inbox"
              >
                <MessageList
                  messages={filtered}
                  selectedId={selectedId}
                  onSelect={onSelect}
                  query={query}
                  onQueryChange={setQuery}
                  totalCount={messages.length}
                />
              </section>

              <section
                className={[
                  "min-h-0 flex-1 overflow-hidden rounded-2xl border border-black/5 bg-white/70 backdrop-blur-xl shadow-[0_1px_2px_rgba(0,0,0,0.03),0_24px_48px_-32px_rgba(0,0,0,0.18)]",
                  mobileView === "detail" ? "flex w-full" : "hidden md:flex",
                  "flex-col",
                ].join(" ")}
                aria-label="Message detail"
              >
                <MessageDetail
                  message={selected}
                  onDelete={(id) => setConfirmOpen({ kind: "one", id })}
                  onBack={() => setMobileView("list")}
                  showBack
                />
              </section>
            </>
          )}
        </main>

        <MailFooter
          lastReceived={lastReceived ? formatTime(lastReceived) : null}
          storagePath=".devcloud/data"
          apiOk
          updatedLabel={updatedLabel}
        />
      </div>

      <ConfirmDialog
        open={confirmOpen?.kind === "all"}
        title="Clear all messages?"
        description="This removes messages from the local devcloud inbox."
        confirmLabel="Clear all"
        onCancel={() => setConfirmOpen(null)}
        onConfirm={handleClearAll}
      />
      <ConfirmDialog
        open={confirmOpen?.kind === "one"}
        title="Delete this message?"
        description="The selected message will be removed from the local inbox."
        confirmLabel="Delete"
        onCancel={() => setConfirmOpen(null)}
        onConfirm={() => {
          if (confirmOpen?.kind === "one") handleDelete(confirmOpen.id);
          setConfirmOpen(null);
        }}
      />
    </div>
  );
}
