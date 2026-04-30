import { Inbox, Search, Settings, Terminal, Activity, HelpCircle } from "lucide-react";

const top = [
  { icon: Inbox, label: "Inbox", active: true },
  { icon: Search, label: "Search", disabled: true },
  { icon: Activity, label: "Activity", disabled: true },
  { icon: Terminal, label: "SMTP log", disabled: true },
];
const bottom = [
  { icon: HelpCircle, label: "Help", disabled: true },
  { icon: Settings, label: "Settings", disabled: true },
];

export function SidebarRail() {
  return (
    <aside
      className="hidden md:flex w-14 shrink-0 flex-col items-center justify-between border-r border-black/5 bg-white/60 py-3 backdrop-blur-xl"
      aria-label="Primary"
    >
      <div className="flex flex-col items-center gap-1">
        <div className="mb-2 grid size-9 place-items-center rounded-xl bg-gradient-to-br from-[#1f8a64] to-[#176B4D] text-white shadow-[0_4px_14px_-4px_rgba(23,107,77,0.5)]">
          <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
            <rect x="3" y="5" width="18" height="14" rx="3" />
            <path d="m3 7 9 6 9-6" />
          </svg>
        </div>
        {top.map(({ icon: Icon, label, active, disabled }) => (
          <button
            key={label}
            aria-label={label}
            title={disabled ? `${label} is not available in v0` : label}
            disabled={disabled}
            aria-disabled={disabled}
            className={[
              "group relative grid size-9 place-items-center rounded-xl transition-colors duration-150",
              "focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D] focus-visible:ring-offset-1",
              disabled ? "cursor-not-allowed opacity-40" : "",
              active
                ? "bg-[#DDEFE7] text-[#176B4D]"
                : "text-[#5F675D] hover:bg-black/[0.04] hover:text-[#1D211C]",
            ].join(" ")}
          >
            <Icon className="size-4" />
            {active && (
              <span className="absolute -left-[7px] top-1/2 h-5 w-[3px] -translate-y-1/2 rounded-r-full bg-[#176B4D]" aria-hidden />
            )}
          </button>
        ))}
      </div>
      <div className="flex flex-col items-center gap-1">
        {bottom.map(({ icon: Icon, label, disabled }) => (
          <button
            key={label}
            aria-label={label}
            title={disabled ? `${label} is not available in v0` : label}
            disabled={disabled}
            aria-disabled={disabled}
            className="grid size-9 cursor-not-allowed place-items-center rounded-xl text-[#5F675D] opacity-40 hover:bg-black/[0.04] hover:text-[#1D211C] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#176B4D]"
          >
            <Icon className="size-4" />
          </button>
        ))}
        <div className="mt-1 size-7 rounded-full bg-gradient-to-br from-[#9bb7a5] to-[#5F675D] ring-2 ring-white" aria-label="Account" />
      </div>
    </aside>
  );
}
