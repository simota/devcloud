export type Message = {
  id: string;
  subject: string;
  from: string;
  to: string[];
  receivedAt: string;
  snippet: string;
  body: string;
  html?: string;
  headers: Record<string, string>;
  raw: string;
  parseWarning?: boolean;
  isNew?: boolean;
};

export const initialMessages: Message[] = [
  {
    id: "01HF7KZ9X2N3M4P5Q6R7S8T9U0",
    subject: "Welcome to devcloud Mail",
    from: "noreply@devcloud.local",
    to: ["you@example.com"],
    receivedAt: "2026-04-30T10:00:12Z",
    snippet: "Your local SMTP inbox is ready. Send mail to localhost:1025 and it will appear here.",
    body: `Hi there,

Your local SMTP inbox is ready. Send mail to localhost:1025 and it will appear here.

You can inspect raw source, headers, and the rendered preview from the right panel.

— devcloud`,
    headers: {
      "Message-ID": "<01HF7KZ9X2N3M4P5Q6R7S8T9U0@devcloud.local>",
      From: "noreply@devcloud.local",
      To: "you@example.com",
      Subject: "Welcome to devcloud Mail",
      Date: "Thu, 30 Apr 2026 10:00:12 +0000",
      "Content-Type": "text/plain; charset=utf-8",
      "MIME-Version": "1.0",
    },
    raw: `Message-ID: <01HF7KZ9X2N3M4P5Q6R7S8T9U0@devcloud.local>
From: noreply@devcloud.local
To: you@example.com
Subject: Welcome to devcloud Mail
Date: Thu, 30 Apr 2026 10:00:12 +0000
Content-Type: text/plain; charset=utf-8
MIME-Version: 1.0

Hi there,

Your local SMTP inbox is ready. Send mail to localhost:1025 and it will appear here.

You can inspect raw source, headers, and the rendered preview from the right panel.

— devcloud`,
  },
  {
    id: "01HF7L1A4B5C6D7E8F9G0H1I2J",
    subject: "[staging] Deploy succeeded — build #4821",
    from: "ci@devcloud.local",
    to: ["dev-team@example.com", "ops@example.com"],
    receivedAt: "2026-04-30T09:54:03Z",
    snippet: "Build 4821 deployed to staging in 2m 14s. 0 failed checks. View pipeline for full logs.",
    body: `Build 4821 deployed to staging in 2m 14s.

Branch: main
Commit: a3f9c2e
Checks: 24 passed, 0 failed
Duration: 2m 14s

View pipeline: https://ci.devcloud.local/runs/4821`,
    headers: {
      "Message-ID": "<01HF7L1A4B5C6D7E8F9G0H1I2J@ci.devcloud.local>",
      From: "ci@devcloud.local",
      To: "dev-team@example.com, ops@example.com",
      Subject: "[staging] Deploy succeeded — build #4821",
      Date: "Thu, 30 Apr 2026 09:54:03 +0000",
      "Content-Type": "text/plain; charset=utf-8",
    },
    raw: `Message-ID: <01HF7L1A4B5C6D7E8F9G0H1I2J@ci.devcloud.local>
From: ci@devcloud.local
To: dev-team@example.com, ops@example.com
Subject: [staging] Deploy succeeded — build #4821
Date: Thu, 30 Apr 2026 09:54:03 +0000
Content-Type: text/plain; charset=utf-8

Build 4821 deployed to staging in 2m 14s.

Branch: main
Commit: a3f9c2e
Checks: 24 passed, 0 failed
Duration: 2m 14s

View pipeline: https://ci.devcloud.local/runs/4821`,
  },
  {
    id: "01HF7L2K8L9M0N1O2P3Q4R5S6T",
    subject: "Password reset requested",
    from: "auth@devcloud.local",
    to: ["alice.long.address.for.truncation@really-long-corporate-domain.example.com"],
    receivedAt: "2026-04-30T09:48:31Z",
    snippet: "Use the link below to reset your password. The link expires in 30 minutes.",
    body: "Use the link below to reset your password. The link expires in 30 minutes.\n\nhttps://app.devcloud.local/reset?token=abc123",
    html: `<div style="font-family:system-ui;line-height:1.6"><h2 style="margin:0 0 12px">Reset your password</h2><p>Use the button below to reset your password. The link expires in <strong>30 minutes</strong>.</p><p><a href="#" style="display:inline-block;padding:10px 16px;background:#176B4D;color:#fff;border-radius:6px;text-decoration:none">Reset password</a></p><p style="color:#5F675D;font-size:12px">If you did not request this, ignore this email.</p></div>`,
    headers: {
      "Message-ID": "<01HF7L2K8L9M0N1O2P3Q4R5S6T@auth.devcloud.local>",
      From: "auth@devcloud.local",
      To: "alice.long.address.for.truncation@really-long-corporate-domain.example.com",
      Subject: "Password reset requested",
      Date: "Thu, 30 Apr 2026 09:48:31 +0000",
      "Content-Type": "multipart/alternative; boundary=\"b1\"",
    },
    raw: `Message-ID: <01HF7L2K8L9M0N1O2P3Q4R5S6T@auth.devcloud.local>
From: auth@devcloud.local
To: alice.long.address.for.truncation@really-long-corporate-domain.example.com
Subject: Password reset requested
Date: Thu, 30 Apr 2026 09:48:31 +0000
Content-Type: multipart/alternative; boundary="b1"

--b1
Content-Type: text/plain; charset=utf-8

Use the link below to reset your password. The link expires in 30 minutes.

https://app.devcloud.local/reset?token=abc123

--b1
Content-Type: text/html; charset=utf-8

<div><h2>Reset your password</h2><p>Use the button below...</p></div>

--b1--`,
  },
  {
    id: "01HF7L3M9N0O1P2Q3R4S5T6U7V",
    subject: "",
    from: "monitor@devcloud.local",
    to: ["ops@example.com"],
    receivedAt: "2026-04-30T09:31:08Z",
    snippet: "queue depth 1284 — above warning threshold (1000)",
    body: "queue depth 1284 — above warning threshold (1000)\n\nbroker: redis-primary\nregion: local",
    parseWarning: true,
    headers: {
      "Message-ID": "<01HF7L3M9N0O1P2Q3R4S5T6U7V@monitor.devcloud.local>",
      From: "monitor@devcloud.local",
      To: "ops@example.com",
      Date: "Thu, 30 Apr 2026 09:31:08 +0000",
      "Content-Type": "text/plain; charset=utf-8",
    },
    raw: `Message-ID: <01HF7L3M9N0O1P2Q3R4S5T6U7V@monitor.devcloud.local>
From: monitor@devcloud.local
To: ops@example.com
Date: Thu, 30 Apr 2026 09:31:08 +0000
Content-Type: text/plain; charset=utf-8

queue depth 1284 — above warning threshold (1000)

broker: redis-primary
region: local`,
  },
  {
    id: "01HF7L4P0Q1R2S3T4U5V6W7X8Y",
    subject: "Invoice #INV-2026-0418 from Acme",
    from: "billing@acme.example.com",
    to: ["accounts@example.com"],
    receivedAt: "2026-04-30T08:12:44Z",
    snippet: "Your monthly invoice is attached. Total due: $248.00. Payment due May 14, 2026.",
    body: "Your monthly invoice is attached.\n\nTotal due: $248.00\nPayment due: May 14, 2026\n\nThank you,\nAcme Billing",
    headers: {
      "Message-ID": "<01HF7L4P0Q1R2S3T4U5V6W7X8Y@acme.example.com>",
      From: "billing@acme.example.com",
      To: "accounts@example.com",
      Subject: "Invoice #INV-2026-0418 from Acme",
      Date: "Thu, 30 Apr 2026 08:12:44 +0000",
      "Content-Type": "text/plain; charset=utf-8",
    },
    raw: `Message-ID: <01HF7L4P0Q1R2S3T4U5V6W7X8Y@acme.example.com>
From: billing@acme.example.com
To: accounts@example.com
Subject: Invoice #INV-2026-0418 from Acme
Date: Thu, 30 Apr 2026 08:12:44 +0000
Content-Type: text/plain; charset=utf-8

Your monthly invoice is attached.

Total due: $248.00
Payment due: May 14, 2026

Thank you,
Acme Billing`,
  },
  {
    id: "01HF7L5Q1R2S3T4U5V6W7X8Y9Z",
    subject: "Weekly digest — 12 new comments, 3 mentions",
    from: "digest@devcloud.local",
    to: ["you@example.com"],
    receivedAt: "2026-04-30T07:00:00Z",
    snippet: "Catch up on this week's activity across your projects.",
    body: "Catch up on this week's activity across your projects.\n\n• 12 new comments\n• 3 mentions\n• 1 review requested",
    headers: {
      "Message-ID": "<01HF7L5Q1R2S3T4U5V6W7X8Y9Z@devcloud.local>",
      From: "digest@devcloud.local",
      To: "you@example.com",
      Subject: "Weekly digest — 12 new comments, 3 mentions",
      Date: "Thu, 30 Apr 2026 07:00:00 +0000",
      "Content-Type": "text/plain; charset=utf-8",
    },
    raw: `Message-ID: <01HF7L5Q1R2S3T4U5V6W7X8Y9Z@devcloud.local>
From: digest@devcloud.local
To: you@example.com
Subject: Weekly digest — 12 new comments, 3 mentions
Date: Thu, 30 Apr 2026 07:00:00 +0000
Content-Type: text/plain; charset=utf-8

Catch up on this week's activity across your projects.

• 12 new comments
• 3 mentions
• 1 review requested`,
  },
];

export function formatTime(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  if (sameDay) {
    return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
  }
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

export function formatFull(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function middleTruncate(text: string, max = 32): string {
  if (text.length <= max) return text;
  const head = Math.ceil((max - 1) / 2);
  const tail = Math.floor((max - 1) / 2);
  return `${text.slice(0, head)}…${text.slice(-tail)}`;
}
