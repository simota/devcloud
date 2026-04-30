package dashboard

const serviceIndexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>devcloud Services</title>
  <style>
    :root {
      --surface-base: #f7f8f5;
      --surface-panel: #ffffff;
      --text-primary: #1d211c;
      --text-secondary: #5f675d;
      --border: #d9ded5;
      --accent: #176b4d;
      --accent-soft: #ddefe7;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(180deg, var(--surface-base), #f2f4ee);
      color: var(--text-primary);
      font: 14px/20px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      width: min(960px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 48px 0;
    }
    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 24px;
    }
    h1 {
      margin: 0;
      font-size: 28px;
      line-height: 36px;
      font-weight: 650;
    }
    p {
      margin: 6px 0 0;
      color: var(--text-secondary);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .service {
      display: block;
      min-height: 150px;
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--surface-panel);
      color: inherit;
      text-decoration: none;
      box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }
    .service:hover {
      border-color: #b9c4b5;
      box-shadow: 0 8px 22px rgba(24, 35, 26, 0.08);
    }
    .service h2 {
      margin: 0 0 8px;
      font-size: 18px;
      line-height: 24px;
    }
    .meta {
      display: inline-flex;
      margin-top: 18px;
      border-radius: 999px;
      padding: 4px 10px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 650;
      font-size: 12px;
    }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>devcloud</h1>
        <p>Local service dashboards for development and E2E inspection.</p>
      </div>
    </header>
    <section class="grid" aria-label="Services">
      <a class="service" href="/mail">
        <h2>Mail</h2>
        <p>Inspect messages received by the local SMTP server.</p>
        <span class="meta">SMTP <code>127.0.0.1:1025</code></span>
      </a>
      <a class="service" href="/s3">
        <h2>S3</h2>
        <p>Browse buckets, objects, metadata, and local S3 activity.</p>
        <span class="meta">S3 <code>127.0.0.1:4566</code></span>
      </a>
    </section>
  </main>
</body>
</html>`

const indexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>devcloud Mail</title>
  <style>
    :root {
      --surface-base: #f7f8f5;
      --surface-panel: #ffffff;
      --surface-subtle: #eef1ec;
      --text-primary: #1d211c;
      --text-secondary: #5f675d;
      --border: #d9ded5;
      --accent: #176b4d;
      --accent-soft: #ddefe7;
      --warning: #9a5b13;
      --danger: #b42318;
      --code-bg: #101511;
      --code-text: #e8efe7;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(180deg, var(--surface-base), #f2f4ee);
      color: var(--text-primary);
      font: 14px/20px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    button, input {
      font: inherit;
    }

    button {
      cursor: pointer;
    }

    .app {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
      min-height: 100vh;
    }

    .header, .footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px 20px;
      border-bottom: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.78);
    }

    .footer {
      border-top: 1px solid var(--border);
      border-bottom: 0;
      color: var(--text-secondary);
      font-size: 12px;
      line-height: 16px;
      flex-wrap: wrap;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }

    .brand h1 {
      margin: 0;
      font-size: 18px;
      line-height: 24px;
      font-weight: 600;
    }

    .meta-row {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text-secondary);
      font-size: 12px;
      line-height: 16px;
      font-weight: 500;
      flex-wrap: wrap;
    }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 4px 10px;
      color: var(--accent);
      background: var(--accent-soft);
      font-weight: 600;
    }

    .dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--accent);
    }

    .endpoint {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: var(--text-primary);
    }

    .actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .button {
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 7px 10px;
      background: var(--surface-panel);
      color: var(--text-primary);
      font-weight: 500;
    }

    .button:hover {
      border-color: #bbc4b6;
    }

    .button.danger {
      color: var(--danger);
    }

    .layout {
      display: grid;
      grid-template-columns: minmax(300px, 380px) minmax(0, 1fr);
      gap: 16px;
      min-height: 0;
      padding: 16px;
    }

    .pane {
      min-height: 0;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.82);
      overflow: hidden;
    }

    .inbox {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
    }

    .inbox-tools {
      padding: 12px;
      border-bottom: 1px solid var(--border);
    }

    .search {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 10px;
      background: #fff;
      color: var(--text-primary);
    }

    .message-list {
      margin: 0;
      padding: 8px;
      list-style: none;
      overflow: auto;
    }

    .message-row {
      width: 100%;
      display: grid;
      gap: 4px;
      margin: 0 0 6px;
      border: 1px solid transparent;
      border-radius: 8px;
      padding: 10px;
      background: transparent;
      color: inherit;
      text-align: left;
    }

    .message-row:hover,
    .message-row.selected {
      background: var(--surface-panel);
      border-color: var(--border);
    }

    .row-top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      min-width: 0;
      font-size: 12px;
      line-height: 16px;
      color: var(--text-secondary);
    }

    .from, .subject, .snippet {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .subject {
      color: var(--text-primary);
      font-weight: 600;
    }

    .snippet {
      color: var(--text-secondary);
      font-size: 12px;
      line-height: 16px;
    }

    .detail {
      display: grid;
      grid-template-rows: auto auto minmax(0, 1fr);
    }

    .detail-head {
      padding: 20px 24px 14px;
      border-bottom: 1px solid var(--border);
    }

    .detail-title {
      margin: 0 0 8px;
      font-size: 18px;
      line-height: 24px;
      font-weight: 600;
    }

    .tabs {
      display: flex;
      gap: 6px;
      padding: 10px 24px;
      border-bottom: 1px solid var(--border);
    }

    .tab {
      border: 0;
      border-radius: 8px;
      padding: 6px 10px;
      background: transparent;
      color: var(--text-secondary);
      font-weight: 600;
    }

    .tab.active {
      background: var(--surface-subtle);
      color: var(--text-primary);
    }

    .detail-body {
      min-height: 0;
      overflow: auto;
      padding: 20px 24px 24px;
    }

    .preview {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font: 14px/22px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    .raw {
      margin: 0;
      border-radius: 8px;
      padding: 16px;
      overflow: auto;
      background: var(--code-bg);
      color: var(--code-text);
      font: 12px/18px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .headers {
      width: 100%;
      border-collapse: collapse;
      font: 12px/18px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }

    .headers th, .headers td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
      word-break: break-word;
    }

    .headers th {
      width: 160px;
      color: var(--text-secondary);
      font-weight: 600;
    }

    .empty {
      display: grid;
      place-items: center;
      min-height: 260px;
      padding: 32px;
      color: var(--text-secondary);
      text-align: center;
    }

    .empty strong {
      display: block;
      margin-bottom: 4px;
      color: var(--text-primary);
      font-size: 14px;
    }

    .warn {
      color: var(--warning);
      font-weight: 600;
    }

    @media (max-width: 719px) {
      .header {
        align-items: flex-start;
        flex-direction: column;
      }

      .actions {
        width: 100%;
      }

      .button {
        flex: 1;
      }

      .layout {
        grid-template-columns: 1fr;
      }

      .pane.detail {
        min-height: 55vh;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <header class="header">
      <div class="brand">
        <div>
          <h1>devcloud Mail</h1>
          <div class="meta-row">
            <span class="endpoint">smtp://localhost:1025</span>
            <span class="status"><span class="dot" aria-hidden="true"></span>Running</span>
            <span id="message-count">0 messages</span>
          </div>
        </div>
      </div>
      <div class="actions">
        <button class="button" type="button" id="refresh">Refresh</button>
        <button class="button danger" type="button" id="clear">Clear all</button>
      </div>
    </header>

    <main class="layout">
      <section class="pane inbox" aria-label="Inbox">
        <div class="inbox-tools">
          <input class="search" id="filter" type="search" placeholder="Filter by subject, from, to" aria-label="Filter messages">
        </div>
        <ul class="message-list" id="messages" aria-label="Messages"></ul>
      </section>

      <section class="pane detail" aria-label="Message detail">
        <div class="detail-head">
          <h2 class="detail-title" id="detail-title">No message selected</h2>
          <div class="meta-row" id="detail-meta">Messages accepted by SMTP appear here.</div>
        </div>
        <div class="tabs" role="tablist" aria-label="Message views">
          <button class="tab active" type="button" data-tab="preview" role="tab" aria-selected="true">Preview</button>
          <button class="tab" type="button" data-tab="headers" role="tab" aria-selected="false">Headers</button>
          <button class="tab" type="button" data-tab="raw" role="tab" aria-selected="false">Raw</button>
        </div>
        <div class="detail-body" id="detail-body"></div>
      </section>
    </main>

    <footer class="footer">
      <span id="last-received">Last received: none</span>
      <span>Storage: .devcloud/data</span>
      <span id="api-state">API: checking</span>
    </footer>
  </div>

  <script>
    const state = {
      messages: [],
      filtered: [],
      selectedId: "",
      tab: "preview",
      rawCache: new Map()
    };

    const el = {
      count: document.getElementById("message-count"),
      list: document.getElementById("messages"),
      title: document.getElementById("detail-title"),
      meta: document.getElementById("detail-meta"),
      body: document.getElementById("detail-body"),
      filter: document.getElementById("filter"),
      refresh: document.getElementById("refresh"),
      clear: document.getElementById("clear"),
      lastReceived: document.getElementById("last-received"),
      apiState: document.getElementById("api-state")
    };

    function formatTime(value) {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return date.toLocaleString([], { dateStyle: "medium", timeStyle: "medium" });
    }

    function snippet(message) {
      return (message.textBody || message.htmlBody || message.parseError || "").replace(/\s+/g, " ").trim();
    }

    function matches(message, query) {
      const haystack = [
        message.subject || "",
        message.from || "",
        ...(message.to || []),
        snippet(message)
      ].join(" ").toLowerCase();
      return haystack.includes(query.toLowerCase());
    }

    async function loadMessages() {
      el.apiState.textContent = "API: checking";
      const response = await fetch("/api/messages", { headers: { "Accept": "application/json" } });
      if (!response.ok) throw new Error("GET /api/messages failed: " + response.status);
      const payload = await response.json();
      state.messages = Array.isArray(payload.messages) ? payload.messages : [];
      if (!state.messages.some((message) => message.id === state.selectedId)) {
        state.selectedId = state.messages[0]?.id || "";
      }
      applyFilter();
      el.apiState.textContent = "API: OK";
    }

    function applyFilter() {
      const query = el.filter.value.trim();
      state.filtered = query ? state.messages.filter((message) => matches(message, query)) : state.messages.slice();
      render();
    }

    function render() {
      el.count.textContent = state.messages.length + (state.messages.length === 1 ? " message" : " messages");
      const last = state.messages[0]?.receivedAt;
      el.lastReceived.textContent = "Last received: " + (last ? formatTime(last) : "none");
      renderList();
      renderDetail();
    }

    function renderList() {
      el.list.replaceChildren();
      if (state.filtered.length === 0) {
        const empty = document.createElement("li");
        empty.className = "empty";
        empty.innerHTML = "<div><strong>No messages</strong><span>Send mail to localhost:1025 and refresh the inbox.</span></div>";
        el.list.append(empty);
        return;
      }
      for (const message of state.filtered) {
        const item = document.createElement("li");
        const button = document.createElement("button");
        button.type = "button";
        button.className = "message-row" + (message.id === state.selectedId ? " selected" : "");
        button.addEventListener("click", () => {
          state.selectedId = message.id;
          state.tab = "preview";
          render();
        });

        const top = document.createElement("div");
        top.className = "row-top";
        const from = document.createElement("span");
        from.className = "from";
        from.textContent = message.from || "(unknown sender)";
        const received = document.createElement("span");
        received.textContent = formatTime(message.receivedAt);
        top.append(from, received);

        const subject = document.createElement("div");
        subject.className = "subject";
        subject.textContent = message.subject || "(No subject)";

        const summary = document.createElement("div");
        summary.className = "snippet";
        summary.textContent = snippet(message) || (message.to || []).join(", ") || message.id;

        button.append(top, subject, summary);
        item.append(button);
        el.list.append(item);
      }
    }

    function selectedMessage() {
      return state.messages.find((message) => message.id === state.selectedId) || null;
    }

    function renderDetail() {
      const message = selectedMessage();
      if (!message) {
        el.title.textContent = "No message selected";
        el.meta.textContent = "Messages accepted by SMTP appear here.";
        el.body.innerHTML = '<div class="empty"><div><strong>Inbox is waiting</strong><span>Use smtp://localhost:1025 from your app or test suite.</span></div></div>';
        return;
      }

      el.title.textContent = message.subject || "(No subject)";
      el.meta.textContent = (message.from || "(unknown sender)") + " to " + ((message.to || []).join(", ") || "(no recipients)") + " - " + formatTime(message.receivedAt) + " - " + message.id;
      document.querySelectorAll(".tab").forEach((tab) => {
        const active = tab.dataset.tab === state.tab;
        tab.classList.toggle("active", active);
        tab.setAttribute("aria-selected", active ? "true" : "false");
      });

      if (state.tab === "headers") {
        renderHeaders(message);
      } else if (state.tab === "raw") {
        renderRaw(message);
      } else {
        renderPreview(message);
      }
    }

    function renderPreview(message) {
      const pre = document.createElement("pre");
      pre.className = "preview";
      pre.textContent = message.textBody || message.htmlBody || message.parseError || "(No preview body)";
      el.body.replaceChildren(pre);
    }

    function renderHeaders(message) {
      const table = document.createElement("table");
      table.className = "headers";
      const tbody = document.createElement("tbody");
      const headers = message.headers || {};
      const names = Object.keys(headers).sort();
      if (message.parseError) {
        const row = document.createElement("tr");
        const name = document.createElement("th");
        name.scope = "row";
        name.className = "warn";
        name.textContent = "Parse-Error";
        const value = document.createElement("td");
        value.className = "warn";
        value.textContent = message.parseError;
        row.append(name, value);
        tbody.append(row);
      }
      for (const key of names) {
        const row = document.createElement("tr");
        const name = document.createElement("th");
        name.scope = "row";
        name.textContent = key;
        const value = document.createElement("td");
        value.textContent = Array.isArray(headers[key]) ? headers[key].join(", ") : String(headers[key]);
        row.append(name, value);
        tbody.append(row);
      }
      if (tbody.children.length === 0) {
        const row = document.createElement("tr");
        const value = document.createElement("td");
        value.colSpan = 2;
        value.textContent = "No parsed headers.";
        row.append(value);
        tbody.append(row);
      }
      table.append(tbody);
      el.body.replaceChildren(table);
    }

    async function renderRaw(message) {
      const pre = document.createElement("pre");
      pre.className = "raw";
      pre.textContent = "Loading raw source...";
      el.body.replaceChildren(pre);

      if (!state.rawCache.has(message.id)) {
        const response = await fetch("/api/messages/" + encodeURIComponent(message.id) + "/raw");
        if (!response.ok) throw new Error("GET raw failed: " + response.status);
        state.rawCache.set(message.id, await response.text());
      }
      if (selectedMessage()?.id === message.id && state.tab === "raw") {
        pre.textContent = state.rawCache.get(message.id) || "";
      }
    }

    document.querySelectorAll(".tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        state.tab = tab.dataset.tab;
        renderDetail();
      });
    });

    el.filter.addEventListener("input", applyFilter);
    el.refresh.addEventListener("click", () => {
      loadMessages().catch(showError);
    });
    el.clear.addEventListener("click", async () => {
      if (!window.confirm("Clear all messages from the local devcloud inbox?")) return;
      const response = await fetch("/api/messages", { method: "DELETE" });
      if (!response.ok) throw new Error("DELETE /api/messages failed: " + response.status);
      state.selectedId = "";
      state.rawCache.clear();
      await loadMessages();
    });

    function showError(error) {
      el.apiState.textContent = "API: error";
      el.body.innerHTML = "";
      const box = document.createElement("div");
      box.className = "empty";
      const text = document.createElement("div");
      const title = document.createElement("strong");
      title.textContent = "Dashboard request failed";
      const detail = document.createElement("span");
      detail.textContent = error.message || String(error);
      text.append(title, detail);
      box.append(text);
      el.body.append(box);
    }

    loadMessages().catch(showError);
  </script>
</body>
</html>`
