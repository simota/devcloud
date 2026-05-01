package dashboard

const gcsIndexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>devcloud GCS</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7f6;
      --surface: #ffffff;
      --surface-soft: #f9fbfa;
      --line: #d9e0dc;
      --line-strong: #bdcbc4;
      --text: #17201c;
      --muted: #61706a;
      --muted-2: #83918b;
      --green: #10684d;
      --green-soft: #e4f3ec;
      --blue: #1c5d99;
      --blue-soft: #e7f0fa;
      --amber: #8a5b14;
      --amber-soft: #fff2d8;
      --shadow: 0 1px 2px rgba(15, 26, 21, 0.05), 0 18px 42px rgba(15, 26, 21, 0.08);
    }
    * { box-sizing: border-box; }
    html, body { min-height: 100%; }
    body {
      margin: 0;
      background:
        radial-gradient(circle at 8% -10%, rgba(28, 93, 153, 0.10), transparent 28rem),
        radial-gradient(circle at 88% 0%, rgba(16, 104, 77, 0.11), transparent 24rem),
        linear-gradient(180deg, #fcfdfb 0%, var(--bg) 44%, #eef2ef 100%);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    a { color: var(--blue); text-decoration: none; }
    a:hover { text-decoration: underline; }
    button, input { font: inherit; }
    button { cursor: pointer; }
    .app {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      min-height: 100vh;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 4;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 68px;
      padding: 12px 18px;
      border-bottom: 1px solid rgba(217, 224, 220, 0.92);
      background: rgba(255, 255, 255, 0.84);
      backdrop-filter: blur(10px);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }
    .brand-mark {
      width: 36px;
      height: 36px;
      display: grid;
      place-items: center;
      border: 1px solid #b7d6c8;
      border-radius: 8px;
      background: linear-gradient(160deg, #edf8f3, #ffffff);
      color: var(--green);
      font-weight: 850;
    }
    h1 { margin: 0; font-size: 17px; line-height: 22px; font-weight: 760; letter-spacing: 0; }
    .subhead {
      margin-top: 2px;
      color: var(--muted);
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: min(68vw, 720px);
    }
    .top-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
    }
    .switcher {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f1f4f1;
    }
    .switcher a {
      min-width: 44px;
      padding: 5px 9px;
      border-radius: 8px;
      color: var(--muted);
      font-weight: 680;
      font-size: 12px;
      text-align: center;
      text-decoration: none;
    }
    .switcher a.active {
      color: var(--green);
      background: white;
      box-shadow: 0 1px 2px rgba(15, 26, 21, 0.06);
    }
    .button {
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--surface);
      color: var(--text);
      padding: 8px 11px;
      font-weight: 700;
      text-align: center;
      min-height: 36px;
    }
    .button:hover { border-color: var(--line-strong); box-shadow: 0 5px 16px rgba(15, 26, 21, 0.08); text-decoration: none; }
    main {
      display: grid;
      grid-template-columns: 286px minmax(0, 1fr) 360px;
      gap: 12px;
      padding: 12px;
      min-height: calc(100vh - 68px);
    }
    .pane {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.86);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .pane-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      min-height: 54px;
      padding: 14px;
      border-bottom: 1px solid var(--line);
      background: rgba(249, 251, 250, 0.82);
    }
    .pane-title {
      margin: 0;
      color: #35443d;
      font-size: 13px;
      line-height: 18px;
      font-weight: 820;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    .pane-count { color: var(--muted); font-size: 12px; }
    .pane-body { padding: 14px; }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 12px;
    }
    .chip {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 9px;
      background: var(--surface-soft);
    }
    .chip-label { color: var(--muted); font-size: 11px; line-height: 14px; }
    .chip-value {
      margin-top: 2px;
      color: var(--text);
      font-size: 12px;
      font-weight: 760;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .bucket-list { display: grid; gap: 8px; }
    .bucket {
      width: 100%;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      text-align: left;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      color: var(--text);
      padding: 10px;
    }
    .bucket:hover { border-color: var(--line-strong); background: #fbfcfb; }
    .bucket.active { border-color: #88c2a8; background: linear-gradient(180deg, #f1fbf6, #ffffff); }
    .bucket-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-weight: 780;
    }
    .bucket-meta { margin-top: 3px; color: var(--muted); font-size: 12px; }
    .count-pill {
      align-self: start;
      border-radius: 999px;
      padding: 3px 8px;
      background: var(--green-soft);
      color: var(--green);
      font-size: 12px;
      font-weight: 780;
    }
    .toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      padding: 14px;
      border-bottom: 1px solid var(--line);
      background: rgba(249, 251, 250, 0.74);
    }
    input {
      width: 100%;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 8px 10px;
      background: white;
      color: var(--text);
    }
    input:focus { outline: 3px solid rgba(28, 93, 153, 0.18); border-color: #7aa5cc; }
    .object-shell { padding: 0; overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; background: var(--surface); }
    th, td { padding: 11px 14px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: middle; }
    th { color: var(--muted); font-weight: 760; background: #fbfcfb; white-space: nowrap; }
    tr.object-row { cursor: pointer; }
    tr.object-row:hover, tr.object-row.active { background: #f2f7f4; }
    .object-name { color: var(--blue); font-weight: 760; overflow-wrap: anywhere; }
    .object-kind {
      display: inline-grid;
      place-items: center;
      width: 24px;
      height: 24px;
      margin-right: 8px;
      border-radius: 7px;
      background: var(--blue-soft);
      color: var(--blue);
      font-size: 12px;
      font-weight: 830;
    }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    .muted { color: var(--muted); }
    .empty {
      color: var(--muted);
      padding: 28px;
      min-height: 128px;
      display: grid;
      align-content: center;
      gap: 4px;
    }
    .empty strong { color: var(--text); font-size: 14px; }
    .detail {
      display: grid;
      gap: 12px;
      padding: 14px;
    }
    .detail-title {
      margin: 0;
      color: var(--text);
      font-size: 15px;
      line-height: 20px;
      overflow-wrap: anywhere;
    }
    .detail-actions {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }
    .metric {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface-soft);
      padding: 9px;
    }
    .metric-label { color: var(--muted); font-size: 11px; line-height: 14px; }
    .metric-value {
      margin-top: 3px;
      font-size: 12px;
      font-weight: 760;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    dl { margin: 0; display: grid; gap: 8px; }
    dt { color: var(--muted); font-size: 11px; line-height: 14px; }
    dd { margin: 0; color: var(--text); font-size: 12px; overflow-wrap: anywhere; }
    .metadata-list {
      display: grid;
      gap: 6px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .metadata-list li {
      display: grid;
      grid-template-columns: minmax(0, 0.44fr) minmax(0, 0.56fr);
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 7px 8px;
      background: #ffffff;
    }
    .session-list { display: grid; gap: 8px; }
    .session {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface-soft);
      padding: 9px;
    }
    .session-head {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: var(--text);
      font-size: 12px;
      font-weight: 760;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 999px;
      padding: 2px 8px;
      background: var(--amber-soft);
      color: var(--amber);
      font-size: 11px;
      font-weight: 780;
      white-space: nowrap;
    }
    footer {
      grid-column: 1 / -1;
      color: var(--muted);
      font-size: 12px;
      padding: 0 2px 2px;
    }
    @media (max-width: 1120px) {
      main { grid-template-columns: 280px minmax(0, 1fr); }
      .detail-pane { grid-column: 1 / -1; }
    }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      .top-actions { width: 100%; justify-content: flex-start; }
      main { grid-template-columns: 1fr; padding: 10px; }
      .toolbar { grid-template-columns: 1fr; }
      .metric-grid, .status-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="app">
    <header>
      <div class="brand">
        <div class="brand-mark" aria-hidden="true">G</div>
        <div>
          <h1>devcloud GCS</h1>
          <div class="subhead" id="status">Loading local GCS status...</div>
        </div>
      </div>
      <div class="top-actions">
        <nav class="switcher" aria-label="Service dashboards">
          <a href="/mail">Mail</a>
          <a href="/s3">S3</a>
          <a class="active" href="/gcs">GCS</a>
        </nav>
        <button type="button" class="button" id="refresh">Refresh</button>
      </div>
    </header>
    <main>
      <section class="pane">
        <div class="pane-head">
          <h2 class="pane-title">Buckets</h2>
          <span class="pane-count" id="bucket-count">0 buckets</span>
        </div>
        <div class="pane-body">
          <div class="status-grid">
            <div class="chip">
              <div class="chip-label">Endpoint</div>
              <div class="chip-value" id="endpoint">-</div>
            </div>
            <div class="chip">
              <div class="chip-label">Project</div>
              <div class="chip-value" id="project">-</div>
            </div>
          </div>
          <div class="bucket-list" id="buckets"></div>
        </div>
      </section>

      <section class="pane">
        <div class="pane-head">
          <h2 class="pane-title">Objects</h2>
          <span class="pane-count" id="object-count">0 objects</span>
        </div>
        <div class="toolbar">
          <input id="prefix" type="search" placeholder="Filter prefix, e.g. docs/" autocomplete="off">
          <button type="button" class="button" id="apply-prefix">Apply</button>
        </div>
        <div id="objects" class="empty">
          <strong>Select a bucket.</strong>
          <span>Objects, generations, and metadata will appear here.</span>
        </div>
      </section>

      <section class="pane detail-pane">
        <div class="pane-head">
          <h2 class="pane-title">Object Detail</h2>
          <span class="pane-count" id="session-count">0 uploads</span>
        </div>
        <div id="detail" class="empty">
          <strong>No object selected.</strong>
          <span>Select an object to inspect metadata and download it.</span>
        </div>
      </section>

      <footer id="activity">Ready.</footer>
    </main>
  </div>
  <script>
    const state = {
      buckets: [],
      objects: [],
      sessions: [],
      selectedBucket: "",
      selectedObjectName: "",
      prefix: ""
    };
    const bucketsEl = document.querySelector("#buckets");
    const objectsEl = document.querySelector("#objects");
    const detailEl = document.querySelector("#detail");
    const bucketCountEl = document.querySelector("#bucket-count");
    const objectCountEl = document.querySelector("#object-count");
    const sessionCountEl = document.querySelector("#session-count");
    const statusEl = document.querySelector("#status");
    const endpointEl = document.querySelector("#endpoint");
    const projectEl = document.querySelector("#project");
    const prefixEl = document.querySelector("#prefix");
    const activityEl = document.querySelector("#activity");
    const refreshEl = document.querySelector("#refresh");
    const applyPrefixEl = document.querySelector("#apply-prefix");

    function escapeHTML(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "\"": "&quot;",
        "'": "&#39;"
      })[char]);
    }

    function formatBytes(value) {
      const size = Number(value || 0);
      if (size < 1024) return size + " B";
      if (size < 1024 * 1024) return (size / 1024).toFixed(1) + " KB";
      if (size < 1024 * 1024 * 1024) return (size / 1024 / 1024).toFixed(1) + " MB";
      return (size / 1024 / 1024 / 1024).toFixed(1) + " GB";
    }

    function formatDate(value) {
      if (!value) return "-";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return String(value);
      return date.toLocaleString();
    }

    async function getJSON(path) {
      const response = await fetch(path);
      if (!response.ok) {
        throw new Error(path + " returned " + response.status);
      }
      return response.json();
    }

    function setActivity(message) {
      activityEl.textContent = message;
    }

    async function loadStatus() {
      const status = await getJSON("/api/gcs/status");
      endpointEl.textContent = status.endpoint || "-";
      projectEl.textContent = status.project || "-";
      statusEl.textContent = status.running ? (status.endpoint + " · project " + status.project) : "GCS service is disabled";
    }

    async function loadBuckets() {
      const data = await getJSON("/api/gcs/buckets");
      state.buckets = data.buckets || [];
      bucketCountEl.textContent = state.buckets.length + (state.buckets.length === 1 ? " bucket" : " buckets");
      if (!state.buckets.length) {
        state.selectedBucket = "";
        bucketsEl.innerHTML = '<div class="empty"><strong>No buckets yet.</strong><span>Create one through the GCS JSON API, then refresh.</span></div>';
        renderObjects([]);
        return;
      }
      if (!state.buckets.some((bucket) => bucket.name === state.selectedBucket)) {
        state.selectedBucket = state.buckets[0].name;
      }
      renderBuckets();
      await loadObjects();
    }

    function renderBuckets() {
      bucketsEl.innerHTML = state.buckets.map((bucket, index) =>
        '<button type="button" class="bucket ' + (bucket.name === state.selectedBucket ? "active" : "") + '" data-index="' + index + '">' +
          '<div>' +
            '<div class="bucket-name">' + escapeHTML(bucket.name) + '</div>' +
            '<div class="bucket-meta">' + escapeHTML(bucket.gcsUri) + '</div>' +
          '</div>' +
          '<span class="count-pill">' + Number(bucket.objectCount || 0) + '</span>' +
        '</button>'
      ).join("");
      bucketsEl.querySelectorAll(".bucket").forEach((button) => {
        button.addEventListener("click", async () => {
          const bucket = state.buckets[Number(button.dataset.index)];
          state.selectedBucket = bucket.name;
          state.selectedObjectName = "";
          renderBuckets();
          await loadObjects();
        });
      });
    }

    async function loadObjects() {
      if (!state.selectedBucket) {
        renderObjects([]);
        return;
      }
      const path = "/api/gcs/buckets/" + encodeURIComponent(state.selectedBucket) + "/objects?prefix=" + encodeURIComponent(state.prefix);
      const data = await getJSON(path);
      state.objects = data.objects || [];
      renderObjects(state.objects);
      if (state.objects.length && !state.objects.some((object) => object.name === state.selectedObjectName)) {
        await selectObject(state.objects[0].name);
      } else if (!state.objects.length) {
        renderDetail(null);
      }
    }

    function renderObjects(objects) {
      objectCountEl.textContent = objects.length + (objects.length === 1 ? " object" : " objects");
      if (!state.selectedBucket) {
        objectsEl.className = "empty";
        objectsEl.innerHTML = '<strong>Select a bucket.</strong><span>Objects, generations, and metadata will appear here.</span>';
        return;
      }
      if (!objects.length) {
        objectsEl.className = "empty";
        objectsEl.innerHTML = '<strong>No objects found.</strong><span>This bucket has no objects for the current prefix.</span>';
        return;
      }
      objectsEl.className = "object-shell";
      objectsEl.innerHTML =
        '<table>' +
          '<thead><tr><th>Name</th><th>Size</th><th>Generation</th><th>Type</th><th>Updated</th></tr></thead>' +
          '<tbody>' +
            objects.map((object, index) =>
              '<tr class="object-row ' + (object.name === state.selectedObjectName ? "active" : "") + '" data-index="' + index + '">' +
                '<td><span class="object-kind">O</span><span class="object-name">' + escapeHTML(object.name) + '</span><div class="muted mono">' + escapeHTML(object.gcsUri) + '</div></td>' +
                '<td>' + escapeHTML(formatBytes(object.size)) + '</td>' +
                '<td class="mono">' + escapeHTML(object.generation) + '</td>' +
                '<td>' + escapeHTML(object.contentType || "application/octet-stream") + '</td>' +
                '<td>' + escapeHTML(formatDate(object.updated)) + '</td>' +
              '</tr>'
            ).join("") +
          '</tbody>' +
        '</table>';
      objectsEl.querySelectorAll(".object-row").forEach((row) => {
        row.addEventListener("click", async () => {
          const object = state.objects[Number(row.dataset.index)];
          await selectObject(object.name);
        });
      });
    }

    async function selectObject(name) {
      state.selectedObjectName = name;
      renderObjects(state.objects);
      const path = "/api/gcs/buckets/" + encodeURIComponent(state.selectedBucket) + "/objects/" + encodeURIComponent(name);
      const object = await getJSON(path);
      renderDetail(object);
    }

    function renderDetail(object) {
      if (!object) {
        detailEl.className = "empty";
        detailEl.innerHTML = '<strong>No object selected.</strong><span>Select an object to inspect metadata and download it.</span>';
        return;
      }
      const metadata = object.metadata || {};
      const metadataKeys = Object.keys(metadata).sort();
      const metadataHTML = metadataKeys.length
        ? '<ul class="metadata-list">' + metadataKeys.map((key) =>
            '<li><span class="mono">' + escapeHTML(key) + '</span><span>' + escapeHTML(metadata[key]) + '</span></li>'
          ).join("") + '</ul>'
        : '<div class="muted">No user metadata.</div>';
      detailEl.className = "detail";
      detailEl.innerHTML =
        '<div>' +
          '<h3 class="detail-title">' + escapeHTML(object.name) + '</h3>' +
          '<div class="muted mono">' + escapeHTML(object.gcsUri) + '</div>' +
        '</div>' +
        '<div class="detail-actions">' +
          '<a class="button" href="' + escapeHTML(object.downloadUrl) + '">Download</a>' +
          '<button type="button" class="button" id="copy-uri">Copy URI</button>' +
        '</div>' +
        '<div class="metric-grid">' +
          '<div class="metric"><div class="metric-label">Size</div><div class="metric-value">' + escapeHTML(formatBytes(object.size)) + '</div></div>' +
          '<div class="metric"><div class="metric-label">Storage class</div><div class="metric-value">' + escapeHTML(object.storageClass || "STANDARD") + '</div></div>' +
          '<div class="metric"><div class="metric-label">Generation</div><div class="metric-value mono">' + escapeHTML(object.generation) + '</div></div>' +
          '<div class="metric"><div class="metric-label">Metageneration</div><div class="metric-value mono">' + escapeHTML(object.metageneration) + '</div></div>' +
        '</div>' +
        '<dl>' +
          '<div><dt>Content type</dt><dd>' + escapeHTML(object.contentType || "application/octet-stream") + '</dd></div>' +
          '<div><dt>ETag</dt><dd class="mono">' + escapeHTML(object.etag) + '</dd></div>' +
          '<div><dt>CRC32C</dt><dd class="mono">' + escapeHTML(object.crc32c || "-") + '</dd></div>' +
          '<div><dt>Updated</dt><dd>' + escapeHTML(formatDate(object.updated)) + '</dd></div>' +
        '</dl>' +
        '<div>' +
          '<div class="pane-title" style="margin-bottom:8px">Metadata</div>' +
          metadataHTML +
        '</div>' +
        '<div>' +
          '<div class="pane-title" style="margin-bottom:8px">Upload Sessions</div>' +
          renderSessionsForObject(object.name) +
        '</div>';
      const copyButton = document.querySelector("#copy-uri");
      if (copyButton) {
        copyButton.addEventListener("click", async () => {
          try {
            await navigator.clipboard.writeText(object.gcsUri || "");
            setActivity("Copied " + object.gcsUri);
          } catch (error) {
            setActivity("Unable to copy URI.");
          }
        });
      }
    }

    function renderSessionsForObject(name) {
      const sessions = state.sessions.filter((session) => session.bucket === state.selectedBucket && session.name === name);
      if (!sessions.length) {
        return '<div class="muted">No active upload sessions for this object.</div>';
      }
      return '<div class="session-list">' + sessions.map((session) =>
        '<div class="session">' +
          '<div class="session-head"><span class="mono">' + escapeHTML(session.id) + '</span><span class="badge">' + escapeHTML(formatBytes(session.receivedBytes)) + '</span></div>' +
          '<div class="muted">' + escapeHTML(session.contentType || "application/octet-stream") + ' · ' + escapeHTML(formatDate(session.createdAt)) + '</div>' +
        '</div>'
      ).join("") + '</div>';
    }

    async function loadSessions() {
      try {
        const data = await getJSON("/api/gcs/upload-sessions");
        state.sessions = data.sessions || [];
      } catch (error) {
        state.sessions = [];
      }
      sessionCountEl.textContent = state.sessions.length + (state.sessions.length === 1 ? " upload" : " uploads");
    }

    async function refreshAll() {
      setActivity("Refreshing GCS dashboard...");
      await loadStatus();
      await loadSessions();
      await loadBuckets();
      setActivity("Updated " + new Date().toLocaleTimeString());
    }

    refreshEl.addEventListener("click", () => {
      refreshAll().catch((error) => {
        setActivity(error.message);
      });
    });
    applyPrefixEl.addEventListener("click", () => {
      state.prefix = prefixEl.value;
      state.selectedObjectName = "";
      loadObjects().catch((error) => setActivity(error.message));
    });
    prefixEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        state.prefix = prefixEl.value;
        state.selectedObjectName = "";
        loadObjects().catch((error) => setActivity(error.message));
      }
    });

    refreshAll().catch((error) => {
      statusEl.textContent = "Unable to load GCS status";
      bucketsEl.innerHTML = '<div class="empty"><strong>Unable to load buckets.</strong><span>' + escapeHTML(error.message) + '</span></div>';
      setActivity(error.message);
    });
  </script>
</body>
</html>`
