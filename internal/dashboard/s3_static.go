package dashboard

const s3IndexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>devcloud S3</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f4;
      --panel: #ffffff;
      --panel-soft: #fbfcf9;
      --line: #dde2d8;
      --line-strong: #c8d1c2;
      --text: #171c16;
      --muted: #667063;
      --muted-2: #899286;
      --green: #0f6b4d;
      --green-soft: #e4f3eb;
      --blue: #1d5d9b;
      --blue-soft: #e8f1fb;
      --amber: #8a5a13;
      --shadow: 0 1px 2px rgba(20, 28, 18, 0.05), 0 14px 34px rgba(20, 28, 18, 0.08);
    }
    * { box-sizing: border-box; }
    html, body { min-height: 100%; }
    body {
      margin: 0;
      background:
        radial-gradient(circle at 12% 0%, rgba(15, 107, 77, 0.10), transparent 26rem),
        linear-gradient(180deg, #fbfcf9 0%, var(--bg) 38%, #eff2eb 100%);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, input { font: inherit; }
    button { cursor: pointer; }
    .app {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) auto;
      min-height: 100vh;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 5;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 64px;
      padding: 12px 18px;
      border-bottom: 1px solid rgba(221, 226, 216, 0.9);
      background: rgba(255, 255, 255, 0.82);
      backdrop-filter: blur(10px);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }
    .brand-mark {
      width: 34px;
      height: 34px;
      border: 1px solid #b9d7c8;
      border-radius: 8px;
      display: grid;
      place-items: center;
      background: linear-gradient(160deg, #ecf8f1, #ffffff);
      color: var(--green);
      font-weight: 800;
    }
    h1 { margin: 0; font-size: 17px; line-height: 22px; font-weight: 700; }
    .subhead { margin-top: 2px; color: var(--muted); font-size: 12px; }
    .top-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    .switcher {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #f1f4ee;
    }
    .switcher a {
      min-width: 44px;
      padding: 5px 9px;
      border-radius: 8px;
      color: var(--muted);
      text-decoration: none;
      font-weight: 650;
      font-size: 12px;
      text-align: center;
    }
    .switcher a.active {
      color: var(--green);
      background: white;
      box-shadow: 0 1px 2px rgba(20, 28, 18, 0.06);
    }
    .button {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--panel);
      color: var(--text);
      padding: 8px 12px;
      font-weight: 650;
      transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
    }
    .button:hover { border-color: var(--line-strong); box-shadow: 0 4px 14px rgba(20, 28, 18, 0.08); }
    .button.primary { background: var(--green); border-color: var(--green); color: white; }
    main {
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr) 360px;
      gap: 12px;
      padding: 12px;
      min-height: calc(100vh - 114px);
    }
    .pane {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.84);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .pane-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      min-height: 52px;
      padding: 14px;
      border-bottom: 1px solid var(--line);
      background: rgba(251, 252, 249, 0.8);
    }
    .pane-title { margin: 0; font-size: 13px; line-height: 18px; font-weight: 800; text-transform: uppercase; color: #3d4739; letter-spacing: 0; }
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
      background: var(--panel-soft);
    }
    .chip-label { color: var(--muted); font-size: 11px; line-height: 14px; }
    .chip-value { margin-top: 2px; color: var(--text); font-weight: 700; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .bucket-list { display: grid; gap: 8px; }
    .bucket {
      width: 100%;
      text-align: left;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 10px;
    }
    .bucket:hover { border-color: var(--line-strong); background: #fbfcf9; }
    .bucket.active { border-color: #8bc3a8; background: linear-gradient(180deg, #f3fbf7, #ffffff); }
    .bucket-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 750; }
    .bucket-meta { margin-top: 3px; color: var(--muted); font-size: 12px; }
    .count-pill {
      align-self: start;
      border-radius: 999px;
      padding: 3px 8px;
      background: var(--green-soft);
      color: var(--green);
      font-size: 12px;
      font-weight: 750;
    }
    .toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      padding: 14px;
      border-bottom: 1px solid var(--line);
      background: rgba(251, 252, 249, 0.72);
    }
    input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 11px;
      background: white;
      color: var(--text);
    }
    input:focus { outline: 3px solid rgba(29, 93, 155, 0.18); border-color: #7aa6cf; }
    .object-shell { padding: 0; }
    table { width: 100%; border-collapse: collapse; background: var(--panel); }
    th, td { padding: 11px 14px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: middle; }
    th { color: var(--muted); font-weight: 750; background: #fbfcf9; }
    tr.object-row { cursor: pointer; }
    tr.object-row:hover, tr.object-row.active { background: #f2f7f4; }
    .key { color: var(--blue); font-weight: 700; overflow-wrap: anywhere; }
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
      font-weight: 800;
    }
    .muted { color: var(--muted); }
    .empty {
      color: var(--muted);
      padding: 28px;
      border: 1px dashed var(--line-strong);
      border-radius: 8px;
      background: linear-gradient(180deg, #ffffff, #fbfcf9);
    }
    .empty strong { display: block; color: var(--text); margin-bottom: 4px; }
    .inspector-hero {
      display: grid;
      gap: 10px;
      padding: 16px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, #f3fbf7, #ffffff);
    }
    .object-title { margin: 0; color: var(--blue); font-size: 18px; line-height: 24px; overflow-wrap: anywhere; }
    .action-row { display: flex; gap: 8px; flex-wrap: wrap; }
    .link-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 34px;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 7px 10px;
      color: var(--text);
      background: white;
      text-decoration: none;
      font-weight: 700;
      font-size: 13px;
    }
    .link-button.primary { color: white; border-color: var(--green); background: var(--green); }
    .kv { display: grid; grid-template-columns: 118px minmax(0, 1fr); gap: 10px 12px; font-size: 13px; }
    .kv dt { color: var(--muted); }
    .kv dd { margin: 0; overflow-wrap: anywhere; }
    .metadata-list {
      display: grid;
      gap: 8px;
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid var(--line);
    }
    .meta-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      background: #fbfcf9;
    }
    .meta-key { color: var(--muted); font-size: 12px; }
    .meta-value { margin-top: 2px; overflow-wrap: anywhere; font-weight: 650; }
    code {
      background: #111711;
      color: #e8efe7;
      border-radius: 6px;
      padding: 2px 5px;
      font-size: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    footer {
      display: flex;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
      padding: 10px 18px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
      background: rgba(255, 255, 255, 0.82);
    }
    @media (max-width: 1050px) {
      main { grid-template-columns: 240px minmax(0, 1fr); }
      aside.inspector { grid-column: 1 / -1; }
    }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      main { display: block; padding: 8px; }
      .pane { margin-bottom: 8px; }
      .status-grid { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr; }
      footer { align-items: flex-start; flex-direction: column; gap: 4px; }
    }
  </style>
</head>
<body>
  <div class="app">
    <header>
      <div class="brand">
        <div class="brand-mark">S3</div>
        <div>
          <h1>devcloud S3</h1>
          <div class="subhead">Object Explorer for local S3-compatible storage</div>
        </div>
      </div>
      <div class="top-actions">
        <nav class="switcher" aria-label="Service switcher">
          <a href="/mail">Mail</a>
          <a class="active" href="/s3">S3</a>
        </nav>
        <button id="refreshButton" class="button primary" type="button">Refresh</button>
      </div>
    </header>
    <main>
      <aside class="pane">
        <div class="pane-head">
          <h2 class="pane-title">Buckets</h2>
          <span id="bucketCount" class="pane-count">0 buckets</span>
        </div>
        <div class="pane-body">
          <div class="status-grid" id="statusMeta">
            <div class="chip"><div class="chip-label">Endpoint</div><div class="chip-value">Loading</div></div>
            <div class="chip"><div class="chip-label">Region</div><div class="chip-value">Loading</div></div>
            <div class="chip"><div class="chip-label">Auth</div><div class="chip-value">Loading</div></div>
            <div class="chip"><div class="chip-label">State</div><div class="chip-value">Loading</div></div>
          </div>
          <div id="bucketList" class="bucket-list"></div>
        </div>
      </aside>
      <section class="pane">
        <div class="pane-head">
          <h2 class="pane-title" id="objectPaneTitle">Objects</h2>
          <span id="objectCount" class="pane-count">0 objects</span>
        </div>
        <div class="toolbar">
          <input id="prefixInput" aria-label="Prefix" placeholder="Filter by prefix, for example docs/">
          <button id="prefixButton" class="button" type="button">Apply</button>
        </div>
        <div id="objects" class="object-shell"></div>
      </section>
      <aside class="pane inspector">
        <div class="pane-head">
          <h2 class="pane-title">Inspector</h2>
          <span class="pane-count">metadata</span>
        </div>
        <div id="inspector" class="pane-body">
          <div class="empty"><strong>No object selected</strong>Select an object to inspect metadata, S3 URI, ETag, and download path.</div>
        </div>
      </aside>
    </main>
    <footer>
      <span id="activity">S3 API loading</span>
      <span>Storage <code>.devcloud/data</code></span>
    </footer>
  </div>
  <script>
    const state = { buckets: [], selectedBucket: "", objects: [], selectedObject: null, prefix: "" };
    const bucketList = document.querySelector("#bucketList");
    const objects = document.querySelector("#objects");
    const inspector = document.querySelector("#inspector");
    const activity = document.querySelector("#activity");
    const prefixInput = document.querySelector("#prefixInput");
    const bucketCount = document.querySelector("#bucketCount");
    const objectCount = document.querySelector("#objectCount");
    const objectPaneTitle = document.querySelector("#objectPaneTitle");

    async function getJSON(path) {
      const response = await fetch(path);
      if (!response.ok) throw new Error(path + " returned " + response.status);
      return response.json();
    }

    function escapeHTML(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      })[char]);
    }

    function formatBytes(bytes) {
      const value = Number(bytes || 0);
      if (value < 1024) return value + " B";
      const units = ["KB", "MB", "GB", "TB"];
      let size = value / 1024;
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size = size / 1024;
        unit++;
      }
      return size.toFixed(size >= 10 ? 0 : 1) + " " + units[unit];
    }

    function shortDate(value) {
      if (!value) return "";
      return new Date(value).toLocaleString();
    }

    async function refresh() {
      try {
        const status = await getJSON("/api/s3/status");
        document.querySelector("#statusMeta").innerHTML =
          '<div class="chip"><div class="chip-label">Endpoint</div><div class="chip-value">' + escapeHTML(status.endpoint) + '</div></div>' +
          '<div class="chip"><div class="chip-label">Region</div><div class="chip-value">' + escapeHTML(status.region) + '</div></div>' +
          '<div class="chip"><div class="chip-label">Auth</div><div class="chip-value">' + escapeHTML(status.authMode) + '</div></div>' +
          '<div class="chip"><div class="chip-label">State</div><div class="chip-value">' + escapeHTML(status.status) + '</div></div>';
        activity.textContent = "S3 API " + status.status;
        const data = await getJSON("/api/s3/buckets");
        state.buckets = data.buckets || [];
        bucketCount.textContent = state.buckets.length + (state.buckets.length === 1 ? " bucket" : " buckets");
        if (!state.buckets.some(bucket => bucket.name === state.selectedBucket)) {
          state.selectedBucket = state.buckets.length ? state.buckets[0].name : "";
          state.selectedObject = null;
        }
        renderBuckets();
        await loadObjects();
      } catch (error) {
        activity.textContent = error.message;
        bucketList.innerHTML = '<div class="empty"><strong>S3 API unavailable</strong>Start devcloud with S3 enabled and refresh this page.</div>';
      }
    }

    function renderBuckets() {
      if (!state.buckets.length) {
        bucketList.innerHTML = '<div class="empty"><strong>No buckets yet</strong>Create one with awscli-local or your SDK, then refresh.</div>';
        objects.innerHTML = '<div class="empty"><strong>Waiting for objects</strong>Create a bucket or upload with an S3 client.</div>';
        objectCount.textContent = "0 objects";
        return;
      }
      bucketList.innerHTML = state.buckets.map((bucket, index) =>
        '<button type="button" class="bucket ' + (bucket.name === state.selectedBucket ? 'active' : '') + '" data-index="' + index + '">' +
        '<span><span class="bucket-name">' + escapeHTML(bucket.name) + '</span><span class="bucket-meta">' + escapeHTML(shortDate(bucket.creationDate)) + '</span></span>' +
        '<span class="count-pill">' + escapeHTML(bucket.objectCount) + '</span></button>'
      ).join("");
      bucketList.querySelectorAll("button").forEach(button => {
        button.addEventListener("click", async () => {
          state.selectedBucket = state.buckets[Number(button.dataset.index)].name;
          state.selectedObject = null;
          renderBuckets();
          await loadObjects();
        });
      });
    }

    async function loadObjects() {
      if (!state.selectedBucket) return;
      const path = "/api/s3/buckets/" + encodeURIComponent(state.selectedBucket) + "/objects?prefix=" + encodeURIComponent(state.prefix);
      const data = await getJSON(path);
      state.objects = data.objects || [];
      objectPaneTitle.textContent = state.selectedBucket + (state.prefix ? " / " + state.prefix : "");
      objectCount.textContent = state.objects.length + (state.objects.length === 1 ? " object" : " objects");
      renderObjects();
    }

    function renderObjects() {
      if (!state.objects.length) {
        objects.innerHTML = '<div class="pane-body"><div class="empty"><strong>This prefix is empty</strong>Upload an object or change the prefix filter.</div></div>';
        inspector.innerHTML = '<div class="empty"><strong>No object selected</strong>Select an object to inspect metadata and download path.</div>';
        return;
      }
      objects.innerHTML = '<table><thead><tr><th>Name</th><th>Size</th><th>Modified</th><th>Type</th></tr></thead><tbody>' +
        state.objects.map((object, index) =>
          '<tr class="object-row ' + (state.selectedObject && state.selectedObject.key === object.key ? 'active' : '') + '" data-index="' + index + '">' +
          '<td class="key"><span class="object-kind">O</span>' + escapeHTML(object.key) + '</td>' +
          '<td>' + escapeHTML(formatBytes(object.size)) + '</td>' +
          '<td class="muted">' + escapeHTML(shortDate(object.lastModified)) + '</td>' +
          '<td class="muted">' + escapeHTML(object.contentType || "application/octet-stream") + '</td></tr>'
        ).join("") + '</tbody></table>';
      objects.querySelectorAll("tr.object-row").forEach(row => {
        row.addEventListener("click", () => {
          state.selectedObject = state.objects[Number(row.dataset.index)];
          renderObjects();
          renderInspector();
        });
      });
    }

    function renderInspector() {
      const object = state.selectedObject;
      if (!object) return;
      const metadata = object.metadata || {};
      const metadataHTML = Object.keys(metadata).length
        ? '<div class="metadata-list">' + Object.keys(metadata).map(key =>
            '<div class="meta-item"><div class="meta-key">x-amz-meta-' + escapeHTML(key) + '</div><div class="meta-value">' + escapeHTML(metadata[key]) + '</div></div>'
          ).join("") + '</div>'
        : '<div class="metadata-list"><div class="empty"><strong>No user metadata</strong>This object has no x-amz-meta-* values.</div></div>';
      inspector.innerHTML =
        '<div class="inspector-hero">' +
          '<h3 class="object-title">' + escapeHTML(object.key) + '</h3>' +
          '<div class="action-row">' +
            '<a class="link-button primary" href="' + escapeHTML(object.downloadUrl) + '">Download</a>' +
            '<a class="link-button" href="' + escapeHTML(object.downloadUrl) + '" target="_blank" rel="noreferrer">Open raw</a>' +
          '</div>' +
        '</div>' +
        '<div class="pane-body"><dl class="kv">' +
          '<dt>S3 URI</dt><dd><code>' + escapeHTML(object.s3Uri) + '</code></dd>' +
          '<dt>ETag</dt><dd><code>' + escapeHTML(object.etag) + '</code></dd>' +
          '<dt>Content-Type</dt><dd>' + escapeHTML(object.contentType || "application/octet-stream") + '</dd>' +
          '<dt>Size</dt><dd>' + escapeHTML(formatBytes(object.size)) + ' <span class="muted">(' + escapeHTML(object.size) + ' bytes)</span></dd>' +
          '<dt>Modified</dt><dd>' + escapeHTML(shortDate(object.lastModified)) + '</dd>' +
        '</dl>' + metadataHTML + '</div>';
    }

    document.querySelector("#refreshButton").addEventListener("click", refresh);
    document.querySelector("#prefixButton").addEventListener("click", async () => {
      state.prefix = prefixInput.value;
      await loadObjects();
    });
    prefixInput.addEventListener("keydown", async event => {
      if (event.key === "Enter") {
        state.prefix = prefixInput.value;
        await loadObjects();
      }
    });
    refresh();
  </script>
</body>
</html>`
