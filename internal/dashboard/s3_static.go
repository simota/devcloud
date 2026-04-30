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
      --base: #f7f8f5;
      --panel: #ffffff;
      --subtle: #eef1ec;
      --text: #1d211c;
      --muted: #5f675d;
      --border: #d9ded5;
      --accent: #176b4d;
      --object: #245b8f;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--base);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 56px;
      padding: 12px 18px;
      border-bottom: 1px solid var(--border);
      background: var(--panel);
    }
    h1 { margin: 0; font-size: 18px; line-height: 24px; font-weight: 600; }
    .meta { display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); font-size: 12px; }
    button {
      border: 1px solid var(--border);
      border-radius: 6px;
      background: var(--panel);
      color: var(--text);
      padding: 7px 10px;
      font: inherit;
      cursor: pointer;
    }
    button.primary { background: var(--accent); border-color: var(--accent); color: white; }
    main {
      display: grid;
      grid-template-columns: 260px minmax(0, 1fr) 340px;
      min-height: calc(100vh - 96px);
    }
    aside, section {
      min-width: 0;
      border-right: 1px solid var(--border);
      background: var(--panel);
    }
    section { background: transparent; }
    .pane { padding: 16px; }
    .pane h2 { margin: 0 0 12px; font-size: 14px; line-height: 20px; }
    .bucket-list { display: grid; gap: 6px; }
    .bucket {
      width: 100%;
      text-align: left;
      display: flex;
      justify-content: space-between;
      gap: 8px;
    }
    .bucket.active { border-color: var(--accent); color: var(--accent); }
    .toolbar {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-bottom: 12px;
    }
    input {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      background: var(--panel);
    }
    table { width: 100%; border-collapse: collapse; background: var(--panel); }
    th, td { padding: 9px 10px; border-bottom: 1px solid var(--border); text-align: left; font-size: 13px; }
    th { color: var(--muted); font-weight: 600; }
    tr.object-row { cursor: pointer; }
    tr.object-row:hover { background: var(--subtle); }
    .key { color: var(--object); font-weight: 500; overflow-wrap: anywhere; }
    .empty { color: var(--muted); padding: 24px; border: 1px dashed var(--border); border-radius: 8px; background: var(--panel); }
    .kv { display: grid; grid-template-columns: 112px minmax(0, 1fr); gap: 8px 12px; font-size: 13px; }
    .kv dt { color: var(--muted); }
    .kv dd { margin: 0; overflow-wrap: anywhere; }
    code { background: #101511; color: #e8efe7; border-radius: 4px; padding: 2px 4px; font-size: 12px; }
    footer {
      display: flex;
      gap: 16px;
      padding: 10px 18px;
      border-top: 1px solid var(--border);
      color: var(--muted);
      font-size: 12px;
      background: var(--panel);
    }
    @media (max-width: 900px) {
      main { grid-template-columns: 220px minmax(0, 1fr); }
      aside.inspector { grid-column: 1 / -1; border-top: 1px solid var(--border); }
    }
    @media (max-width: 700px) {
      header { align-items: flex-start; flex-direction: column; }
      main { display: block; }
      aside, section { border-right: 0; border-bottom: 1px solid var(--border); }
      footer { flex-direction: column; gap: 4px; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>devcloud S3</h1>
      <div class="meta" id="statusMeta">
        <span>Endpoint loading</span>
        <span>Region loading</span>
        <span>Auth loading</span>
      </div>
    </div>
    <div>
      <button id="refreshButton" class="primary" type="button">Refresh</button>
    </div>
  </header>
  <main>
    <aside class="pane">
      <h2>Buckets</h2>
      <div id="bucketList" class="bucket-list"></div>
    </aside>
    <section class="pane">
      <div class="toolbar">
        <input id="prefixInput" aria-label="Prefix" placeholder="Prefix filter">
        <button id="prefixButton" type="button">Apply</button>
      </div>
      <div id="objects"></div>
    </section>
    <aside class="pane inspector">
      <h2>Object inspector</h2>
      <div id="inspector" class="empty">Select an object to inspect metadata and download path.</div>
    </aside>
  </main>
  <footer>
    <span id="activity">S3 API loading</span>
    <span>Storage <code>.devcloud/data</code></span>
  </footer>
  <script>
    const state = { buckets: [], selectedBucket: "", objects: [], selectedObject: null, prefix: "" };
    const bucketList = document.querySelector("#bucketList");
    const objects = document.querySelector("#objects");
    const inspector = document.querySelector("#inspector");
    const activity = document.querySelector("#activity");
    const prefixInput = document.querySelector("#prefixInput");

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

    async function refresh() {
      try {
        const status = await getJSON("/api/s3/status");
        document.querySelector("#statusMeta").innerHTML =
          "<span>" + escapeHTML(status.endpoint) + "</span><span>" + escapeHTML(status.region) + "</span><span>" + escapeHTML(status.authMode) + " auth</span>";
        activity.textContent = "S3 API " + status.status;
        const data = await getJSON("/api/s3/buckets");
        state.buckets = data.buckets || [];
        if (!state.selectedBucket && state.buckets.length) state.selectedBucket = state.buckets[0].name;
        renderBuckets();
        await loadObjects();
      } catch (error) {
        activity.textContent = error.message;
        bucketList.innerHTML = '<div class="empty">S3 API unavailable</div>';
      }
    }

    function renderBuckets() {
      if (!state.buckets.length) {
        bucketList.innerHTML = '<div class="empty">No buckets yet</div>';
        objects.innerHTML = '<div class="empty">Create a bucket or upload with an S3 client.</div>';
        return;
      }
      bucketList.innerHTML = state.buckets.map((bucket, index) =>
        '<button type="button" class="bucket ' + (bucket.name === state.selectedBucket ? 'active' : '') + '" data-index="' + index + '">' +
        '<span>' + escapeHTML(bucket.name) + '</span><span>' + escapeHTML(bucket.objectCount) + '</span></button>'
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
      renderObjects();
    }

    function renderObjects() {
      if (!state.objects.length) {
        objects.innerHTML = '<div class="empty">This bucket is empty</div>';
        inspector.innerHTML = '<div class="empty">Select an object to inspect metadata and download path.</div>';
        return;
      }
      objects.innerHTML = '<table><thead><tr><th>Name</th><th>Size</th><th>Modified</th></tr></thead><tbody>' +
        state.objects.map((object, index) =>
          '<tr class="object-row" data-index="' + index + '"><td class="key">' + escapeHTML(object.key) + '</td><td>' + escapeHTML(object.size) + '</td><td>' + escapeHTML(new Date(object.lastModified).toLocaleString()) + '</td></tr>'
        ).join("") + '</tbody></table>';
      objects.querySelectorAll("tr.object-row").forEach(row => {
        row.addEventListener("click", () => {
          state.selectedObject = state.objects[Number(row.dataset.index)];
          renderInspector();
        });
      });
    }

    function renderInspector() {
      const object = state.selectedObject;
      if (!object) return;
      const metadata = object.metadata || {};
      inspector.innerHTML = '<dl class="kv">' +
        '<dt>Key</dt><dd>' + escapeHTML(object.key) + '</dd>' +
        '<dt>S3 URI</dt><dd><code>' + escapeHTML(object.s3Uri) + '</code></dd>' +
        '<dt>ETag</dt><dd><code>' + escapeHTML(object.etag) + '</code></dd>' +
        '<dt>Content-Type</dt><dd>' + escapeHTML(object.contentType) + '</dd>' +
        '<dt>Size</dt><dd>' + escapeHTML(object.size) + ' bytes</dd>' +
        Object.keys(metadata).map(key => '<dt>meta:' + escapeHTML(key) + '</dt><dd>' + escapeHTML(metadata[key]) + '</dd>').join("") +
        '</dl>';
    }

    document.querySelector("#refreshButton").addEventListener("click", refresh);
    document.querySelector("#prefixButton").addEventListener("click", async () => {
      state.prefix = prefixInput.value;
      await loadObjects();
    });
    refresh();
  </script>
</body>
</html>`
