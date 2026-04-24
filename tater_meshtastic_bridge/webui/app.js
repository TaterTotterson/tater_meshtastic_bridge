const VIEW_META = {
  dashboard: { title: "Dashboard", subtitle: "Live status, stats, and recent mesh activity." },
  radio: { title: "Radio", subtitle: "Owner details, URLs, canned text, position, and device actions." },
  chat: { title: "Chat", subtitle: "Read-only Meshtastic traffic seen by the bridge." },
  nodes: { title: "Nodes", subtitle: "Current mesh nodes, past nodes, and sighting history." },
  channels: { title: "Channels", subtitle: "Inspect channel state, import/export URLs, and edit channel records." },
  configs: { title: "Configs", subtitle: "Browse and edit local and module config sections on the radio." },
  audit: { title: "Audit", subtitle: "Track bridge-side configuration changes and admin actions." },
  settings: { title: "Settings", subtitle: "Bridge console auth, refresh behavior, and local preferences." },
};

const state = {
  view: "dashboard",
  token: safeStorageGet("tater_meshtastic_bridge_token", ""),
  windowHours: Number.parseInt(safeStorageGet("tater_meshtastic_bridge_window_hours", "24"), 10) || 24,
  data: {
    status: {},
    stats: {},
    device: {},
    config: {},
    channels: [],
    nodes: [],
    messages: [],
    audit: [],
  },
  selectedNodeId: "",
  nodeHistory: [],
  notice: null,
  pollTimer: 0,
  loading: false,
  lastLoadedAt: "",
};

const ROOT_PATH = (() => {
  const raw = String(window.location.pathname || "/ui").replace(/\/+$/, "");
  return raw || "/ui";
})();
const API_BASE = `${ROOT_PATH}/api`;

function safeStorageGet(key, fallback = "") {
  try {
    const value = window.localStorage.getItem(String(key || ""));
    return value === null ? fallback : value;
  } catch {
    return fallback;
  }
}

function safeStorageSet(key, value) {
  try {
    window.localStorage.setItem(String(key || ""), String(value ?? ""));
  } catch {
    // Ignore storage failures.
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatTs(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "Unknown";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.valueOf())) {
    return text;
  }
  return parsed.toLocaleString();
}

function prettyJson(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch {
    return "{}";
  }
}

function setNotice(message, type = "info") {
  const text = String(message || "").trim();
  state.notice = text ? { message: text, type: String(type || "info").trim().toLowerCase() } : null;
  renderNotice();
}

function renderNotice() {
  const root = document.getElementById("notice-root");
  if (!root) {
    return;
  }
  if (!state.notice) {
    root.innerHTML = "";
    return;
  }
  const kind = state.notice.type === "error" ? "error" : "";
  root.innerHTML = `<div class="notice ${kind}">${escapeHtml(state.notice.message)}</div>`;
}

function withAuthHeaders(headers = {}) {
  const out = { Accept: "application/json", ...headers };
  const token = String(state.token || "").trim();
  if (token) {
    out.Authorization = `Bearer ${token}`;
  }
  return out;
}

async function apiFetch(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: withAuthHeaders(options.headers || {}),
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    const detail = payload && payload.detail ? payload.detail : `Request failed (${response.status})`;
    throw new Error(String(detail));
  }
  return payload || {};
}

async function loadBootstrap() {
  state.loading = true;
  try {
    const payload = await apiFetch(`/bootstrap?window_hours=${encodeURIComponent(state.windowHours)}`);
    state.data = {
      status: payload.status || {},
      stats: payload.stats || {},
      device: payload.device || {},
      config: payload.config || {},
      channels: Array.isArray(payload.channels) ? payload.channels : [],
      nodes: Array.isArray(payload.nodes) ? payload.nodes : [],
      messages: Array.isArray(payload.messages) ? payload.messages : [],
      audit: Array.isArray(payload.audit) ? payload.audit : [],
    };
    state.lastLoadedAt = new Date().toISOString();
    if (!state.selectedNodeId && state.data.nodes[0]?.node_id) {
      state.selectedNodeId = String(state.data.nodes[0].node_id);
    }
    if (state.selectedNodeId) {
      await loadNodeHistory(state.selectedNodeId);
    }
    setNotice("");
  } catch (error) {
    setNotice(error.message || "Failed to load bridge data.", "error");
  } finally {
    state.loading = false;
    render();
  }
}

async function loadNodeHistory(nodeId) {
  const token = String(nodeId || "").trim();
  if (!token) {
    state.nodeHistory = [];
    return;
  }
  try {
    const payload = await apiFetch(`/nodes/${encodeURIComponent(token)}/history?limit=80`);
    state.nodeHistory = Array.isArray(payload.history) ? payload.history : [];
    state.selectedNodeId = token;
  } catch (error) {
    setNotice(error.message || "Failed to load node history.", "error");
  }
}

function statusPill(connected, reconnectState) {
  const isConnected = Boolean(connected);
  const stateText = String(reconnectState || "").trim() || (isConnected ? "connected" : "disconnected");
  const klass = isConnected ? "ok" : stateText.includes("connect") ? "warn" : "error";
  return `<span class="status-pill ${klass}">${escapeHtml(isConnected ? "Connected" : "Offline")} · ${escapeHtml(stateText)}</span>`;
}

function renderMetaRows(rows) {
  return rows
    .filter((row) => row && row.value !== undefined && row.value !== null && String(row.value).trim() !== "")
    .map(
      (row) => `
        <div class="meta-row">
          <div class="meta-key">${escapeHtml(row.label)}</div>
          <div class="meta-value">${row.html ? row.value : escapeHtml(row.value)}</div>
        </div>
      `
    )
    .join("");
}

function messageCard(message) {
  const fromName = message?.from?.long_name || message?.from?.short_name || message?.from?.node_id || "Unknown";
  const toName = message?.to?.long_name || message?.to?.short_name || message?.to?.node_id || "Unknown";
  const body = String(message?.text || "").trim();
  const direction = String(message?.direction || "inbound").trim();
  return `
    <article class="message-card">
      <div class="message-meta">
        <span>${escapeHtml(direction)}</span>
        <span>${escapeHtml(fromName)} → ${escapeHtml(toName)}</span>
        <span>ch ${escapeHtml(message?.channel ?? 0)}</span>
        <span>${escapeHtml(formatTs(message?.timestamp))}</span>
      </div>
      <div class="message-body ${body ? "" : "empty"}">${body ? escapeHtml(body) : "No text payload on this packet."}</div>
    </article>
  `;
}

function auditCard(item) {
  return `
    <article class="audit-item">
      <div class="audit-meta">
        <span>${escapeHtml(item?.action || "action")}</span>
        <span>${escapeHtml(item?.target || "target")}</span>
        <span>${escapeHtml(item?.status || "status")}</span>
        <span>${escapeHtml(formatTs(item?.timestamp))}</span>
      </div>
      <div class="code-block">${escapeHtml(prettyJson(item?.details || {}))}</div>
    </article>
  `;
}

function renderDashboard() {
  const status = state.data.status || {};
  const stats = state.data.stats || {};
  const device = state.data.device || {};
  const topNodes = Array.isArray(stats.top_nodes) ? stats.top_nodes : [];
  const eventTypes = Array.isArray(stats.event_types) ? stats.event_types : [];
  const recentMessages = (state.data.messages || []).slice(-8).reverse();

  return `
    <section class="card-grid">
      <div class="metric-card">
        <div class="metric-label">Bridge</div>
        <div class="metric-value">${statusPill(status.connected, status.reconnect_state)}</div>
        <div class="metric-note">${escapeHtml(status.device_name || "Meshtastic")} · last seen ${escapeHtml(formatTs(status.last_seen))}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Messages (${escapeHtml(stats.window_hours || state.windowHours)}h)</div>
        <div class="metric-value">${escapeHtml(stats.recent_messages || 0)}</div>
        <div class="metric-note">${escapeHtml(stats.recent_inbound_messages || 0)} inbound · ${escapeHtml(stats.recent_outbound_messages || 0)} outbound</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Known Nodes</div>
        <div class="metric-value">${escapeHtml(stats.known_nodes || 0)}</div>
        <div class="metric-note">${escapeHtml((state.data.nodes || []).filter((item) => item.live).length)} live in the current bridge session</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Events</div>
        <div class="metric-value">${escapeHtml(stats.total_events || 0)}</div>
        <div class="metric-note">Last event ${escapeHtml(formatTs(stats.last_event_at))}</div>
      </div>
    </section>

    <section class="split-grid">
      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Device Snapshot</h3>
            <p>Current radio identity, URLs, and bridge state.</p>
          </div>
        </div>
        <div class="meta-list">
          ${renderMetaRows([
            { label: "Long Name", value: device?.local_node?.long_name || status?.local_node?.long_name || "" },
            { label: "Short Name", value: device?.local_node?.short_name || status?.local_node?.short_name || "" },
            { label: "Node ID", value: device?.local_node?.node_id || status?.local_node?.node_id || "" },
            { label: "Reconnect State", value: status?.reconnect_state || "" },
            { label: "Device URL", value: device?.urls?.primary || "" },
            { label: "Admin URL", value: device?.urls?.all || "" },
            { label: "Database", value: status?.database_path || "" },
            { label: "Last Error", value: status?.last_error || "" },
          ])}
        </div>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Mesh Activity</h3>
            <p>Who is talking the most and what kind of events the bridge is seeing.</p>
          </div>
        </div>
        <div class="stack-list">
          <div>
            <div class="helper">Top talkers</div>
            <div class="chip-row">
              ${
                topNodes.length
                  ? topNodes
                      .map(
                        (item) =>
                          `<span class="chip">${escapeHtml(item.long_name || item.short_name || item.node_id || "node")} · ${escapeHtml(item.count || 0)}</span>`
                      )
                      .join("")
                  : `<span class="muted">No inbound message traffic recorded yet.</span>`
              }
            </div>
          </div>
          <div>
            <div class="helper">Event mix</div>
            <div class="chip-row">
              ${
                eventTypes.length
                  ? eventTypes
                      .map((item) => `<span class="chip">${escapeHtml(item.event_type || "event")} · ${escapeHtml(item.count || 0)}</span>`)
                      .join("")
                  : `<span class="muted">No events recorded yet.</span>`
              }
            </div>
          </div>
        </div>
      </section>
    </section>

    <section class="panel">
      <div class="section-head">
        <div>
          <h3>Recent Chat</h3>
          <p>Read-only view of the most recent mesh messages passing through the bridge.</p>
        </div>
      </div>
      <div class="message-list">
        ${recentMessages.length ? recentMessages.map((item) => messageCard(item)).join("") : `<div class="muted">No messages recorded yet.</div>`}
      </div>
    </section>
  `;
}

function renderRadio() {
  const status = state.data.status || {};
  const device = state.data.device || {};
  const localNode = device?.local_node || status?.local_node || {};
  const urls = device?.urls || {};

  return `
    <section class="split-grid">
      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Identity</h3>
            <p>Update the local node's visible owner names.</p>
          </div>
        </div>
        <form data-action="save-owner" class="field-grid">
          <div class="field">
            <label for="owner-long-name">Long Name</label>
            <input id="owner-long-name" name="long_name" value="${escapeHtml(localNode.long_name || "")}" />
          </div>
          <div class="field">
            <label for="owner-short-name">Short Name</label>
            <input id="owner-short-name" name="short_name" maxlength="4" value="${escapeHtml(localNode.short_name || "")}" />
          </div>
          <div class="field">
            <label for="owner-licensed">Licensed</label>
            <select id="owner-licensed" name="is_licensed">
              <option value="false">No</option>
              <option value="true">Yes</option>
            </select>
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn" type="submit">Save Owner</button>
          </div>
        </form>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Channel URLs</h3>
            <p>Export current URLs or apply a full/add-only URL from another node.</p>
          </div>
        </div>
        <div class="meta-list">
          ${renderMetaRows([
            { label: "Primary URL", value: urls.primary || "" },
            { label: "Full URL", value: urls.all || "" },
          ])}
        </div>
        <form data-action="set-channel-url" class="field-grid">
          <div class="field">
            <label for="channel-url">Channel URL</label>
            <input id="channel-url" name="url" placeholder="https://meshtastic.org/..." />
          </div>
          <div class="field">
            <label for="channel-url-mode">Mode</label>
            <select id="channel-url-mode" name="add_only">
              <option value="false">Replace channels</option>
              <option value="true">Add new channels only</option>
            </select>
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn" type="submit">Apply URL</button>
          </div>
        </form>
      </section>
    </section>

    <section class="split-grid">
      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Fixed Position</h3>
            <p>Write a fixed latitude, longitude, and altitude to the local node.</p>
          </div>
        </div>
        <form data-action="save-position" class="field-grid">
          <div class="field">
            <label for="position-latitude">Latitude</label>
            <input id="position-latitude" name="latitude" placeholder="41.8781" />
          </div>
          <div class="field">
            <label for="position-longitude">Longitude</label>
            <input id="position-longitude" name="longitude" placeholder="-87.6298" />
          </div>
          <div class="field">
            <label for="position-altitude">Altitude</label>
            <input id="position-altitude" name="altitude" placeholder="180" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn" type="submit">Save Position</button>
          </div>
        </form>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Device Actions</h3>
            <p>Guarded actions for the local node. These can interrupt the radio.</p>
          </div>
        </div>
        <form data-action="device-action" class="field-grid">
          <div class="field">
            <label for="device-action-kind">Action</label>
            <select id="device-action-kind" name="action">
              <option value="reboot">Reboot</option>
              <option value="shutdown">Shutdown</option>
              <option value="reboot_ota">Reboot OTA</option>
              <option value="enter_dfu_mode">Enter DFU Mode</option>
              <option value="exit_simulator">Exit Simulator</option>
            </select>
          </div>
          <div class="field">
            <label for="device-action-seconds">Delay Seconds</label>
            <input id="device-action-seconds" name="seconds" value="10" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn danger-btn" type="submit">Run Action</button>
          </div>
        </form>
      </section>
    </section>

    <section class="split-grid">
      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Canned Messages</h3>
            <p>Set the radio's canned message string for supported firmware.</p>
          </div>
        </div>
        <form data-action="save-canned-message" class="field">
          <label for="canned-message-text">Message Text</label>
          <textarea id="canned-message-text" name="text" placeholder="Yes|No|On my way"></textarea>
          <button class="action-btn" type="submit">Save Canned Message</button>
        </form>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Ringtone</h3>
            <p>Update the external notification ringtone string where supported.</p>
          </div>
        </div>
        <form data-action="save-ringtone" class="field">
          <label for="ringtone-text">Ringtone</label>
          <textarea id="ringtone-text" name="text" placeholder="d=4,o=5,b=100:c,e,g"></textarea>
          <button class="action-btn" type="submit">Save Ringtone</button>
        </form>
      </section>
    </section>
  `;
}

function renderChat() {
  const messages = [...(state.data.messages || [])].reverse();
  return `
    <section class="panel">
      <div class="section-head">
        <div>
          <h3>Read-Only Message Log</h3>
          <p>This view mirrors mesh traffic seen by the bridge. It does not provide a send box.</p>
        </div>
      </div>
      <div class="message-list">
        ${messages.length ? messages.map((item) => messageCard(item)).join("") : `<div class="muted">No messages recorded yet.</div>`}
      </div>
    </section>
  `;
}

function nodeRow(node) {
  const label = node.long_name || node.short_name || node.node_id || "Unknown";
  return `
    <tr>
      <td>${escapeHtml(label)}</td>
      <td>${escapeHtml(node.node_id || "")}</td>
      <td>${escapeHtml(node.live ? "live" : "history")}</td>
      <td>${escapeHtml(node.last_heard ? formatTs(node.last_heard) : formatTs(node.last_seen))}</td>
      <td>${escapeHtml(node.sighting_count || 0)}</td>
      <td><button class="inline-btn secondary-btn" type="button" data-node-id="${escapeHtml(node.node_id || "")}">History</button></td>
    </tr>
  `;
}

function renderNodes() {
  const nodes = state.data.nodes || [];
  const selected = nodes.find((item) => item.node_id === state.selectedNodeId) || {};
  return `
    <section class="split-grid">
      <section class="table-panel">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Node ID</th>
              <th>State</th>
              <th>Last Seen</th>
              <th>Sightings</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            ${nodes.length ? nodes.map((item) => nodeRow(item)).join("") : `<tr><td colspan="6" class="muted">No nodes recorded yet.</td></tr>`}
          </tbody>
        </table>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>${escapeHtml(selected.long_name || selected.short_name || selected.node_id || "Node history")}</h3>
            <p>Past sightings, updates, and packets for the selected node.</p>
          </div>
        </div>
        <div class="node-history-list">
          ${
            state.nodeHistory.length
              ? state.nodeHistory
                  .map(
                    (item) => `
                      <article class="history-item">
                        <div class="history-meta">
                          <span>${escapeHtml(item.event_type || "event")}</span>
                          <span>${escapeHtml(formatTs(item.timestamp))}</span>
                        </div>
                        <div class="code-block">${escapeHtml(prettyJson(item.payload || {}))}</div>
                      </article>
                    `
                  )
                  .join("")
              : `<div class="muted">Select a node to load its history.</div>`
          }
        </div>
      </section>
    </section>
  `;
}

function renderChannels() {
  const channels = state.data.channels || [];
  const urls = state.data.device?.urls || {};
  return `
    <section class="panel">
      <div class="section-head">
        <div>
          <h3>Channel Summary</h3>
          <p>Export URLs and edit individual channel protobuf records as JSON.</p>
        </div>
      </div>
      <div class="meta-list">
        ${renderMetaRows([
          { label: "Primary URL", value: urls.primary || "" },
          { label: "Full URL", value: urls.all || "" },
        ])}
      </div>
    </section>

    <section class="channel-grid">
      ${
        channels.length
          ? channels
              .map(
                (channel) => `
                  <article class="channel-card">
                    <div class="section-head">
                      <div>
                        <h3>${escapeHtml(channel.name || `Channel ${channel.index}`)}</h3>
                        <p>${escapeHtml(channel.role || "UNKNOWN")} · index ${escapeHtml(channel.index)}</p>
                      </div>
                    </div>
                    <form data-action="save-channel-json" data-index="${escapeHtml(channel.index)}" class="field">
                      <label for="channel-json-${escapeHtml(channel.index)}">Channel JSON</label>
                      <textarea id="channel-json-${escapeHtml(channel.index)}" name="channel_json">${escapeHtml(prettyJson(channel.raw || {}))}</textarea>
                      <div class="action-row">
                        <button class="action-btn" type="submit">Save Channel</button>
                        ${
                          Number(channel.index) > 0
                            ? `<button class="action-btn danger-btn" type="button" data-action-click="delete-channel" data-index="${escapeHtml(channel.index)}">Delete Secondary Channel</button>`
                            : ``
                        }
                      </div>
                    </form>
                  </article>
                `
              )
              .join("")
          : `<div class="muted">No channel details are available yet.</div>`
      }
    </section>
  `;
}

function renderConfigs() {
  const config = state.data.config || {};
  const localSections = Object.entries(config.local || {});
  const moduleSections = Object.entries(config.module || {});
  return `
    <section class="config-grid">
      <article class="config-card">
        <div class="section-head">
          <div>
            <h3>Local Config Sections</h3>
            <p>Save only after reviewing the full JSON for the section you are editing.</p>
          </div>
        </div>
        ${
          localSections.length
            ? localSections
                .map(
                  ([section, value]) => `
                    <form data-action="save-config-section" data-scope="local" data-section="${escapeHtml(section)}" class="field">
                      <label for="config-local-${escapeHtml(section)}">${escapeHtml(section)}</label>
                      <textarea id="config-local-${escapeHtml(section)}" name="config_json">${escapeHtml(prettyJson(value))}</textarea>
                      <button class="action-btn" type="submit">Save ${escapeHtml(section)}</button>
                    </form>
                  `
                )
                .join("")
            : `<div class="muted">No local config snapshot is available yet.</div>`
        }
      </article>

      <article class="config-card">
        <div class="section-head">
          <div>
            <h3>Module Config Sections</h3>
            <p>These mirror module-level settings like telemetry, MQTT, audio, and related features.</p>
          </div>
        </div>
        ${
          moduleSections.length
            ? moduleSections
                .map(
                  ([section, value]) => `
                    <form data-action="save-config-section" data-scope="module" data-section="${escapeHtml(section)}" class="field">
                      <label for="config-module-${escapeHtml(section)}">${escapeHtml(section)}</label>
                      <textarea id="config-module-${escapeHtml(section)}" name="config_json">${escapeHtml(prettyJson(value))}</textarea>
                      <button class="action-btn" type="submit">Save ${escapeHtml(section)}</button>
                    </form>
                  `
                )
                .join("")
            : `<div class="muted">No module config snapshot is available yet.</div>`
        }
      </article>
    </section>
  `;
}

function renderAudit() {
  const audit = state.data.audit || [];
  return `
    <section class="panel">
      <div class="section-head">
        <div>
          <h3>Admin Audit Log</h3>
          <p>Every write action from this bridge UI or API lands here.</p>
        </div>
      </div>
      <div class="audit-list">
        ${audit.length ? audit.map((item) => auditCard(item)).join("") : `<div class="muted">No admin changes have been recorded yet.</div>`}
      </div>
    </section>
  `;
}

function renderSettings() {
  return `
    <section class="split-grid">
      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Console Auth</h3>
            <p>If the bridge has an API token, store it locally here so the browser can call the admin endpoints.</p>
          </div>
        </div>
        <form data-action="save-token" class="field-grid">
          <div class="field">
            <label for="api-token">Bearer Token</label>
            <input id="api-token" name="token" value="${escapeHtml(state.token)}" />
          </div>
          <div class="field">
            <label for="window-hours">Stats Window (hours)</label>
            <input id="window-hours" name="window_hours" value="${escapeHtml(state.windowHours)}" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn" type="submit">Save Local Settings</button>
          </div>
        </form>
      </section>

      <section class="panel">
        <div class="section-head">
          <div>
            <h3>Session</h3>
            <p>Quick bridge console details for this browser session.</p>
          </div>
        </div>
        <div class="meta-list">
          ${renderMetaRows([
            { label: "API Base", value: API_BASE },
            { label: "Last Refresh", value: state.lastLoadedAt ? formatTs(state.lastLoadedAt) : "Not loaded yet" },
            { label: "Current View", value: state.view },
            { label: "Selected Node", value: state.selectedNodeId || "" },
          ])}
        </div>
      </section>
    </section>
  `;
}

function renderSidebarStatus() {
  const root = document.getElementById("sidebar-status");
  if (!root) {
    return;
  }
  const status = state.data.status || {};
  root.innerHTML = `
    ${statusPill(status.connected, status.reconnect_state)}
    <div class="helper" style="margin-top: 8px;">${escapeHtml(status.device_name || "Meshtastic")} · ${escapeHtml(formatTs(status.last_seen))}</div>
  `;
}

function renderView() {
  const root = document.getElementById("view-root");
  if (!root) {
    return;
  }
  if (state.view === "dashboard") {
    root.innerHTML = renderDashboard();
    return;
  }
  if (state.view === "radio") {
    root.innerHTML = renderRadio();
    return;
  }
  if (state.view === "chat") {
    root.innerHTML = renderChat();
    return;
  }
  if (state.view === "nodes") {
    root.innerHTML = renderNodes();
    return;
  }
  if (state.view === "channels") {
    root.innerHTML = renderChannels();
    return;
  }
  if (state.view === "configs") {
    root.innerHTML = renderConfigs();
    return;
  }
  if (state.view === "audit") {
    root.innerHTML = renderAudit();
    return;
  }
  root.innerHTML = renderSettings();
}

function renderHeader() {
  const meta = VIEW_META[state.view] || VIEW_META.dashboard;
  const title = document.getElementById("view-title");
  const subtitle = document.getElementById("view-subtitle");
  if (title) {
    title.textContent = meta.title;
  }
  if (subtitle) {
    subtitle.textContent = meta.subtitle;
  }
  document.querySelectorAll(".nav-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === state.view);
  });
}

function render() {
  renderHeader();
  renderSidebarStatus();
  renderNotice();
  renderView();
}

async function handleFormSubmit(form) {
  const action = String(form?.dataset?.action || "").trim();
  if (!action) {
    return;
  }

  try {
    if (action === "save-owner") {
      const payload = {
        long_name: form.long_name.value.trim() || null,
        short_name: form.short_name.value.trim() || null,
        is_licensed: form.is_licensed.value === "true",
      };
      await apiFetch("/device/owner", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice("Owner settings updated.");
      await loadBootstrap();
      return;
    }

    if (action === "set-channel-url") {
      const payload = {
        url: form.url.value.trim(),
        add_only: form.add_only.value === "true",
      };
      await apiFetch("/device/channel-url", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice("Channel URL applied.");
      await loadBootstrap();
      return;
    }

    if (action === "save-position") {
      const payload = {
        latitude: Number.parseFloat(form.latitude.value),
        longitude: Number.parseFloat(form.longitude.value),
        altitude: Number.parseInt(form.altitude.value || "0", 10) || 0,
      };
      await apiFetch("/device/fixed-position", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice("Fixed position saved.");
      await loadBootstrap();
      return;
    }

    if (action === "device-action") {
      const kind = form.action.value;
      const payload = { seconds: Number.parseInt(form.seconds.value || "10", 10) || 0 };
      const confirmed = window.confirm(`Run '${kind}' on the radio? This can interrupt the device.`);
      if (!confirmed) {
        return;
      }
      await apiFetch(`/device/action/${encodeURIComponent(kind)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice(`Device action '${kind}' sent.`);
      await loadBootstrap();
      return;
    }

    if (action === "save-canned-message" || action === "save-ringtone") {
      const endpoint = action === "save-canned-message" ? "/device/canned-message" : "/device/ringtone";
      await apiFetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: form.text.value }),
      });
      setNotice(action === "save-canned-message" ? "Canned message saved." : "Ringtone saved.");
      await loadBootstrap();
      return;
    }

    if (action === "save-channel-json") {
      const index = String(form.dataset.index || "").trim();
      const channel = JSON.parse(form.channel_json.value);
      await apiFetch(`/channels/${encodeURIComponent(index)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ channel }),
      });
      setNotice(`Channel ${index} updated.`);
      await loadBootstrap();
      return;
    }

    if (action === "save-config-section") {
      const scope = String(form.dataset.scope || "").trim();
      const section = String(form.dataset.section || "").trim();
      const values = JSON.parse(form.config_json.value);
      await apiFetch(`/config/${encodeURIComponent(scope)}/${encodeURIComponent(section)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ values }),
      });
      setNotice(`${scope}.${section} saved.`);
      await loadBootstrap();
      return;
    }

    if (action === "save-token") {
      state.token = form.token.value.trim();
      state.windowHours = Number.parseInt(form.window_hours.value || "24", 10) || 24;
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      safeStorageSet("tater_meshtastic_bridge_window_hours", state.windowHours);
      setNotice("Local console settings saved.");
      await loadBootstrap();
      return;
    }
  } catch (error) {
    setNotice(error.message || "Request failed.", "error");
  }
}

async function handleActionClick(button) {
  const action = String(button?.dataset?.actionClick || "").trim();
  if (!action) {
    return;
  }
  try {
    if (action === "delete-channel") {
      const index = String(button.dataset.index || "").trim();
      const confirmed = window.confirm(`Delete secondary channel ${index}?`);
      if (!confirmed) {
        return;
      }
      await apiFetch(`/channels/${encodeURIComponent(index)}`, { method: "DELETE" });
      setNotice(`Channel ${index} deleted.`);
      await loadBootstrap();
      return;
    }

    if (button.dataset.nodeId) {
      await loadNodeHistory(button.dataset.nodeId);
      render();
    }
  } catch (error) {
    setNotice(error.message || "Action failed.", "error");
  }
}

function installEventHandlers() {
  document.querySelectorAll(".nav-btn").forEach((button) => {
    button.addEventListener("click", async () => {
      state.view = button.dataset.view || "dashboard";
      render();
    });
  });

  const refreshButton = document.getElementById("refresh-btn");
  if (refreshButton) {
    refreshButton.addEventListener("click", async () => {
      await loadBootstrap();
    });
  }

  const root = document.getElementById("view-root");
  if (root) {
    root.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = event.target instanceof HTMLFormElement ? event.target : null;
      if (!form) {
        return;
      }
      await handleFormSubmit(form);
    });

    root.addEventListener("click", async (event) => {
      const target = event.target instanceof HTMLElement ? event.target.closest("button") : null;
      if (!target) {
        return;
      }
      if (target.dataset.actionClick || target.dataset.nodeId) {
        await handleActionClick(target);
      }
    });
  }
}

function startPolling() {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
  }
  state.pollTimer = window.setInterval(async () => {
    await loadBootstrap();
  }, 10000);
}

async function boot() {
  installEventHandlers();
  render();
  await loadBootstrap();
  startPolling();
}

window.addEventListener("DOMContentLoaded", () => {
  boot().catch((error) => {
    setNotice(error.message || "Failed to start bridge console.", "error");
  });
});
