const VIEW_META = {
  dashboard: { title: "Dashboard", subtitle: "Live status, stats, and recent mesh activity." },
  radio: { title: "Radio", subtitle: "Owner details, URLs, canned text, position, and device actions." },
  chat: { title: "Chat", subtitle: "Meshtastic traffic plus a manual broadcast bar for quick radio messages." },
  nodes: { title: "Nodes", subtitle: "Current mesh nodes, direct messages, past nodes, and sighting history." },
  channels: { title: "Channels", subtitle: "Inspect channel state, import/export URLs, and edit channel records." },
  firmware: { title: "Firmware", subtitle: "Detect the radio, download firmware, and run guarded update actions." },
  configs: { title: "Configs", subtitle: "Browse and edit local and module config sections on the radio." },
  audit: { title: "Audit", subtitle: "Track bridge-side configuration changes and admin actions." },
  settings: { title: "Settings", subtitle: "Bridge console auth, refresh behavior, and local preferences." },
};

const state = {
  view: "dashboard",
  token: safeStorageGet("tater_meshtastic_bridge_token", ""),
  windowHours: Number.parseInt(safeStorageGet("tater_meshtastic_bridge_window_hours", "24"), 10) || 24,
  chatChannel: safeStorageGet("tater_meshtastic_bridge_chat_channel", ""),
  selectedChannelIndex: safeStorageGet("tater_meshtastic_bridge_selected_channel", ""),
  configScope: safeStorageGet("tater_meshtastic_bridge_config_scope", "local"),
  configSections: {
    local: safeStorageGet("tater_meshtastic_bridge_config_section_local", ""),
    module: safeStorageGet("tater_meshtastic_bridge_config_section_module", ""),
  },
  data: {
    status: {},
    stats: {},
    device: {},
    settings: {},
    config: {},
    channels: [],
    nodes: [],
    messages: [],
    audit: [],
  },
  channelShare: {
    loading: false,
    loaded: false,
    error: "",
    shares: [],
  },
  selectedNodeId: "",
  nodeHistory: [],
  nodeModal: {
    open: false,
    nodeId: "",
    nodeNum: 0,
    entity: {},
    history: [],
    loading: false,
    error: "",
  },
  notice: null,
  pollTimer: 0,
  loading: false,
  lastLoadedAt: "",
  lastBootstrapPollAt: 0,
  latestMessageEventId: 0,
  formDirty: false,
  configDrafts: {},
  configDirty: {},
  bleScan: {
    loading: false,
    devices: null,
    scannedAt: "",
    error: "",
  },
  firmware: {
    status: null,
    releases: [],
    files: [],
    loadingReleases: false,
    loadingFiles: false,
    includePrerelease: safeStorageGet("tater_meshtastic_bridge_firmware_prerelease", "false") === "true",
    error: "",
  },
};

const ROOT_PATH = (() => {
  const raw = String(window.location.pathname || "/ui").replace(/\/+$/, "");
  return raw || "/ui";
})();
const API_BASE = `${ROOT_PATH}/api`;
const MAX_CHAT_MESSAGES = 500;
const LORA_REGION_FALLBACK_OPTIONS = [
  "UNSET",
  "US",
  "EU_433",
  "EU_868",
  "CN",
  "JP",
  "ANZ",
  "KR",
  "TW",
  "RU",
  "IN",
  "NZ_865",
  "TH",
  "LORA_24",
  "UA_433",
  "UA_868",
  "MY_433",
  "MY_919",
  "SG_923",
  "PH_433",
  "PH_868",
  "PH_915",
  "ANZ_433",
  "KZ_433",
  "KZ_863",
  "NP_865",
  "BR_902",
];
const LORA_MODEM_PRESET_FALLBACK_OPTIONS = [
  "LONG_FAST",
  "LONG_SLOW",
  "VERY_LONG_SLOW",
  "MEDIUM_SLOW",
  "MEDIUM_FAST",
  "SHORT_SLOW",
  "SHORT_FAST",
  "LONG_MODERATE",
  "SHORT_TURBO",
  "LONG_TURBO",
];
const nodeMapState = {
  map: null,
  markers: [],
  container: null,
};

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

function deepClone(value, fallback = {}) {
  try {
    return JSON.parse(JSON.stringify(value ?? fallback));
  } catch {
    return fallback;
  }
}

function labelizeName(value) {
  return String(value || "")
    .trim()
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function configDraftKey(scope, section) {
  return `${String(scope || "").trim()}:${String(section || "").trim()}`;
}

function getConfigDraft(scope, section, fallback = {}) {
  const key = configDraftKey(scope, section);
  if (!Object.prototype.hasOwnProperty.call(state.configDrafts, key)) {
    state.configDrafts[key] = deepClone(fallback, {});
  }
  return state.configDrafts[key];
}

function syncConfigDrafts(config) {
  const seen = new Set();
  ["local", "module"].forEach((scope) => {
    Object.entries(config?.[scope] || {}).forEach(([section, value]) => {
      const key = configDraftKey(scope, section);
      seen.add(key);
      if (!state.configDirty[key]) {
        state.configDrafts[key] = deepClone(value, {});
      }
    });
  });
  (state.data.channels || []).forEach((channel) => {
    const section = String(channel?.index ?? "").trim();
    if (!section) {
      return;
    }
    const key = configDraftKey("channel", section);
    seen.add(key);
    if (!state.configDirty[key]) {
      state.configDrafts[key] = deepClone(channel?.raw || {}, {});
    }
  });
  Object.keys(state.configDrafts).forEach((key) => {
    if (!seen.has(key) && !state.configDirty[key]) {
      delete state.configDrafts[key];
      delete state.configDirty[key];
    }
  });
}

function getChannelBySection(section) {
  return (state.data.channels || []).find((channel) => String(channel?.index ?? "") === String(section ?? "")) || null;
}

function getConfigSchema(scope, section) {
  if (scope === "channel") {
    return getChannelBySection(section)?.schema || null;
  }
  return state.data.config?.schemas?.[scope]?.[section] || null;
}

function getConfigSourceValue(scope, section) {
  if (scope === "channel") {
    return getChannelBySection(section)?.raw || {};
  }
  return state.data.config?.[scope]?.[section] || {};
}

function getConfigFieldSchema(scope, section, fieldName) {
  const schema = getConfigSchema(scope, section);
  return (schema?.fields || []).find((field) => field.name === fieldName) || null;
}

function enumOptionValues(scope, section, fieldName, fallbackValues = []) {
  const field = getConfigFieldSchema(scope, section, fieldName);
  const options = Array.isArray(field?.options) && field.options.length ? field.options.map((option) => option.value) : fallbackValues;
  return options.map((value) => String(value || "").trim()).filter(Boolean);
}

function renderEnumOptions(currentValue, values) {
  return values
    .map((value) => `<option value="${escapeHtml(value)}" ${selectedAttr(currentValue, value)}>${escapeHtml(labelizeName(value))}</option>`)
    .join("");
}

function valueWithDefault(source, fieldName, fallback) {
  if (source && Object.prototype.hasOwnProperty.call(source, fieldName)) {
    return source[fieldName];
  }
  return fallback;
}

function intListFromText(value) {
  return String(value || "")
    .split(/[\s,]+/)
    .map((part) => Number.parseInt(part, 10))
    .filter((part) => Number.isFinite(part));
}

function defaultValueForSchema(schema) {
  if (!schema || typeof schema !== "object") {
    return "";
  }
  if (schema.kind === "message") {
    const out = {};
    (schema.fields || []).forEach((field) => {
      out[field.name] = defaultValueForSchema(field);
    });
    return out;
  }
  if (schema.kind === "array") {
    return [];
  }
  if (schema.kind === "map") {
    return {};
  }
  if (schema.kind === "bool") {
    return Boolean(schema.default);
  }
  if (schema.kind === "int" || schema.kind === "float") {
    return Number(schema.default || 0);
  }
  if (schema.kind === "enum") {
    return schema.default || schema.options?.[0]?.value || "";
  }
  return schema.default ?? "";
}

function getValueAtPath(root, path, fallback = undefined) {
  let cursor = root;
  for (const segment of path || []) {
    if (cursor === undefined || cursor === null) {
      return fallback;
    }
    cursor = cursor[segment];
  }
  return cursor === undefined ? fallback : cursor;
}

function ensureContainer(nextSegment) {
  return typeof nextSegment === "number" ? [] : {};
}

function setValueAtPath(root, path, value) {
  if (!Array.isArray(path) || !path.length) {
    return;
  }
  let cursor = root;
  for (let index = 0; index < path.length - 1; index += 1) {
    const segment = path[index];
    const nextSegment = path[index + 1];
    if (cursor[segment] === undefined || cursor[segment] === null) {
      cursor[segment] = ensureContainer(nextSegment);
    }
    cursor = cursor[segment];
  }
  cursor[path[path.length - 1]] = value;
}

function removeArrayItem(root, fieldPath, index) {
  const arrayValue = getValueAtPath(root, fieldPath, []);
  if (!Array.isArray(arrayValue)) {
    return;
  }
  arrayValue.splice(index, 1);
}

function resolveSchemaNode(schema, path) {
  let node = schema;
  for (const segment of path || []) {
    if (!node || typeof node !== "object") {
      return null;
    }
    if (node.kind === "message") {
      node = (node.fields || []).find((field) => field.name === segment) || null;
      continue;
    }
    if (node.kind === "array") {
      node = node.item_schema || null;
      continue;
    }
    if (node.kind === "map") {
      node = node.value_schema || null;
      continue;
    }
    return null;
  }
  return node;
}

function configPathAttr(path) {
  return escapeHtml(JSON.stringify(path || []));
}

function configDomId(scope, section, path) {
  const token = [scope, section, ...(path || [])]
    .map((part) => String(part))
    .join("-")
    .replaceAll(/[^a-zA-Z0-9_-]+/g, "_");
  return `config-${token}`;
}

function castInputValue(rawValue, schema) {
  if (!schema || typeof schema !== "object") {
    return rawValue;
  }
  if (schema.kind === "bool") {
    return String(rawValue) === "true";
  }
  if (schema.kind === "int") {
    const parsed = Number.parseInt(String(rawValue || "").trim(), 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (schema.kind === "float") {
    const parsed = Number.parseFloat(String(rawValue || "").trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return String(rawValue ?? "");
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

function withAuthHeaders(headers = {}, authToken = undefined) {
  const out = { Accept: "application/json", ...headers };
  const token = String(authToken === undefined ? state.token || "" : authToken || "").trim();
  if (token) {
    out.Authorization = `Bearer ${token}`;
  }
  return out;
}

async function apiFetch(path, options = {}) {
  const { authToken, ...fetchOptions } = options || {};
  const response = await fetch(`${API_BASE}${path}`, {
    ...fetchOptions,
    headers: withAuthHeaders(fetchOptions.headers || {}, authToken),
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    let detail = payload && payload.detail ? payload.detail : `Request failed (${response.status})`;
    if (response.status === 401 && String(detail).toLowerCase().includes("token")) {
      detail = "Bridge API token required. In Settings, save the current Bridge API Token in Browser Access Token, then try again.";
    }
    throw new Error(String(detail));
  }
  return payload || {};
}

function hasUnsavedConfigDrafts() {
  return Object.values(state.configDirty || {}).some(Boolean);
}

function isEditableElement(element) {
  return Boolean(
    element &&
      (element instanceof HTMLInputElement ||
        element instanceof HTMLSelectElement ||
        element instanceof HTMLTextAreaElement ||
        element.isContentEditable)
  );
}

function shouldPauseAutoRefresh() {
  if (state.loading || state.nodeModal.open || state.formDirty || hasUnsavedConfigDrafts()) {
    return true;
  }
  return isEditableElement(document.activeElement);
}

function markGenericFormDirty(element) {
  if (!isEditableElement(element) || element.dataset.configInput === "true" || element.dataset.configMapKey === "true") {
    return;
  }
  const form = element.closest("form");
  if (form?.dataset?.action) {
    state.formDirty = true;
  }
}

async function loadBootstrap({ force = true } = {}) {
  if (!force && shouldPauseAutoRefresh()) {
    return;
  }
  if (force) {
    state.formDirty = false;
  }
  state.loading = true;
  try {
    const payload = await apiFetch(`/bootstrap?window_hours=${encodeURIComponent(state.windowHours)}`);
    state.data = {
      status: payload.status || {},
      stats: payload.stats || {},
      device: payload.device || {},
      settings: payload.settings || {},
      config: payload.config || {},
      channels: Array.isArray(payload.channels) ? payload.channels : [],
      nodes: Array.isArray(payload.nodes) ? payload.nodes : [],
      messages: Array.isArray(payload.messages) ? payload.messages : [],
      audit: Array.isArray(payload.audit) ? payload.audit : [],
    };
    syncConfigDrafts(state.data.config || {});
    state.lastLoadedAt = new Date().toISOString();
    state.lastBootstrapPollAt = Date.now();
    state.latestMessageEventId = latestMessageEventId(state.data.messages);
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
    if (force && state.view === "chat") {
      stickCurrentChatToBottom();
    }
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

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) {
      return text;
    }
  }
  return "";
}

function hasObjectKeys(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length);
}

function nodeIdFromEntity(entity) {
  return firstNonEmpty(entity?.node_id, entity?.id, entity?.user?.id);
}

function nodeNumFromEntity(entity) {
  const raw = firstNonEmpty(entity?.num, entity?.node_num, entity?.nodeNum);
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) ? value : 0;
}

function normalizedLocalNode() {
  const local = state.data.device?.local_node || state.data.status?.local_node || {};
  return {
    ...local,
    node_id: firstNonEmpty(local.node_id, "^local"),
    live: Boolean(state.data.status?.connected),
    last_seen: state.data.status?.last_seen || local.last_seen || "",
  };
}

function nodeMatchesIdentity(node, identity) {
  if (!node || !identity) {
    return false;
  }
  const nodeId = nodeIdFromEntity(node).toLowerCase();
  const identityId = nodeIdFromEntity(identity).toLowerCase();
  if (nodeId && identityId && nodeId === identityId) {
    return true;
  }
  const nodeNum = nodeNumFromEntity(node);
  const identityNum = nodeNumFromEntity(identity);
  return Boolean(nodeNum && identityNum && nodeNum === identityNum);
}

function findKnownNode(identity) {
  const entity = typeof identity === "string" ? { node_id: identity } : identity || {};
  const nodes = state.data.nodes || [];
  const known = nodes.find((node) => nodeMatchesIdentity(node, entity));
  if (known) {
    return known;
  }
  const local = normalizedLocalNode();
  return nodeMatchesIdentity(local, entity) || nodeIdFromEntity(entity) === "^local" ? local : null;
}

function nodeDisplayName(entity, fallback = "Unknown") {
  const known = findKnownNode(entity) || {};
  return (
    firstNonEmpty(
      known.long_name,
      known.short_name,
      entity?.long_name,
      entity?.longName,
      entity?.short_name,
      entity?.shortName,
      nodeIdFromEntity(entity),
      fallback
    ) || "Unknown"
  );
}

function nodeSubtitle(entity) {
  const known = findKnownNode(entity) || {};
  const nodeId = firstNonEmpty(known.node_id, nodeIdFromEntity(entity));
  const shortName = firstNonEmpty(known.short_name, entity?.short_name, entity?.shortName);
  const bits = [shortName, nodeId].filter(Boolean);
  return bits.length ? bits.join(" · ") : "Node details";
}

function messageDomKey(message) {
  return firstNonEmpty(
    message?.event_id,
    message?.message_id,
    `${message?.timestamp || ""}:${message?.direction || ""}:${message?.channel ?? ""}:${String(message?.text || "").slice(0, 40)}`
  );
}

function findMessageByDomKey(key) {
  const token = String(key || "").trim();
  if (!token) {
    return null;
  }
  return (state.data.messages || []).find((message) => messageDomKey(message) === token) || null;
}

function nodeEntityForMessage(message, side) {
  const token = String(side || "from").trim();
  if (token === "to") {
    return message?.to || {};
  }
  return message?.from || {};
}

function messageCard(message) {
  const fromName = nodeDisplayName(message?.from);
  const toName = nodeDisplayName(message?.to);
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

function channelKey(value) {
  const text = String(value ?? "").trim();
  return text === "" ? "0" : text;
}

function messageChannelKey(message) {
  return channelKey(message?.channel ?? 0);
}

function channelName(channel) {
  const index = channelKey(channel?.index ?? 0);
  return String(channel?.name || channel?.raw?.settings?.name || "").trim() || `Channel ${index}`;
}

function channelRowsForChat(messages) {
  const counts = new Map();
  const channelMap = new Map();

  (state.data.channels || []).forEach((channel) => {
    const key = channelKey(channel?.index ?? 0);
    channelMap.set(key, {
      key,
      index: Number.parseInt(key, 10),
      name: channelName(channel),
      role: String(channel?.role || "").trim(),
      count: 0,
    });
  });

  messages.forEach((message) => {
    const key = messageChannelKey(message);
    counts.set(key, (counts.get(key) || 0) + 1);
    if (!channelMap.has(key)) {
      channelMap.set(key, {
        key,
        index: Number.parseInt(key, 10),
        name: `Channel ${key}`,
        role: "",
        count: 0,
      });
    }
  });

  const rows = Array.from(channelMap.values()).map((row) => ({
    ...row,
    count: counts.get(row.key) || 0,
  }));

  rows.sort((a, b) => {
    const aNum = Number.isFinite(a.index) ? a.index : 9999;
    const bNum = Number.isFinite(b.index) ? b.index : 9999;
    return aNum - bNum || String(a.name).localeCompare(String(b.name));
  });

  return rows;
}

function activeChatChannel(rows, messages) {
  const savedRaw = String(state.chatChannel || "").trim();
  const saved = savedRaw ? channelKey(savedRaw) : "";
  if (saved && rows.some((row) => row.key === saved)) {
    return saved;
  }
  const sortedMessages = [...messages].sort(compareMessages);
  const latestMessage = sortedMessages[sortedMessages.length - 1];
  const fallback = latestMessage ? messageChannelKey(latestMessage) : rows[0]?.key || "0";
  state.chatChannel = fallback;
  safeStorageSet("tater_meshtastic_bridge_chat_channel", fallback);
  return fallback;
}

function compareMessages(a, b) {
  const aId = Number(a?.event_id ?? a?.id ?? 0);
  const bId = Number(b?.event_id ?? b?.id ?? 0);
  if (aId || bId) {
    return aId - bId;
  }
  const aTs = Date.parse(String(a?.timestamp || ""));
  const bTs = Date.parse(String(b?.timestamp || ""));
  return (Number.isNaN(aTs) ? 0 : aTs) - (Number.isNaN(bTs) ? 0 : bTs);
}

function messageEventId(message) {
  const value = Number(message?.event_id ?? message?.id ?? 0);
  return Number.isFinite(value) ? value : 0;
}

function latestMessageEventId(messages) {
  return (messages || []).reduce((latest, message) => Math.max(latest, messageEventId(message)), 0);
}

function mergeMessages(messages) {
  const byKey = new Map();
  [...(state.data.messages || []), ...(messages || [])].forEach((message) => {
    const key = messageDomKey(message);
    if (key) {
      byKey.set(key, message);
    }
  });
  const merged = Array.from(byKey.values()).sort(compareMessages);
  state.data.messages = merged.slice(Math.max(0, merged.length - MAX_CHAT_MESSAGES));
  state.latestMessageEventId = Math.max(state.latestMessageEventId || 0, latestMessageEventId(state.data.messages));
}

function chatLogElement() {
  return document.querySelector(".mesh-chat-log");
}

function chatIsNearBottom(element) {
  if (!element) {
    return true;
  }
  return element.scrollHeight - element.scrollTop - element.clientHeight < 80;
}

function stickCurrentChatToBottom() {
  const scrollToBottom = () => {
    const element = chatLogElement();
    if (element) {
      element.scrollTop = element.scrollHeight;
    }
  };
  scrollToBottom();
  window.requestAnimationFrame(scrollToBottom);
  window.setTimeout(scrollToBottom, 0);
  window.setTimeout(scrollToBottom, 140);
}

function renderChatWithScroll({ forceBottom = false } = {}) {
  const currentLog = chatLogElement();
  const shouldStick = forceBottom || chatIsNearBottom(currentLog);
  const bottomOffset = currentLog ? currentLog.scrollHeight - currentLog.scrollTop : 0;
  renderView();
  renderNodeModal();
  const nextLog = chatLogElement();
  if (!nextLog) {
    return;
  }
  if (shouldStick) {
    stickCurrentChatToBottom();
    return;
  }
  nextLog.scrollTop = Math.max(0, nextLog.scrollHeight - bottomOffset);
}

function handleChatComposerKeydown(event) {
  const target = event.target instanceof HTMLTextAreaElement ? event.target : null;
  if (!target || !["mesh-chat-input", "node-direct-message-input"].includes(target.id)) {
    return;
  }
  if (event.key !== "Enter" || event.shiftKey || event.metaKey || event.ctrlKey || event.altKey || event.isComposing) {
    return;
  }
  event.preventDefault();
  target.closest("form")?.requestSubmit();
}

async function pollChatMessages() {
  const sinceId = state.latestMessageEventId || latestMessageEventId(state.data.messages);
  try {
    const payload = await apiFetch(`/messages?since_id=${encodeURIComponent(sinceId)}&limit=200`);
    const messages = Array.isArray(payload.messages) ? payload.messages : [];
    if (!messages.length) {
      return;
    }
    mergeMessages(messages);
    if (state.view === "chat") {
      renderChatWithScroll();
    }
  } catch (error) {
    setNotice(error.message || "Failed to load new chat messages.", "error");
    renderNotice();
  }
}

function initialsForName(name) {
  const text = String(name || "").trim();
  if (!text) {
    return "?";
  }
  const parts = text.split(/\s+/).filter(Boolean);
  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }
  return `${parts[0][0] || ""}${parts[1][0] || ""}`.toUpperCase();
}

function renderMeshChatMessage(message) {
  const direction = String(message?.direction || "inbound").trim().toLowerCase();
  const roleClass = direction === "outbound" ? "assistant" : "user";
  const fromName = nodeDisplayName(message?.from);
  const toName = nodeDisplayName(message?.to);
  const activeEntity = message?.from || {};
  const displayName = nodeDisplayName(activeEntity, roleClass === "assistant" ? "Tater" : "Unknown");
  const body = String(message?.text || "").trim();
  const meta = `${direction || "message"} · ${fromName} -> ${toName} · ${formatTs(message?.timestamp)}`;
  const nodeId = nodeIdFromEntity(activeEntity);
  const nodeNum = nodeNumFromEntity(activeEntity);
  const clickAttrs = `
    data-action-click="open-chat-node"
    data-message-key="${escapeHtml(messageDomKey(message))}"
    data-node-side="from"
    data-node-id="${escapeHtml(nodeId)}"
    data-node-num="${escapeHtml(nodeNum || "")}"
  `;
  const avatarHtml = `
    <button class="chat-avatar node-link" type="button" ${clickAttrs} aria-label="View ${escapeHtml(displayName)} node details">
      <div class="chat-avatar-fallback ${roleClass === "user" ? "user" : ""}">${escapeHtml(initialsForName(displayName))}</div>
    </button>
  `;
  const bubbleHtml = `
    <div class="bubble ${escapeHtml(roleClass)}">
      <button class="role node-name-button" type="button" ${clickAttrs}>${escapeHtml(displayName)}</button>
      <div class="bubble-body ${body ? "" : "empty"}">${body ? escapeHtml(body) : "No text payload on this packet."}</div>
      <div class="mesh-chat-message-meta">${escapeHtml(meta)}</div>
    </div>
  `;

  return `
    <article class="chat-row ${escapeHtml(roleClass)}">
      ${roleClass === "user" ? `${bubbleHtml}${avatarHtml}` : `${avatarHtml}${bubbleHtml}`}
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

function renderLoRaSettingsPanel() {
  const lora = state.data.config?.local?.lora || {};
  const regionOptions = enumOptionValues("local", "lora", "region", LORA_REGION_FALLBACK_OPTIONS);
  const presetOptions = enumOptionValues("local", "lora", "modem_preset", LORA_MODEM_PRESET_FALLBACK_OPTIONS);
  const loraValue = (fieldName, fallback) => valueWithDefault(lora, fieldName, fallback);
  const boolOptions = (value) => `
    <option value="true" ${selectedAttr(value, true)}>True</option>
    <option value="false" ${selectedAttr(value, false)}>False</option>
  `;

  return `
    <section class="panel lora-settings-panel">
      <div class="section-head">
        <div>
          <h3>LoRa Radio Settings</h3>
          <p>Friendly controls for region, range preset, hops, frequency slot, TX, RX boost, and advanced modem tuning.</p>
        </div>
        <span class="chip">Saves to local.lora</span>
      </div>
      <form data-action="save-lora-settings" class="lora-settings-form">
        <div class="lora-settings-groups">
          <section class="lora-settings-group">
            <h4>Region & Range</h4>
            <div class="field-grid">
              <div class="field">
                <label for="lora-region">Region</label>
                <select id="lora-region" name="region">
                  ${renderEnumOptions(firstNonEmpty(loraValue("region", "UNSET"), "UNSET"), regionOptions)}
                </select>
                <div class="helper">Choose the legal radio region for where this node is operating.</div>
              </div>
              <div class="field">
                <label for="lora-use-preset">Use Preset</label>
                <select id="lora-use-preset" name="use_preset">
                  ${boolOptions(loraValue("use_preset", true))}
                </select>
                <div class="helper">True uses the preset/range mode instead of manual modem tuning.</div>
              </div>
              <div class="field">
                <label for="lora-modem-preset">Range / Modem Preset</label>
                <select id="lora-modem-preset" name="modem_preset">
                  ${renderEnumOptions(firstNonEmpty(loraValue("modem_preset", "LONG_FAST"), "LONG_FAST"), presetOptions)}
                </select>
              </div>
              <div class="field">
                <label for="lora-hop-limit">Hop Limit</label>
                <input id="lora-hop-limit" name="hop_limit" type="number" min="0" step="1" value="${escapeHtml(loraValue("hop_limit", 3))}" />
              </div>
            </div>
          </section>

          <section class="lora-settings-group">
            <h4>Frequency & Power</h4>
            <div class="field-grid">
              <div class="field">
                <label for="lora-channel-num">Frequency Slot</label>
                <input id="lora-channel-num" name="channel_num" type="number" min="0" step="1" value="${escapeHtml(loraValue("channel_num", 0))}" />
                <div class="helper">Meshtastic calls this channel_num internally.</div>
              </div>
              <div class="field">
                <label for="lora-tx-enabled">TX Enabled</label>
                <select id="lora-tx-enabled" name="tx_enabled">
                  ${boolOptions(loraValue("tx_enabled", true))}
                </select>
              </div>
              <div class="field">
                <label for="lora-tx-power">TX Power</label>
                <input id="lora-tx-power" name="tx_power" type="number" step="1" value="${escapeHtml(loraValue("tx_power", 0))}" />
                <div class="helper">0 lets firmware choose the default where supported.</div>
              </div>
              <div class="field">
                <label for="lora-rx-boost">RX Boosted Gain</label>
                <select id="lora-rx-boost" name="sx126x_rx_boosted_gain">
                  ${boolOptions(loraValue("sx126x_rx_boosted_gain", false))}
                </select>
              </div>
              <div class="field">
                <label for="lora-override-frequency">Override Frequency</label>
                <input id="lora-override-frequency" name="override_frequency" type="number" step="any" value="${escapeHtml(loraValue("override_frequency", 0))}" />
                <div class="helper">Leave 0 unless you know you need a custom frequency.</div>
              </div>
              <div class="field">
                <label for="lora-frequency-offset">Frequency Offset</label>
                <input id="lora-frequency-offset" name="frequency_offset" type="number" step="any" value="${escapeHtml(loraValue("frequency_offset", 0))}" />
              </div>
            </div>
          </section>

          <section class="lora-settings-group">
            <h4>Advanced Modem</h4>
            <div class="field-grid">
              <div class="field">
                <label for="lora-bandwidth">Bandwidth</label>
                <input id="lora-bandwidth" name="bandwidth" type="number" min="0" step="1" value="${escapeHtml(loraValue("bandwidth", 0))}" />
              </div>
              <div class="field">
                <label for="lora-spread-factor">Spread Factor</label>
                <input id="lora-spread-factor" name="spread_factor" type="number" min="0" step="1" value="${escapeHtml(loraValue("spread_factor", 0))}" />
              </div>
              <div class="field">
                <label for="lora-coding-rate">Coding Rate</label>
                <input id="lora-coding-rate" name="coding_rate" type="number" min="0" step="1" value="${escapeHtml(loraValue("coding_rate", 0))}" />
              </div>
              <div class="field">
                <label for="lora-duty-cycle">Override Duty Cycle</label>
                <select id="lora-duty-cycle" name="override_duty_cycle">
                  ${boolOptions(loraValue("override_duty_cycle", false))}
                </select>
              </div>
              <div class="field">
                <label for="lora-pa-fan-disabled">PA Fan Disabled</label>
                <select id="lora-pa-fan-disabled" name="pa_fan_disabled">
                  ${boolOptions(loraValue("pa_fan_disabled", false))}
                </select>
              </div>
            </div>
          </section>

          <section class="lora-settings-group">
            <h4>Mesh / MQTT Filters</h4>
            <div class="field-grid">
              <div class="field">
                <label for="lora-ignore-mqtt">Ignore MQTT</label>
                <select id="lora-ignore-mqtt" name="ignore_mqtt">
                  ${boolOptions(loraValue("ignore_mqtt", false))}
                </select>
              </div>
              <div class="field">
                <label for="lora-config-ok-mqtt">Config OK To MQTT</label>
                <select id="lora-config-ok-mqtt" name="config_ok_to_mqtt">
                  ${boolOptions(loraValue("config_ok_to_mqtt", false))}
                </select>
              </div>
              <div class="field lora-wide-field">
                <label for="lora-ignore-incoming">Ignore Incoming Node Numbers</label>
                <textarea id="lora-ignore-incoming" name="ignore_incoming" rows="3" placeholder="123456789, 987654321">${escapeHtml((loraValue("ignore_incoming", []) || []).join(", "))}</textarea>
                <div class="helper">Optional comma-separated numeric node IDs to ignore at the LoRa layer.</div>
              </div>
            </div>
          </section>
        </div>
        <div class="action-row">
          <button class="action-btn" type="submit">Save LoRa Settings</button>
          <span class="helper">Changing region, frequency, or modem settings can temporarily disconnect or desync nodes.</span>
        </div>
      </form>
    </section>
  `;
}

function renderRadio() {
  const status = state.data.status || {};
  const device = state.data.device || {};
  const localNode = device?.local_node || status?.local_node || {};
  const urls = device?.urls || {};
  const positionConfig = state.data.config?.local?.position || {};
  const gpsMode = normalizedGpsMode(positionConfig.gps_mode);

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

    ${renderLoRaSettingsPanel()}

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
            <h3>GPS / Position Source</h3>
            <p>Switch between the onboard GPS and a fixed/manual position override.</p>
          </div>
        </div>
        <form data-action="save-position-settings" class="field-grid">
          <div class="field">
            <label for="position-gps-mode">GPS Mode</label>
            <select id="position-gps-mode" name="gps_mode">
              <option value="DISABLED" ${selectedAttr(gpsMode, "DISABLED")}>GPS disabled</option>
              <option value="ENABLED" ${selectedAttr(gpsMode, "ENABLED")}>Use GPS</option>
              <option value="NOT_PRESENT" ${selectedAttr(gpsMode, "NOT_PRESENT")}>No GPS hardware</option>
            </select>
            <div class="helper">Use GPS means the radio can use live satellite position when supported.</div>
          </div>
          <div class="field">
            <label for="position-gps-enabled">GPS Enabled</label>
            <select id="position-gps-enabled" name="gps_enabled">
              <option value="true" ${selectedAttr(firstNonEmpty(positionConfig.gps_enabled, false), true)}>True</option>
              <option value="false" ${selectedAttr(firstNonEmpty(positionConfig.gps_enabled, false), false)}>False</option>
            </select>
          </div>
          <div class="field">
            <label for="position-fixed-position">Fixed Position Override</label>
            <select id="position-fixed-position" name="fixed_position">
              <option value="false" ${selectedAttr(firstNonEmpty(positionConfig.fixed_position, false), false)}>False</option>
              <option value="true" ${selectedAttr(firstNonEmpty(positionConfig.fixed_position, false), true)}>True</option>
            </select>
            <div class="helper">True keeps reporting the saved fixed coordinates instead of live GPS.</div>
          </div>
          <div class="field">
            <label for="position-broadcast-secs">Broadcast Every Seconds</label>
            <input id="position-broadcast-secs" name="position_broadcast_secs" type="number" min="0" step="1" value="${escapeHtml(firstNonEmpty(positionConfig.position_broadcast_secs, 0))}" />
          </div>
          <div class="field">
            <label for="position-smart-enabled">Smart Broadcast</label>
            <select id="position-smart-enabled" name="position_broadcast_smart_enabled">
              <option value="true" ${selectedAttr(firstNonEmpty(positionConfig.position_broadcast_smart_enabled, false), true)}>True</option>
              <option value="false" ${selectedAttr(firstNonEmpty(positionConfig.position_broadcast_smart_enabled, false), false)}>False</option>
            </select>
          </div>
          <div class="field">
            <label for="position-gps-update-interval">GPS Update Interval</label>
            <input id="position-gps-update-interval" name="gps_update_interval" type="number" min="0" step="1" value="${escapeHtml(firstNonEmpty(positionConfig.gps_update_interval, 0))}" />
          </div>
          <div class="field">
            <label for="position-gps-attempt-time">GPS Attempt Time</label>
            <input id="position-gps-attempt-time" name="gps_attempt_time" type="number" min="0" step="1" value="${escapeHtml(firstNonEmpty(positionConfig.gps_attempt_time, 0))}" />
          </div>
          <div class="field">
            <label for="position-smart-distance">Smart Minimum Distance</label>
            <input id="position-smart-distance" name="broadcast_smart_minimum_distance" type="number" min="0" step="1" value="${escapeHtml(firstNonEmpty(positionConfig.broadcast_smart_minimum_distance, 0))}" />
          </div>
          <div class="field">
            <label for="position-smart-interval">Smart Minimum Interval</label>
            <input id="position-smart-interval" name="broadcast_smart_minimum_interval_secs" type="number" min="0" step="1" value="${escapeHtml(firstNonEmpty(positionConfig.broadcast_smart_minimum_interval_secs, 0))}" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn" type="submit">Save GPS Settings</button>
          </div>
        </form>
        <div class="helper">
          For live GPS, use Fixed Position Override = False, GPS Enabled = True, and GPS Mode = Use GPS. For a manual location, save coordinates and set Fixed Position Override = True.
        </div>
      </section>
    </section>

    <section class="split-grid">
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
  const status = state.data.status || {};
  const allMessages = [...(state.data.messages || [])].sort(compareMessages);
  const channelRows = channelRowsForChat(allMessages);
  const activeChannel = activeChatChannel(channelRows, allMessages);
  const activeRow = channelRows.find((row) => row.key === activeChannel) || channelRows[0] || { key: "0", name: "Channel 0", count: 0 };
  const messages = allMessages.filter((message) => messageChannelKey(message) === activeRow.key);
  const connected = Boolean(status.connected);
  return `
    <section class="mesh-chat-shell">
      <aside class="mesh-channel-rail" aria-label="Meshtastic channels">
        <div class="mesh-channel-rail-head">
          <div class="mesh-channel-title">Channels</div>
          <div class="mesh-channel-count">${escapeHtml(channelRows.length)} total</div>
        </div>
        <div class="mesh-channel-list">
          ${
            channelRows.length
              ? channelRows
                  .map(
                    (channel) => `
                      <button
                        type="button"
                        class="mesh-channel-tab ${channel.key === activeRow.key ? "active" : ""}"
                        data-chat-channel="${escapeHtml(channel.key)}"
                      >
                        <span class="mesh-channel-hash">#</span>
                        <span class="mesh-channel-main">
                          <span class="mesh-channel-name">${escapeHtml(channel.name)}</span>
                          <span class="mesh-channel-sub">${escapeHtml(channel.role || `index ${channel.key}`)}</span>
                        </span>
                        <span class="mesh-channel-badge">${escapeHtml(channel.count)}</span>
                      </button>
                    `
                  )
                  .join("")
              : `<div class="muted">No channels have been seen yet.</div>`
          }
        </div>
      </aside>

      <div class="mesh-chat-panel">
        <div class="mesh-chat-head">
          <div>
            <h3># ${escapeHtml(activeRow.name)}</h3>
            <p>Mesh traffic on channel ${escapeHtml(activeRow.key)}. Use the composer below for manual sends.</p>
          </div>
          <span class="status-pill ${messages.length ? "ok" : "warn"}">${escapeHtml(messages.length)} message${messages.length === 1 ? "" : "s"}</span>
        </div>
        <div class="chat-log mesh-chat-log">
          ${messages.length ? messages.map((item) => renderMeshChatMessage(item)).join("") : `<div class="mesh-chat-empty">No messages recorded on this channel yet.</div>`}
        </div>
        <form data-action="send-chat" class="chat-composer-card mesh-chat-composer-card">
          <input type="hidden" name="channel" value="${escapeHtml(activeRow.key)}" />
          <div class="mesh-chat-send-meta">
            <span>Broadcasting on # ${escapeHtml(activeRow.name)}</span>
          </div>
          <div class="chat-composer" role="group" aria-label="Meshtastic chat composer">
            <div class="chat-composer-bar">
              <textarea
                id="mesh-chat-input"
                class="chat-composer-input"
                name="text"
                rows="1"
                maxlength="200"
                placeholder="${connected ? `Message # ${escapeHtml(activeRow.name)}...` : "Radio disconnected"}"
                ${connected ? "" : "disabled"}
              ></textarea>
              <button type="submit" class="chat-composer-send" title="Send mesh message" aria-label="Send mesh message" ${connected ? "" : "disabled"}>
                <span class="chat-composer-icon chat-composer-send-arrow" aria-hidden="true">Send</span>
              </button>
            </div>
            <div class="mesh-chat-composer-help">
              Keep it short for mesh. Enter sends, Shift+Enter adds a line. Direct messages live on the Nodes tab.
            </div>
          </div>
        </form>
      </div>
    </section>
  `;
}

function renderNodeJsonSection(title, value, open = false) {
  if (!hasObjectKeys(value) && !Array.isArray(value)) {
    return "";
  }
  return `
    <details class="node-json-section" ${open ? "open" : ""}>
      <summary>${escapeHtml(title)}</summary>
      <div class="code-block">${escapeHtml(prettyJson(value))}</div>
    </details>
  `;
}

function renderNodeModalHistory(modal) {
  if (modal.loading) {
    return `<div class="muted">Loading recent node history...</div>`;
  }
  if (modal.error) {
    return `<div class="notice error">${escapeHtml(modal.error)}</div>`;
  }
  const history = Array.isArray(modal.history) ? modal.history : [];
  if (!history.length) {
    return `<div class="muted">No stored history for this node yet.</div>`;
  }
  return history
    .slice(0, 12)
    .map(
      (item) => `
        <article class="node-modal-history-item">
          <div class="history-meta">
            <span>${escapeHtml(item.event_type || "event")}</span>
            <span>${escapeHtml(formatTs(item.timestamp))}</span>
          </div>
          <div class="node-modal-history-title">
            ${escapeHtml(firstNonEmpty(item.long_name, item.short_name, item.payload?.text, "Node sighting"))}
          </div>
          ${renderNodeJsonSection("Payload", item.payload || {}, false)}
        </article>
      `
    )
    .join("");
}

function renderNodeModal() {
  const root = document.getElementById("modal-root");
  if (!root) {
    return;
  }
  const modal = state.nodeModal || {};
  if (!modal.open) {
    root.innerHTML = "";
    return;
  }

  const identity = {
    ...(modal.entity || {}),
    node_id: firstNonEmpty(modal.nodeId, modal.entity?.node_id),
    num: modal.nodeNum || nodeNumFromEntity(modal.entity),
  };
  const known = findKnownNode(identity) || {};
  const packetEntity = modal.entity || {};
  const title = nodeDisplayName(identity);
  const subtitle = nodeSubtitle(identity);
  const nodeId = firstNonEmpty(known.node_id, nodeIdFromEntity(identity));
  const nodeNum = nodeNumFromEntity(known) || nodeNumFromEntity(identity);
  const position = known.position || known.raw?.position || {};
  const lastPayload = known.last_payload || {};
  const rawNode = known.raw || {};
  const stateLabel = known.live ? "Live now" : known.node_id ? "Stored node" : "Packet only";

  root.innerHTML = `
    <div class="node-modal-backdrop" data-modal-close="node">
      <section class="node-modal-card" role="dialog" aria-modal="true" aria-labelledby="node-modal-title">
        <div class="node-modal-head">
          <div class="node-modal-title-wrap">
            <div class="chat-avatar node-modal-avatar">
              <div class="chat-avatar-fallback">${escapeHtml(initialsForName(title))}</div>
            </div>
            <div>
              <h3 id="node-modal-title">${escapeHtml(title)}</h3>
              <p>${escapeHtml(subtitle)}</p>
            </div>
          </div>
          <div class="node-modal-actions">
            <span class="status-pill ${known.live ? "ok" : "warn"}">${escapeHtml(stateLabel)}</span>
            <button class="inline-btn secondary-btn" type="button" data-action-click="close-node-modal">Close</button>
          </div>
        </div>

        <div class="node-modal-body">
          <section class="node-modal-section">
            <h4>Node Info</h4>
            <div class="meta-list">
              ${renderMetaRows([
                { label: "Long Name", value: firstNonEmpty(known.long_name, packetEntity.long_name, packetEntity.longName) },
                { label: "Short Name", value: firstNonEmpty(known.short_name, packetEntity.short_name, packetEntity.shortName) },
                { label: "Node ID", value: nodeId },
                { label: "Numeric ID", value: nodeNum || "" },
                { label: "State", value: stateLabel },
                { label: "First Seen", value: known.first_seen ? formatTs(known.first_seen) : "" },
                { label: "Last Seen", value: firstNonEmpty(known.last_heard, known.last_seen) ? formatTs(firstNonEmpty(known.last_heard, known.last_seen)) : "" },
                { label: "Sightings", value: known.sighting_count || "" },
                { label: "Last Event", value: known.last_event_type || "" },
                { label: "SNR", value: known.snr || "" },
                { label: "Hops Away", value: known.hops_away ?? "" },
              ])}
            </div>
          </section>

          <section class="node-modal-section">
            <h4>Position</h4>
            <div class="meta-list">
              ${renderMetaRows([
                { label: "Latitude", value: firstNonEmpty(position.latitude, position.lat) },
                { label: "Longitude", value: firstNonEmpty(position.longitude, position.lon, position.lng) },
                { label: "Altitude", value: firstNonEmpty(position.altitude, position.altitudeI, position.alt) },
                { label: "Time", value: firstNonEmpty(position.time, position.timestamp) ? formatTs(firstNonEmpty(position.time, position.timestamp)) : "" },
              ]) || `<div class="muted">No position data stored for this node.</div>`}
            </div>
          </section>

          <section class="node-modal-section node-modal-wide">
            <h4>Raw Details</h4>
            ${renderNodeJsonSection("Clicked Packet Node", packetEntity, true)}
            ${renderNodeJsonSection("Known Node Record", known, false)}
            ${renderNodeJsonSection("Last Payload", lastPayload, false)}
            ${renderNodeJsonSection("Live Raw Node", rawNode, false)}
          </section>

          <section class="node-modal-section node-modal-wide">
            <h4>Recent History</h4>
            <div class="node-modal-history-list">
              ${renderNodeModalHistory(modal)}
            </div>
          </section>
        </div>
      </section>
    </div>
  `;
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function normalizeCoordinate(value, maxAbs) {
  const parsed = firstFiniteNumber(value);
  if (parsed === null) {
    return null;
  }
  if (Math.abs(parsed) > maxAbs && Math.abs(parsed) <= maxAbs * 10000000) {
    return parsed / 10000000;
  }
  return parsed;
}

function positionFromObject(position, fallbackTimestamp = "") {
  if (!position || typeof position !== "object") {
    return null;
  }
  const lat = normalizeCoordinate(
    firstNonEmpty(position.latitude, position.lat, position.latitudeI, position.latitude_i, position.latitudeE7, position.latitude_e7),
    90
  );
  const lon = normalizeCoordinate(
    firstNonEmpty(position.longitude, position.lon, position.lng, position.longitudeI, position.longitude_i, position.longitudeE7, position.longitude_e7),
    180
  );
  if (lat === null || lon === null || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return null;
  }
  const altitude = firstFiniteNumber(position.altitude, position.altitudeI, position.alt);
  return {
    lat,
    lon,
    altitude,
    timestamp: firstNonEmpty(position.timestamp, position.time, fallbackTimestamp),
  };
}

function payloadPosition(payload, fallbackTimestamp = "") {
  const candidates = [
    payload?.position,
    payload?.raw?.decoded?.position,
    payload?.raw?.position,
    payload?.decoded?.position,
  ];
  for (const candidate of candidates) {
    const position = positionFromObject(candidate, fallbackTimestamp || payload?.timestamp);
    if (position) {
      return position;
    }
  }
  return null;
}

function nodePosition(node, history = []) {
  const candidates = [
    { position: positionFromObject(node?.position, firstNonEmpty(node?.last_heard, node?.last_seen)), source: "Live node position" },
    { position: payloadPosition(node?.last_payload, node?.last_payload?.timestamp), source: "Last packet position" },
    ...history.map((item) => ({
      position: payloadPosition(item?.payload, item?.timestamp),
      source: `${labelizeName(item?.event_type || "history")} history`,
    })),
  ];
  const match = candidates.find((item) => item.position);
  return match ? { ...match.position, source: match.source } : null;
}

function formatCoordinate(value) {
  return Number.isFinite(value) ? value.toFixed(5) : "";
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function mapUrlForPosition(position) {
  if (!position) {
    return "";
  }
  return `https://www.openstreetmap.org/?mlat=${encodeURIComponent(position.lat)}&mlon=${encodeURIComponent(position.lon)}#map=16/${encodeURIComponent(position.lat)}/${encodeURIComponent(position.lon)}`;
}

function positionedNodeRows(nodes, selected) {
  const selectedId = String(selected?.node_id || "");
  const rows = nodes
    .map((node) => ({ node, position: nodePosition(node) }))
    .filter((row) => row.position);
  const selectedPosition = nodePosition(selected, state.nodeHistory);
  if (selectedId && selectedPosition && !rows.some((row) => String(row.node.node_id || "") === selectedId)) {
    rows.push({ node: selected, position: selectedPosition });
  }
  return rows;
}

function mapViewForPositions(rows, selectedPosition) {
  if (!rows.length && !selectedPosition) {
    return { centerLat: 0, centerLon: 0, latSpan: 1, lonSpan: 1 };
  }
  if (selectedPosition) {
    const maxLatDelta = rows.reduce((max, row) => Math.max(max, Math.abs(row.position.lat - selectedPosition.lat)), 0);
    const maxLonDelta = rows.reduce((max, row) => Math.max(max, Math.abs(row.position.lon - selectedPosition.lon)), 0);
    return {
      centerLat: selectedPosition.lat,
      centerLon: selectedPosition.lon,
      latSpan: Math.max(0.02, maxLatDelta * 2.4),
      lonSpan: Math.max(0.02, maxLonDelta * 2.4),
    };
  }
  const lats = rows.map((row) => row.position.lat);
  const lons = rows.map((row) => row.position.lon);
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const minLon = Math.min(...lons);
  const maxLon = Math.max(...lons);
  return {
    centerLat: (minLat + maxLat) / 2,
    centerLon: (minLon + maxLon) / 2,
    latSpan: Math.max(0.02, (maxLat - minLat) * 1.4),
    lonSpan: Math.max(0.02, (maxLon - minLon) * 1.4),
  };
}

function projectMapPosition(position, view) {
  const x = 50 + ((position.lon - view.centerLon) / view.lonSpan) * 100;
  const y = 50 - ((position.lat - view.centerLat) / view.latSpan) * 100;
  return {
    x: Math.min(96, Math.max(4, x)),
    y: Math.min(96, Math.max(4, y)),
  };
}

function nodeStatusClass(node) {
  return node?.live ? "live" : "history";
}

function renderNodeListItem(node, selectedId) {
  const nodeId = String(node?.node_id || "");
  const position = nodePosition(node);
  const isSelected = nodeId && nodeId === selectedId;
  return `
    <button
      type="button"
      class="node-list-item ${isSelected ? "active" : ""}"
      data-node-id="${escapeHtml(nodeId)}"
    >
      <span class="node-status-dot ${escapeHtml(nodeStatusClass(node))}"></span>
      <span class="node-list-main">
        <span class="node-list-name">${escapeHtml(nodeDisplayName(node))}</span>
        <span class="node-list-sub">${escapeHtml(firstNonEmpty(node.short_name, nodeId, "Unknown node"))}</span>
      </span>
      <span class="node-list-meta">
        <span>${escapeHtml(node.live ? "Live" : "Past")}</span>
        <span>${position ? "Mapped" : "No GPS"}</span>
      </span>
    </button>
  `;
}

function renderNodeMap(rows, selected) {
  const selectedPosition = nodePosition(selected, state.nodeHistory);
  const view = mapViewForPositions(rows, selectedPosition);
  const selectedId = String(selected?.node_id || "");
  return `
    <div class="node-map-stage" id="node-map-stage" role="region" aria-label="Last known node locations">
      <div id="node-leaflet-map" class="node-leaflet-map"></div>
      <div class="node-map-grid" aria-hidden="true"></div>
      <div class="node-map-compass">N</div>
      <div class="node-map-center">
        ${escapeHtml(formatCoordinate(view.centerLat))}, ${escapeHtml(formatCoordinate(view.centerLon))}
      </div>
      <div class="node-map-fallback">
        ${
          rows.length
            ? rows
                .map((row) => {
                  const projected = projectMapPosition(row.position, view);
                  const nodeId = String(row.node.node_id || "");
                  const selectedClass = nodeId && nodeId === selectedId ? "selected" : "";
                  return `
                    <button
                      type="button"
                      class="node-map-marker ${escapeHtml(selectedClass)} ${escapeHtml(nodeStatusClass(row.node))}"
                      style="left: ${escapeHtml(projected.x)}%; top: ${escapeHtml(projected.y)}%;"
                      data-node-id="${escapeHtml(nodeId)}"
                      title="${escapeHtml(nodeDisplayName(row.node))}"
                    >
                      <span>${escapeHtml(initialsForName(nodeDisplayName(row.node)))}</span>
                    </button>
                  `;
                })
                .join("")
            : `<div class="node-map-empty">No node locations have been received yet.</div>`
        }
      </div>
    </div>
  `;
}

function renderSelectedNodeInfo(selected) {
  if (!selected?.node_id) {
    return `<div class="muted">Select a node to view details.</div>`;
  }
  const status = state.data.status || {};
  const position = nodePosition(selected, state.nodeHistory);
  const mapUrl = mapUrlForPosition(position);
  const connected = Boolean(status.connected);
  const channels = sortedChannels(state.data.channels || []);
  const defaultChannel = channelKey(state.chatChannel || channels[0]?.index || 0);
  const channelOptions = channels.length
    ? channels
        .map((channel) => {
          const key = channelKey(channel.index ?? 0);
          return `<option value="${escapeHtml(key)}" ${selectedAttr(defaultChannel, key)}># ${escapeHtml(channelName(channel))}</option>`;
        })
        .join("")
    : `<option value="0"># Channel 0</option>`;
  return `
    <article class="node-detail-card">
      <div class="section-head">
        <div>
          <h3>${escapeHtml(nodeDisplayName(selected))}</h3>
          <p>${escapeHtml(nodeSubtitle(selected))}</p>
        </div>
        <span class="status-pill ${selected.live ? "ok" : "warn"}">${escapeHtml(selected.live ? "Live now" : "Past node")}</span>
      </div>
      <div class="meta-list">
        ${renderMetaRows([
          { label: "Node ID", value: selected.node_id || "" },
          { label: "Numeric ID", value: selected.num || "" },
          { label: "First Seen", value: selected.first_seen ? formatTs(selected.first_seen) : "" },
          { label: "Last Seen", value: firstNonEmpty(selected.last_heard, selected.last_seen) ? formatTs(firstNonEmpty(selected.last_heard, selected.last_seen)) : "" },
          { label: "Sightings", value: selected.sighting_count || "" },
          { label: "SNR", value: selected.snr || "" },
          { label: "Hops Away", value: selected.hops_away ?? "" },
          { label: "Location Source", value: position?.source || "" },
          { label: "Latitude", value: position ? formatCoordinate(position.lat) : "" },
          { label: "Longitude", value: position ? formatCoordinate(position.lon) : "" },
          { label: "Altitude", value: position?.altitude ?? "" },
          { label: "Position Time", value: position?.timestamp ? formatTs(position.timestamp) : "" },
          { label: "Open Map", value: mapUrl ? `<a href="${escapeHtml(mapUrl)}" target="_blank" rel="noopener">Open in OpenStreetMap</a>` : "", html: true },
        ]) || `<div class="muted">No details are available for this node yet.</div>`}
      </div>
    </article>
    <article class="node-direct-message-card">
      <div class="section-head compact-head">
        <div>
          <h3>Direct Message</h3>
          <p>Send a short manual message straight to ${escapeHtml(nodeDisplayName(selected))}.</p>
        </div>
        <span class="status-pill ${connected ? "ok" : "warn"}">${escapeHtml(connected ? "Radio connected" : "Radio disconnected")}</span>
      </div>
      <form data-action="send-node-message" class="node-direct-message-form">
        <input type="hidden" name="destination" value="${escapeHtml(selected.node_id || "")}" />
        <div class="mesh-chat-send-meta">
          <span>Destination: ${escapeHtml(selected.node_id || "")}</span>
          <label>
            Channel
            <select name="channel" ${connected ? "" : "disabled"}>
              ${channelOptions}
            </select>
          </label>
        </div>
        <div class="chat-composer" role="group" aria-label="Direct Meshtastic message composer">
          <div class="chat-composer-bar">
            <textarea
              id="node-direct-message-input"
              class="chat-composer-input"
              name="text"
              rows="1"
              maxlength="200"
              placeholder="${connected ? `Message ${escapeHtml(nodeDisplayName(selected))}...` : "Radio disconnected"}"
              ${connected ? "" : "disabled"}
            ></textarea>
            <button type="submit" class="chat-composer-send" title="Send direct message" aria-label="Send direct message" ${connected ? "" : "disabled"}>
              <span class="chat-composer-icon chat-composer-send-arrow" aria-hidden="true">Send</span>
            </button>
          </div>
          <div class="mesh-chat-composer-help">Direct messages still ride over the selected Meshtastic channel. Keep them short.</div>
        </div>
      </form>
    </article>
  `;
}

function historySummary(item) {
  const payload = item?.payload || {};
  const position = payloadPosition(payload, item?.timestamp);
  return [
    payload.text ? `Text: ${payload.text}` : "",
    payload.channel !== undefined ? `Channel ${payload.channel}` : "",
    payload.delivery ? `Delivery: ${payload.delivery}` : "",
    payload.portnum ? `Port: ${payload.portnum}` : "",
    position ? `Location: ${formatCoordinate(position.lat)}, ${formatCoordinate(position.lon)}` : "",
  ].filter(Boolean);
}

function renderNodeHistoryItem(item) {
  const summary = historySummary(item);
  return `
    <article class="node-event-card">
      <div class="history-meta">
        <span>${escapeHtml(labelizeName(item?.event_type || "event"))}</span>
        <span>${escapeHtml(formatTs(item?.timestamp))}</span>
      </div>
      <div class="node-event-title">${escapeHtml(firstNonEmpty(item?.long_name, item?.short_name, item?.payload?.from?.long_name, item?.payload?.text, "Node update"))}</div>
      ${
        summary.length
          ? `<div class="chip-row">${summary.map((bit) => `<span class="chip">${escapeHtml(bit)}</span>`).join("")}</div>`
          : `<p class="muted">No extra details on this event.</p>`
      }
    </article>
  `;
}

function renderNodes() {
  const nodes = state.data.nodes || [];
  const selected = nodes.find((item) => item.node_id === state.selectedNodeId) || nodes[0] || {};
  const selectedId = String(selected?.node_id || "");
  const locatedRows = positionedNodeRows(nodes, selected);
  const liveCount = nodes.filter((node) => node.live).length;
  return `
    <section class="nodes-workspace">
      <aside class="node-list-panel">
        <div class="section-head">
          <div>
            <h3>Mesh Nodes</h3>
            <p>${escapeHtml(nodes.length)} known · ${escapeHtml(liveCount)} live · ${escapeHtml(locatedRows.length)} mapped</p>
          </div>
        </div>
        <div class="node-list">
          ${nodes.length ? nodes.map((node) => renderNodeListItem(node, selectedId)).join("") : `<div class="muted">No nodes recorded yet.</div>`}
        </div>
      </aside>

      <section class="node-map-panel">
        <div class="section-head">
          <div>
            <h3>Live Node Map</h3>
            <p>Click a node in the list or map to center on its last known location.</p>
          </div>
          <span class="status-pill ${locatedRows.length ? "ok" : "warn"}">${escapeHtml(locatedRows.length)} located</span>
        </div>
        ${renderNodeMap(locatedRows, selected)}
        ${renderSelectedNodeInfo(selected)}
        <section class="node-history-panel">
          <div class="section-head">
            <div>
              <h3>Recent Activity</h3>
              <p>Readable events for the selected node. Raw packet JSON is hidden here to keep this view useful.</p>
            </div>
          </div>
          <div class="node-history-list compact">
            ${state.nodeHistory.length ? state.nodeHistory.map((item) => renderNodeHistoryItem(item)).join("") : `<div class="muted">Select a node to load its history.</div>`}
          </div>
        </section>
      </section>
    </section>
  `;
}

function destroyNodeMap() {
  if (nodeMapState.map) {
    nodeMapState.map.remove();
  }
  nodeMapState.map = null;
  nodeMapState.markers = [];
  nodeMapState.container = null;
}

function nodeMapPopupHtml(row) {
  const position = row.position;
  const node = row.node || {};
  return `
    <div class="node-map-popup">
      <strong>${escapeHtml(nodeDisplayName(node))}</strong>
      <span>${escapeHtml(firstNonEmpty(node.short_name, node.node_id, "Unknown node"))}</span>
      <span>${escapeHtml(node.live ? "Live now" : "Past node")} · ${escapeHtml(formatCoordinate(position.lat))}, ${escapeHtml(formatCoordinate(position.lon))}</span>
      ${position.altitude !== null && position.altitude !== undefined ? `<span>Altitude ${escapeHtml(position.altitude)}m</span>` : ""}
    </div>
  `;
}

function nodeLeafletIcon(node, selected) {
  const L = window.L;
  const classes = ["node-leaflet-marker", nodeStatusClass(node)];
  if (selected) {
    classes.push("selected");
  }
  return L.divIcon({
    className: "",
    html: `<div class="${escapeHtml(classes.join(" "))}"><span>${escapeHtml(initialsForName(nodeDisplayName(node)))}</span></div>`,
    iconSize: selected ? [48, 48] : [38, 38],
    iconAnchor: selected ? [24, 24] : [19, 19],
    popupAnchor: [0, selected ? -24 : -19],
  });
}

async function selectNodeFromMap(nodeId) {
  const token = String(nodeId || "").trim();
  if (!token) {
    return;
  }
  state.selectedNodeId = token;
  await loadNodeHistory(token);
  render();
}

function syncRealNodeMap() {
  const stage = document.getElementById("node-map-stage");
  const container = document.getElementById("node-leaflet-map");
  if (state.view !== "nodes" || !stage || !container) {
    destroyNodeMap();
    return;
  }

  if (!window.L) {
    stage.classList.remove("leaflet-ready");
    stage.classList.add("leaflet-unavailable");
    return;
  }

  const nodes = state.data.nodes || [];
  const selected = nodes.find((item) => item.node_id === state.selectedNodeId) || nodes[0] || {};
  const selectedId = String(selected?.node_id || "");
  const rows = positionedNodeRows(nodes, selected);
  const selectedPosition = nodePosition(selected, state.nodeHistory);

  if (nodeMapState.container !== container) {
    destroyNodeMap();
  }
  if (!nodeMapState.map) {
    nodeMapState.map = window.L.map(container, {
      zoomControl: true,
      attributionControl: true,
      scrollWheelZoom: true,
    });
    window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(nodeMapState.map);
    nodeMapState.container = container;
  }

  nodeMapState.markers.forEach((marker) => marker.remove());
  nodeMapState.markers = [];
  rows.forEach((row) => {
    const nodeId = String(row.node.node_id || "");
    const isSelected = nodeId && nodeId === selectedId;
    const marker = window.L.marker([row.position.lat, row.position.lon], {
      icon: nodeLeafletIcon(row.node, isSelected),
      title: nodeDisplayName(row.node),
    });
    marker.bindPopup(nodeMapPopupHtml(row));
    marker.on("click", () => {
      if (nodeId && nodeId !== state.selectedNodeId) {
        selectNodeFromMap(nodeId).catch((error) => setNotice(error.message || "Failed to select node.", "error"));
      }
    });
    marker.addTo(nodeMapState.map);
    nodeMapState.markers.push(marker);
  });

  if (selectedPosition) {
    nodeMapState.map.setView([selectedPosition.lat, selectedPosition.lon], Math.max(nodeMapState.map.getZoom() || 0, 13), { animate: true });
  } else if (rows.length) {
    const bounds = window.L.latLngBounds(rows.map((row) => [row.position.lat, row.position.lon]));
    nodeMapState.map.fitBounds(bounds, { padding: [42, 42], maxZoom: 13, animate: true });
  } else {
    nodeMapState.map.setView([0, 0], 2);
  }

  stage.classList.add("leaflet-ready");
  stage.classList.remove("leaflet-unavailable");
  window.requestAnimationFrame(() => {
    if (nodeMapState.map) {
      nodeMapState.map.invalidateSize();
    }
  });
}

function sortedChannels(channels) {
  return [...(channels || [])].sort((a, b) => {
    const aIndex = Number.parseInt(String(a?.index ?? "0"), 10);
    const bIndex = Number.parseInt(String(b?.index ?? "0"), 10);
    return aIndex - bIndex || channelName(a).localeCompare(channelName(b));
  });
}

function activeChannel(channels) {
  const saved = String(state.selectedChannelIndex || "").trim();
  const active = channels.find((channel) => String(channel?.index ?? "") === saved) || channels[0] || null;
  if (active) {
    state.selectedChannelIndex = String(active.index ?? "");
    safeStorageSet("tater_meshtastic_bridge_selected_channel", state.selectedChannelIndex);
  }
  return active;
}

function channelDirty(channel) {
  return Boolean(state.configDirty[configDraftKey("channel", String(channel?.index ?? ""))]);
}

function renderChannelTabs(channels, activeIndex) {
  return `
    <aside class="channel-rail" aria-label="Meshtastic channel list">
      <div class="channel-rail-head">
        <div>
          <h3>Channels</h3>
          <p>${escapeHtml(channels.length)} configured</p>
        </div>
      </div>
      <div class="channel-tab-list">
        ${
          channels.length
            ? channels
                .map((channel) => {
                  const index = String(channel?.index ?? "");
                  const dirty = channelDirty(channel);
                  return `
                    <button
                      type="button"
                      class="channel-tab ${index === activeIndex ? "active" : ""}"
                      data-channel-tab="${escapeHtml(index)}"
                    >
                      <span class="channel-tab-index">${escapeHtml(index || "?")}</span>
                      <span class="channel-tab-main">
                        <span class="channel-tab-name">${escapeHtml(channelName(channel))}</span>
                        <span class="channel-tab-sub">${escapeHtml(channel?.role || "UNKNOWN")}${dirty ? " · unsaved" : ""}</span>
                      </span>
                    </button>
                  `;
                })
                .join("")
            : `<div class="muted">No channel details are available yet.</div>`
        }
      </div>
    </aside>
  `;
}

function renderChannelEditor(channel) {
  if (!channel) {
    return `<article class="panel"><div class="muted">No channel details are available yet.</div></article>`;
  }
  const section = String(channel.index);
  const schema = getConfigSchema("channel", section);
  const draft = getConfigDraft("channel", section, channel.raw || {});
  const dirty = channelDirty(channel);
  return `
    <article class="channel-editor-card">
      <form data-action="save-config-section" data-scope="channel" data-section="${escapeHtml(section)}" class="config-section-form">
        <div class="section-head">
          <div>
            <h3>${escapeHtml(channelName(channel))}</h3>
            <p>${escapeHtml(channel.role || "UNKNOWN")} · channel index ${escapeHtml(section)}</p>
          </div>
          <div class="chip-row">
            <span class="chip ${dirty ? "chip-warn" : ""}">${escapeHtml(dirty ? "Unsaved changes" : "Saved")}</span>
            ${Number(channel.index) === 0 ? `<span class="chip">Primary</span>` : `<span class="chip">Secondary</span>`}
          </div>
        </div>
        <div class="channel-info-strip">
          <div>
            <span>Name</span>
            <strong>${escapeHtml(channelName(channel))}</strong>
          </div>
          <div>
            <span>Role</span>
            <strong>${escapeHtml(channel.role || "UNKNOWN")}</strong>
          </div>
          <div>
            <span>Index</span>
            <strong>${escapeHtml(section)}</strong>
          </div>
        </div>
        ${
          schema?.kind === "message"
            ? renderConfigMessageFields("channel", section, schema, [], draft)
            : `
                <div class="field">
                  <label for="channel-json-${escapeHtml(section)}">Channel JSON</label>
                  <textarea id="channel-json-${escapeHtml(section)}" name="config_json">${escapeHtml(prettyJson(draft || {}))}</textarea>
                </div>
              `
        }
        <details class="config-advanced">
          <summary>Advanced JSON preview</summary>
          <div class="code-block">${escapeHtml(prettyJson(draft || {}))}</div>
        </details>
        <div class="action-row">
          <button
            class="inline-btn secondary-btn"
            type="button"
            data-action-click="reset-config-section"
            data-scope="channel"
            data-section="${escapeHtml(section)}"
          >
            Reset Changes
          </button>
          <button class="action-btn" type="submit">Save Channel</button>
          ${
            Number(channel.index) > 0
              ? `<button class="action-btn danger-btn" type="button" data-action-click="delete-channel" data-index="${escapeHtml(section)}">Delete Secondary Channel</button>`
              : ``
          }
        </div>
      </form>
    </article>
  `;
}

function renderChannelSharePanel() {
  const shareState = state.channelShare || {};
  const shares = Array.isArray(shareState.shares) ? shareState.shares : [];
  const body = shareState.error
    ? `<div class="notice error">${escapeHtml(shareState.error)}</div>`
    : shares.length
      ? `
          <div class="channel-share-grid">
            ${shares
              .map((share) => {
                const url = String(share.url || "").trim();
                return `
                  <article class="channel-share-card">
                    <div class="channel-share-qr" aria-label="${escapeHtml(share.label || "Channel")} QR code">
                      ${share.svg || ""}
                    </div>
                    <div class="channel-share-copy">
                      <h4>${escapeHtml(share.label || "Channel QR")}</h4>
                      <p>${escapeHtml(share.description || "Scan from the Meshtastic app to import this channel information.")}</p>
                      <textarea readonly>${escapeHtml(url)}</textarea>
                    </div>
                  </article>
                `;
              })
              .join("")}
          </div>
        `
      : `<div class="muted">Generate QR codes after the bridge has a channel URL snapshot. Use Refresh from the Channels tab if the radio just connected.</div>`;
  return `
    <section class="panel channel-share-panel">
      <div class="section-head">
        <div>
          <h3>Share QR Codes</h3>
          <p>Scan these from the Meshtastic app to import this radio's channel information.</p>
        </div>
        <button class="action-btn" type="button" data-action-click="load-channel-share" ${shareState.loading ? "disabled" : ""}>
          ${shareState.loading ? "Generating..." : shareState.loaded ? "Refresh QR" : "Generate QR"}
        </button>
      </div>
      ${body}
    </section>
  `;
}

function renderChannels() {
  const channels = sortedChannels(state.data.channels || []);
  const selected = activeChannel(channels);
  const selectedIndex = String(selected?.index ?? "");
  const urls = state.data.device?.urls || {};
  const dirtyCount = channels.filter((channel) => channelDirty(channel)).length;
  return `
    <section class="channel-workspace">
      <section class="panel channel-overview-panel">
        <div class="section-head">
          <div>
            <h3>Channel Management</h3>
            <p>Pick one channel to inspect or edit. Import/export URLs stay here so the editor stays focused.</p>
          </div>
          <div class="chip-row">
            <span class="chip">${escapeHtml(channels.length)} channels</span>
            ${dirtyCount ? `<span class="chip chip-warn">${escapeHtml(dirtyCount)} unsaved</span>` : `<span class="chip">All saved</span>`}
          </div>
        </div>
        <div class="channel-url-layout">
          <div class="meta-list channel-url-meta">
            ${renderMetaRows([
              { label: "Primary URL", value: urls.primary || "" },
              { label: "Full URL", value: urls.all || "" },
            ]) || `<div class="muted">No channel URLs are available yet.</div>`}
          </div>
          <form data-action="set-channel-url" class="field-grid channel-url-form">
            <div class="field">
              <label for="channel-url">Import Channel URL</label>
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
        </div>
      </section>

      ${renderChannelSharePanel()}

      <section class="channel-browser">
        ${renderChannelTabs(channels, selectedIndex)}
        <div class="channel-detail">
          ${renderChannelEditor(selected)}
        </div>
      </section>
    </section>
  `;
}

function firmwareStatusValue(label, value) {
  return { label, value: value || "" };
}

function renderFirmwareAsset(asset, release) {
  const matched = Number(asset.match_score || 0) > 0;
  return `
    <article class="firmware-asset ${matched ? "matched" : ""}">
      <div>
        <strong>${escapeHtml(asset.name || "Firmware asset")}</strong>
        <div class="helper">
          ${escapeHtml(formatBytes(asset.size))} · ${escapeHtml(asset.download_count || 0)} downloads${matched ? " · likely match" : ""}
        </div>
      </div>
      <button
        class="inline-btn"
        type="button"
        data-action-click="firmware-download"
        data-asset-url="${escapeHtml(asset.browser_download_url || "")}"
        data-asset-name="${escapeHtml(asset.name || "")}"
        data-tag-name="${escapeHtml(release.tag_name || "")}"
      >Download</button>
    </article>
  `;
}

function renderFirmwareRelease(release) {
  const assets = Array.isArray(release.assets) ? release.assets : [];
  const matched = assets.filter((asset) => Number(asset.match_score || 0) > 0);
  const shownAssets = matched.length ? matched : assets.slice(0, 8);
  return `
    <article class="firmware-release-card">
      <div class="section-head compact-head">
        <div>
          <h4>${escapeHtml(release.name || release.tag_name || "Firmware Release")}</h4>
          <p>${escapeHtml(release.tag_name || "")} · ${escapeHtml(formatTs(release.published_at))}${release.prerelease ? " · prerelease" : ""}</p>
        </div>
        ${release.html_url ? `<a class="inline-btn" href="${escapeHtml(release.html_url)}" target="_blank" rel="noopener">Release Notes</a>` : ""}
      </div>
      <div class="firmware-asset-list">
        ${
          shownAssets.length
            ? shownAssets.map((asset) => renderFirmwareAsset(asset, release)).join("")
            : `<div class="muted">No firmware-looking assets were found on this release.</div>`
        }
      </div>
    </article>
  `;
}

function renderFirmwareFileOptions(files) {
  return files.length
    ? files.map((file) => `<option value="${escapeHtml(file.path || "")}">${escapeHtml(file.name || "firmware")} (${escapeHtml(formatBytes(file.size))})</option>`).join("")
    : `<option value="">No downloaded firmware files</option>`;
}

function renderFirmware() {
  const fw = state.firmware || {};
  const status = fw.status || {};
  const releases = Array.isArray(fw.releases) ? fw.releases : [];
  const files = Array.isArray(fw.files) ? fw.files : [];
  const candidates = Array.isArray(status.target_candidates) ? status.target_candidates : [];
  return `
    <section class="firmware-workspace">
      <section class="firmware-alpha-warning" role="alert">
        <div>
          <strong>Alpha testing warning</strong>
          <p>
            Firmware tools in the bridge are experimental and should be used at your own risk. Double-check the detected hardware, keep a known-good recovery path available, and prefer the official Meshtastic Web Flasher for anything critical.
          </p>
        </div>
      </section>

      <section class="panel firmware-status-panel">
        <div class="section-head">
          <div>
            <h3>Firmware Detection</h3>
            <p>Detect the connected radio and match release assets where possible.</p>
          </div>
          <div class="chip-row">
            <button class="inline-btn" type="button" data-action-click="firmware-refresh-status">Refresh Detection</button>
            <a class="inline-btn" href="https://flasher.meshtastic.org/" target="_blank" rel="noopener">Open Web Flasher</a>
          </div>
        </div>
        ${fw.error ? `<div class="notice error">${escapeHtml(fw.error)}</div>` : ""}
        <div class="meta-list">
          ${renderMetaRows([
            firmwareStatusValue("Connected", status.connected ? "Yes" : "No"),
            firmwareStatusValue("Transport", status.transport || ""),
            firmwareStatusValue("Firmware Version", status.firmware_version || ""),
            firmwareStatusValue("Hardware Model", status.hardware_model || ""),
            firmwareStatusValue("Device Role", status.device_role || ""),
            firmwareStatusValue("Firmware Cache", status.cache_dir || ""),
          ])}
        </div>
        <div class="chip-row firmware-candidates">
          ${
            candidates.length
              ? candidates.map((candidate) => `<span class="chip">${escapeHtml(candidate)}</span>`).join("")
              : `<span class="muted">No hardware target candidates yet. Refresh after the radio is connected.</span>`
          }
        </div>
      </section>

      <section class="panel firmware-release-panel">
        <div class="section-head">
          <div>
            <h3>Download Firmware</h3>
            <p>Fetch Meshtastic release assets from GitHub and download them into the bridge firmware cache.</p>
          </div>
          <div class="chip-row">
            <button class="inline-btn secondary-btn" type="button" data-action-click="firmware-prerelease-toggle">
              ${fw.includePrerelease ? "Showing prereleases" : "Stable only"}
            </button>
            <button class="action-btn" type="button" data-action-click="firmware-load-releases" ${fw.loadingReleases ? "disabled" : ""}>
              ${fw.loadingReleases ? "Loading..." : "Load Releases"}
            </button>
          </div>
        </div>
        <div class="firmware-release-list">
          ${
            releases.length
              ? releases.map((release) => renderFirmwareRelease(release)).join("")
              : `<div class="muted">Load releases to see downloadable firmware assets. Matching assets are ranked first when the radio model is detected.</div>`
          }
        </div>
      </section>

      <section class="panel firmware-files-panel">
        <div class="section-head">
          <div>
            <h3>Flash / Update</h3>
            <p>Use downloaded firmware files for supported ESP32 WiFi/TCP OTA updates, or prep the radio for USB/DFU flashing.</p>
          </div>
          <button class="inline-btn" type="button" data-action-click="firmware-refresh-files" ${fw.loadingFiles ? "disabled" : ""}>
            ${fw.loadingFiles ? "Refreshing..." : "Refresh Files"}
          </button>
        </div>
        <div class="firmware-warning">
          OTA update here uses the Meshtastic CLI and requires ESP32 firmware support plus a reachable WiFi/TCP host. For full erase, bootloader recovery, nRF52, RP2040, or USB-only boards, use the official Web Flasher or drag-and-drop flow.
        </div>
        <form data-action="firmware-ota-update" class="field-grid firmware-ota-form">
          <div class="field">
            <label for="firmware-file-path">Downloaded Firmware</label>
            <select id="firmware-file-path" name="file_path">
              ${renderFirmwareFileOptions(files)}
            </select>
          </div>
          <div class="field">
            <label for="firmware-tcp-host">Meshtastic TCP Host</label>
            <input id="firmware-tcp-host" name="tcp_host" placeholder="10.4.20.210 or radio.local" />
          </div>
          <div class="field">
            <label for="firmware-timeout">Timeout Seconds</label>
            <input id="firmware-timeout" name="timeout_seconds" type="number" min="60" max="1800" step="30" value="600" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <button class="action-btn danger-btn" type="submit" ${files.length ? "" : "disabled"}>Start OTA Update</button>
          </div>
        </form>
        <div class="firmware-file-list">
          ${
            files.length
              ? files.map((file) => `<div class="firmware-file-row"><span>${escapeHtml(file.name)}</span><span>${escapeHtml(file.tag_name || "")}</span><span>${escapeHtml(formatBytes(file.size))}</span></div>`).join("")
              : `<div class="muted">No firmware files downloaded yet.</div>`
          }
        </div>
        <div class="action-row firmware-prep-actions">
          <button class="inline-btn secondary-btn" type="button" data-action-click="firmware-device-action" data-device-action="enter_dfu_mode">Enter DFU Mode</button>
          <button class="inline-btn secondary-btn" type="button" data-action-click="firmware-device-action" data-device-action="reboot_ota">Reboot OTA</button>
        </div>
      </section>
    </section>
  `;
}

function renderConfigInputControl(scope, section, schema, path, value, label, compact = false) {
  const id = configDomId(scope, section, path);
  const kind = String(schema?.kind || "string");
  const currentValue = value ?? defaultValueForSchema(schema);
  const helperBits = [];
  if (kind === "enum" && Array.isArray(schema?.options) && schema.options.length) {
    helperBits.push(`${schema.options.length} available options`);
  } else if (kind === "bool") {
    helperBits.push("Select true or false");
  } else if (kind === "int") {
    helperBits.push("Whole number");
  } else if (kind === "float") {
    helperBits.push("Decimal number");
  }
  const helper = helperBits.length ? `<div class="helper">${escapeHtml(helperBits.join(" · "))}</div>` : "";
  const fieldClass = compact ? "field config-field compact" : "field config-field";

  if (kind === "bool") {
    return `
      <div class="${fieldClass}">
        <label for="${escapeHtml(id)}">${escapeHtml(label)}</label>
        <select
          id="${escapeHtml(id)}"
          data-config-input="true"
          data-scope="${escapeHtml(scope)}"
          data-section="${escapeHtml(section)}"
          data-config-path="${configPathAttr(path)}"
        >
          <option value="true" ${currentValue === true ? "selected" : ""}>True</option>
          <option value="false" ${currentValue === false ? "selected" : ""}>False</option>
        </select>
        ${helper}
      </div>
    `;
  }

  if (kind === "enum") {
    return `
      <div class="${fieldClass}">
        <label for="${escapeHtml(id)}">${escapeHtml(label)}</label>
        <select
          id="${escapeHtml(id)}"
          data-config-input="true"
          data-scope="${escapeHtml(scope)}"
          data-section="${escapeHtml(section)}"
          data-config-path="${configPathAttr(path)}"
        >
          ${(schema.options || [])
            .map(
              (option) =>
                `<option value="${escapeHtml(option.value)}" ${String(currentValue || "") === String(option.value) ? "selected" : ""}>${escapeHtml(option.label || option.value)}</option>`
            )
            .join("")}
        </select>
        ${helper}
      </div>
    `;
  }

  const inputType = kind === "int" || kind === "float" ? "number" : "text";
  const step = kind === "float" ? "any" : "1";
  return `
    <div class="${fieldClass}">
      <label for="${escapeHtml(id)}">${escapeHtml(label)}</label>
      <input
        id="${escapeHtml(id)}"
        type="${escapeHtml(inputType)}"
        step="${escapeHtml(step)}"
        value="${escapeHtml(currentValue ?? "")}"
        data-config-input="true"
        data-scope="${escapeHtml(scope)}"
        data-section="${escapeHtml(section)}"
        data-config-path="${configPathAttr(path)}"
      />
      ${helper}
    </div>
  `;
}

function renderConfigArrayField(scope, section, fieldSchema, path, value) {
  const items = Array.isArray(value) ? value : [];
  const itemSchema = fieldSchema.item_schema || { kind: "string" };
  return `
    <section class="config-group">
      <div class="config-group-head">
        <div>
          <h4>${escapeHtml(fieldSchema.label || labelizeName(fieldSchema.name))}</h4>
          <p>${escapeHtml(items.length ? `${items.length} item${items.length === 1 ? "" : "s"}` : "No entries yet.")}</p>
        </div>
        <button
          class="inline-btn secondary-btn"
          type="button"
          data-action-click="config-array-add"
          data-scope="${escapeHtml(scope)}"
          data-section="${escapeHtml(section)}"
          data-field-path="${configPathAttr(path)}"
        >
          Add Item
        </button>
      </div>
      <div class="config-array-list">
        ${
          items.length
            ? items
                .map((item, index) => {
                  const itemPath = [...path, index];
                  const body =
                    itemSchema.kind === "message"
                      ? renderConfigMessageFields(scope, section, itemSchema, itemPath, item || {})
                      : renderConfigInputControl(
                          scope,
                          section,
                          itemSchema,
                          itemPath,
                          item,
                          `${fieldSchema.label || labelizeName(fieldSchema.name)} ${index + 1}`,
                          true
                        );
                  return `
                    <article class="config-array-item">
                      <div class="config-array-item-head">
                        <div class="helper">Item ${escapeHtml(index + 1)}</div>
                        <button
                          class="inline-btn secondary-btn"
                          type="button"
                          data-action-click="config-array-remove"
                          data-scope="${escapeHtml(scope)}"
                          data-section="${escapeHtml(section)}"
                          data-field-path="${configPathAttr(path)}"
                          data-index="${escapeHtml(index)}"
                        >
                          Remove
                        </button>
                      </div>
                      ${body}
                    </article>
                  `;
                })
                .join("")
            : `<div class="muted">No entries yet.</div>`
        }
      </div>
    </section>
  `;
}

function renderConfigMapField(scope, section, fieldSchema, path, value) {
  const entries = Object.entries(value || {});
  const valueSchema = fieldSchema.value_schema || { kind: "string" };
  return `
    <section class="config-group">
      <div class="config-group-head">
        <div>
          <h4>${escapeHtml(fieldSchema.label || labelizeName(fieldSchema.name))}</h4>
          <p>${escapeHtml(entries.length ? `${entries.length} key/value entries` : "No entries yet.")}</p>
        </div>
        <button
          class="inline-btn secondary-btn"
          type="button"
          data-action-click="config-map-add"
          data-scope="${escapeHtml(scope)}"
          data-section="${escapeHtml(section)}"
          data-field-path="${configPathAttr(path)}"
        >
          Add Entry
        </button>
      </div>
      <div class="config-map-list">
        ${
          entries.length
            ? entries
                .map(([key, entryValue]) => {
                  const entryPath = [...path, key];
                  const valueMarkup =
                    valueSchema.kind === "message"
                      ? renderConfigMessageFields(scope, section, valueSchema, entryPath, entryValue || {})
                      : renderConfigInputControl(scope, section, valueSchema, entryPath, entryValue, "Value", true);
                  return `
                    <article class="config-map-item">
                      <div class="config-map-row">
                        <div class="field config-field compact">
                          <label>Key</label>
                          <input
                            value="${escapeHtml(key)}"
                            data-config-map-key="true"
                            data-scope="${escapeHtml(scope)}"
                            data-section="${escapeHtml(section)}"
                            data-field-path="${configPathAttr(path)}"
                            data-map-key="${escapeHtml(key)}"
                          />
                        </div>
                        <button
                          class="inline-btn secondary-btn"
                          type="button"
                          data-action-click="config-map-remove"
                          data-scope="${escapeHtml(scope)}"
                          data-section="${escapeHtml(section)}"
                          data-field-path="${configPathAttr(path)}"
                          data-map-key="${escapeHtml(key)}"
                        >
                          Remove
                        </button>
                      </div>
                      ${valueMarkup}
                    </article>
                  `;
                })
                .join("")
            : `<div class="muted">No entries yet.</div>`
        }
      </div>
    </section>
  `;
}

function renderConfigField(scope, section, fieldSchema, path, value) {
  if (fieldSchema.kind === "message") {
    return `
      <section class="config-group">
        <div class="config-group-head">
          <div>
            <h4>${escapeHtml(fieldSchema.label || labelizeName(fieldSchema.name))}</h4>
            <p>${escapeHtml((fieldSchema.fields || []).length)} fields</p>
          </div>
        </div>
        ${renderConfigMessageFields(scope, section, fieldSchema, path, value || {})}
      </section>
    `;
  }

  if (fieldSchema.kind === "array") {
    return renderConfigArrayField(scope, section, fieldSchema, path, value);
  }

  if (fieldSchema.kind === "map") {
    return renderConfigMapField(scope, section, fieldSchema, path, value);
  }

  return renderConfigInputControl(scope, section, fieldSchema, path, value, fieldSchema.label || labelizeName(fieldSchema.name));
}

function renderConfigMessageFields(scope, section, schema, path, values) {
  return `
    <div class="config-fields-grid">
      ${(schema.fields || [])
        .map((field) => renderConfigField(scope, section, field, [...path, field.name], getValueAtPath(values || {}, [field.name], defaultValueForSchema(field))))
        .join("")}
    </div>
  `;
}

function renderConfigSection(scope, section, value, schema) {
  const draft = getConfigDraft(scope, section, value || {});
  const dirty = Boolean(state.configDirty[configDraftKey(scope, section)]);
  const label = schema?.label || labelizeName(section);
  const summary = schema?.fields?.length ? `${schema.fields.length} available fields` : "Manual JSON fallback";

  return `
    <article class="config-card">
      <form data-action="save-config-section" data-scope="${escapeHtml(scope)}" data-section="${escapeHtml(section)}" class="config-section-form">
        <div class="section-head">
          <div>
            <h3>${escapeHtml(label)}</h3>
            <p>${escapeHtml(summary)}</p>
          </div>
          <div class="chip-row">
            <span class="chip ${dirty ? "chip-warn" : ""}">${escapeHtml(dirty ? "Unsaved changes" : "Saved")}</span>
          </div>
        </div>
        ${
          schema?.kind === "message"
            ? renderConfigMessageFields(scope, section, schema, [], draft)
            : `
                <div class="field">
                  <label for="config-fallback-${escapeHtml(scope)}-${escapeHtml(section)}">${escapeHtml(label)}</label>
                  <textarea id="config-fallback-${escapeHtml(scope)}-${escapeHtml(section)}" name="config_json">${escapeHtml(prettyJson(draft || {}))}</textarea>
                </div>
              `
        }
        <details class="config-advanced">
          <summary>Advanced JSON preview</summary>
          <div class="code-block">${escapeHtml(prettyJson(draft || {}))}</div>
        </details>
        <div class="action-row">
          <button
            class="inline-btn secondary-btn"
            type="button"
            data-action-click="reset-config-section"
            data-scope="${escapeHtml(scope)}"
            data-section="${escapeHtml(section)}"
          >
            Reset Changes
          </button>
          <button class="action-btn" type="submit">Save ${escapeHtml(label)}</button>
        </div>
      </form>
    </article>
  `;
}

function configSectionLabel(scope, section) {
  return getConfigSchema(scope, section)?.label || labelizeName(section);
}

function sortedConfigEntries(scope, sections) {
  return Object.entries(sections || {}).sort(([sectionA], [sectionB]) => {
    const labelA = configSectionLabel(scope, sectionA);
    const labelB = configSectionLabel(scope, sectionB);
    return labelA.localeCompare(labelB) || sectionA.localeCompare(sectionB);
  });
}

function activeConfigScope(scopeGroups) {
  const saved = String(state.configScope || "local").trim();
  const active = scopeGroups.find((group) => group.key === saved && group.sections.length) || scopeGroups.find((group) => group.sections.length) || scopeGroups[0];
  state.configScope = active?.key || "local";
  safeStorageSet("tater_meshtastic_bridge_config_scope", state.configScope);
  return active;
}

function activeConfigSection(scope, sections) {
  const saved = String(state.configSections?.[scope] || "").trim();
  const active = sections.find(([section]) => section === saved) || sections[0] || ["", {}];
  const section = String(active[0] || "").trim();
  if (section) {
    state.configSections[scope] = section;
    safeStorageSet(`tater_meshtastic_bridge_config_section_${scope}`, section);
  }
  return active;
}

function configGroupDirtyCount(scope, sections) {
  return sections.filter(([section]) => state.configDirty[configDraftKey(scope, section)]).length;
}

function renderConfigScopeTabs(scopeGroups, activeScope) {
  return `
    <div class="config-scope-tabs" role="tablist" aria-label="Config groups">
      ${scopeGroups
        .map((group) => {
          const dirtyCount = configGroupDirtyCount(group.key, group.sections);
          return `
            <button
              type="button"
              class="config-scope-tab ${group.key === activeScope.key ? "active" : ""}"
              data-config-scope-tab="${escapeHtml(group.key)}"
            >
              <span>${escapeHtml(group.title)}</span>
              <small>${escapeHtml(group.sections.length)} section${group.sections.length === 1 ? "" : "s"}${dirtyCount ? ` · ${dirtyCount} unsaved` : ""}</small>
            </button>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderConfigSectionTabs(scope, sections, activeSection) {
  return `
    <aside class="config-section-rail" aria-label="${escapeHtml(scope)} config sections">
      <div class="config-section-rail-head">
        <div>
          <h3>${escapeHtml(labelizeName(scope))}</h3>
          <p>${escapeHtml(sections.length)} sorted sections</p>
        </div>
      </div>
      <div class="config-section-tabs">
        ${
          sections.length
            ? sections
                .map(([section]) => {
                  const schema = getConfigSchema(scope, section);
                  const label = configSectionLabel(scope, section);
                  const dirty = Boolean(state.configDirty[configDraftKey(scope, section)]);
                  const summary = schema?.fields?.length ? `${schema.fields.length} fields` : "JSON fallback";
                  return `
                    <button
                      type="button"
                      class="config-section-tab ${section === activeSection ? "active" : ""}"
                      data-config-section-tab="${escapeHtml(section)}"
                    >
                      <span class="config-section-name">${escapeHtml(label)}</span>
                      <span class="config-section-sub">${escapeHtml(summary)}${dirty ? " · unsaved" : ""}</span>
                    </button>
                  `;
                })
                .join("")
            : `<div class="muted">No config sections are available yet.</div>`
        }
      </div>
    </aside>
  `;
}

function renderConfigs() {
  const config = state.data.config || {};
  const scopeGroups = [
    {
      key: "local",
      title: "Local",
      description: "Core radio behavior, LoRa, Bluetooth, network, position, display, and power settings.",
      sections: sortedConfigEntries("local", config.local || {}),
    },
    {
      key: "module",
      title: "Module",
      description: "Optional feature modules like telemetry, MQTT, serial, audio, canned messages, and alerts.",
      sections: sortedConfigEntries("module", config.module || {}),
    },
  ];
  const activeScope = activeConfigScope(scopeGroups);
  const [activeSection, activeValue] = activeConfigSection(activeScope.key, activeScope.sections);
  return `
    <section class="config-workspace">
      <section class="panel config-overview-panel">
        <div class="section-head">
          <div>
            <h3>Device Configs</h3>
            <p>Pick a group and section to edit typed Meshtastic settings without scrolling through every section at once.</p>
          </div>
          <div class="chip-row">
            <span class="chip">${escapeHtml(scopeGroups.reduce((total, group) => total + group.sections.length, 0))} sections</span>
            ${
              hasUnsavedConfigDrafts()
                ? `<span class="chip chip-warn">${escapeHtml(Object.values(state.configDirty).filter(Boolean).length)} unsaved</span>`
                : `<span class="chip">All saved</span>`
            }
          </div>
        </div>
        ${renderConfigScopeTabs(scopeGroups, activeScope)}
      </section>

      <section class="config-browser">
        ${renderConfigSectionTabs(activeScope.key, activeScope.sections, activeSection)}
        <div class="config-section-detail">
          <div class="section-head">
            <div>
              <h3>${escapeHtml(activeScope.title)} Config</h3>
              <p>${escapeHtml(activeScope.description)}</p>
            </div>
          </div>
          ${
            activeSection
              ? renderConfigSection(activeScope.key, activeSection, activeValue, getConfigSchema(activeScope.key, activeSection))
              : `<article class="panel"><div class="muted">No ${escapeHtml(activeScope.key)} config snapshot is available yet.</div></article>`
          }
        </div>
      </section>
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

function selectedAttr(value, expected) {
  return String(value) === String(expected) ? "selected" : "";
}

function normalizedGpsMode(value) {
  const token = String(value ?? "").trim().toUpperCase();
  if (token === "1") {
    return "ENABLED";
  }
  if (token === "2") {
    return "NOT_PRESENT";
  }
  if (["DISABLED", "ENABLED", "NOT_PRESENT"].includes(token)) {
    return token;
  }
  return "DISABLED";
}

function sourceLabel(source) {
  const token = String(source || "").trim().toLowerCase();
  if (token === "ui") {
    return "UI saved";
  }
  if (token === "env") {
    return ".env override";
  }
  if (token === "fixed") {
    return "Fixed";
  }
  return "Default";
}

function settingHelper(name, sources, extra = "") {
  const bits = [`Source: ${sourceLabel(sources?.[name])}`];
  if (extra) {
    bits.push(extra);
  }
  return `<div class="helper">${escapeHtml(bits.join(" · "))}</div>`;
}

function renderBleScanPanel(values = {}) {
  const scan = state.bleScan || {};
  const devices = Array.isArray(scan.devices) ? scan.devices : null;
  const currentAddress = String(values.device_address || "").trim();
  const currentName = String(values.device_name || "").trim();
  const resultsHtml = devices
    ? devices.length
      ? devices
          .map((device) => {
            const name = String(device.name || "").trim();
            const address = String(device.address || "").trim();
            const matchesConfig =
              Boolean(device.matches_config) ||
              Boolean((currentAddress && address === currentAddress) || (currentName && name === currentName));
            return `
              <article class="ble-device-card ${matchesConfig ? "selected" : ""}">
                <div>
                  <strong>${escapeHtml(name || "Unnamed Meshtastic device")}</strong>
                  <div class="helper">${escapeHtml(address || "No BLE address reported")}</div>
                  <div class="chip-row">
                    ${matchesConfig ? `<span class="chip">Current target</span>` : ""}
                    ${device.rssi !== null && device.rssi !== undefined ? `<span class="chip">RSSI ${escapeHtml(device.rssi)}</span>` : ""}
                  </div>
                </div>
                <button
                  class="inline-btn"
                  type="button"
                  data-action-click="select-ble-device"
                  data-name="${escapeHtml(name)}"
                  data-address="${escapeHtml(address)}"
                >Use This Device</button>
              </article>
            `;
          })
          .join("")
      : `<div class="muted">No Meshtastic BLE devices found. Make sure Bluetooth is on, the radio is advertising, and no other app is connected.</div>`
    : `<div class="muted">Run a scan to find nearby Meshtastic BLE radios. This uses the same scan as meshtastic --ble-scan.</div>`;
  return `
    <div class="ble-scan-panel">
      <div class="section-head compact-head">
        <div>
          <h4>Find BLE Device</h4>
          <p>Scan nearby Meshtastic radios and fill the bridge target without guessing the name or address.</p>
        </div>
        <button class="action-btn" type="button" data-action-click="ble-scan" ${scan.loading ? "disabled" : ""}>
          ${scan.loading ? "Scanning..." : "Scan for Devices"}
        </button>
      </div>
      ${scan.error ? `<div class="notice error">${escapeHtml(scan.error)}</div>` : ""}
      ${scan.scannedAt ? `<div class="helper">Last scan: ${escapeHtml(formatTs(scan.scannedAt))}</div>` : ""}
      <div class="ble-device-list">
        ${resultsHtml}
      </div>
    </div>
  `;
}

function renderSettings() {
  const bridge = state.data.settings || {};
  const values = bridge.values || {};
  const active = bridge.active || {};
  const sources = bridge.sources || {};
  const envOverrides = bridge.env_overrides || {};
  const restartRequired = Boolean(bridge.restart_required);
  const restartKeys = new Set(bridge.restart_required_keys || []);
  const portLocked = Boolean(envOverrides.port);
  const restartHint = restartRequired
    ? "Restart the bridge for pending startup settings to take effect."
    : "Most saved settings apply right away. Startup settings are marked below.";
  return `
    <section class="settings-workspace">
      <section class="panel settings-wide">
        <div class="section-head">
          <div>
            <h3>Connection Settings</h3>
            <p>Set this browser's access token, pick the BLE radio, and save the bridge connection target from one place.</p>
          </div>
          <div class="chip-row">
            <span class="chip ${restartRequired ? "chip-warn" : ""}">${escapeHtml(restartHint)}</span>
          </div>
        </div>
        <form data-action="save-token" class="field-grid browser-token-form">
          <div class="field">
            <label for="api-token">Browser Access Token</label>
            <input id="api-token" name="token" value="${escapeHtml(state.token)}" autocomplete="current-password" placeholder="Current Bridge API Token" />
            <div class="helper">This is saved only in this browser. If you see "Invalid or missing API token", put the current Bridge API Token here first.</div>
          </div>
          <div class="field">
            <label for="window-hours">Stats Window (hours)</label>
            <input id="window-hours" name="window_hours" value="${escapeHtml(state.windowHours)}" />
          </div>
          <div class="field">
            <label>&nbsp;</label>
            <div class="token-action-stack">
              <button class="action-btn" type="submit">Save Browser Access</button>
              <button class="inline-btn danger-btn" type="button" data-action-click="clear-api-token-local">Clear Bridge Token</button>
            </div>
          </div>
        </form>
        <form data-action="save-connection-settings" class="settings-form connection-settings-form">
          <div class="field-grid connection-settings-grid">
            <div class="field">
              <label for="runtime-device-name">BLE Device Name</label>
              <input id="runtime-device-name" name="device_name" value="${escapeHtml(values.device_name || "")}" placeholder="ThinkNode M6" />
              ${settingHelper("device_name", sources, "Reconnect or restart after changing the target device.")}
            </div>
            <div class="field">
              <label for="runtime-device-address">BLE Device Address</label>
              <input id="runtime-device-address" name="device_address" value="${escapeHtml(values.device_address || "")}" placeholder="BLE address from meshtastic --ble-scan" />
              ${settingHelper("device_address", sources, "Address is usually more reliable than name.")}
            </div>
            <div class="field runtime-scan-field">
              ${renderBleScanPanel(values)}
            </div>
            <div class="field">
              <label for="runtime-api-token">Bridge API Token</label>
              <input id="runtime-api-token" name="api_token" value="${escapeHtml(values.api_token || "")}" autocomplete="new-password" />
              ${settingHelper("api_token", sources, "Optional server-side token. The browser token above must match this after auth is enabled.")}
            </div>
          </div>
          <div class="action-row">
            <button class="action-btn" type="submit">Save Connection Settings</button>
            <span class="helper">This only saves the BLE target and bridge API token. It does not require the radio to be connected.</span>
          </div>
        </form>

        <div class="settings-subhead">
          <h4>Advanced Runtime Settings</h4>
          <p>These are bridge behavior and startup settings. They are separate from the BLE connection target.</p>
        </div>
        <form data-action="save-runtime-settings" class="settings-form">
          <div class="field-grid runtime-settings-grid">
            <div class="field">
              <label for="runtime-host">Host</label>
              <input id="runtime-host" value="${escapeHtml(values.host || "0.0.0.0")}" readonly />
              ${settingHelper("host", sources, "Always binds on all interfaces.")}
            </div>
            <div class="field">
              <label for="runtime-port">Port</label>
              <input id="runtime-port" name="port" type="number" min="1" max="65535" value="${escapeHtml(values.port ?? 8433)}" ${portLocked ? "readonly" : ""} />
              ${settingHelper(
                "port",
                sources,
                portLocked ? "Locked by MESHTASTIC_BRIDGE_PORT until you remove it from .env." : restartKeys.has("port") ? "Restart required after changing." : "Restart required after changing."
              )}
            </div>
            <div class="field">
              <label for="runtime-log-level">Log Level</label>
              <select id="runtime-log-level" name="log_level">
                ${["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
                  .map((level) => `<option value="${escapeHtml(level)}" ${selectedAttr(values.log_level || "INFO", level)}>${escapeHtml(level)}</option>`)
                  .join("")}
              </select>
              ${settingHelper("log_level", sources)}
            </div>
            <div class="field">
              <label for="runtime-reconnect">Reconnect Seconds</label>
              <input id="runtime-reconnect" name="reconnect_seconds" type="number" min="1" step="0.5" value="${escapeHtml(values.reconnect_seconds ?? 10)}" />
              ${settingHelper("reconnect_seconds", sources)}
            </div>
            <div class="field">
              <label for="runtime-connect-timeout">Connect Timeout Seconds</label>
              <input id="runtime-connect-timeout" name="connect_timeout_seconds" type="number" min="5" step="1" value="${escapeHtml(values.connect_timeout_seconds ?? 60)}" />
              ${settingHelper("connect_timeout_seconds", sources, "Used on the next BLE connection attempt.")}
            </div>
            <div class="field">
              <label for="runtime-event-buffer">Event Buffer Size</label>
              <input id="runtime-event-buffer" name="event_buffer_size" type="number" min="50" step="1" value="${escapeHtml(values.event_buffer_size ?? 500)}" />
              ${settingHelper("event_buffer_size", sources, "Restart required to resize the in-memory buffer.")}
            </div>
            <div class="field">
              <label for="runtime-hop-limit">Default Hop Limit</label>
              <input id="runtime-hop-limit" name="default_hop_limit" type="number" step="1" value="${escapeHtml(values.default_hop_limit ?? "")}" placeholder="Use radio default" />
              ${settingHelper("default_hop_limit", sources)}
            </div>
            <div class="field">
              <label for="runtime-shutdown-timeout">Shutdown Timeout Seconds</label>
              <input id="runtime-shutdown-timeout" name="shutdown_timeout_seconds" type="number" min="0.1" step="0.1" value="${escapeHtml(values.shutdown_timeout_seconds ?? 2)}" />
              ${settingHelper("shutdown_timeout_seconds", sources)}
            </div>
            <div class="field">
              <label for="runtime-outbound-events">Include Outbound Events</label>
              <select id="runtime-outbound-events" name="include_outbound_events">
                <option value="true" ${selectedAttr(Boolean(values.include_outbound_events), true)}>True</option>
                <option value="false" ${selectedAttr(Boolean(values.include_outbound_events), false)}>False</option>
              </select>
              ${settingHelper("include_outbound_events", sources)}
            </div>
            <div class="field">
              <label for="runtime-want-ack">Want ACK</label>
              <select id="runtime-want-ack" name="want_ack">
                <option value="true" ${selectedAttr(Boolean(values.want_ack), true)}>True</option>
                <option value="false" ${selectedAttr(Boolean(values.want_ack), false)}>False</option>
              </select>
              ${settingHelper("want_ack", sources)}
            </div>
            <div class="field">
              <label for="runtime-no-nodes">Skip Initial Node Load</label>
              <select id="runtime-no-nodes" name="no_nodes">
                <option value="true" ${selectedAttr(Boolean(values.no_nodes), true)}>True</option>
                <option value="false" ${selectedAttr(Boolean(values.no_nodes), false)}>False</option>
              </select>
              ${settingHelper("no_nodes", sources, "Used on the next BLE connection attempt.")}
            </div>
            <div class="field">
              <label for="runtime-database-path">Database Path</label>
              <input id="runtime-database-path" value="${escapeHtml(values.database_path || "")}" readonly />
              ${settingHelper("database_path", sources, "Startup-only; use MESHTASTIC_DATABASE_PATH to move it.")}
            </div>
          </div>
          <div class="meta-list settings-active-list">
            ${renderMetaRows([
              { label: "Active Host", value: active.host || "" },
              { label: "Active Port", value: active.port || "" },
              { label: "Saved Port", value: values.port || "" },
              { label: "Restart Needed", value: restartRequired ? "Yes" : "No" },
            ])}
          </div>
          <div class="action-row">
            <button class="action-btn" type="submit">Save Advanced Settings</button>
            <button class="inline-btn danger-btn" type="button" data-action-click="clear-runtime-settings">Clear Saved Settings</button>
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

      <section class="panel danger-zone-panel">
        <div class="section-head">
          <div>
            <h3>Danger Zone</h3>
            <p>Remove all saved bridge data when you want the console to behave like a fresh install.</p>
          </div>
        </div>
        <div class="danger-zone-card">
          <div>
            <strong>Clear Everything</strong>
            <p>This deletes chat history, events, known nodes, node sightings, snapshots, audit logs, and saved bridge settings. The bridge process keeps running.</p>
          </div>
          <button class="action-btn danger-btn" type="button" data-action-click="clear-all-data">Clear Everything</button>
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
  if (state.view === "firmware") {
    root.innerHTML = renderFirmware();
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
  renderNodeModal();
  window.requestAnimationFrame(syncRealNodeMap);
}

async function handleFormSubmit(form) {
  const action = String(form?.dataset?.action || "").trim();
  if (!action) {
    return;
  }

  try {
    if (action === "send-chat" || action === "send-node-message") {
      const text = String(form.elements.text?.value || "").trim();
      if (!text) {
        setNotice("Type a message before sending.", "error");
        return;
      }
      const channel = Number.parseInt(String(form.elements.channel?.value || "0"), 10) || 0;
      const destination = action === "send-node-message" ? String(form.elements.destination?.value || "").trim() : "broadcast";
      if (!destination) {
        setNotice("Select a node before sending a direct message.", "error");
        return;
      }
      const result = await apiFetch("/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text, channel, destination }),
      });
      form.elements.text.value = "";
      state.formDirty = false;
      if (result.message) {
        mergeMessages([result.message]);
      }
      setNotice(action === "send-node-message" ? `Direct message sent on channel ${channel}.` : `Broadcast sent on channel ${channel}.`);
      if (action === "send-node-message") {
        await loadNodeHistory(destination);
        render();
      } else {
        renderChatWithScroll({ forceBottom: true });
      }
      return;
    }

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

    if (action === "save-lora-settings") {
      const intValue = (name) => {
        const parsed = Number.parseInt(String(form.elements[name]?.value || "0"), 10);
        return Number.isFinite(parsed) ? parsed : 0;
      };
      const floatValue = (name) => {
        const parsed = Number.parseFloat(String(form.elements[name]?.value || "0"));
        return Number.isFinite(parsed) ? parsed : 0;
      };
      const payload = {
        values: {
          region: form.elements.region.value,
          use_preset: form.elements.use_preset.value === "true",
          modem_preset: form.elements.modem_preset.value,
          hop_limit: intValue("hop_limit"),
          channel_num: intValue("channel_num"),
          tx_enabled: form.elements.tx_enabled.value === "true",
          tx_power: intValue("tx_power"),
          sx126x_rx_boosted_gain: form.elements.sx126x_rx_boosted_gain.value === "true",
          override_frequency: floatValue("override_frequency"),
          frequency_offset: floatValue("frequency_offset"),
          bandwidth: intValue("bandwidth"),
          spread_factor: intValue("spread_factor"),
          coding_rate: intValue("coding_rate"),
          override_duty_cycle: form.elements.override_duty_cycle.value === "true",
          pa_fan_disabled: form.elements.pa_fan_disabled.value === "true",
          ignore_mqtt: form.elements.ignore_mqtt.value === "true",
          config_ok_to_mqtt: form.elements.config_ok_to_mqtt.value === "true",
          ignore_incoming: intListFromText(form.elements.ignore_incoming.value),
        },
      };
      await apiFetch("/config/local/lora", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice("LoRa settings saved.");
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

    if (action === "save-position-settings") {
      const intValue = (name) => {
        const parsed = Number.parseInt(String(form.elements[name]?.value || "0"), 10);
        return Number.isFinite(parsed) ? parsed : 0;
      };
      const payload = {
        values: {
          gps_mode: form.elements.gps_mode.value,
          gps_enabled: form.elements.gps_enabled.value === "true",
          fixed_position: form.elements.fixed_position.value === "true",
          position_broadcast_secs: intValue("position_broadcast_secs"),
          position_broadcast_smart_enabled: form.elements.position_broadcast_smart_enabled.value === "true",
          gps_update_interval: intValue("gps_update_interval"),
          gps_attempt_time: intValue("gps_attempt_time"),
          broadcast_smart_minimum_distance: intValue("broadcast_smart_minimum_distance"),
          broadcast_smart_minimum_interval_secs: intValue("broadcast_smart_minimum_interval_secs"),
        },
      };
      await apiFetch("/config/local/position", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice("GPS and position settings saved.");
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
      const key = configDraftKey(scope, section);
      const values =
        form.config_json && !getConfigSchema(scope, section)
          ? JSON.parse(form.config_json.value)
          : deepClone(getConfigDraft(scope, section, getConfigSourceValue(scope, section)), {});
      if (scope === "channel") {
        await apiFetch(`/channels/${encodeURIComponent(section)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ channel: values }),
        });
      } else {
        await apiFetch(`/config/${encodeURIComponent(scope)}/${encodeURIComponent(section)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ values }),
        });
      }
      state.configDirty[key] = false;
      setNotice(scope === "channel" ? `Channel ${section} saved.` : `${scope}.${section} saved.`);
      await loadBootstrap();
      return;
    }

    if (action === "save-connection-settings") {
      const nextToken = form.api_token.value.trim();
      const payload = {
        device_name: form.device_name.value.trim(),
        device_address: form.device_address.value.trim(),
        api_token: nextToken,
      };
      const requestAuthToken = String(state.token || "").trim() || nextToken;
      const requestOptions = {
        method: "POST",
        authToken: requestAuthToken,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      };
      let result;
      try {
        result = await apiFetch("/settings", requestOptions);
      } catch (error) {
        const canRetryWithTypedToken =
          nextToken &&
          nextToken !== requestAuthToken &&
          String(error?.message || "").toLowerCase().includes("token");
        if (!canRetryWithTypedToken) {
          throw error;
        }
        result = await apiFetch("/settings", { ...requestOptions, authToken: nextToken });
      }
      state.token = nextToken;
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      const restartNeeded = Boolean(result.restart_required);
      setNotice(restartNeeded ? "Connection settings saved. Restart or reconnect the bridge to use the selected BLE radio." : "Connection settings saved.");
      await loadBootstrap();
      return;
    }

    if (action === "save-runtime-settings") {
      const hopLimit = form.default_hop_limit.value.trim();
      const payload = {
        port: Number.parseInt(form.port.value || "8433", 10) || 8433,
        reconnect_seconds: Number.parseFloat(form.reconnect_seconds.value || "10") || 10,
        connect_timeout_seconds: Number.parseInt(form.connect_timeout_seconds.value || "60", 10) || 60,
        event_buffer_size: Number.parseInt(form.event_buffer_size.value || "500", 10) || 500,
        log_level: form.log_level.value,
        include_outbound_events: form.include_outbound_events.value === "true",
        want_ack: form.want_ack.value === "true",
        default_hop_limit: hopLimit ? Number.parseInt(hopLimit, 10) : null,
        no_nodes: form.no_nodes.value === "true",
        shutdown_timeout_seconds: Number.parseFloat(form.shutdown_timeout_seconds.value || "2") || 2,
      };
      const result = await apiFetch("/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const restartNeeded = Boolean(result.restart_required);
      setNotice(restartNeeded ? "Advanced bridge settings saved. Restart the bridge for startup changes to take effect." : "Advanced bridge settings saved.");
      await loadBootstrap();
      return;
    }

    if (action === "firmware-ota-update") {
      const filePath = String(form.elements.file_path?.value || "").trim();
      const tcpHost = String(form.elements.tcp_host?.value || "").trim();
      const timeoutSeconds = Number.parseInt(String(form.elements.timeout_seconds?.value || "600"), 10) || 600;
      if (!filePath || !tcpHost) {
        setNotice("Choose a downloaded firmware file and enter the radio TCP host before starting OTA.", "error");
        return;
      }
      const confirmed = window.confirm(
        "Start firmware OTA update now? This can take several minutes and should only be used for supported ESP32 WiFi/TCP devices."
      );
      if (!confirmed) {
        return;
      }
      const payload = await apiFetch("/firmware/ota-update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: filePath, tcp_host: tcpHost, timeout_seconds: timeoutSeconds }),
      });
      setNotice(payload.ok ? "Firmware OTA update completed." : "Firmware OTA update finished with warnings.", payload.ok ? "info" : "error");
      await loadFirmwareStatus({ refresh: true });
      return;
    }

    if (action === "save-token") {
      state.token = form.token.value.trim();
      state.windowHours = Number.parseInt(form.window_hours.value || "24", 10) || 24;
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      safeStorageSet("tater_meshtastic_bridge_window_hours", state.windowHours);
      setNotice("Browser access saved.");
      await loadBootstrap();
      return;
    }
  } catch (error) {
    setNotice(error.message || "Request failed.", "error");
  }
}

function closeNodeModal() {
  state.nodeModal = {
    open: false,
    nodeId: "",
    nodeNum: 0,
    entity: {},
    history: [],
    loading: false,
    error: "",
  };
  render();
}

async function openChatNodeModal(button) {
  const message = findMessageByDomKey(button.dataset.messageKey);
  const packetEntity = nodeEntityForMessage(message, button.dataset.nodeSide);
  const fallbackEntity = {
    node_id: button.dataset.nodeId || "",
    num: Number.parseInt(button.dataset.nodeNum || "0", 10) || 0,
  };
  const entity = hasObjectKeys(packetEntity) ? packetEntity : fallbackEntity;
  const known = findKnownNode(entity) || {};
  const nodeId = firstNonEmpty(known.node_id, nodeIdFromEntity(entity), button.dataset.nodeId);
  const nodeNum = nodeNumFromEntity(known) || nodeNumFromEntity(entity) || fallbackEntity.num;

  state.nodeModal = {
    open: true,
    nodeId,
    nodeNum,
    entity: {
      ...entity,
      node_id: firstNonEmpty(nodeId, nodeIdFromEntity(entity)),
      num: nodeNum || nodeNumFromEntity(entity),
    },
    history: [],
    loading: Boolean(nodeId),
    error: "",
  };
  render();

  if (!nodeId) {
    state.nodeModal.loading = false;
    render();
    return;
  }

  try {
    const payload = await apiFetch(`/nodes/${encodeURIComponent(nodeId)}/history?limit=40`);
    if (state.nodeModal.open && state.nodeModal.nodeId === nodeId) {
      state.nodeModal.history = Array.isArray(payload.history) ? payload.history : [];
      state.nodeModal.error = "";
    }
  } catch (error) {
    if (state.nodeModal.open && state.nodeModal.nodeId === nodeId) {
      state.nodeModal.error = error.message || "Failed to load node history.";
    }
  } finally {
    if (state.nodeModal.open && state.nodeModal.nodeId === nodeId) {
      state.nodeModal.loading = false;
      render();
    }
  }
}

function readRuntimeSettingsDraft() {
  const forms = document.querySelectorAll(
    'form[data-action="save-token"], form[data-action="save-connection-settings"], form[data-action="save-runtime-settings"]'
  );
  if (!forms.length) {
    return null;
  }
  const draft = {};
  forms.forEach((form) => {
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    Array.from(form.elements).forEach((element) => {
      if ((element instanceof HTMLInputElement || element instanceof HTMLSelectElement || element instanceof HTMLTextAreaElement) && element.name) {
        draft[element.name] = element.value;
      }
    });
  });
  return draft;
}

function restoreRuntimeSettingsDraft(draft) {
  if (!draft || typeof draft !== "object") {
    return;
  }
  const forms = document.querySelectorAll(
    'form[data-action="save-token"], form[data-action="save-connection-settings"], form[data-action="save-runtime-settings"]'
  );
  forms.forEach((form) => {
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    Object.entries(draft).forEach(([name, value]) => {
      const element = form.elements.namedItem(name);
      if (element instanceof HTMLInputElement || element instanceof HTMLSelectElement || element instanceof HTMLTextAreaElement) {
        element.value = String(value ?? "");
      }
    });
  });
}

async function runBleScan(button) {
  const draft = readRuntimeSettingsDraft();
  state.bleScan = {
    loading: true,
    devices: state.bleScan.devices,
    scannedAt: state.bleScan.scannedAt,
    error: "",
  };
  render();
  restoreRuntimeSettingsDraft(draft);
  setNotice("Scanning for Meshtastic BLE devices. This usually takes about 10 seconds.");

  try {
    const payload = await apiFetch("/ble/scan", { method: "POST" });
    state.bleScan = {
      loading: false,
      devices: Array.isArray(payload.devices) ? payload.devices : [],
      scannedAt: payload.finished_at || new Date().toISOString(),
      error: "",
    };
    setNotice(`BLE scan complete. Found ${state.bleScan.devices.length} Meshtastic device${state.bleScan.devices.length === 1 ? "" : "s"}.`);
  } catch (error) {
    state.bleScan = {
      loading: false,
      devices: state.bleScan.devices,
      scannedAt: state.bleScan.scannedAt,
      error: error.message || "BLE scan failed.",
    };
    setNotice(state.bleScan.error, "error");
  } finally {
    render();
    restoreRuntimeSettingsDraft(draft);
    if (button instanceof HTMLButtonElement) {
      button.focus();
    }
  }
}

function selectBleDevice(button) {
  const name = String(button.dataset.name || "").trim();
  const address = String(button.dataset.address || "").trim();
  const nameInput = document.getElementById("runtime-device-name");
  const addressInput = document.getElementById("runtime-device-address");
  if (nameInput instanceof HTMLInputElement) {
    nameInput.value = name;
  }
  if (addressInput instanceof HTMLInputElement) {
    addressInput.value = address;
  }
  state.formDirty = true;
  setNotice("BLE device filled in. Save Connection Settings when you are ready to use it.");
}

async function loadChannelShare({ refresh = false } = {}) {
  state.channelShare = {
    ...state.channelShare,
    loading: true,
    error: "",
  };
  render();
  try {
    const payload = await apiFetch(`/channels/share${refresh ? "?refresh=true" : ""}`);
    state.channelShare = {
      loading: false,
      loaded: true,
      error: "",
      shares: Array.isArray(payload.shares) ? payload.shares : [],
    };
    setNotice(state.channelShare.shares.length ? "Channel QR codes generated." : "No channel URLs are available to share yet.");
  } catch (error) {
    state.channelShare = {
      ...state.channelShare,
      loading: false,
      loaded: true,
      error: error.message || "Failed to generate channel QR codes.",
    };
    setNotice(state.channelShare.error, "error");
  } finally {
    render();
  }
}

async function loadFirmwareStatus({ refresh = false } = {}) {
  try {
    const payload = await apiFetch(`/firmware/status${refresh ? "?refresh=true" : ""}`);
    state.firmware.status = payload || {};
    state.firmware.error = "";
  } catch (error) {
    state.firmware.error = error.message || "Failed to detect firmware status.";
    setNotice(state.firmware.error, "error");
  } finally {
    render();
  }
}

async function loadFirmwareFiles() {
  state.firmware.loadingFiles = true;
  render();
  try {
    const payload = await apiFetch("/firmware/files");
    state.firmware.files = Array.isArray(payload.files) ? payload.files : [];
    state.firmware.error = "";
  } catch (error) {
    state.firmware.error = error.message || "Failed to load downloaded firmware files.";
    setNotice(state.firmware.error, "error");
  } finally {
    state.firmware.loadingFiles = false;
    render();
  }
}

async function loadFirmwareReleases() {
  state.firmware.loadingReleases = true;
  state.firmware.error = "";
  render();
  try {
    const include = state.firmware.includePrerelease ? "true" : "false";
    const payload = await apiFetch(`/firmware/releases?include_prerelease=${encodeURIComponent(include)}&limit=8`);
    state.firmware.releases = Array.isArray(payload.releases) ? payload.releases : [];
    setNotice(`Loaded ${state.firmware.releases.length} firmware release${state.firmware.releases.length === 1 ? "" : "s"}.`);
  } catch (error) {
    state.firmware.error = error.message || "Failed to load firmware releases.";
    setNotice(state.firmware.error, "error");
  } finally {
    state.firmware.loadingReleases = false;
    render();
  }
}

async function ensureFirmwareLoaded() {
  if (!state.firmware.status) {
    await loadFirmwareStatus();
  }
  if (!state.firmware.files.length) {
    await loadFirmwareFiles();
  }
}

async function handleActionClick(button) {
  const action = String(button?.dataset?.actionClick || "").trim();
  if (!action) {
    if (button.dataset.nodeId) {
      await loadNodeHistory(button.dataset.nodeId);
      render();
    }
    return;
  }
  try {
    if (action === "open-chat-node") {
      await openChatNodeModal(button);
      return;
    }

    if (action === "close-node-modal") {
      closeNodeModal();
      return;
    }

    if (action === "ble-scan") {
      await runBleScan(button);
      return;
    }

    if (action === "select-ble-device") {
      selectBleDevice(button);
      return;
    }

    if (action === "clear-api-token-local") {
      const confirmed = window.confirm(
        "Clear the Bridge API Token now? This only works from the bridge host itself, and it will leave the bridge API/UI unlocked until you save a new token."
      );
      if (!confirmed) {
        return;
      }
      await apiFetch("/auth/clear-token", { method: "POST", authToken: "" });
      state.token = "";
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      setNotice("Bridge API token cleared. You can now save connection settings or set a fresh token.");
      await loadBootstrap();
      return;
    }

    if (action === "load-channel-share") {
      await loadChannelShare({ refresh: true });
      return;
    }

    if (action === "firmware-refresh-status") {
      await loadFirmwareStatus({ refresh: true });
      return;
    }

    if (action === "firmware-refresh-files") {
      await loadFirmwareFiles();
      return;
    }

    if (action === "firmware-load-releases") {
      await loadFirmwareReleases();
      return;
    }

    if (action === "firmware-prerelease-toggle") {
      state.firmware.includePrerelease = !state.firmware.includePrerelease;
      safeStorageSet("tater_meshtastic_bridge_firmware_prerelease", state.firmware.includePrerelease ? "true" : "false");
      await loadFirmwareReleases();
      return;
    }

    if (action === "firmware-download") {
      const assetName = String(button.dataset.assetName || "").trim();
      const assetUrl = String(button.dataset.assetUrl || "").trim();
      const tagName = String(button.dataset.tagName || "").trim();
      const confirmed = window.confirm(`Download firmware asset '${assetName}' into the bridge cache?`);
      if (!confirmed) {
        return;
      }
      await apiFetch("/firmware/download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ asset_name: assetName, asset_url: assetUrl, tag_name: tagName }),
      });
      setNotice(`Downloaded ${assetName}.`);
      await loadFirmwareFiles();
      return;
    }

    if (action === "firmware-device-action") {
      const kind = String(button.dataset.deviceAction || "").trim();
      const confirmed = window.confirm(`Send '${kind}' to the connected radio? This may disconnect Bluetooth and put the device into update mode.`);
      if (!confirmed) {
        return;
      }
      await apiFetch(`/device/action/${encodeURIComponent(kind)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ seconds: 10 }),
      });
      setNotice(`Firmware prep action '${kind}' sent.`);
      await loadBootstrap();
      return;
    }

    if (action === "clear-runtime-settings") {
      const confirmed = window.confirm(
        "Clear all saved bridge runtime settings? This resets the BLE target, API token, reconnect options, and UI-managed port back to defaults. Message and node history will be kept."
      );
      if (!confirmed) {
        return;
      }
      const result = await apiFetch("/settings", { method: "DELETE" });
      state.token = "";
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      state.bleScan = { loading: false, devices: null, scannedAt: "", error: "" };
      const restartNeeded = Boolean(result.restart_required);
      setNotice(restartNeeded ? "Saved bridge settings cleared. Restart the bridge before connecting to a new device." : "Saved bridge settings cleared.");
      await loadBootstrap();
      return;
    }

    if (action === "clear-all-data") {
      const confirmed = window.confirm(
        "Clear EVERYTHING from the bridge database? This deletes chat/messages, events, known nodes, snapshots, audit logs, and saved settings. This cannot be undone."
      );
      if (!confirmed) {
        return;
      }
      const typed = window.prompt("Type CLEAR to permanently delete all bridge data.");
      if (String(typed || "").trim() !== "CLEAR") {
        setNotice("Clear everything cancelled.");
        return;
      }
      const result = await apiFetch("/data", { method: "DELETE" });
      state.token = "";
      safeStorageSet("tater_meshtastic_bridge_token", state.token);
      state.bleScan = { loading: false, devices: null, scannedAt: "", error: "" };
      state.channelShare = { loading: false, loaded: false, error: "", shares: [] };
      state.selectedNodeId = "";
      state.nodeHistory = [];
      state.latestMessageEventId = 0;
      const restartNeeded = Boolean(result.restart_required);
      setNotice(restartNeeded ? "All bridge data cleared. Restart the bridge before connecting to a new device." : "All bridge data cleared.");
      await loadBootstrap();
      return;
    }

    if (action === "reset-config-section") {
      const scope = String(button.dataset.scope || "").trim();
      const section = String(button.dataset.section || "").trim();
      const key = configDraftKey(scope, section);
      state.configDrafts[key] = deepClone(getConfigSourceValue(scope, section), {});
      state.configDirty[key] = false;
      render();
      setNotice(`${scope}.${section} reset to the latest bridge snapshot.`);
      return;
    }

    if (action === "config-array-add") {
      const scope = String(button.dataset.scope || "").trim();
      const section = String(button.dataset.section || "").trim();
      const fieldPath = JSON.parse(String(button.dataset.fieldPath || "[]"));
      const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
      const sectionSchema = getConfigSchema(scope, section);
      const fieldSchema = resolveSchemaNode(sectionSchema, fieldPath);
      const arrayValue = getValueAtPath(draft, fieldPath, []);
      const nextValue = defaultValueForSchema(fieldSchema?.item_schema || { kind: "string" });
      if (!Array.isArray(arrayValue)) {
        setValueAtPath(draft, fieldPath, [nextValue]);
      } else {
        arrayValue.push(nextValue);
      }
      state.configDirty[configDraftKey(scope, section)] = true;
      render();
      return;
    }

    if (action === "config-array-remove") {
      const scope = String(button.dataset.scope || "").trim();
      const section = String(button.dataset.section || "").trim();
      const fieldPath = JSON.parse(String(button.dataset.fieldPath || "[]"));
      const index = Number.parseInt(String(button.dataset.index || "0"), 10) || 0;
      const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
      removeArrayItem(draft, fieldPath, index);
      state.configDirty[configDraftKey(scope, section)] = true;
      render();
      return;
    }

    if (action === "config-map-add") {
      const scope = String(button.dataset.scope || "").trim();
      const section = String(button.dataset.section || "").trim();
      const fieldPath = JSON.parse(String(button.dataset.fieldPath || "[]"));
      const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
      const sectionSchema = getConfigSchema(scope, section);
      const fieldSchema = resolveSchemaNode(sectionSchema, fieldPath);
      const rawMapValue = getValueAtPath(draft, fieldPath, {});
      const mapValue = rawMapValue && typeof rawMapValue === "object" && !Array.isArray(rawMapValue) ? rawMapValue : {};
      let nextKey = `entry_${Object.keys(mapValue || {}).length + 1}`;
      while (mapValue && Object.prototype.hasOwnProperty.call(mapValue, nextKey)) {
        nextKey = `${nextKey}_1`;
      }
      mapValue[nextKey] = defaultValueForSchema(fieldSchema?.value_schema || { kind: "string" });
      setValueAtPath(draft, fieldPath, mapValue);
      state.configDirty[configDraftKey(scope, section)] = true;
      render();
      return;
    }

    if (action === "config-map-remove") {
      const scope = String(button.dataset.scope || "").trim();
      const section = String(button.dataset.section || "").trim();
      const fieldPath = JSON.parse(String(button.dataset.fieldPath || "[]"));
      const mapKey = String(button.dataset.mapKey || "").trim();
      const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
      const rawMapValue = getValueAtPath(draft, fieldPath, {});
      const mapValue = rawMapValue && typeof rawMapValue === "object" && !Array.isArray(rawMapValue) ? rawMapValue : {};
      if (mapValue && typeof mapValue === "object") {
        delete mapValue[mapKey];
        state.configDirty[configDraftKey(scope, section)] = true;
        render();
      }
      return;
    }

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
      state.formDirty = false;
      render();
      if (state.view === "chat") {
        stickCurrentChatToBottom();
      }
      if (state.view === "firmware") {
        await ensureFirmwareLoaded();
      }
    });
  });

  const refreshButton = document.getElementById("refresh-btn");
  if (refreshButton) {
    refreshButton.addEventListener("click", async () => {
      await loadBootstrap();
    });
  }

  const handleDelegatedClick = async (event) => {
    const element = event.target instanceof HTMLElement ? event.target : null;
    if (element?.dataset.modalClose === "node") {
      closeNodeModal();
      return;
    }
    const target = element ? element.closest("button") : null;
    if (!target) {
      return;
    }
    if (target.dataset.chatChannel !== undefined) {
      state.chatChannel = channelKey(target.dataset.chatChannel);
      safeStorageSet("tater_meshtastic_bridge_chat_channel", state.chatChannel);
      if (state.view === "chat") {
        renderChatWithScroll({ forceBottom: true });
      } else {
        render();
      }
      return;
    }
    if (target.dataset.configScopeTab !== undefined) {
      const scope = String(target.dataset.configScopeTab || "").trim();
      if (scope) {
        state.configScope = scope;
        safeStorageSet("tater_meshtastic_bridge_config_scope", scope);
        render();
      }
      return;
    }
    if (target.dataset.configSectionTab !== undefined) {
      const section = String(target.dataset.configSectionTab || "").trim();
      if (section) {
        state.configSections[state.configScope] = section;
        safeStorageSet(`tater_meshtastic_bridge_config_section_${state.configScope}`, section);
        render();
      }
      return;
    }
    if (target.dataset.channelTab !== undefined) {
      const index = String(target.dataset.channelTab || "").trim();
      if (index) {
        state.selectedChannelIndex = index;
        safeStorageSet("tater_meshtastic_bridge_selected_channel", index);
        render();
      }
      return;
    }
    if (target.dataset.actionClick || target.dataset.nodeId) {
      await handleActionClick(target);
    }
  };

  const root = document.getElementById("view-root");
  if (root) {
    const handleConfigDraftInput = (event) => {
      const element =
        event.target instanceof HTMLInputElement || event.target instanceof HTMLSelectElement || event.target instanceof HTMLTextAreaElement
          ? event.target
          : null;
      if (!element) {
        return;
      }
      markGenericFormDirty(element);

      if (element.dataset.configMapKey === "true" && event.type === "change") {
        const scope = String(element.dataset.scope || "").trim();
        const section = String(element.dataset.section || "").trim();
        const fieldPath = JSON.parse(String(element.dataset.fieldPath || "[]"));
        const oldKey = String(element.dataset.mapKey || "").trim();
        const newKey = String(element.value || "").trim();
        if (!newKey || newKey === oldKey) {
          element.value = oldKey;
          return;
        }
        const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
        const rawMapValue = getValueAtPath(draft, fieldPath, {});
        const mapValue = rawMapValue && typeof rawMapValue === "object" && !Array.isArray(rawMapValue) ? rawMapValue : {};
        if (Object.prototype.hasOwnProperty.call(mapValue, newKey)) {
          setNotice(`Key '${newKey}' already exists in ${scope}.${section}.`, "error");
          element.value = oldKey;
          return;
        }
        mapValue[newKey] = mapValue[oldKey];
        delete mapValue[oldKey];
        element.dataset.mapKey = newKey;
        state.configDirty[configDraftKey(scope, section)] = true;
        render();
        return;
      }

      if (element.dataset.configInput !== "true") {
        return;
      }

      const scope = String(element.dataset.scope || "").trim();
      const section = String(element.dataset.section || "").trim();
      const path = JSON.parse(String(element.dataset.configPath || "[]"));
      const draft = getConfigDraft(scope, section, getConfigSourceValue(scope, section));
      const sectionSchema = getConfigSchema(scope, section);
      const schemaNode = resolveSchemaNode(sectionSchema, path);
      setValueAtPath(draft, path, castInputValue(element.value, schemaNode));
      state.configDirty[configDraftKey(scope, section)] = true;
    };

    root.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = event.target instanceof HTMLFormElement ? event.target : null;
      if (!form) {
        return;
      }
      await handleFormSubmit(form);
    });

    root.addEventListener("click", handleDelegatedClick);

    root.addEventListener("input", handleConfigDraftInput);
    root.addEventListener("change", handleConfigDraftInput);
    root.addEventListener("keydown", handleChatComposerKeydown);
  }

  const modalRoot = document.getElementById("modal-root");
  if (modalRoot) {
    modalRoot.addEventListener("click", handleDelegatedClick);
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.nodeModal.open) {
      closeNodeModal();
    }
  });
}

function startPolling() {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
    state.pollTimer = 0;
  }
}

async function boot() {
  installEventHandlers();
  render();
  await loadBootstrap();
}

window.addEventListener("DOMContentLoaded", () => {
  boot().catch((error) => {
    setNotice(error.message || "Failed to start bridge console.", "error");
  });
});
