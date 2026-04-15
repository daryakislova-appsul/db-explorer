/* ── DB Explorer Dashboard — Main Application ──────────────────────────────── */

let DATA = null;
let currentView = "overview";
let currentTable = null;
let activeTagFilters = new Set();

// ── Boot ─────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  const resp = await fetch("data/schema.json");
  DATA = await resp.json();
  renderTopbarStats();
  renderSidebar();
  renderOverview();
  setupSearch();
  setupKeyboard();
});

// ── Formatting ───────────────────────────────────────────────────────────────
function fmtRows(n) {
  if (n == null) return "?";
  if (n >= 1e12) return (n / 1e12).toFixed(1) + "T";
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return n.toString();
}

function fmtBytes(b) {
  if (b == null || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let i = 0;
  let v = b;
  while (Math.abs(v) >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(1) + " " + units[i];
}

function escHtml(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// ── Topbar Stats ─────────────────────────────────────────────────────────────
function renderTopbarStats() {
  const el = document.getElementById("topbar-stats");
  const s = DATA.summary;
  const totalRows = Object.values(s).reduce((a, b) => a + (b.total_rows || 0), 0);
  const totalBytes = Object.values(s).reduce((a, b) => a + (b.total_bytes || 0), 0);
  el.innerHTML = `
    <div class="stat-item"><span class="stat-value">${DATA.total_tables}</span><span class="stat-label">Tables</span></div>
    <div class="stat-item"><span class="stat-value">${fmtRows(DATA.total_columns)}</span><span class="stat-label">Columns</span></div>
    <div class="stat-item"><span class="stat-value">${fmtRows(totalRows)}</span><span class="stat-label">Rows</span></div>
    <div class="stat-item"><span class="stat-value">${fmtBytes(totalBytes)}</span><span class="stat-label">Storage</span></div>
  `;
}

// ── Sidebar ──────────────────────────────────────────────────────────────────
function renderSidebar() {
  const el = document.getElementById("sidebar-content");
  let html = "";

  // Tag filter bar
  const allTags = new Set();
  Object.values(DATA.tables).forEach(t => (t.tags || []).forEach(tag => allTags.add(tag)));
  const sortedTags = [...allTags].sort();

  html += `<div class="tag-filter-bar">`;
  sortedTags.forEach(tag => {
    html += `<span class="tag-filter" data-tag="${tag}" onclick="toggleTagFilter('${tag}')">${tag}</span>`;
  });
  html += `</div>`;

  DATA.databases.forEach(db => {
    const tables = Object.values(DATA.tables).filter(t => t.database === db);
    tables.sort((a, b) => a.table.localeCompare(b.table));

    html += `<div class="sidebar-section" data-db="${db}">`;
    html += `<div class="sidebar-db-header" onclick="toggleDbSection('${db}')">
      <span class="chevron">&#9660;</span>
      <span>${db.toUpperCase()}</span>
      <span class="db-badge">${tables.length}</span>
    </div>`;
    html += `<ul class="sidebar-table-list" id="sidebar-list-${db}">`;
    tables.forEach(t => {
      const dotClass = `dot-${db}`;
      html += `<li class="sidebar-table-item" data-table="${t.full_name}" data-tags="${(t.tags || []).join(',')}" onclick="selectTable('${t.full_name}')">
        <span class="table-dot ${dotClass}"></span>
        <span class="table-name-text">${t.table}</span>
      </li>`;
    });
    html += `</ul></div>`;
  });

  el.innerHTML = html;
}

function toggleDbSection(db) {
  const header = document.querySelector(`.sidebar-section[data-db="${db}"] .sidebar-db-header`);
  const list = document.getElementById(`sidebar-list-${db}`);
  header.classList.toggle("collapsed");
  list.classList.toggle("hidden");
}

function toggleTagFilter(tag) {
  if (activeTagFilters.has(tag)) {
    activeTagFilters.delete(tag);
  } else {
    activeTagFilters.add(tag);
  }
  // Update UI
  document.querySelectorAll(".tag-filter").forEach(el => {
    el.classList.toggle("active", activeTagFilters.has(el.dataset.tag));
  });
  filterSidebar();
}

function filterSidebar() {
  document.querySelectorAll(".sidebar-table-item").forEach(el => {
    if (activeTagFilters.size === 0) {
      el.classList.remove("hidden");
      return;
    }
    const tags = (el.dataset.tags || "").split(",");
    const match = [...activeTagFilters].some(f => tags.includes(f));
    el.classList.toggle("hidden", !match);
  });
}

// ── Table Selection ──────────────────────────────────────────────────────────
function selectTable(fullName) {
  currentTable = fullName;
  currentView = "detail";

  document.querySelectorAll(".sidebar-table-item").forEach(el => {
    el.classList.toggle("active", el.dataset.table === fullName);
  });

  renderTableDetail(DATA.tables[fullName]);
}

// ── Overview ─────────────────────────────────────────────────────────────────
function renderOverview() {
  const main = document.getElementById("main-content");
  const s = DATA.summary;

  let html = `<div class="overview fade-in">`;
  html += `<h2>Database Explorer</h2>`;

  // Cards
  html += `<div class="overview-cards">`;
  DATA.databases.forEach(db => {
    const d = s[db];
    html += `<div class="overview-card card-${db}" onclick="focusDb('${db}')">
      <h3>${db}</h3>
      <div class="card-value">${d.table_count}</div>
      <div class="card-detail">tables &middot; ${fmtRows(d.total_rows)} rows &middot; ${fmtBytes(d.total_bytes)}</div>
    </div>`;
  });
  html += `</div>`;

  // Charts: Top tables by row count
  html += `<div class="charts-grid">`;
  html += renderBarChart("Top Tables by Row Count", getTopTablesByRows(15), "rows");
  html += renderBarChart("Top Tables by Storage Size", getTopTablesByBytes(15), "bytes");
  html += `</div>`;

  // Lineage
  html += renderMainLineage();

  html += `</div>`;
  main.innerHTML = html;

  // Render mermaid
  if (window.mermaid) {
    mermaid.run({ nodes: document.querySelectorAll(".mermaid") });
  }
}

function focusDb(db) {
  // Collapse all, expand target
  DATA.databases.forEach(d => {
    const header = document.querySelector(`.sidebar-section[data-db="${d}"] .sidebar-db-header`);
    const list = document.getElementById(`sidebar-list-${d}`);
    if (d === db) {
      header.classList.remove("collapsed");
      list.classList.remove("hidden");
    } else {
      header.classList.add("collapsed");
      list.classList.add("hidden");
    }
  });
}

function getTopTablesByRows(n) {
  return Object.values(DATA.tables)
    .filter(t => (t.rows || 0) > 0 && !t.table.includes("__dbt_new_data"))
    .sort((a, b) => (b.rows || 0) - (a.rows || 0))
    .slice(0, n);
}

function getTopTablesByBytes(n) {
  return Object.values(DATA.tables)
    .filter(t => (t.bytes || 0) > 0 && !t.table.includes("__dbt_new_data"))
    .sort((a, b) => (b.bytes || 0) - (a.bytes || 0))
    .slice(0, n);
}

function renderBarChart(title, items, metric) {
  if (items.length === 0) return "";
  const maxVal = Math.max(...items.map(t => t[metric] || 0));
  const colors = { gold: "#d29922", bronze: "#db6d28", silver: "#8b949e" };

  let html = `<div class="chart-box"><h3>${title}</h3><div class="chart-bar-container">`;
  items.forEach(t => {
    const val = t[metric] || 0;
    const pct = maxVal > 0 ? (val / maxVal * 100) : 0;
    const color = colors[t.database] || "#58a6ff";
    const display = metric === "rows" ? fmtRows(val) : fmtBytes(val);
    html += `<div class="chart-bar-row">
      <span class="chart-bar-label" onclick="selectTable('${t.full_name}')" title="${t.full_name}">${t.full_name}</span>
      <div class="chart-bar-track"><div class="chart-bar-fill" style="width:${pct}%;background:${color}"></div></div>
      <span class="chart-bar-value">${display}</span>
    </div>`;
  });
  html += `</div></div>`;
  return html;
}

// ── Main Lineage Diagram ─────────────────────────────────────────────────────
function renderMainLineage() {
  let mmd = `graph LR\n`;
  mmd += `  classDef gold fill:#d29922,stroke:#9e6a03,color:#000\n`;
  mmd += `  classDef silver fill:#8b949e,stroke:#6e7681,color:#000\n`;
  mmd += `  classDef bronze fill:#db6d28,stroke:#bd561d,color:#000\n`;

  // Key pipeline
  const edges = [
    ["bronze.SessionEventStore", "silver.stg_user_sessions"],
    ["silver.stg_user_sessions", "silver.user_sessions"],
    ["silver.user_sessions", "silver.user_activity_daily"],
    ["silver.user_activity_daily", "gold.ua_user_lifetime_activity_daily"],

    ["bronze.inAppsStore", "silver.stg_inapp_purchases"],
    ["bronze.ReceiptDataStore", "silver.stg_inapp_purchases"],
    ["bronze.ProductPurchaseStore", "silver.stg_inapp_purchases"],
    ["bronze.ProductPurchaseStore", "silver.stg_inapp_refunds"],
    ["bronze.AppleS2SNotificationsStore", "silver.stg_inapp_refunds"],
    ["silver.stg_inapp_purchases", "silver.inapp_table"],
    ["silver.stg_inapp_refunds", "silver.inapp_table"],
    ["silver.inapp_table", "gold.ua_user_lifetime_activity_daily"],

    ["bronze.RevenueCatStore", "silver.stg_subscription_transactions"],
    ["bronze.AppleS2SNotificationsStore", "silver.stg_subscription_transactions"],
    ["bronze.SubscriptionPurchaseStore", "silver.stg_subscription_transactions"],
    ["silver.stg_subscription_transactions", "silver.subscription_table"],
    ["silver.subscription_table", "gold.ua_user_lifetime_activity_daily"],

    ["bronze.applovin_impression_data", "silver.applovin_ad_revenue_daily"],
    ["silver.applovin_ad_revenue_daily", "silver.applovin_ad_user_daily"],
    ["silver.applovin_ad_user_daily", "gold.ua_user_lifetime_activity_daily"],

    ["bronze.ADJUSTInstallsStoreStore", "silver.users_scd2"],
    ["bronze.ADJUSTReInstallsStoreStore", "silver.users_scd2"],
    ["silver.users_scd2", "gold.ua_user_lifetime_activity_daily"],
  ];

  const nodeIds = new Set();
  edges.forEach(([from, to]) => { nodeIds.add(from); nodeIds.add(to); });

  nodeIds.forEach(id => {
    const safe = id.replace(/\./g, "_");
    const short = id.split(".")[1];
    const db = id.split(".")[0];
    mmd += `  ${safe}["${short}"]\n`;
    mmd += `  class ${safe} ${db}\n`;
  });

  edges.forEach(([from, to]) => {
    mmd += `  ${from.replace(/\./g, "_")} --> ${to.replace(/\./g, "_")}\n`;
  });

  return `<div class="lineage-section">
    <h3>Data Lineage: Bronze -> Silver -> Gold Pipeline</h3>
    <div class="mermaid">${mmd}</div>
  </div>`;
}

// ── Table Detail ─────────────────────────────────────────────────────────────
function renderTableDetail(t) {
  const main = document.getElementById("main-content");
  const dotClass = `dot-${t.database}`;

  let html = `<div class="table-detail fade-in">`;

  // Header
  html += `<div class="table-detail-header">
    <h2><span class="db-dot ${dotClass}"></span>${t.full_name}</h2>
    <div class="table-detail-meta">
      <span><span class="meta-icon">&#9638;</span> ${t.columns.length} columns</span>
      <span><span class="meta-icon">&#9776;</span> ${fmtRows(t.rows)} rows</span>
      <span><span class="meta-icon">&#128190;</span> ${fmtBytes(t.bytes)}</span>
      <span><span class="meta-icon">&#9881;</span> ${t.engine || "?"}</span>
    </div>`;

  // Tags
  if (t.tags && t.tags.length > 0) {
    html += `<div class="tags">`;
    t.tags.forEach(tag => {
      html += `<span class="tag tag-${tag}" onclick="toggleTagFilter('${tag}')">${tag}</span>`;
    });
    html += `</div>`;
  }
  html += `</div>`;

  // Info blocks
  html += `<div class="info-grid">`;

  if (t.purpose) {
    html += `<div class="info-block full-width">
      <div class="info-block-label">Purpose</div>
      <div class="info-block-value">${escHtml(t.purpose)}</div>
    </div>`;
  }

  if (t.time_col) {
    html += `<div class="info-block">
      <div class="info-block-label">Time Filter Column</div>
      <div class="info-block-value">${escHtml(t.time_col)}</div>
      ${t.time_warn ? `<div class="info-block-value warn" style="margin-top:6px">${escHtml(t.time_warn)}</div>` : ""}
    </div>`;
  }

  if (t.revenue) {
    html += `<div class="info-block">
      <div class="info-block-label">Revenue Formula</div>
      <div class="info-block-value" style="font-family:monospace;font-size:12px">${escHtml(t.revenue)}</div>
    </div>`;
  }

  if (t.lineage) {
    html += `<div class="info-block full-width">
      <div class="info-block-label">Lineage</div>
      <div class="info-block-value">${escHtml(t.lineage)}</div>
    </div>`;
  }

  html += `</div>`;

  // Lineage upstream / downstream
  if ((t.lineage_upstream && t.lineage_upstream.length) || (t.lineage_downstream && t.lineage_downstream.length)) {
    html += renderTableLineage(t);
  }

  // Notes
  if (t.notes && t.notes.length > 0) {
    html += `<div style="margin-bottom:24px">`;
    t.notes.forEach(n => {
      html += `<div class="note-item">${escHtml(n)}</div>`;
    });
    html += `</div>`;
  }

  // Schema table
  html += `<div class="schema-section">
    <h3>Schema (${t.columns.length} columns)</h3>
    <table class="schema-table">
    <thead><tr><th>#</th><th>Column</th><th>Type</th><th>Comment</th></tr></thead>
    <tbody>`;

  t.columns.forEach((col, i) => {
    html += `<tr>
      <td class="col-num">${i + 1}</td>
      <td class="col-name">${escHtml(col.name)}</td>
      <td class="col-type">${escHtml(col.type)}</td>
      <td class="col-comment">${escHtml(col.comment)}</td>
    </tr>`;
  });

  html += `</tbody></table></div>`;
  html += `</div>`;

  main.innerHTML = html;

  // Render mermaid if present
  if (window.mermaid) {
    mermaid.run({ nodes: document.querySelectorAll(".mermaid") });
  }
}

function renderTableLineage(t) {
  let mmd = `graph LR\n`;
  mmd += `  classDef current fill:#58a6ff,stroke:#1f6feb,color:#000\n`;
  mmd += `  classDef gold fill:#d29922,stroke:#9e6a03,color:#000\n`;
  mmd += `  classDef silver fill:#8b949e,stroke:#6e7681,color:#000\n`;
  mmd += `  classDef bronze fill:#db6d28,stroke:#bd561d,color:#000\n`;

  const self = t.full_name.replace(/\./g, "_");
  const selfShort = t.table;
  mmd += `  ${self}["${selfShort}"]\n`;
  mmd += `  class ${self} current\n`;

  (t.lineage_upstream || []).forEach(up => {
    const safe = up.replace(/\./g, "_");
    const short = up.split(".")[1];
    const db = up.split(".")[0];
    mmd += `  ${safe}["${short}"]\n`;
    mmd += `  class ${safe} ${db}\n`;
    mmd += `  ${safe} --> ${self}\n`;
  });

  (t.lineage_downstream || []).forEach(down => {
    const safe = down.replace(/\./g, "_");
    const short = down.split(".")[1];
    const db = down.split(".")[0];
    mmd += `  ${safe}["${short}"]\n`;
    mmd += `  class ${safe} ${db}\n`;
    mmd += `  ${self} --> ${safe}\n`;
  });

  return `<div class="lineage-section" style="margin-bottom:24px">
    <h3>Data Lineage</h3>
    <div class="mermaid">${mmd}</div>
  </div>`;
}

// ── Search ───────────────────────────────────────────────────────────────────
function setupSearch() {
  const input = document.getElementById("search-input");
  let debounceTimer;

  input.addEventListener("input", () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      const q = input.value.trim().toLowerCase();
      if (q.length < 2) {
        if (currentView === "search") {
          renderOverview();
          currentView = "overview";
        }
        return;
      }
      performSearch(q);
    }, 200);
  });

  input.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      input.value = "";
      input.blur();
      if (currentView === "search") {
        renderOverview();
        currentView = "overview";
      }
    }
  });
}

function performSearch(query) {
  currentView = "search";
  const results = [];

  Object.values(DATA.tables).forEach(t => {
    let score = 0;
    let matches = [];

    // Table name match
    if (t.full_name.toLowerCase().includes(query)) {
      score += 100;
      matches.push({ type: "table", text: t.full_name });
    }

    // Purpose match
    if (t.purpose && t.purpose.toLowerCase().includes(query)) {
      score += 50;
      matches.push({ type: "purpose", text: t.purpose });
    }

    // Tag match
    (t.tags || []).forEach(tag => {
      if (tag.toLowerCase().includes(query)) {
        score += 30;
        matches.push({ type: "tag", text: tag });
      }
    });

    // Column name match
    t.columns.forEach(col => {
      if (col.name.toLowerCase().includes(query)) {
        score += 20;
        matches.push({ type: "column", text: `${col.name} (${col.type})` });
      }
    });

    // Column type match
    t.columns.forEach(col => {
      if (col.type.toLowerCase().includes(query)) {
        score += 5;
        matches.push({ type: "type", text: `${col.name}: ${col.type}` });
      }
    });

    // Notes match
    (t.notes || []).forEach(note => {
      if (note.toLowerCase().includes(query)) {
        score += 10;
        matches.push({ type: "note", text: note });
      }
    });

    if (score > 0) {
      results.push({ table: t, score, matches });
    }
  });

  results.sort((a, b) => b.score - a.score);
  renderSearchResults(query, results);
}

function renderSearchResults(query, results) {
  const main = document.getElementById("main-content");
  let html = `<div class="search-results fade-in">`;
  html += `<h2>${results.length} result${results.length !== 1 ? "s" : ""} for "${escHtml(query)}"</h2>`;

  if (results.length === 0) {
    html += `<p style="color:var(--text-muted)">No tables or columns match your search.</p>`;
  }

  results.slice(0, 50).forEach(r => {
    const t = r.table;
    const dotClass = `dot-${t.database}`;
    // Deduplicate matches
    const seen = new Set();
    const uniqueMatches = r.matches.filter(m => {
      const key = m.type + ":" + m.text;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 4);

    html += `<div class="search-result-item" onclick="selectTable('${t.full_name}')">
      <div class="search-result-table"><span class="table-dot ${dotClass}" style="display:inline-block;vertical-align:middle;margin-right:6px"></span>${t.full_name}</div>`;

    if (t.purpose) {
      html += `<div class="search-result-purpose">${escHtml(t.purpose)}</div>`;
    }

    uniqueMatches.forEach(m => {
      const highlighted = escHtml(m.text).replace(
        new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, "gi"),
        "<mark>$1</mark>"
      );
      html += `<div class="search-result-match">${m.type}: ${highlighted}</div>`;
    });

    if (t.tags && t.tags.length) {
      html += `<div class="tags" style="margin-top:6px">`;
      t.tags.forEach(tag => {
        html += `<span class="tag tag-${tag}" style="font-size:10px;padding:1px 6px">${tag}</span>`;
      });
      html += `</div>`;
    }

    html += `</div>`;
  });

  html += `</div>`;
  main.innerHTML = html;
}

// ── Keyboard ─────────────────────────────────────────────────────────────────
function setupKeyboard() {
  document.addEventListener("keydown", (e) => {
    // Ctrl/Cmd + K to focus search
    if ((e.ctrlKey || e.metaKey) && e.key === "k") {
      e.preventDefault();
      document.getElementById("search-input").focus();
    }
    // Escape to go back to overview
    if (e.key === "Escape" && currentView !== "overview") {
      document.getElementById("search-input").value = "";
      renderOverview();
      currentView = "overview";
      document.querySelectorAll(".sidebar-table-item").forEach(el => el.classList.remove("active"));
    }
  });
}

// ── Home navigation ──────────────────────────────────────────────────────────
function goHome() {
  currentView = "overview";
  currentTable = null;
  document.getElementById("search-input").value = "";
  document.querySelectorAll(".sidebar-table-item").forEach(el => el.classList.remove("active"));
  renderOverview();
}
