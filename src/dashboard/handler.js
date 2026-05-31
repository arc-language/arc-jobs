'use strict'

// Dashboard handler — serves /_arc/jobs admin UI and JSON API.
// Called from emitted server.js: _arc_jobs_handle(req, _queues, _schedules)
// Returns a Response (Bun-compatible), or null if path doesn't match.

const { nextFireTime } = require('../core/scheduler')

// ── SSE broadcaster ─────────────────────────────────────────────────────────

const _sseClients = new Set()  // ReadableStreamDefaultController
let _sseBroadcastInterval = null
let _sseBroadcastQueues = null

function _ensureSharedBroadcast(queues) {
  _sseBroadcastQueues = queues
  if (_sseBroadcastInterval) return
  _sseBroadcastInterval = setInterval(async () => {
    if (_sseClients.size === 0) return
    const stats = await _collectStats(_sseBroadcastQueues).catch(() => ({}))
    const firstQueueStats = Object.values(stats)[0] ?? {}
    _sseSend({ type: 'tick', ...firstQueueStats })
  }, 2000)
}

function _sseSend(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`
  for (const client of _sseClients) {
    try { client.enqueue(msg) } catch (_) { _sseClients.delete(client) }
  }
}

// Progress listeners keyed by job id
const _progressListeners = new Map()  // jobId → Set<controller>

function broadcastProgress(jobId, pct, meta = {}) {
  const msg = `data: ${JSON.stringify({ jobId, pct, ...meta })}\n\n`
  const listeners = _progressListeners.get(jobId)
  if (listeners) {
    for (const c of listeners) {
      try { c.enqueue(msg) } catch (_) { listeners.delete(c) }
    }
    if (pct >= 100 || listeners.size === 0) _progressListeners.delete(jobId)
  }
}

// ── HTML asset (self-contained, no external deps) ───────────────────────────

const _DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>arc-jobs dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0 }
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --text: #e2e8f0; --muted: #64748b; --accent: #6366f1;
    --green: #22c55e; --yellow: #eab308; --red: #ef4444; --blue: #3b82f6;
    --font: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  }
  body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; line-height: 1.5 }
  a { color: var(--accent); text-decoration: none }

  /* Layout */
  .shell { display: grid; grid-template-columns: 200px 1fr; min-height: 100vh }
  .sidebar { background: var(--surface); border-right: 1px solid var(--border); padding: 24px 16px; display: flex; flex-direction: column; gap: 4px }
  .sidebar-logo { font-weight: 700; font-size: 16px; color: var(--accent); margin-bottom: 16px; padding: 0 8px }
  .nav-item { padding: 6px 12px; border-radius: 6px; cursor: pointer; color: var(--muted); transition: all .15s; white-space: nowrap; background: none; border: none; font-size: 14px; font-family: var(--font); text-align: left; width: 100% }
  .nav-item:hover, .nav-item.active { background: rgba(99,102,241,.15); color: var(--text) }
  .main { padding: 32px; overflow: auto }
  .page { display: none }
  .page.active { display: block }

  /* Stats row */
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 28px }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px }
  .stat-value { font-size: 28px; font-weight: 700; line-height: 1 }
  .stat-label { color: var(--muted); font-size: 12px; margin-top: 4px }
  .stat.green .stat-value { color: var(--green) }
  .stat.yellow .stat-value { color: var(--yellow) }
  .stat.red .stat-value { color: var(--red) }
  .stat.blue .stat-value { color: var(--blue) }

  /* Queue tabs */
  .tab-bar { display: flex; gap: 4px; border-bottom: 1px solid var(--border); margin-bottom: 20px }
  .tab { padding: 8px 16px; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent; transition: all .15s; margin-bottom: -1px; background: none; border-top: none; border-left: none; border-right: none; font-size: 14px; font-family: var(--font) }
  .tab:hover { color: var(--text) }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent) }

  /* Table */
  table { width: 100%; border-collapse: collapse }
  thead th { text-align: left; color: var(--muted); font-size: 12px; font-weight: 500; padding: 8px 12px; border-bottom: 1px solid var(--border); text-transform: uppercase; letter-spacing: .05em }
  tbody td { padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: middle }
  tbody tr:hover { background: rgba(255,255,255,.02) }
  .empty { color: var(--muted); text-align: center; padding: 32px; font-size: 13px }

  /* Badges */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .04em }
  .badge-pending  { background: rgba(234,179,8,.15);   color: var(--yellow) }
  .badge-running  { background: rgba(59,130,246,.15);  color: var(--blue) }
  .badge-completed{ background: rgba(34,197,94,.15);   color: var(--green) }
  .badge-failed   { background: rgba(239,68,68,.15);   color: var(--red) }
  .badge-cancelled{ background: rgba(100,116,139,.15); color: var(--muted) }
  .badge-high  { background: rgba(239,68,68,.1);  color: var(--red) }
  .badge-normal{ background: rgba(100,116,139,.1); color: var(--muted) }
  .badge-low   { background: rgba(34,197,94,.1);  color: var(--green) }

  /* Progress bar */
  .prog-wrap { background: var(--border); border-radius: 99px; height: 6px; min-width: 80px; overflow: hidden }
  .prog-fill { height: 100%; background: var(--accent); border-radius: 99px; transition: width .3s }

  /* Button */
  .btn { padding: 4px 12px; border-radius: 5px; border: 1px solid var(--border); background: var(--surface); color: var(--text); cursor: pointer; font-size: 12px; transition: all .15s }
  .btn:hover { border-color: var(--accent); color: var(--accent) }
  .btn-danger:hover { border-color: var(--red); color: var(--red) }
  .btn-primary { background: var(--accent); border-color: var(--accent); color: #fff }
  .btn-primary:hover { background: #4f52e8; color: #fff }

  /* Code mono */
  .mono { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px; color: var(--muted) }

  /* Refresh indicator */
  .top-bar { display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px }
  .top-bar h2 { font-size: 18px; font-weight: 600 }
  .refresh-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; animation: pulse 2s infinite }
  @keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: .4 } }
  @media (prefers-reduced-motion: reduce) { .refresh-dot { animation: none } }
  .status-bar { font-size: 11px; color: var(--muted); display: flex; align-items: center; gap: 6px }

  /* Sections */
  .section-title { font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; margin-bottom: 12px; margin-top: 24px }

  /* Modal-ish error toast */
  #toast { position: fixed; bottom: 24px; right: 24px; background: var(--red); color: #fff; padding: 10px 18px; border-radius: 8px; font-size: 13px; display: none; z-index: 999; display: flex; align-items: center; gap: 10px }
  #toast-dismiss { background: none; border: none; color: #fff; font-size: 16px; cursor: pointer; padding: 0; line-height: 1; opacity: .8 }
  #toast-dismiss:hover { opacity: 1 }

  /* Skip-to-content */
  .skip-link { position: absolute; top: -48px; left: 0; background: var(--accent); color: #fff; padding: 8px 16px; text-decoration: none; border-radius: 4px; z-index: 1000; font-size: 14px }
  .skip-link:focus { top: 8px; left: 8px }
</style>
</head>
<body>
<a href="#main-content" class="skip-link">Skip to main content</a>
<div class="shell">
  <nav class="sidebar" aria-label="Dashboard navigation">
    <div class="sidebar-logo">arc-jobs</div>
    <button class="nav-item active" data-page="overview" aria-current="page">Overview</button>
    <button class="nav-item" data-page="jobs">Active Jobs</button>
    <button class="nav-item" data-page="schedules">Schedules</button>
    <button class="nav-item" data-page="locks">Locks</button>
    <button class="nav-item" data-page="dlq">Dead Letter</button>
  </nav>
  <main class="main" id="main-content">

    <!-- Overview -->
    <div class="page active" id="page-overview">
      <div class="top-bar">
        <h2>Overview</h2>
        <div class="status-bar"><span class="refresh-dot" aria-hidden="true"></span> live · 2s refresh</div>
      </div>
      <div id="queue-tabs" class="tab-bar"></div>
      <div class="stats" id="stats-row">
        <div class="stat blue"><div class="stat-value" id="stat-pending">–</div><div class="stat-label">Pending</div></div>
        <div class="stat yellow"><div class="stat-value" id="stat-running">–</div><div class="stat-label">Running</div></div>
        <div class="stat green"><div class="stat-value" id="stat-completed">–</div><div class="stat-label">Completed</div></div>
        <div class="stat red"><div class="stat-value" id="stat-failed">–</div><div class="stat-label">Failed</div></div>
      </div>
    </div>

    <!-- Active Jobs -->
    <div class="page" id="page-jobs">
      <div class="top-bar"><h2>Active Jobs</h2><div class="status-bar"><span class="refresh-dot" aria-hidden="true"></span> live</div></div>
      <div id="queue-tabs-jobs" class="tab-bar"></div>
      <table id="jobs-table">
        <thead><tr><th scope="col">Job</th><th scope="col">Args</th><th scope="col">Priority</th><th scope="col">Status</th><th scope="col">Progress</th><th scope="col">Age</th><th scope="col"></th></tr></thead>
        <tbody id="jobs-body"><tr><td colspan="7" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>

    <!-- Schedules -->
    <div class="page" id="page-schedules">
      <div class="top-bar"><h2>Schedules</h2></div>
      <table id="sched-table">
        <thead><tr><th scope="col">Job</th><th scope="col">Cron</th><th scope="col">Queue</th><th scope="col">Next Fire</th></tr></thead>
        <tbody id="sched-body"><tr><td colspan="4" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>

    <!-- Locks -->
    <div class="page" id="page-locks">
      <div class="top-bar"><h2>Active @unique Locks</h2></div>
      <table id="locks-table">
        <thead><tr><th scope="col">Key</th><th scope="col">TTL Remaining</th><th scope="col"></th></tr></thead>
        <tbody id="locks-body"><tr><td colspan="3" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>

    <!-- DLQ -->
    <div class="page" id="page-dlq">
      <div class="top-bar">
        <h2>Dead Letter Queue</h2>
        <button class="btn btn-primary" onclick="replayAll()">Replay All</button>
      </div>
      <div id="queue-tabs-dlq" class="tab-bar"></div>
      <table id="dlq-table">
        <thead><tr><th scope="col">Job</th><th scope="col">Args</th><th scope="col">Error</th><th scope="col">Failed At</th><th scope="col"></th></tr></thead>
        <tbody id="dlq-body"><tr><td colspan="5" class="empty">No failed jobs</td></tr></tbody>
      </table>
    </div>

  </main>
</div>
<div id="toast" role="alert" aria-live="assertive" aria-atomic="true" style="display:none"><span id="toast-msg"></span><button id="toast-dismiss" onclick="document.getElementById('toast').style.display='none'" aria-label="Dismiss notification">×</button></div>

<script>
const BASE = '/_arc/jobs'
let _queues = []
let _activeQueue = null
let _activePage = 'overview'

// ── Navigation ───────────────────────────────────────────────────────────────

document.querySelectorAll('.nav-item').forEach(el => {
  el.addEventListener('click', () => {
    document.querySelectorAll('.nav-item').forEach(n => { n.classList.remove('active'); n.removeAttribute('aria-current') })
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'))
    el.classList.add('active')
    el.setAttribute('aria-current', 'page')
    _activePage = el.dataset.page
    document.getElementById('page-' + _activePage).classList.add('active')
    refresh()
  })
})

// ── Helpers ──────────────────────────────────────────────────────────────────

function badge(cls, text) { return '<span class="badge badge-' + esc(cls) + '">' + esc(String(text)) + '</span>' }
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') }
function age(ms) {
  const s = Math.floor((Date.now() - ms) / 1000)
  if (s < 60) return s + 's'
  if (s < 3600) return Math.floor(s/60) + 'm'
  return Math.floor(s/3600) + 'h'
}
function toast(msg, isErr = true) {
  const el = document.getElementById('toast')
  document.getElementById('toast-msg').textContent = msg
  el.style.background = isErr ? 'var(--red)' : 'var(--green)'
  el.style.display = 'flex'
  setTimeout(() => el.style.display = 'none', 5000)
}
function renderTabs(containerId, onSelect) {
  const bar = document.getElementById(containerId)
  if (!bar || !_queues.length) return
  if (bar.children.length === _queues.length) return
  bar.setAttribute('role', 'tablist')
  bar.innerHTML = ''
  _queues.forEach((q, i) => {
    const t = document.createElement('button')
    t.className = 'tab' + (i === 0 ? ' active' : '')
    t.setAttribute('role', 'tab')
    t.setAttribute('aria-selected', i === 0 ? 'true' : 'false')
    t.setAttribute('tabindex', i === 0 ? '0' : '-1')
    t.textContent = q
    t.addEventListener('click', () => {
      bar.querySelectorAll('.tab').forEach(x => { x.classList.remove('active'); x.setAttribute('aria-selected', 'false'); x.setAttribute('tabindex', '-1') })
      t.classList.add('active')
      t.setAttribute('aria-selected', 'true')
      t.setAttribute('tabindex', '0')
      _activeQueue = q
      onSelect(q)
    })
    bar.appendChild(t)
  })
  if (!bar._arcTabsKeydown) {
    bar.addEventListener('keydown', e => {
      const tabs = [...bar.querySelectorAll('.tab')]
      const idx = tabs.indexOf(document.activeElement)
      if (idx === -1) return
      if (e.key === 'ArrowRight') { e.preventDefault(); tabs[(idx + 1) % tabs.length].focus() }
      if (e.key === 'ArrowLeft')  { e.preventDefault(); tabs[(idx - 1 + tabs.length) % tabs.length].focus() }
      if (e.key === 'Home')       { e.preventDefault(); tabs[0].focus() }
      if (e.key === 'End')        { e.preventDefault(); tabs[tabs.length - 1].focus() }
    })
    bar._arcTabsKeydown = true
  }
  if (!_activeQueue) _activeQueue = _queues[0]
}

// ── API ──────────────────────────────────────────────────────────────────────

async function api(path, opts) {
  try {
    const r = await fetch(BASE + path, opts)
    if (!r.ok) throw new Error(await r.text())
    return r.json()
  } catch (e) { toast(e.message); return null }
}

async function refresh() {
  const data = await api('/api/overview')
  if (!data) return
  _queues = data.queues.map(q => q.name)
  renderTabs('queue-tabs', loadOverview)
  renderTabs('queue-tabs-jobs', loadJobs)
  renderTabs('queue-tabs-dlq', loadDlq)
  const activeQ = _activeQueue ?? _queues[0]
  if (_activePage === 'overview') _renderOverviewData(data.queues.find(q => q.name === activeQ))
  if (_activePage === 'jobs')     loadJobs(activeQ)
  if (_activePage === 'schedules') loadSchedules()
  if (_activePage === 'locks')    loadLocks()
  if (_activePage === 'dlq')      loadDlq(activeQ)
}

function _renderOverviewData(q) {
  if (!q) return
  document.getElementById('stat-pending').textContent   = q.pending
  document.getElementById('stat-running').textContent   = q.running
  document.getElementById('stat-completed').textContent = q.completed
  document.getElementById('stat-failed').textContent    = q.failed
}

async function loadOverview(qName) {
  const data = await api('/api/overview')
  _renderOverviewData(data?.queues?.find(x => x.name === qName))
}

async function loadJobs(qName) {
  const data = await api('/api/jobs?queue=' + (qName ?? ''))
  const tbody = document.getElementById('jobs-body')
  if (!data?.jobs?.length) { tbody.innerHTML = '<tr><td colspan="7" class="empty">No active jobs</td></tr>'; return }
  tbody.innerHTML = data.jobs.map(j => {
    const pct = j.progress != null ? Math.round(Math.min(100, j.progress)) : null
    const prog = pct != null
      ? '<div class="prog-wrap" role="progressbar" aria-valuenow="' + pct + '" aria-valuemin="0" aria-valuemax="100" aria-label="Job progress ' + pct + '%"><div class="prog-fill" style="width:' + pct + '%"></div></div> ' + pct + '%'
      : '–'
    return '<tr>' +
      '<td><span class="mono">' + esc(j.name) + '</span></td>' +
      '<td><span class="mono" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;display:block">' + esc(JSON.stringify(j.args ?? [])) + '</span></td>' +
      '<td>' + badge(j.priority ?? 'normal', j.priority ?? 'normal') + '</td>' +
      '<td>' + badge(j.status, j.status) + '</td>' +
      '<td>' + prog + '</td>' +
      '<td>' + (j.started_at ? age(j.started_at) : '–') + '</td>' +
      '<td><button class="btn btn-danger" onclick="cancelJob(' + JSON.stringify(j.id) + ')">Cancel</button></td>' +
      '</tr>'
  }).join('')
}

async function loadSchedules() {
  const data = await api('/api/schedules')
  const tbody = document.getElementById('sched-body')
  if (!data?.schedules?.length) { tbody.innerHTML = '<tr><td colspan="4" class="empty">No scheduled jobs</td></tr>'; return }
  tbody.innerHTML = data.schedules.map(s =>
    '<tr>' +
    '<td><span class="mono">' + esc(s.job) + '</span></td>' +
    '<td><span class="mono">' + esc(s.cron) + '</span></td>' +
    '<td>' + esc(s.queue) + '</td>' +
    '<td>' + esc(s.next) + '</td>' +
    '</tr>'
  ).join('')
}

async function loadLocks() {
  const data = await api('/api/locks')
  const tbody = document.getElementById('locks-body')
  if (!data?.locks?.length) { tbody.innerHTML = '<tr><td colspan="3" class="empty">No active locks</td></tr>'; return }
  tbody.innerHTML = data.locks.map(l => {
    const ttl = Math.max(0, Math.round((l.expiresAt - Date.now()) / 1000))
    return '<tr>' +
      '<td><span class="mono">' + esc(l.key) + '</span></td>' +
      '<td>' + ttl + 's</td>' +
      '<td><button class="btn btn-danger" onclick="unlockKey(' + JSON.stringify(l.key) + ')">Force Unlock</button></td>' +
      '</tr>'
  }).join('')
}

async function loadDlq(qName) {
  const data = await api('/api/dlq?queue=' + (qName ?? ''))
  const tbody = document.getElementById('dlq-body')
  if (!data?.jobs?.length) { tbody.innerHTML = '<tr><td colspan="5" class="empty">No failed jobs 🎉</td></tr>'; return }
  tbody.innerHTML = data.jobs.map(j =>
    '<tr>' +
    '<td><span class="mono">' + esc(j.name) + '</span></td>' +
    '<td><span class="mono">' + esc(JSON.stringify(j.args ?? [])) + '</span></td>' +
    '<td style="color:var(--red);max-width:240px;overflow:hidden;text-overflow:ellipsis">' + esc(j.error ?? '') + '</td>' +
    '<td class="mono">' + esc(j.failedAt ? new Date(j.failedAt).toLocaleString() : '–') + '</td>' +
    '<td><button class="btn" onclick="replayJob(' + JSON.stringify(j.id) + ')">Replay</button></td>' +
    '</tr>'
  ).join('')
}

// ── Actions ──────────────────────────────────────────────────────────────────

async function cancelJob(id) {
  if (!confirm('Cancel this job? This cannot be undone.')) return
  const res = await api('/api/jobs/' + id + '/cancel', { method: 'POST' })
  if (res) { toast('Job cancelled', false); setTimeout(refresh, 200) }
}

async function replayJob(id) {
  const res = await api('/api/dlq/' + id + '/replay', { method: 'POST' })
  if (res) { toast('Job replayed', false); setTimeout(refresh, 200) }
}

async function replayAll() {
  if (!confirm('Replay ALL dead jobs? This will re-enqueue every failed job.')) return
  const q = _activeQueue ?? _queues[0] ?? ''
  const res = await api('/api/dlq/replay?queue=' + q, { method: 'POST' })
  if (res) { toast('All dead jobs replayed', false); setTimeout(refresh, 300) }
}

async function unlockKey(key) {
  if (!confirm('Force-release lock "' + key + '"? The job holding it may misbehave.')) return
  const res = await api('/api/locks/' + encodeURIComponent(key), { method: 'DELETE' })
  if (res) { toast('Lock released', false); setTimeout(loadLocks, 200) }
}

// ── SSE live updates ─────────────────────────────────────────────────────────

let _sseConnected = false

function connectSSE() {
  const es = new EventSource(BASE + '/events')
  es.onmessage = e => {
    _sseConnected = true
    try {
      const d = JSON.parse(e.data)
      if (d.type === 'tick') {
        document.getElementById('stat-pending').textContent   = d.pending   ?? '–'
        document.getElementById('stat-running').textContent   = d.running   ?? '–'
        document.getElementById('stat-completed').textContent = d.completed ?? '–'
        document.getElementById('stat-failed').textContent    = d.failed    ?? '–'
      }
    } catch (_) {}
  }
  es.onerror = () => { _sseConnected = false; setTimeout(connectSSE, 3000) }
}

// ── Boot ─────────────────────────────────────────────────────────────────────

refresh()
connectSSE()
// Only poll when SSE is disconnected — SSE tick covers live stats when healthy
setInterval(() => { if (!_sseConnected) refresh() }, 2000)
</script>
</body>
</html>`

// ── Auth guard ───────────────────────────────────────────────────────────────

async function _checkAuth(req, auth) {
  if (!auth) {
    // Fallback: secret token via env var
    const secret = process.env.ARC_JOBS_SECRET
    if (!secret) return true  // no guard configured — allow (warn at startup, not per-request)
    const authHeader = req.headers.get('authorization') ?? ''
    const token = authHeader.replace(/^Bearer\s+/i, '') || (new URL(req.url).searchParams.get('token') ?? '')
    return token === secret
  }
  return auth(req)
}

const _401_HTML = '<!DOCTYPE html><html><head><title>Unauthorized</title></head><body style="font-family:sans-serif;text-align:center;padding:80px;background:#0f1117;color:#e2e8f0"><h2 style="color:#ef4444">401 Unauthorized</h2><p>You must be an admin to access the arc-jobs dashboard.</p></body></html>'

let _startupWarnedUnprotected = false

// ── Request handler ──────────────────────────────────────────────────────────

/**
 * @param {Request} req
 * @param {Record<string, import('../types').Queue>} queues
 * @param {Array} schedules
 * @param {{ auth?: (req: Request) => Promise<boolean> }} opts
 *   `auth`: async function that returns true if the request is authorized.
 *   In Arc apps, pass `async (req) => { const s = await auth.session(req); return s?.role === 'admin' || s?.role === 'superuser' }`.
 *   Falls back to ARC_JOBS_SECRET env var token check if omitted.
 */
// ── Route handlers ────────────────────────────────────────────────────────────

function _routeDashboard() {
  return new Response(_DASHBOARD_HTML, {
    headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' },
  })
}

function _routeEvents(queues) {
  _ensureSharedBroadcast(queues)
  let _sseCtrl
  const stream = new ReadableStream({
    start(controller) {
      _sseCtrl = controller
      _sseClients.add(controller)
      _collectStats(queues).then(stats => {
        const firstQueueStats = Object.values(stats)[0] ?? {}
        try { controller.enqueue(`data: ${JSON.stringify({ type: 'tick', ...firstQueueStats })}\n\n`) } catch (_) {}
      }).catch(() => {})
    },
    cancel() { _sseClients.delete(_sseCtrl) },
  })
  return new Response(stream, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' },
  })
}

function _routeProgress(jobId) {
  let _ctrl
  const stream = new ReadableStream({
    start(controller) {
      _ctrl = controller
      let set = _progressListeners.get(jobId)
      if (!set) { set = new Set(); _progressListeners.set(jobId, set) }
      set.add(controller)
    },
    cancel() {
      const set = _progressListeners.get(jobId)
      if (set && _ctrl) {
        set.delete(_ctrl)
        if (set.size === 0) _progressListeners.delete(jobId)
      }
    },
  })
  return new Response(stream, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' } })
}

async function _routeOverview(queues) {
  const stats = await _collectStats(queues)
  return _json({ queues: Object.entries(stats).map(([name, s]) => ({ name, ...s })) })
}

function _routeSchedules(schedules) {
  return _json({ schedules: schedules.map(s => ({
    job: s.job, cron: s.cron, queue: s.queue ?? 'default', next: _fmtNext(nextFireTime(s.cron)),
  })) })
}

async function _routeLocks(queues) {
  return _json({ locks: await _collectLocks(queues) })
}

async function _routeJobs(url, queues) {
  const qName = url.searchParams.get('queue') ?? Object.keys(queues)[0]
  const adapter = queues[qName]?._adapter
  if (!adapter) return _json({ jobs: [] })
  return _json({ jobs: await _listActiveJobs(adapter) })
}

async function _routeCancelJob(id, queues) {
  for (const q of Object.values(queues)) {
    try { await q.cancel(id) } catch (e) {
      console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', event: 'cancel_error', id, error: e?.message ?? String(e) }))
    }
  }
  return _json({ ok: true })
}

async function _routeDlq(url, queues) {
  const qName = url.searchParams.get('queue') ?? Object.keys(queues)[0]
  const adapter = queues[qName]?._adapter
  return _json({ jobs: adapter ? await adapter.dead() : [] })
}

async function _routeReplayDeadAll(url, queues) {
  const qName = url.searchParams.get('queue') ?? Object.keys(queues)[0]
  const q = queues[qName]
  return _json({ replayed: q ? await q.replayDead() : 0 })
}

async function _routeReplayOne(id, queues) {
  let found = false
  for (const q of Object.values(queues)) {
    try { if (await q._adapter.replayOne(id)) { found = true; break } } catch (e) {
      console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', event: 'replay_one_error', id, error: e?.message ?? String(e) }))
    }
  }
  if (!found) return _json({ error: 'Job not found in dead letter queue' }, 404)
  return _json({ ok: true })
}

async function _routeDeleteLock(key, queues) {
  for (const q of Object.values(queues)) {
    try { await q.releaseLock(key) } catch (e) {
      console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', event: 'release_lock_error', key, error: e?.message ?? String(e) }))
    }
  }
  return _json({ ok: true })
}

async function arcJobsHandle(req, queues = {}, schedules = [], opts = {}) {
  if (!_startupWarnedUnprotected && !opts.auth && !process.env.ARC_JOBS_SECRET) {
    _startupWarnedUnprotected = true
    console.warn(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', event: 'jobs_dashboard_unprotected', msg: 'arc-jobs dashboard has no auth configured. Set opts.auth or ARC_JOBS_SECRET env var.' }))
  }

  const url = new URL(req.url)
  if (!url.pathname.startsWith('/_arc/jobs')) return null

  const sub = url.pathname.slice('/_arc/jobs'.length) || '/'
  const method = req.method.toUpperCase()

  if (!sub.match(/^\/api\/jobs\/[^/]+\/progress$/)) {
    const ok = await _checkAuth(req, opts.auth)
    if (!ok) {
      return sub === '/' || sub === ''
        ? new Response(_401_HTML, { status: 401, headers: { 'Content-Type': 'text/html; charset=utf-8' } })
        : new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' } })
    }
  }

  if (sub === '/' || sub === '')                    return _routeDashboard()
  if (sub === '/events')                            return _routeEvents(queues)
  if (sub === '/api/overview')                      return _routeOverview(queues)
  if (sub === '/api/schedules')                     return _routeSchedules(schedules)
  if (sub === '/api/locks')                         return _routeLocks(queues)

  const progMatch      = sub.match(/^\/api\/jobs\/([^/]+)\/progress$/)
  const jobsMatch      = sub.match(/^\/api\/jobs(\?.*)?$/)
  const cancelMatch    = sub.match(/^\/api\/jobs\/([^/]+)\/cancel$/)
  const dlqReplayAll   = sub.match(/^\/api\/dlq\/replay$/)
  const dlqReplayOne   = sub.match(/^\/api\/dlq\/([^/]+)\/replay$/)
  const dlqMatch       = sub.match(/^\/api\/dlq(\?.*)?$/)
  const lockDelete     = sub.match(/^\/api\/locks\/(.+)$/)

  if (progMatch    && method === 'GET')    return _routeProgress(progMatch[1])
  if (jobsMatch    && method === 'GET')    return _routeJobs(url, queues)
  if (cancelMatch  && method === 'POST')   return _routeCancelJob(cancelMatch[1], queues)
  if (dlqReplayAll && method === 'POST')   return _routeReplayDeadAll(url, queues)
  if (dlqReplayOne && method === 'POST')   return _routeReplayOne(dlqReplayOne[1], queues)
  if (dlqMatch     && method === 'GET')    return _routeDlq(url, queues)
  if (lockDelete   && method === 'DELETE') return _routeDeleteLock(decodeURIComponent(lockDelete[1]), queues)

  return new Response('Not found', { status: 404 })
}

// ── Internal helpers ─────────────────────────────────────────────────────────

function _json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

async function _collectStats(queues) {
  const stats = {}
  for (const [name, q] of Object.entries(queues)) {
    try {
      stats[name] = await q._adapter.stats()
    } catch (_) {
      stats[name] = { pending: 0, running: 0, completed: 0, failed: 0 }
    }
  }
  return stats
}

async function _listActiveJobs(adapter) {
  try {
    const jobs = await adapter.listActive()
    return jobs.map(j => ({ ...j, priority: _scoreToPriority(j.priority) }))
  } catch (e) {
    console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', event: 'list_active_jobs_error', error: e?.message ?? String(e) }))
    return []
  }
}

async function _collectLocks(queues) {
  const locks = []
  for (const [, q] of Object.entries(queues)) {
    try { locks.push(...await q._adapter.listLocks()) } catch (_) {}
  }
  return locks
}

function _scoreToPriority(score) {
  if (score >= 10) return 'high'
  if (score <= 1) return 'low'
  return 'normal'
}

function _tryParse(s) {
  try { return JSON.parse(s) } catch (_) { return s }
}

function _fmtNext(d) {
  if (!d) return 'unknown'
  return d.toLocaleString()
}

module.exports = { arcJobsHandle, broadcastProgress, _sseSend }
