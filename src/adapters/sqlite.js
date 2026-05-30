'use strict'

const { BaseAdapter, _PRIORITY } = require('./base')

// SQLite adapter — persistent, zero external dependencies.
// Uses bun:sqlite (Bun) or better-sqlite3 (Node.js).
// Reuses Arc's existing _db connection when passed via opts.db.
// Handles ~15k jobs/sec in WAL mode — right for most production apps.

const _DDL = `
CREATE TABLE IF NOT EXISTS _arc_jobs (
  id TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  name TEXT NOT NULL,
  args TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 5,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  scheduled_at INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  started_at INTEGER,
  completed_at INTEGER,
  error TEXT,
  progress REAL DEFAULT 0,
  lock_key TEXT,
  locked_until INTEGER,
  created_at INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE INDEX IF NOT EXISTS _arc_jobs_queue_claim
  ON _arc_jobs(queue, status, priority DESC, scheduled_at ASC);
CREATE UNIQUE INDEX IF NOT EXISTS _arc_jobs_lock
  ON _arc_jobs(lock_key)
  WHERE lock_key IS NOT NULL AND status IN ('pending','running');
`

class SqliteAdapter extends BaseAdapter {
  constructor(opts = {}) {
    super(opts)
    this._db = opts.db ?? _openDb(opts.path ?? 'app.db')
    this._queueName = opts.name ?? 'default'
    this._pollTimer = null
    // Create table on first use
    for (const stmt of _DDL.split(';').map(s => s.trim()).filter(Boolean)) {
      this._db.run ? this._db.run(stmt) : this._db.prepare(stmt).run()
    }
  }

  _run(sql, ...params) {
    // Bun:sqlite uses .run(), better-sqlite3 uses .prepare().run()
    if (this._db.run) return this._db.run(sql, ...params)
    return this._db.prepare(sql).run(...params)
  }

  _get(sql, ...params) {
    if (this._db.query) return this._db.query(sql).get(...params)
    return this._db.prepare(sql).get(...params)
  }

  _all(sql, ...params) {
    if (this._db.query) return this._db.query(sql).all(...params)
    return this._db.prepare(sql).all(...params)
  }

  async enqueue(name, args, opts = {}) {
    const id = crypto.randomUUID()
    const priority = _PRIORITY[opts.priority] ?? 5
    const scheduledAt = opts.at ? +opts.at : Date.now() + (opts.delayMs ?? 0)
    const maxAttempts = opts.maxAttempts ?? 3

    if (opts.idempotencyKey) {
      const existing = this._get(
        `SELECT id, status FROM _arc_jobs WHERE lock_key = ?1 AND status IN ('pending','running')`,
        opts.idempotencyKey
      )
      if (existing) return existing.id
    }

    this._run(
      `INSERT INTO _arc_jobs (id, queue, name, args, priority, scheduled_at, max_attempts, lock_key, locked_until)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)`,
      id, this._queueName, name, JSON.stringify(args ?? []),
      priority, scheduledAt, maxAttempts,
      opts.idempotencyKey ?? null,
      opts.idempotencyKey ? Date.now() + (opts.lockTtlMs ?? 3600000) : null
    )

    if (scheduledAt <= Date.now() && this._registry) {
      setTimeout(() => this.tryProcess(this._registry), 0)
    }
    return id
  }

  async dequeue() {
    // Atomic claim: UPDATE ... WHERE status='pending' ensures no double-processing
    const row = this._get(
      `SELECT id, name, args, attempts, max_attempts
       FROM _arc_jobs
       WHERE queue = ?1 AND status = 'pending' AND scheduled_at <= ?2
       ORDER BY priority DESC, scheduled_at ASC
       LIMIT 1`,
      this._queueName, Date.now()
    )
    if (!row) return null

    const claimed = this._run(
      `UPDATE _arc_jobs SET status = 'running', started_at = ?2
       WHERE id = ?1 AND status = 'pending'`,
      row.id, Date.now()
    )
    const changes = claimed.changes ?? claimed
    if (!changes) return null  // another worker claimed it

    return {
      id: row.id,
      name: row.name,
      args: JSON.parse(row.args),
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      startedAt: Date.now(),
    }
  }

  async complete(id) {
    this._run(
      `UPDATE _arc_jobs SET status = 'completed', completed_at = ?2, progress = 100
       WHERE id = ?1`,
      id, Date.now()
    )
  }

  async fail(id, error, attempts, maxAttempts) {
    if (attempts >= maxAttempts) {
      this._run(
        `UPDATE _arc_jobs SET status = 'failed', error = ?2, attempts = ?3, completed_at = ?4
         WHERE id = ?1`,
        id, error, attempts, Date.now()
      )
    } else {
      const delay = 1000 * Math.pow(2, attempts - 1) + Math.random() * 500
      this._run(
        `UPDATE _arc_jobs SET status = 'pending', attempts = ?2, scheduled_at = ?3, error = ?4
         WHERE id = ?1`,
        id, attempts, Date.now() + delay, error
      )
    }
  }

  async updateProgress(id, pct) {
    this._run(`UPDATE _arc_jobs SET progress = ?2 WHERE id = ?1`, id, Math.min(100, Math.max(0, pct)))
  }

  async status(id) {
    const row = this._get(`SELECT id, name, status, progress, error, created_at, started_at, completed_at FROM _arc_jobs WHERE id = ?1`, id)
    return row ?? { id, status: 'unknown' }
  }

  async size() {
    const row = this._get(`SELECT COUNT(*) as c FROM _arc_jobs WHERE queue = ?1 AND status = 'pending' AND scheduled_at <= ?2`, this._queueName, Date.now())
    return row?.c ?? 0
  }

  async dead() {
    return this._all(`SELECT id, name, args, error, created_at FROM _arc_jobs WHERE queue = ?1 AND status = 'failed' ORDER BY completed_at DESC LIMIT 100`, this._queueName)
  }

  async replayDead() {
    const jobs = await this.dead()
    for (const job of jobs) {
      this._run(
        `UPDATE _arc_jobs SET status = 'pending', attempts = 0, error = NULL, scheduled_at = ?2 WHERE id = ?1`,
        job.id, Date.now()
      )
    }
    if (this._registry) setTimeout(() => this.tryProcess(this._registry), 0)
    return jobs.length
  }

  async acquireLock(key, ttlMs) {
    try {
      const result = this._run(
        `UPDATE _arc_jobs SET locked_until = ?2
         WHERE lock_key = ?1 AND (locked_until IS NULL OR locked_until < ?3) AND status IN ('pending','running')`,
        key, Date.now() + ttlMs, Date.now()
      )
      const changes = result.changes ?? result
      if (changes) return true
      // No existing row with this lock_key — check if there's an active lock
      const existing = this._get(
        `SELECT 1 FROM _arc_jobs WHERE lock_key = ?1 AND locked_until > ?2 AND status IN ('pending','running')`,
        key, Date.now()
      )
      return !existing
    } catch (_) {
      return false
    }
  }

  async releaseLock(key) {
    this._run(`UPDATE _arc_jobs SET locked_until = NULL WHERE lock_key = ?1`, key)
  }

  // Start polling for delayed jobs (called when scheduler or delayed enqueue is used)
  startPoller(registry, intervalMs = 1000) {
    this._registry = registry
    if (this._pollTimer) return
    this._pollTimer = setInterval(() => this.tryProcess(registry), intervalMs)
  }

  stopPoller() {
    if (this._pollTimer) { clearInterval(this._pollTimer); this._pollTimer = null }
  }
}

function _openDb(filePath) {
  // Try bun:sqlite first, then better-sqlite3
  try {
    const { Database } = require('bun:sqlite')
    const db = new Database(filePath)
    db.run('PRAGMA journal_mode=WAL')
    db.run('PRAGMA synchronous=NORMAL')
    return db
  } catch (_) {}
  try {
    const Database = require('better-sqlite3')
    const db = new Database(filePath)
    db.pragma('journal_mode = WAL')
    db.pragma('synchronous = NORMAL')
    return db
  } catch (_) {
    throw new Error('[arc-jobs] SQLite adapter requires bun:sqlite (Bun) or better-sqlite3 (Node.js)')
  }
}

module.exports = { SqliteAdapter }
