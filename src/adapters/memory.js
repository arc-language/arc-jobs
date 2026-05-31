'use strict'

const { BaseAdapter, _PRIORITY } = require('./base')

// In-memory adapter — development default and test mode.
// In NODE_ENV=test: synchronous, does NOT auto-process (call Queue.flush() explicitly).
// Jobs lost on process restart — not for production persistence.

class MemoryAdapter extends BaseAdapter {
  constructor(opts = {}) {
    super(opts)
    this._pending = []     // { id, name, args, priority, attempts, maxAttempts, scheduledAt }
    this._inFlight = new Map()  // id → { name, args } for jobs currently running
    this._completed = []
    this._failed = []
    this._dead = []
    this._locks = new Map()  // key → expiresAt
    this._registry = null    // set by createQueue()
    this._testMode = process.env.NODE_ENV === 'test'
  }

  async enqueue(name, args, opts = {}) {
    const id = crypto.randomUUID()
    const priority = _PRIORITY[opts.priority] ?? 5
    const scheduledAt = opts.at ? +opts.at : Date.now() + (opts.delayMs ?? 0)
    this._pending.push({ id, name, args: args ?? [], priority, attempts: 0, maxAttempts: opts.maxAttempts ?? 3, scheduledAt })
    this._pending.sort((a, b) => b.priority - a.priority || a.scheduledAt - b.scheduledAt)
    if (!this._testMode && this._registry) {
      setTimeout(() => this.tryProcess(this._registry), 0)
    }
    return id
  }

  async dequeue() {
    const now = Date.now()
    const idx = this._pending.findIndex(j => j.scheduledAt <= now)
    if (idx === -1) return null
    const [job] = this._pending.splice(idx, 1)
    this._inFlight.set(job.id, { name: job.name, args: job.args })
    return { ...job, startedAt: now }
  }

  async complete(id) {
    const job = this._inFlight.get(id)
    this._inFlight.delete(id)
    this._completed.push({ id, name: job?.name, completedAt: Date.now() })
    if (this._completed.length > 1000) this._completed.splice(0, this._completed.length - 1000)
  }

  async fail(id, error, attempts, maxAttempts) {
    const original = this._inFlight.get(id)
    if (attempts >= maxAttempts) {
      this._dead.push({
        id,
        name: original?.name ?? id,
        args: original?.args ?? [],
        error,
        failedAt: new Date().toISOString(),
      })
      if (this._dead.length > 1000) this._dead.splice(0, this._dead.length - 1000)
    }
    this._inFlight.delete(id)
  }

  async status(id) {
    if (this._completed.some(j => j.id === id)) return { id, status: 'completed' }
    if (this._dead.some(j => j.id === id)) return { id, status: 'failed' }
    if (this._pending.some(j => j.id === id)) return { id, status: 'pending' }
    return { id, status: 'unknown' }
  }

  async size() {
    return this._pending.filter(j => j.scheduledAt <= Date.now()).length
  }

  async dead() {
    return [...this._dead]
  }

  async replayDead() {
    const jobs = this._dead.splice(0)
    for (const job of jobs) {
      if (this._registry?.[job.name]) {
        await this.enqueue(job.name, job.args ?? [])
      }
    }
    return jobs.length
  }

  async acquireLock(key, ttlMs) {
    const exp = this._locks.get(key)
    if (exp && exp > Date.now()) return false
    this._locks.set(key, Date.now() + ttlMs)
    return true
  }

  async releaseLock(key) {
    this._locks.delete(key)
  }

  // ── Test helpers ────────────────────────────────────────────────────────────

  pending(name) { return this._pending.filter(j => !name || j.name === name) }
  completed(name) { return this._completed.filter(j => !name || j.name === name) }

  assertEnqueued(name, args) {
    const found = this._pending.some(j => j.name === name && JSON.stringify(j.args) === JSON.stringify(args))
    if (!found) throw new Error(`arc-jobs: no pending job '${name}' with args ${JSON.stringify(args)}`)
  }

  assertQueue(name, queueName) {
    const found = this._pending.some(j => j.name === name)
    if (!found) throw new Error(`arc-jobs: no pending job '${name}'`)
  }

  async flush() {
    if (!this._registry) throw new Error('arc-jobs: flush() called before registry was set')
    const saved = this._testMode
    this._testMode = false
    await this.tryProcess(this._registry)
    await this.drain(60000)
    this._testMode = saved
  }

  reset() {
    this._pending = []
    this._completed = []
    this._failed = []
    this._dead = []
    this._locks = new Map()
  }
}

module.exports = { MemoryAdapter }
