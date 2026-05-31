'use strict'

const { BaseAdapter, _PRIORITY } = require('./base')

// In-memory adapter — development default and test mode.
// In NODE_ENV=test: synchronous, does NOT auto-process (call Queue.flush() explicitly).
// Jobs lost on process restart — not for production persistence.

function _insertSorted(arr, job) {
  let lo = 0, hi = arr.length
  while (lo < hi) {
    const mid = (lo + hi) >>> 1
    const m = arr[mid]
    if (m.priority > job.priority || (m.priority === job.priority && m.scheduledAt <= job.scheduledAt)) lo = mid + 1
    else hi = mid
  }
  arr.splice(lo, 0, job)
}

class MemoryAdapter extends BaseAdapter {
  constructor(opts = {}) {
    super(opts)
    this._pending = []     // { id, name, args, priority, attempts, maxAttempts, scheduledAt }
    this._inFlight = new Map()  // id → { name, args } for jobs currently running
    this._completed = []
    this._dead = []
    this._locks = new Map()  // key → expiresAt
    this._statusMap = new Map()  // id → status string for O(1) status lookups
    this._registry = null    // set by createQueue()
    this._testMode = process.env.NODE_ENV === 'test'
  }

  async enqueue(name, args, opts = {}) {
    const id = crypto.randomUUID()
    const priority = _PRIORITY[opts.priority] ?? 5
    const scheduledAt = opts.at ? +opts.at : Date.now() + (opts.delayMs ?? 0)
    const job = { id, name, args: args ?? [], priority, attempts: 0, maxAttempts: opts.maxAttempts ?? 3, scheduledAt }
    _insertSorted(this._pending, job)
    this._statusMap.set(id, 'pending')
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
    this._statusMap.set(job.id, 'running')
    return { ...job, startedAt: now }
  }

  async complete(id) {
    const job = this._inFlight.get(id)
    this._inFlight.delete(id)
    this._completed.push({ id, name: job?.name, completedAt: Date.now() })
    if (this._completed.length > 1000) this._completed.splice(0, this._completed.length - 1000)
    this._statusMap.set(id, 'completed')
  }

  async fail(id, error, attempts, maxAttempts) {
    const original = this._inFlight.get(id)
    this._inFlight.delete(id)
    if (attempts >= maxAttempts) {
      this._dead.push({
        id,
        name: original?.name ?? id,
        args: original?.args ?? [],
        error,
        failedAt: new Date().toISOString(),
      })
      if (this._dead.length > 1000) this._dead.splice(0, this._dead.length - 1000)
      this._statusMap.set(id, 'failed')
    } else {
      const delay = 1000 * 2 ** (attempts - 1) + Math.random() * 500
      const retryJob = { ...(original ?? {}), id, attempts, scheduledAt: Date.now() + Math.round(delay) }
      _insertSorted(this._pending, retryJob)
      this._statusMap.set(id, 'pending')
    }
  }

  async status(id) {
    const s = this._statusMap.get(id)
    return s ? { id, status: s } : { id, status: 'unknown' }
  }

  async size() {
    return this._pending.filter(j => j.scheduledAt <= Date.now()).length
  }

  async dead() {
    return [...this._dead]
  }

  async replayDead() {
    const toReplay = []
    const toKeep = []
    for (const job of this._dead) {
      if (this._registry?.[job.name]) toReplay.push(job)
      else toKeep.push(job)
    }
    this._dead = toKeep
    for (const job of toReplay) await this.enqueue(job.name, job.args ?? [])
    console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', queue: this.name, event: 'replay_dead', count: toReplay.length }))
    return toReplay.length
  }

  async cancel(id) {
    const idx = this._pending.findIndex(j => j.id === id)
    if (idx !== -1) this._pending.splice(idx, 1)
    this._inFlight.delete(id)
    this._statusMap.set(id, 'cancelled')
  }

  async replayOne(id) {
    const idx = this._dead.findIndex(j => j.id === id)
    if (idx === -1) return false
    const [job] = this._dead.splice(idx, 1)
    await this.enqueue(job.name, job.args ?? [])
    this._statusMap.delete(id)  // old id status cleared; new enqueue assigns a fresh id
    console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', queue: this.name, event: 'replay_one', id, job: job.name }))
    return true
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

  assertQueue(name) {
    const found = this._pending.some(j => j.name === name)
    if (!found) throw new Error(`arc-jobs: no pending job '${name}'`)
  }

  async flush() {
    if (!this._registry) throw new Error('arc-jobs: flush() called before registry was set')
    const saved = this._testMode
    this._testMode = false
    try {
      await this.tryProcess(this._registry)
      await this.drain(60000)
    } finally {
      this._testMode = saved
    }
  }

  reset() {
    this._pending = []
    this._inFlight = new Map()
    this._completed = []
    this._dead = []
    this._locks = new Map()
    this._statusMap = new Map()
  }
}

module.exports = { MemoryAdapter }
