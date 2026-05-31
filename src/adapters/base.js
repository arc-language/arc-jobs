'use strict'

// Base adapter — defines the interface all queue backends must implement.
// Subclasses override these methods; shared retry/DLQ logic lives here.

const _PRIORITY = { high: 10, normal: 5, low: 1 }

class BaseAdapter {
  constructor(opts = {}) {
    this.name = opts.name ?? 'default'
    this._timeout = opts.timeout ?? +(process.env.ARC_JOB_TIMEOUT ?? 30000)
    this._concurrency = opts.concurrency ?? +(process.env.ARC_JOB_CONCURRENCY ?? 1)
    this._running = 0
    this._drainResolvers = []
  }

  // ── Must override ──────────────────────────────────────────────────────────

  async enqueue(name, args, opts = {}) { throw new Error('not implemented') }
  async dequeue() { throw new Error('not implemented') }       // returns Job | null
  async complete(id) { throw new Error('not implemented') }
  async fail(id, error, attempts, maxAttempts) { throw new Error('not implemented') }
  async updateProgress(id, pct) {}                             // optional: override for persistence
  async cancel(id) { throw new Error('not implemented') }
  async replayOne(id) { throw new Error('not implemented') }
  async status(id) { throw new Error('not implemented') }
  async size() { throw new Error('not implemented') }
  async dead() { throw new Error('not implemented') }
  async replayDead() { throw new Error('not implemented') }
  async acquireLock(key, ttlMs) { return true }                // optional: override for @unique
  async releaseLock(key) {}

  // ── Shared processor ──────────────────────────────────────────────────────

  priorityScore(p) {
    return _PRIORITY[p] ?? 5
  }

  async _runJob(job, registry) {
    const entry = registry[job.name]
    if (!entry) {
      await this.fail(job.id, `no handler registered for job '${job.name}'`, job.attempts + 1, 1)
      return
    }

    const timeoutMs = entry.timeoutMs ?? this._timeout
    try {
      await Promise.race([
        entry.fn(...(job.args ?? [])),
        new Promise((_, rej) => setTimeout(() => rej(new Error(`job timed out after ${timeoutMs}ms`)), timeoutMs)),
      ])
      await this.complete(job.id)
      if (entry.thenJob && registry[entry.thenJob]) {
        try {
          await this.enqueue(entry.thenJob, job.args ?? [])
        } catch (e) {
          console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', queue: this.name, job: job.name, event: 'then_enqueue_failed', thenJob: entry.thenJob, error: e?.message ?? String(e) }))
        }
      }
      console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', queue: this.name, job: job.name, event: 'completed', ms: Date.now() - (job.startedAt ?? Date.now()) }))
    } catch (e) {
      const attempts = (job.attempts ?? 0) + 1
      const maxAttempts = entry.maxRetries ?? job.maxAttempts ?? 3
      await this.fail(job.id, e?.message ?? String(e), attempts, maxAttempts)
      if (attempts < maxAttempts) {
        const backoff = (entry.backoffMs ?? 1000) * Math.pow(2, attempts - 1) + Math.random() * 500
        console.warn(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', queue: this.name, job: job.name, event: 'retry', attempt: attempts, backoffMs: Math.round(backoff) }))
      } else {
        console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', queue: this.name, job: job.name, event: 'dlq', error: e?.message ?? String(e) }))
      }
    }
  }

  async tryProcess(registry) {
    while (this._running < this._concurrency) {
      const job = await this.dequeue()
      if (!job) break
      this._running++
      this._runJob(job, registry).finally(() => {
        this._running--
        setImmediate(() => this.tryProcess(registry))
        this._maybeDrain()
      })
    }
  }

  _maybeDrain() {
    this.size().then(n => {
      if (n === 0 && this._running === 0) {
        const resolvers = this._drainResolvers.splice(0)
        for (const r of resolvers) r(null)
      }
    }).catch(err => {
      const resolvers = this._drainResolvers.splice(0)
      for (const r of resolvers) r(err)
    })
  }

  drain(timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      const check = async () => {
        const n = await this.size().catch(err => { reject(err); return -1 })
        if (n === -1) return
        if (n === 0 && this._running === 0) return resolve()
        const timer = setTimeout(() => reject(new Error(`[arc-jobs] drain timed out after ${timeoutMs}ms`)), timeoutMs)
        this._drainResolvers.push(err => { clearTimeout(timer); err ? reject(err) : resolve() })
      }
      check()
    })
  }
}

module.exports = { BaseAdapter, _PRIORITY }
