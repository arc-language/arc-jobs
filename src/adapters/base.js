'use strict'

const { log, errMsg } = require('../log')

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

  async enqueue(_name, _args, _opts = {}) { throw new Error('not implemented') }
  async dequeue() { throw new Error('not implemented') }       // returns Job | null
  async complete(_id) { throw new Error('not implemented') }
  async fail(_id, _error, _attempts, _maxAttempts) { throw new Error('not implemented') }
  async updateProgress(_id, _pct) {}                           // optional: override for persistence
  async cancel(_id) { throw new Error('not implemented') }
  async replayOne(_id) { throw new Error('not implemented') }
  async status(_id) { throw new Error('not implemented') }
  async size() { throw new Error('not implemented') }
  async dead() { throw new Error('not implemented') }
  async replayDead() { throw new Error('not implemented') }
  async acquireLock(_key, _ttlMs) { return true }              // optional: override for @unique
  async releaseLock(_key) {}
  async stats() { return { pending: await this.size(), running: 0, completed: 0, failed: 0 } }
  async listActive() { return [] }
  async listLocks() { return [] }

  // ── Shared processor ──────────────────────────────────────────────────────

  _log(level, fields) { log(level, { queue: this.name, ...fields }) }

  _backoff(attempts, baseMs = 1000) {
    return baseMs * 2 ** (attempts - 1) + Math.random() * 500
  }

  priorityScore(p) {
    return _PRIORITY[p] ?? 5
  }

  async _handleJobSuccess(job, entry, registry) {
    try {
      await this.complete(job.id)
      if (entry.thenJob && registry[entry.thenJob]) {
        try { await this.enqueue(entry.thenJob, job.args ?? []) } catch (e) {
          this._log('error', { job: job.name, event: 'then_enqueue_failed', thenJob: entry.thenJob, error: errMsg(e) })
        }
      }
    } catch (storErr) {
      this._log('error', { job: job.name, event: 'complete_storage_error', error: errMsg(storErr) })
    }
    this._log('info', { job: job.name, event: 'completed', ms: Date.now() - (job.startedAt ?? Date.now()) })
  }

  async _handleJobFailure(job, entry, e) {
    const attempts = (job.attempts ?? 0) + 1
    const maxAttempts = entry.maxRetries ?? job.maxAttempts ?? 3
    try { await this.fail(job.id, errMsg(e), attempts, maxAttempts) } catch (storErr) {
      this._log('error', { job: job.name, event: 'fail_storage_error', error: errMsg(storErr) })
    }
    if (attempts < maxAttempts) {
      const backoff = this._backoff(attempts, entry.backoffMs)
      this._log('warn', { job: job.name, event: 'retry', attempt: attempts, backoffMs: Math.round(backoff) })
    } else {
      this._log('error', { job: job.name, event: 'dlq', error: errMsg(e) })
    }
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
      await this._handleJobSuccess(job, entry, registry)
    } catch (e) {
      await this._handleJobFailure(job, entry, e)
    }
  }

  async tryProcess(registry) {
    while (this._running < this._concurrency) {
      let job
      try { job = await this.dequeue() } catch (e) {
        this._log('error', { event: 'dequeue_error', error: errMsg(e) })
        break
      }
      if (!job) break
      this._running++
      this._runJob(job, registry).finally(() => {
        this._running--
        if (this._running < this._concurrency) setImmediate(() => this.tryProcess(registry))
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
        this._maybeDrain()  // close TOCTOU window: re-check in case queue drained between size() and push()
      }
      check()
    })
  }
}

module.exports = { BaseAdapter, _PRIORITY }
