'use strict'

const { BaseAdapter, _PRIORITY } = require('./base')

// Redis adapter — high-throughput, multi-worker deployments.
// Uses Bun.Redis (built-in) on Bun, or ioredis on Node.js.
// Priority queues via sorted sets. @unique via SET NX PX (celery-once semantics).

class RedisAdapter extends BaseAdapter {
  constructor(opts = {}) {
    super(opts)
    this._url = opts.url ?? process.env.REDIS_URL ?? 'redis://localhost:6379'
    this._clientPromise = null
    this._queueKey = `arc:jobs:${this.name}:ready`
    this._delayedKey = `arc:jobs:${this.name}:delayed`
    this._metaPrefix = `arc:job:`
    this._pollTimer = null
  }

  get _dlqKey() { return `arc:jobs:${this.name}:dlq` }

  _getClient() {
    if (!this._clientPromise) this._clientPromise = this._connect()
    return this._clientPromise
  }

  async _connect() {
    try {
      return new Bun.Redis(this._url)
    } catch (_) {}
    try {
      const { Redis } = require('ioredis')
      return new Redis(this._url)
    } catch (_) {}
    throw new Error('[arc-jobs] Redis adapter requires Bun.Redis (Bun) or ioredis (npm install ioredis)')
  }

  async enqueue(name, args, opts = {}) {
    const r = await this._getClient()
    const id = crypto.randomUUID()
    const priority = _PRIORITY[opts.priority] ?? 5
    const payload = JSON.stringify({ id, name, args: args ?? [], priority, attempts: 0, maxAttempts: opts.maxAttempts ?? 3 })
    const meta = { status: 'pending', createdAt: Date.now(), name, args: JSON.stringify(args ?? []), priority, maxAttempts: opts.maxAttempts ?? 3 }

    if (opts.at || opts.delayMs) {
      const score = opts.at ? +opts.at : Date.now() + opts.delayMs
      await r.zadd(this._delayedKey, score, payload)
    } else {
      await r.zadd(this._queueKey, priority, payload)
    }
    await r.hset(this._metaPrefix + id, meta)
    return id
  }

  async dequeue() {
    const r = await this._getClient()
    await this._promoteDelayed(r)
    const result = await r.zpopmax(this._queueKey, 1)
    if (!result || result.length === 0) return null
    const payload = Array.isArray(result) ? result[0] : result
    if (!payload) return null
    let job
    try { job = JSON.parse(typeof payload === 'string' ? payload : payload[0]) } catch (_) { return null }
    if (!job) return null
    const now = Date.now()
    await r.hset(this._metaPrefix + job.id, { status: 'running', startedAt: now })
    return { ...job, startedAt: now }
  }

  async _promoteDelayed(r) {
    const ready = await r.zrangebyscore(this._delayedKey, '-inf', Date.now())
    if (!ready?.length) return
    for (const payload of ready) {
      let job
      try { job = JSON.parse(payload) } catch (_) { continue }
      try {
        await r.zrem(this._delayedKey, payload)
        await r.zadd(this._queueKey, job.priority ?? 5, payload)
      } catch (e) {
        // Best-effort restore to delayed set to avoid silent job loss
        try { await r.zadd(this._delayedKey, job.scheduledAt ?? Date.now() + 60000, payload) } catch (_) {}
        console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', queue: this.name, event: 'promote_delayed_failed', error: e?.message ?? String(e) }))
      }
    }
  }

  async complete(id) {
    const r = await this._getClient()
    await r.hset(this._metaPrefix + id, { status: 'completed', completedAt: Date.now() })
    // expire metadata after 24h
    await r.expire(this._metaPrefix + id, 86400)
  }

  async fail(id, error, attempts, maxAttempts) {
    const r = await this._getClient()
    // Fetch metadata upfront — needed for both retry payload and DLQ entry (name+args)
    const meta = await r.hgetall(this._metaPrefix + id)
    let metaArgs
    try { metaArgs = JSON.parse(meta?.args ?? '[]') } catch (_) { metaArgs = [] }
    const dlqEntry = JSON.stringify({ id, name: meta?.name, args: metaArgs, error, failedAt: new Date().toISOString() })
    if (attempts >= maxAttempts) {
      await r.hset(this._metaPrefix + id, { status: 'failed', error, attempts, completedAt: Date.now() })
      await r.lpush(this._dlqKey, dlqEntry)
    } else {
      const delay = Math.round(1000 * 2 ** (attempts - 1) + Math.random() * 500)
      if (meta?.name) {
        const payload = JSON.stringify({ id, name: meta.name, args: metaArgs, priority: +(meta.priority ?? 5), attempts, maxAttempts })
        await r.zadd(this._delayedKey, Date.now() + delay, payload)
        await r.hset(this._metaPrefix + id, { status: 'pending', attempts, error })
      } else {
        // Metadata expired — push to DLQ rather than silently dropping the retry
        await r.lpush(this._dlqKey, dlqEntry)
      }
    }
  }

  async cancel(id) {
    const r = await this._getClient()
    await r.hset(this._metaPrefix + id, { status: 'cancelled', completedAt: Date.now() })
    await r.expire(this._metaPrefix + id, 86400)
  }

  async replayOne(id) {
    const r = await this._getClient()
    const items = await r.lrange(this._dlqKey, 0, -1)
    for (const item of items) {
      let job
      try { job = JSON.parse(item) } catch (_) { continue }
      if (job?.id === id && job?.name) {
        await this.enqueue(job.name, job.args ?? [])
        await r.lrem(this._dlqKey, 1, item)
        console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', queue: this.name, event: 'replay_one', id, job: job.name }))
        return true
      }
    }
    return false
  }

  async status(id) {
    const r = await this._getClient()
    const meta = await r.hgetall(this._metaPrefix + id)
    if (!meta || Object.keys(meta).length === 0) return { id, status: 'unknown' }
    return { id, ...meta }
  }

  async size() {
    const r = await this._getClient()
    return await r.zcard(this._queueKey)
  }

  async dead() {
    const r = await this._getClient()
    const items = await r.lrange(this._dlqKey, 0, 99)
    return items.flatMap(i => { try { return [JSON.parse(i)] } catch (_) { return [] } })
  }

  async replayDead() {
    const r = await this._getClient()
    // RENAME atomically grabs the list — concurrent fail() pushes after this point land on the
    // original key and are not lost; this tmp key is ours alone to drain.
    const tmpKey = `${this._dlqKey}:replay:${Date.now()}`
    try { await r.rename(this._dlqKey, tmpKey) } catch (_) { return 0 }  // key didn't exist
    const items = await r.lrange(tmpKey, 0, -1)
    await r.del(tmpKey)
    let count = 0
    for (const item of items) {
      let job
      try { job = JSON.parse(item) } catch (_) { continue }
      if (job?.name) {
        await this.enqueue(job.name, job.args ?? [])
        count++
      }
    }
    console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', queue: this.name, event: 'replay_dead', count }))
    return count
  }

  // @unique locks via SET NX PX — exact celery-once semantics, atomic
  async acquireLock(key, ttlMs) {
    const r = await this._getClient()
    const result = await r.set(`arc:lock:${key}`, '1', 'NX', 'PX', ttlMs)
    return result === 'OK' || result === 1
  }

  async releaseLock(key) {
    const r = await this._getClient()
    await r.del(`arc:lock:${key}`)
  }

  startPoller(registry, intervalMs = 500) {
    this._registry = registry
    if (this._pollTimer) return
    this._pollTimer = setInterval(() => this.tryProcess(registry), intervalMs)
  }

  stopPoller() {
    if (this._pollTimer) { clearInterval(this._pollTimer); this._pollTimer = null }
  }

  async disconnect() {
    const client = this._clientPromise ? await this._clientPromise.catch(() => null) : null
    this._clientPromise = null
    if (client?.disconnect) await client.disconnect()
    else if (client?.quit) await client.quit()
  }
}

module.exports = { RedisAdapter }
