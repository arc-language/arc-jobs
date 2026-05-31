'use strict'

const { test, describe, beforeEach } = require('bun:test')
const assert = require('assert')
const { RedisAdapter } = require('../src/adapters/redis')

// Minimal in-memory mock of the Redis client interface used by RedisAdapter
class MockRedis {
  constructor() {
    this._zsets = new Map()   // key → [{score, value}]
    this._hashes = new Map()  // key → {field: value}
    this._lists = new Map()   // key → [value, ...]
    this._strings = new Map() // key → {value, expiresAt}
  }

  async zadd(key, score, value) {
    if (!this._zsets.has(key)) this._zsets.set(key, [])
    const zset = this._zsets.get(key)
    const idx = zset.findIndex(e => e.value === value)
    if (idx >= 0) zset[idx].score = score
    else zset.push({ score, value })
    return 1
  }

  async zpopmax(key) {
    const zset = this._zsets.get(key) ?? []
    if (!zset.length) return []
    zset.sort((a, b) => b.score - a.score)
    const [top] = zset.splice(0, 1)
    return [top.value]
  }

  async zcard(key) {
    return (this._zsets.get(key) ?? []).length
  }

  async zrangebyscore(key, _min, max) {
    const zset = this._zsets.get(key) ?? []
    const maxVal = max === '+inf' ? Infinity : +max
    return zset.filter(e => e.score <= maxVal).map(e => e.value)
  }

  async zrem(key, value) {
    const zset = this._zsets.get(key)
    if (!zset) return 0
    const idx = zset.findIndex(e => e.value === value)
    if (idx >= 0) { zset.splice(idx, 1); return 1 }
    return 0
  }

  async hset(key, obj) {
    if (!this._hashes.has(key)) this._hashes.set(key, {})
    Object.assign(this._hashes.get(key), obj)
    return Object.keys(obj).length
  }

  async hgetall(key) {
    const h = this._hashes.get(key)
    return h && Object.keys(h).length ? { ...h } : {}
  }

  async expire() { return 1 }

  async lpush(key, value) {
    if (!this._lists.has(key)) this._lists.set(key, [])
    this._lists.get(key).unshift(value)
    return this._lists.get(key).length
  }

  async lrange(key, start, end) {
    const list = this._lists.get(key) ?? []
    return end === -1 ? [...list.slice(start)] : [...list.slice(start, end + 1)]
  }

  async lrem(key, _count, value) {
    const list = this._lists.get(key)
    if (!list) return 0
    const idx = list.indexOf(value)
    if (idx >= 0) { list.splice(idx, 1); return 1 }
    return 0
  }

  async rename(oldKey, newKey) {
    if (this._lists.has(oldKey)) {
      this._lists.set(newKey, this._lists.get(oldKey))
      this._lists.delete(oldKey)
    } else if (this._zsets.has(oldKey)) {
      this._zsets.set(newKey, this._zsets.get(oldKey))
      this._zsets.delete(oldKey)
    } else {
      throw new Error('ERR no such key')
    }
    return 'OK'
  }

  async del(key) {
    return [this._strings, this._lists, this._zsets, this._hashes]
      .reduce((n, m) => n + (m.delete(key) ? 1 : 0), 0)
  }

  async set(key, value, ...opts) {
    const nx = opts.includes('NX')
    const pxIdx = opts.indexOf('PX')
    const ttlMs = pxIdx >= 0 ? +opts[pxIdx + 1] : null
    if (nx) {
      const entry = this._strings.get(key)
      if (entry && (!entry.expiresAt || entry.expiresAt > Date.now())) return null
    }
    this._strings.set(key, { value, expiresAt: ttlMs ? Date.now() + ttlMs : null })
    return 'OK'
  }

  async disconnect() {}
  async quit() {}
}

function makeAdapter(name = 'test') {
  const adapter = new RedisAdapter({ name })
  const mock = new MockRedis()
  adapter._clientPromise = Promise.resolve(mock)
  return { adapter, mock }
}

describe('RedisAdapter', () => {
  let adapter, mock

  beforeEach(() => {
    ;({ adapter, mock } = makeAdapter())
  })

  test('enqueue returns a string UUID', async () => {
    const id = await adapter.enqueue('TestJob', [1, 2])
    assert.strictEqual(typeof id, 'string')
    assert.ok(id.length > 0)
  })

  test('enqueue adds job to ready sorted set', async () => {
    await adapter.enqueue('TestJob', [])
    assert.strictEqual(await adapter.size(), 1)
  })

  test('enqueue with delayMs stores in delayed set (not ready)', async () => {
    await adapter.enqueue('DelayedJob', [], { delayMs: 60000 })
    assert.strictEqual(await adapter.size(), 0)
    const delayed = await mock.zrangebyscore(adapter._delayedKey, '-inf', '+inf')
    assert.strictEqual(delayed.length, 1)
  })

  test('enqueue with at: stores in delayed set', async () => {
    await adapter.enqueue('FutureJob', [], { at: new Date(Date.now() + 60000) })
    assert.strictEqual(await adapter.size(), 0)
  })

  test('enqueue stores metadata hash', async () => {
    const id = await adapter.enqueue('TestJob', [42, 'hello'])
    const meta = await mock.hgetall(`arc:job:${id}`)
    assert.strictEqual(meta.name, 'TestJob')
    assert.strictEqual(meta.status, 'pending')
  })

  test('dequeue returns null when queue is empty', async () => {
    const job = await adapter.dequeue()
    assert.strictEqual(job, null)
  })

  test('dequeue returns and removes pending job', async () => {
    await adapter.enqueue('TestJob', [99])
    const job = await adapter.dequeue()
    assert.ok(job !== null)
    assert.strictEqual(job.name, 'TestJob')
    assert.deepStrictEqual(job.args, [99])
  })

  test('dequeue promotes delayed jobs that are now ready', async () => {
    const payload = JSON.stringify({ id: 'past-id', name: 'ReadyJob', args: [], priority: 5, attempts: 0, maxAttempts: 3 })
    await mock.zadd(adapter._delayedKey, Date.now() - 1000, payload)
    const job = await adapter.dequeue()
    assert.ok(job !== null)
    assert.strictEqual(job.name, 'ReadyJob')
  })

  test('dequeue marks job metadata as running', async () => {
    await adapter.enqueue('TestJob', [])
    const job = await adapter.dequeue()
    const meta = await mock.hgetall(`arc:job:${job.id}`)
    assert.strictEqual(meta.status, 'running')
  })

  test('complete marks job metadata as completed and sets expiry', async () => {
    await adapter.enqueue('TestJob', [])
    const job = await adapter.dequeue()
    await adapter.complete(job.id)
    const meta = await mock.hgetall(`arc:job:${job.id}`)
    assert.strictEqual(meta.status, 'completed')
  })

  test('fail at max retries pushes to DLQ', async () => {
    await adapter.enqueue('BrokenJob', [])
    const job = await adapter.dequeue()
    await adapter.fail(job.id, 'boom', 3, 3)
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 1)
    assert.strictEqual(dead[0].name, 'BrokenJob')
    assert.strictEqual(dead[0].error, 'boom')
  })

  test('fail at max retries marks metadata as failed', async () => {
    await adapter.enqueue('BrokenJob', [])
    const job = await adapter.dequeue()
    await adapter.fail(job.id, 'err', 3, 3)
    const meta = await mock.hgetall(`arc:job:${job.id}`)
    assert.strictEqual(meta.status, 'failed')
  })

  test('fail below max retries re-queues to delayed set', async () => {
    await adapter.enqueue('RetryJob', [])
    const job = await adapter.dequeue()
    await adapter.fail(job.id, 'transient', 1, 3)
    const delayed = await mock.zrangebyscore(adapter._delayedKey, '-inf', '+inf')
    assert.strictEqual(delayed.length, 1)
  })

  test('fail below max retries when metadata missing falls back to DLQ', async () => {
    // Simulate metadata expiry — hgetall returns empty obj
    await adapter.fail('ghost-id', 'no-meta', 1, 3)
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 1)
  })

  test('cancel marks metadata as cancelled', async () => {
    await adapter.enqueue('CancelJob', [])
    const job = await adapter.dequeue()
    await adapter.cancel(job.id)
    const meta = await mock.hgetall(`arc:job:${job.id}`)
    assert.strictEqual(meta.status, 'cancelled')
  })

  test('status returns unknown for nonexistent id', async () => {
    const s = await adapter.status('does-not-exist')
    assert.strictEqual(s.status, 'unknown')
  })

  test('status returns job metadata fields', async () => {
    await adapter.enqueue('TestJob', [])
    const job = await adapter.dequeue()
    const s = await adapter.status(job.id)
    assert.ok(s.name || s.status)
  })

  test('size returns count of ready pending jobs', async () => {
    await adapter.enqueue('A', [])
    await adapter.enqueue('B', [])
    assert.strictEqual(await adapter.size(), 2)
  })

  test('dead returns parsed DLQ entries', async () => {
    await adapter.enqueue('FailJob', [1, 2])
    const job = await adapter.dequeue()
    await adapter.fail(job.id, 'crash', 3, 3)
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 1)
    assert.strictEqual(dead[0].error, 'crash')
    assert.ok(dead[0].failedAt)
  })

  test('dead skips malformed DLQ entries', async () => {
    await mock.lpush(adapter._dlqKey, 'not-json{{{')
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 0)
  })

  test('replayDead re-enqueues all DLQ jobs and returns count', async () => {
    await adapter.enqueue('FailJob', [])
    const j1 = await adapter.dequeue()
    await adapter.fail(j1.id, 'err', 3, 3)
    await adapter.enqueue('FailJob', [])
    const j2 = await adapter.dequeue()
    await adapter.fail(j2.id, 'err', 3, 3)

    const count = await adapter.replayDead()
    assert.strictEqual(count, 2)
    assert.strictEqual(await adapter.size(), 2)
  })

  test('replayDead returns 0 when DLQ is empty', async () => {
    const count = await adapter.replayDead()
    assert.strictEqual(count, 0)
  })

  test('replayDead skips malformed DLQ entries', async () => {
    await mock.lpush(adapter._dlqKey, 'not-json')
    await mock.lpush(adapter._dlqKey, JSON.stringify({ name: 'GoodJob', args: [] }))
    const count = await adapter.replayDead()
    assert.strictEqual(count, 1)
  })

  test('replayOne replays specific job from DLQ by id', async () => {
    await adapter.enqueue('DeadJob', [99])
    const job = await adapter.dequeue()
    await adapter.fail(job.id, 'err', 3, 3)
    const found = await adapter.replayOne(job.id)
    assert.strictEqual(found, true)
    assert.strictEqual(await adapter.size(), 1)
  })

  test('replayOne returns false when id is not in DLQ', async () => {
    const found = await adapter.replayOne('no-such-id')
    assert.strictEqual(found, false)
  })

  describe('@unique locks', () => {
    test('acquireLock returns true first time', async () => {
      const ok = await adapter.acquireLock('my-lock', 60000)
      assert.strictEqual(ok, true)
    })

    test('acquireLock returns false when lock is held', async () => {
      await adapter.acquireLock('my-lock', 60000)
      const second = await adapter.acquireLock('my-lock', 60000)
      assert.strictEqual(second, false)
    })

    test('releaseLock allows re-acquire', async () => {
      await adapter.acquireLock('my-lock', 60000)
      await adapter.releaseLock('my-lock')
      const ok = await adapter.acquireLock('my-lock', 60000)
      assert.strictEqual(ok, true)
    })
  })

  describe('startPoller / stopPoller', () => {
    test('startPoller starts processing interval', () => {
      adapter.startPoller({}, 100)
      assert.ok(adapter._pollTimer !== null)
      adapter.stopPoller()
    })

    test('startPoller is idempotent (double call keeps first timer)', () => {
      adapter.startPoller({}, 100)
      const timer1 = adapter._pollTimer
      adapter.startPoller({}, 100)
      assert.strictEqual(adapter._pollTimer, timer1)
      adapter.stopPoller()
    })

    test('stopPoller clears the interval', () => {
      adapter.startPoller({}, 100)
      adapter.stopPoller()
      assert.strictEqual(adapter._pollTimer, null)
    })

    test('stopPoller is safe to call when not running', () => {
      assert.doesNotThrow(() => adapter.stopPoller())
    })
  })

  test('disconnect resolves without error', async () => {
    await assert.doesNotReject(() => adapter.disconnect())
  })

  test('_dlqKey includes queue name', () => {
    assert.ok(adapter._dlqKey.includes('test'))
    assert.strictEqual(typeof adapter._dlqKey, 'string')
  })

  test('_promoteDelayed skips malformed payloads', async () => {
    await mock.zadd(adapter._delayedKey, Date.now() - 1000, 'not-json')
    // Should not throw — malformed payload is skipped
    await assert.doesNotReject(() => adapter.dequeue())
  })
})
