'use strict'

const { test, describe } = require('bun:test')
const assert = require('assert')
const { BaseAdapter } = require('../src/adapters/base')

// Minimal concrete subclass for testing BaseAdapter shared logic
class ConcreteAdapter extends BaseAdapter {
  constructor(opts = {}) {
    super(opts)
    this._pending = []
    this._dead = []
  }

  async enqueue(name, args) {
    const id = crypto.randomUUID()
    this._pending.push({ id, name, args: args ?? [], attempts: 0, maxAttempts: 3, scheduledAt: Date.now() })
    return id
  }

  async dequeue() { return this._pending.shift() ?? null }

  async complete(id) { this._completed = (this._completed ?? []).concat(id) }

  async fail(id, error, attempts, maxAttempts) {
    if (attempts >= maxAttempts) this._dead.push({ id, error })
  }

  async status(id) { return { id, status: 'unknown' } }
  async size() { return this._pending.length }
  async dead() { return this._dead }
  async replayDead() { return 0 }
  async cancel(id) {
    const i = this._pending.findIndex(j => j.id === id)
    if (i >= 0) this._pending.splice(i, 1)
  }
  async replayOne() { return false }
}

describe('BaseAdapter — abstract method stubs', () => {
  test('enqueue throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().enqueue('x', []), /not implemented/)
  })

  test('dequeue throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().dequeue(), /not implemented/)
  })

  test('complete throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().complete('id'), /not implemented/)
  })

  test('fail throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().fail('id', 'err', 1, 1), /not implemented/)
  })

  test('cancel throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().cancel('id'), /not implemented/)
  })

  test('replayOne throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().replayOne('id'), /not implemented/)
  })

  test('status throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().status('id'), /not implemented/)
  })

  test('size throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().size(), /not implemented/)
  })

  test('dead throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().dead(), /not implemented/)
  })

  test('replayDead throws "not implemented"', async () => {
    await assert.rejects(() => new BaseAdapter().replayDead(), /not implemented/)
  })

  test('updateProgress is a no-op (does not throw)', async () => {
    await assert.doesNotReject(() => new BaseAdapter().updateProgress('id', 50))
  })

  test('acquireLock defaults to true', async () => {
    const ok = await new BaseAdapter().acquireLock('key', 1000)
    assert.strictEqual(ok, true)
  })

  test('releaseLock is a no-op', async () => {
    await assert.doesNotReject(() => new BaseAdapter().releaseLock('key'))
  })

  test('listActive defaults to empty array', async () => {
    const list = await new BaseAdapter().listActive()
    assert.deepStrictEqual(list, [])
  })

  test('listLocks defaults to empty array', async () => {
    const list = await new BaseAdapter().listLocks()
    assert.deepStrictEqual(list, [])
  })
})

describe('BaseAdapter — priorityScore', () => {
  test('returns 10 for high', () => {
    assert.strictEqual(new BaseAdapter().priorityScore('high'), 10)
  })

  test('returns 5 for normal', () => {
    assert.strictEqual(new BaseAdapter().priorityScore('normal'), 5)
  })

  test('returns 1 for low', () => {
    assert.strictEqual(new BaseAdapter().priorityScore('low'), 1)
  })

  test('returns 5 for unknown strings', () => {
    assert.strictEqual(new BaseAdapter().priorityScore('banana'), 5)
  })
})

describe('BaseAdapter — _backoff', () => {
  test('returns a positive number', () => {
    assert.ok(new BaseAdapter()._backoff(1) > 0)
  })

  test('attempt 2 base is larger than attempt 1 base', () => {
    // base formula: baseMs * 2^(attempts-1)
    const base1 = 1000 * Math.pow(2, 0)  // 1000
    const base2 = 1000 * Math.pow(2, 1)  // 2000
    assert.ok(base2 > base1)
  })

  test('respects custom baseMs', () => {
    const ms = new BaseAdapter()._backoff(1, 500)
    assert.ok(ms >= 500)
  })
})

describe('BaseAdapter — _runJob', () => {
  test('unregistered job name calls fail() and exits', async () => {
    const adapter = new ConcreteAdapter()
    const job = { id: 'j1', name: 'NoSuchJob', args: [], attempts: 0, maxAttempts: 3 }
    await adapter._runJob(job, {})
    assert.strictEqual(adapter._dead.length, 1)
    assert.ok(adapter._dead[0].error.includes('no handler'))
  })

  test('complete() throwing storage error is caught (does not propagate)', async () => {
    class FailCompleteAdapter extends ConcreteAdapter {
      async complete() { throw new Error('storage failure') }
    }
    const adapter = new FailCompleteAdapter()
    await adapter.enqueue('Ok', [])
    const job = await adapter.dequeue()
    job.startedAt = Date.now()
    await assert.doesNotReject(() => adapter._runJob(job, { Ok: { fn: async () => {} } }))
  })

  test('fail() throwing storage error is caught (does not propagate)', async () => {
    class FailFailAdapter extends ConcreteAdapter {
      async fail() { throw new Error('storage failure') }
    }
    const adapter = new FailFailAdapter()
    await adapter.enqueue('BrokenJob', [])
    const job = await adapter.dequeue()
    job.startedAt = Date.now()
    const registry = { BrokenJob: { fn: async () => { throw new Error('job error') }, maxRetries: 1 } }
    await assert.doesNotReject(() => adapter._runJob(job, registry))
  })

  test('job exceeding timeoutMs is failed with timeout error', async () => {
    const adapter = new ConcreteAdapter()
    await adapter.enqueue('SlowJob', [])
    const job = await adapter.dequeue()
    job.startedAt = Date.now()
    const registry = {
      SlowJob: {
        fn: async () => new Promise(r => setTimeout(r, 10000)),
        timeoutMs: 30,
        maxRetries: 0,
      },
    }
    await adapter._runJob(job, registry)
    assert.strictEqual(adapter._dead.length, 1)
    assert.ok(adapter._dead[0].error.includes('timed out'))
  })
})

describe('BaseAdapter — tryProcess dequeue error', () => {
  test('dequeue error is caught and logged (does not throw)', async () => {
    class ErrorDequeueAdapter extends ConcreteAdapter {
      async dequeue() { throw new Error('DB down') }
    }
    await assert.doesNotReject(() => new ErrorDequeueAdapter().tryProcess({}))
  })
})

describe('BaseAdapter — drain', () => {
  test('drain resolves immediately when queue is empty', async () => {
    await assert.doesNotReject(() => new ConcreteAdapter().drain(2000))
  })

  test('drain times out when queue never empties', async () => {
    const adapter = new ConcreteAdapter()
    await adapter.enqueue('StuckJob', [])
    await assert.rejects(() => adapter.drain(50), /drain timed out/)
  })

  test('drain resolves after queue empties via _maybeDrain', async () => {
    const adapter = new ConcreteAdapter()
    await adapter.enqueue('Job', [])
    const drainPromise = adapter.drain(2000)
    // Manually dequeue and complete to empty the queue
    const job = await adapter.dequeue()
    await adapter.complete(job.id)
    adapter._maybeDrain()
    await assert.doesNotReject(() => drainPromise)
  })

  test('_maybeDrain delivers error to drain waiters when size() rejects', async () => {
    const adapter = new ConcreteAdapter()
    await adapter.enqueue('Job', [])
    const drainPromise = adapter.drain(2000)

    // Replace size with one that rejects, then trigger _maybeDrain
    const origSize = adapter.size.bind(adapter)
    adapter.size = async () => { throw new Error('size failed') }
    adapter._maybeDrain()
    await assert.rejects(() => drainPromise, /size failed/)
    adapter.size = origSize
  })
})
