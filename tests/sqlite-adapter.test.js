'use strict'

const { test, describe, beforeEach, afterEach } = require('bun:test')
const assert = require('assert')
const { SqliteAdapter } = require('../src/adapters/sqlite')
const { createQueue } = require('../src/core/queue')

// Use in-memory SQLite for tests (:memory: path)
function makeAdapter(name = 'test') {
  let db
  try {
    const { Database } = require('bun:sqlite')
    db = new Database(':memory:')
    db.run('PRAGMA journal_mode=WAL')
  } catch (_) {
    const Database = require('better-sqlite3')
    db = new Database(':memory:')
  }
  return new SqliteAdapter({ name, db })
}

describe('SqliteAdapter', () => {
  let adapter

  beforeEach(() => {
    adapter = makeAdapter()
  })

  test('enqueue returns a string id', async () => {
    const id = await adapter.enqueue('TestJob', [1, 2])
    assert.strictEqual(typeof id, 'string')
    assert.ok(id.length > 0)
  })

  test('dequeue claims a pending job atomically', async () => {
    await adapter.enqueue('TestJob', ['hello'])
    const job = await adapter.dequeue()
    assert.ok(job !== null)
    assert.strictEqual(job.name, 'TestJob')
    assert.deepStrictEqual(job.args, ['hello'])
  })

  test('dequeue returns null when queue is empty', async () => {
    const job = await adapter.dequeue()
    assert.strictEqual(job, null)
  })

  test('dequeue claims only from own queue name', async () => {
    const other = makeAdapter('other')
    await other.enqueue('OtherJob', [])
    const job = await adapter.dequeue()
    assert.strictEqual(job, null)
  })

  test('dequeue respects priority order', async () => {
    await adapter.enqueue('LowJob', [], { priority: 'low' })
    await adapter.enqueue('HighJob', [], { priority: 'high' })
    await adapter.enqueue('NormalJob', [], { priority: 'normal' })

    const first = await adapter.dequeue()
    const second = await adapter.dequeue()
    const third = await adapter.dequeue()

    assert.strictEqual(first.name, 'HighJob')
    assert.strictEqual(second.name, 'NormalJob')
    assert.strictEqual(third.name, 'LowJob')
  })

  test('dequeue respects scheduled_at delay', async () => {
    await adapter.enqueue('DelayedJob', [], { delayMs: 60000 })
    const job = await adapter.dequeue()
    assert.strictEqual(job, null)
  })

  test('complete marks job as completed', async () => {
    const id = await adapter.enqueue('TestJob', [])
    await adapter.dequeue()
    await adapter.complete(id)
    const s = await adapter.status(id)
    assert.strictEqual(s.status, 'completed')
  })

  test('fail below max retries re-schedules job', async () => {
    const id = await adapter.enqueue('RetryJob', [])
    await adapter.dequeue()
    await adapter.fail(id, 'transient error', 1, 3)
    const s = await adapter.status(id)
    assert.strictEqual(s.status, 'pending')
  })

  test('fail at max retries marks job as failed', async () => {
    const id = await adapter.enqueue('BrokenJob', [])
    await adapter.dequeue()
    await adapter.fail(id, 'permanent error', 3, 3)
    const s = await adapter.status(id)
    assert.strictEqual(s.status, 'failed')
  })

  test('dead() returns failed jobs', async () => {
    const id = await adapter.enqueue('BrokenJob', [])
    await adapter.dequeue()
    await adapter.fail(id, 'kaboom', 3, 3)
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 1)
    assert.strictEqual(dead[0].name, 'BrokenJob')
  })

  test('replayDead re-enqueues failed jobs', async () => {
    const id = await adapter.enqueue('BrokenJob', [])
    await adapter.dequeue()
    await adapter.fail(id, 'kaboom', 3, 3)
    await adapter.replayDead()
    const job = await adapter.dequeue()
    assert.ok(job !== null)
    assert.strictEqual(job.name, 'BrokenJob')
  })

  test('size returns count of ready jobs', async () => {
    await adapter.enqueue('A', [])
    await adapter.enqueue('B', [])
    assert.strictEqual(await adapter.size(), 2)
  })

  test('size excludes running jobs', async () => {
    await adapter.enqueue('A', [])
    await adapter.dequeue()  // claims it → running
    assert.strictEqual(await adapter.size(), 0)
  })

  test('updateProgress stores progress value', async () => {
    const id = await adapter.enqueue('ProgressJob', [])
    await adapter.dequeue()
    await adapter.updateProgress(id, 42.5)
    const s = await adapter.status(id)
    assert.ok(Math.abs(s.progress - 42.5) < 0.01)
  })

  test('status returns unknown for nonexistent id', async () => {
    const s = await adapter.status('nonexistent-id')
    assert.strictEqual(s.status, 'unknown')
  })

  describe('@unique lock', () => {
    test('acquireLock returns true on first acquire', async () => {
      const ok = await adapter.acquireLock('test-key', 60000)
      assert.strictEqual(ok, true)
    })

    test('acquireLock prevents duplicate enqueue', async () => {
      // Simulate idempotencyKey-based deduplication
      const id1 = await adapter.enqueue('Invoice', [42], { idempotencyKey: 'inv-42' })
      const id2 = await adapter.enqueue('Invoice', [42], { idempotencyKey: 'inv-42' })
      assert.strictEqual(id1, id2)  // same id returned for duplicate key
    })

    test('releaseLock allows re-acquire', async () => {
      await adapter.acquireLock('release-key', 60000)
      await adapter.releaseLock('release-key')
      const ok = await adapter.acquireLock('release-key', 60000)
      assert.strictEqual(ok, true)
    })
  })
})

describe('SqliteAdapter via createQueue()', () => {
  let queue

  beforeEach(() => {
    let db
    try {
      const { Database } = require('bun:sqlite')
      db = new Database(':memory:')
    } catch (_) {
      const Database = require('better-sqlite3')
      db = new Database(':memory:')
    }
    const adapter = new SqliteAdapter({ name: 'test', db })
    queue = createQueue(adapter)
  })

  test('register + enqueue + drain executes job', async () => {
    let result = null
    queue.register('AddJob', async (a, b) => { result = a + b })
    queue.start(50)
    await queue.enqueue('AddJob', [3, 4])
    await queue.drain(5000)
    assert.strictEqual(result, 7)
  })

  test('failed job appears in dead queue after max retries', async () => {
    queue.register('BrokenJob', async () => { throw new Error('always fails') }, { maxRetries: 1 })
    queue.start(50)
    await queue.enqueue('BrokenJob', [])
    await queue.drain(5000)
    const dead = await queue.dead()
    assert.strictEqual(dead.length, 1)
  })
})
