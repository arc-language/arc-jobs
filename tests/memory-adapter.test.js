'use strict'

const { test, describe, beforeEach } = require('bun:test')
const assert = require('assert')
const { MemoryAdapter } = require('../src/adapters/memory')
const { createQueue } = require('../src/core/queue')

function makeRegistry(handlers = {}) {
  return Object.fromEntries(
    Object.entries(handlers).map(([name, fn]) => [name, { fn, maxRetries: 3, timeoutMs: 5000 }])
  )
}

describe('MemoryAdapter', () => {
  let adapter

  beforeEach(() => {
    process.env.NODE_ENV = 'test'
    adapter = new MemoryAdapter({ name: 'test' })
  })

  test('enqueue returns a string id', async () => {
    const id = await adapter.enqueue('TestJob', [1, 2])
    assert.strictEqual(typeof id, 'string')
    assert.ok(id.length > 0)
  })

  test('enqueue adds job to pending list', async () => {
    await adapter.enqueue('TestJob', ['hello'])
    assert.strictEqual(adapter._pending.length, 1)
    assert.strictEqual(adapter._pending[0].name, 'TestJob')
    assert.deepStrictEqual(adapter._pending[0].args, ['hello'])
  })

  test('dequeue returns and removes the next pending job', async () => {
    await adapter.enqueue('TestJob', [42])
    const job = await adapter.dequeue()
    assert.ok(job !== null)
    assert.strictEqual(job.name, 'TestJob')
    assert.deepStrictEqual(job.args, [42])
    assert.strictEqual(adapter._pending.length, 0)
  })

  test('dequeue returns null when queue is empty', async () => {
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
    assert.strictEqual(job, null)  // not ready yet
  })

  test('complete removes job from completed list', async () => {
    const id = await adapter.enqueue('TestJob', [])
    await adapter.complete(id)
    assert.strictEqual(adapter._completed.length, 1)
  })

  test('fail after max retries moves job to dead', async () => {
    const id = await adapter.enqueue('BrokenJob', [])
    await adapter.fail(id, 'explosion', 3, 3)
    assert.strictEqual(adapter._dead.length, 1)
    assert.strictEqual(adapter._dead[0].error, 'explosion')
  })

  test('fail below max retries does not dead-letter', async () => {
    const id = await adapter.enqueue('RetryJob', [])
    await adapter.fail(id, 'transient error', 1, 3)
    assert.strictEqual(adapter._dead.length, 0)
  })

  test('size returns count of ready pending jobs', async () => {
    await adapter.enqueue('A', [])
    await adapter.enqueue('B', [])
    const n = await adapter.size()
    assert.strictEqual(n, 2)
  })

  test('dead() returns dead jobs', async () => {
    const id = await adapter.enqueue('BrokenJob', [])
    await adapter.fail(id, 'kaboom', 3, 3)
    const dead = await adapter.dead()
    assert.strictEqual(dead.length, 1)
    assert.strictEqual(dead[0].error, 'kaboom')
  })

  test('reset clears all queues', async () => {
    await adapter.enqueue('A', [])
    await adapter.enqueue('B', [])
    adapter.reset()
    assert.strictEqual(adapter._pending.length, 0)
    assert.strictEqual(adapter._completed.length, 0)
    assert.strictEqual(adapter._dead.length, 0)
  })

  describe('@unique lock', () => {
    test('acquireLock returns true on first acquire', async () => {
      const ok = await adapter.acquireLock('test-key', 60000)
      assert.strictEqual(ok, true)
    })

    test('acquireLock returns false if lock is held', async () => {
      await adapter.acquireLock('test-key', 60000)
      const second = await adapter.acquireLock('test-key', 60000)
      assert.strictEqual(second, false)
    })

    test('acquireLock returns true after lock expires', async () => {
      await adapter.acquireLock('expire-key', 1)   // 1ms TTL
      await new Promise(r => setTimeout(r, 10))
      const reacquired = await adapter.acquireLock('expire-key', 60000)
      assert.strictEqual(reacquired, true)
    })

    test('releaseLock allows re-acquire', async () => {
      await adapter.acquireLock('release-key', 60000)
      await adapter.releaseLock('release-key')
      const ok = await adapter.acquireLock('release-key', 60000)
      assert.strictEqual(ok, true)
    })
  })

  describe('test helpers', () => {
    test('assertEnqueued passes when job is pending', async () => {
      await adapter.enqueue('MyJob', [99])
      assert.doesNotThrow(() => adapter.assertEnqueued('MyJob', [99]))
    })

    test('assertEnqueued throws when job is not pending', () => {
      assert.throws(
        () => adapter.assertEnqueued('MyJob', [99]),
        /no pending job 'MyJob'/
      )
    })

    test('assertEnqueued throws on wrong args', async () => {
      await adapter.enqueue('MyJob', [99])
      assert.throws(
        () => adapter.assertEnqueued('MyJob', [100]),
        /no pending job 'MyJob'/
      )
    })

    test('pending() filters by name', async () => {
      await adapter.enqueue('JobA', [1])
      await adapter.enqueue('JobB', [2])
      await adapter.enqueue('JobA', [3])
      assert.strictEqual(adapter.pending('JobA').length, 2)
      assert.strictEqual(adapter.pending('JobB').length, 1)
      assert.strictEqual(adapter.pending().length, 3)
    })
  })
})

describe('MemoryAdapter via createQueue()', () => {
  let queue

  beforeEach(() => {
    process.env.NODE_ENV = 'test'
    queue = createQueue(new MemoryAdapter({ name: 'test' }))
  })

  test('register + flush executes job', async () => {
    let called = false
    queue.register('TestJob', async (x) => { called = true; assert.strictEqual(x, 42) })
    await queue.enqueue('TestJob', [42])
    await queue.flush()
    assert.strictEqual(called, true)
  })

  test('flush executes multiple jobs', async () => {
    const results = []
    queue.register('PushJob', async (n) => { results.push(n) })
    await queue.enqueue('PushJob', [1])
    await queue.enqueue('PushJob', [2])
    await queue.enqueue('PushJob', [3])
    await queue.flush()
    assert.deepStrictEqual(results.sort(), [1, 2, 3])
  })

  test('reset + flush is idempotent', async () => {
    queue.register('TestJob', async () => {})
    await queue.enqueue('TestJob', [])
    queue.reset()
    const pending = queue.pending()
    assert.strictEqual(pending.length, 0)
  })

  test('status returns unknown for missing id', async () => {
    const s = await queue.status('nonexistent-id')
    assert.strictEqual(s.status, 'unknown')
  })

  test('size returns 0 after flush', async () => {
    queue.register('TestJob', async () => {})
    await queue.enqueue('TestJob', [])
    assert.strictEqual(await queue.size(), 1)
    await queue.flush()
    assert.strictEqual(await queue.size(), 0)
  })

  test('replayDead re-enqueues failed jobs', async () => {
    let attempts = 0
    queue.register('FlakyJob', async () => {
      attempts++
      if (attempts === 1) throw new Error('first attempt fails')
    }, { maxRetries: 1 })

    await queue.enqueue('FlakyJob', [])
    await queue.flush()
    const dead = await queue.dead()
    assert.strictEqual(dead.length, 1)
    await queue.replayDead()
    await queue.flush()
    assert.strictEqual(attempts, 2)
  })
})
