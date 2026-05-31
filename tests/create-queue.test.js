'use strict'

const { test, describe, beforeEach } = require('bun:test')
const assert = require('assert')
const { MemoryAdapter } = require('../src/adapters/memory')
const { createQueue } = require('../src/core/queue')

function makeQueue() {
  process.env.NODE_ENV = 'test'
  return createQueue(new MemoryAdapter({ name: 'test' }))
}

describe('createQueue()', () => {
  let queue

  beforeEach(() => { queue = makeQueue() })

  test('exposes enqueue, status, size, dead, drain, cancel', () => {
    assert.strictEqual(typeof queue.enqueue, 'function')
    assert.strictEqual(typeof queue.status, 'function')
    assert.strictEqual(typeof queue.size, 'function')
    assert.strictEqual(typeof queue.dead, 'function')
    assert.strictEqual(typeof queue.drain, 'function')
    assert.strictEqual(typeof queue.cancel, 'function')
  })

  test('exposes test helpers on MemoryAdapter', () => {
    assert.strictEqual(typeof queue.reset, 'function')
    assert.strictEqual(typeof queue.flush, 'function')
    assert.strictEqual(typeof queue.pending, 'function')
    assert.strictEqual(typeof queue.assertEnqueued, 'function')
  })

  test('register + enqueue + flush executes job handler', async () => {
    const log = []
    queue.register('LogJob', async (msg) => log.push(msg))
    await queue.enqueue('LogJob', ['hello'])
    await queue.flush()
    assert.deepStrictEqual(log, ['hello'])
  })

  test('job handler receives correct args', async () => {
    let received = null
    queue.register('ArgJob', async (a, b, c) => { received = { a, b, c } })
    await queue.enqueue('ArgJob', [1, 'two', true])
    await queue.flush()
    assert.deepStrictEqual(received, { a: 1, b: 'two', c: true })
  })

  test('failed job moves to dead after max retries', async () => {
    queue.register('BrokenJob', async () => { throw new Error('boom') }, { maxRetries: 1 })
    await queue.enqueue('BrokenJob', [])
    await queue.flush()
    const dead = await queue.dead()
    assert.strictEqual(dead.length, 1)
    assert.ok(dead[0].error.includes('boom'))
  })

  test('replayDead re-enqueues failed jobs', async () => {
    let calls = 0
    queue.register('RetryJob', async () => {
      calls++
      if (calls === 1) throw new Error('first fails')
    }, { maxRetries: 1 })

    await queue.enqueue('RetryJob', [])
    await queue.flush()
    assert.strictEqual(calls, 1)

    await queue.replayDead()
    await queue.flush()
    assert.strictEqual(calls, 2)
  })

  test('cancel removes job from pending', async () => {
    const id = await queue.enqueue('TestJob', [])
    assert.strictEqual(await queue.size(), 1)
    await queue.cancel(id)
    assert.strictEqual(await queue.size(), 0)
  })

  test('multiple queues are independent', async () => {
    const q2 = makeQueue()
    const log1 = [], log2 = []
    queue.register('Job', async (x) => log1.push(x))
    q2.register('Job', async (x) => log2.push(x))

    await queue.enqueue('Job', ['from-q1'])
    await q2.enqueue('Job', ['from-q2'])
    await queue.flush()
    await q2.flush()

    assert.deepStrictEqual(log1, ['from-q1'])
    assert.deepStrictEqual(log2, ['from-q2'])
  })

  test('assertEnqueued works via queue interface', async () => {
    await queue.enqueue('CheckJob', [42, 'hello'])
    assert.doesNotThrow(() => queue.assertEnqueued('CheckJob', [42, 'hello']))
    assert.throws(() => queue.assertEnqueued('CheckJob', [99]), /no pending job/)
  })

  describe('@unique lock via queue.acquireLock', () => {
    test('acquireLock returns true first time', async () => {
      const ok = await queue.acquireLock('my-lock', 60000)
      assert.strictEqual(ok, true)
    })

    test('duplicate lock is rejected', async () => {
      await queue.acquireLock('dupe-lock', 60000)
      const second = await queue.acquireLock('dupe-lock', 60000)
      assert.strictEqual(second, false)
    })

    test('releaseLock allows re-acquire', async () => {
      await queue.acquireLock('temp-lock', 60000)
      await queue.releaseLock('temp-lock')
      const ok = await queue.acquireLock('temp-lock', 60000)
      assert.strictEqual(ok, true)
    })
  })

  describe('updateProgress with _progressBroadcast', () => {
    test('updateProgress calls _progressBroadcast when set', async () => {
      const broadcasts = []
      queue._progressBroadcast = (id, pct, meta) => broadcasts.push({ id, pct, meta })
      await queue.updateProgress('job-123', 55, { step: 'upload' })
      assert.strictEqual(broadcasts.length, 1)
      assert.strictEqual(broadcasts[0].id, 'job-123')
      assert.strictEqual(broadcasts[0].pct, 55)
      assert.deepStrictEqual(broadcasts[0].meta, { step: 'upload' })
      delete queue._progressBroadcast
    })

    test('updateProgress works without _progressBroadcast (no error)', async () => {
      await assert.doesNotReject(() => queue.updateProgress('job-456', 80))
    })
  })

  describe('job @then chain simulation', () => {
    test('thenJob is auto-enqueued by _runJob', async () => {
      const log = []
      queue.register('Step1', async () => log.push('step1'), { thenJob: 'Step2' })
      queue.register('Step2', async () => log.push('step2'))

      await queue.enqueue('Step1', [])
      await queue.flush()
      // Step2 auto-enqueued after Step1 completes
      await queue.flush()

      assert.deepStrictEqual(log, ['step1', 'step2'])
    })
  })
})
