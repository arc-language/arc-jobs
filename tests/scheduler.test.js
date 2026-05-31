'use strict'

const { test, describe } = require('bun:test')
const assert = require('assert')
const { cronMatches, nextFireTime } = require('../src/core/scheduler')

describe('cronMatches', () => {
  test('* * * * * matches any time', () => {
    assert.strictEqual(cronMatches('* * * * *', new Date()), true)
  })

  test('matches specific minute', () => {
    const d = new Date('2026-05-30T09:00:00Z')
    assert.strictEqual(cronMatches('0 9 * * *', d), true)
    assert.strictEqual(cronMatches('1 9 * * *', d), false)
  })

  test('matches specific hour', () => {
    const d = new Date('2026-05-30T14:30:00Z')
    assert.strictEqual(cronMatches('30 14 * * *', d), true)
    assert.strictEqual(cronMatches('30 15 * * *', d), false)
  })

  test('matches day of month', () => {
    const d = new Date('2026-05-15T12:00:00Z')
    assert.strictEqual(cronMatches('0 12 15 * *', d), true)
    assert.strictEqual(cronMatches('0 12 16 * *', d), false)
  })

  test('matches month', () => {
    const d = new Date('2026-05-01T00:00:00Z')
    assert.strictEqual(cronMatches('0 0 1 5 *', d), true)
    assert.strictEqual(cronMatches('0 0 1 6 *', d), false)
  })

  test('matches day of week (0=Sunday)', () => {
    const sunday = new Date('2026-05-31T00:00:00Z')  // a Sunday
    assert.strictEqual(cronMatches('0 0 * * 0', sunday), true)
    assert.strictEqual(cronMatches('0 0 * * 1', sunday), false)
  })

  test('comma lists', () => {
    const d = new Date('2026-05-30T09:00:00Z')
    assert.strictEqual(cronMatches('0 9,17 * * *', d), true)
    assert.strictEqual(cronMatches('0 10,17 * * *', d), false)
  })

  test('step syntax */5', () => {
    const d = new Date()
    d.setMinutes(15)
    assert.strictEqual(cronMatches('*/5 * * * *', d), true)
    d.setMinutes(17)
    assert.strictEqual(cronMatches('*/5 * * * *', d), false)
  })

  test('range syntax 9-17', () => {
    const d = new Date('2026-05-30T12:00:00Z')
    assert.strictEqual(cronMatches('0 9-17 * * *', d), true)
    const d2 = new Date('2026-05-30T18:00:00Z')
    assert.strictEqual(cronMatches('0 9-17 * * *', d2), false)
  })

  test('rejects invalid field count', () => {
    assert.strictEqual(cronMatches('* * * *', new Date()), false)
    assert.strictEqual(cronMatches('* * * * * *', new Date()), false)
  })

  test('common cron expressions', () => {
    // Every day at midnight
    const midnight = new Date('2026-05-30T00:00:00Z')
    assert.strictEqual(cronMatches('0 0 * * *', midnight), true)

    // Every Monday at 9am
    const monday9am = new Date('2026-06-01T09:00:00Z')  // a Monday
    assert.strictEqual(cronMatches('0 9 * * 1', monday9am), true)

    // First of month at noon
    const firstNoon = new Date('2026-06-01T12:00:00Z')
    assert.strictEqual(cronMatches('0 12 1 * *', firstNoon), true)
  })
})

describe('nextFireTime', () => {
  test('returns a Date for valid expression', () => {
    const next = nextFireTime('0 9 * * *')
    assert.ok(next instanceof Date)
    assert.ok(next > new Date())
  })

  test('returns a future Date for a different cron pattern (30 14 * * *)', () => {
    const next = nextFireTime('30 14 * * *')
    assert.ok(next > new Date())
  })

  test('next fire time matches the cron expression', () => {
    const next = nextFireTime('0 9 * * *')
    // Should fire at minute=0, hour=9
    assert.strictEqual(next.getUTCMinutes(), 0)
    assert.strictEqual(next.getUTCHours(), 9)
  })

  test('returns null for impossible expression (minute=60 never matches)', () => {
    // minute field is 60 — getMinutes() is always 0-59, so this never fires
    const next = nextFireTime('60 * * * *')
    assert.strictEqual(next, null)
  })
})

describe('startScheduler', () => {
  const { startScheduler } = require('../src/core/scheduler')

  function captureInterval(fn) {
    let captured
    const orig = globalThis.setInterval
    globalThis.setInterval = (cb, _ms) => { captured = cb; return orig(() => {}, 9999999) }
    const handle = fn()
    globalThis.setInterval = orig
    clearInterval(handle)
    return captured
  }

  test('fires enqueue when cron matches current time', async () => {
    const enqueued = []
    const queues = { default: { enqueue: async (name, args) => { enqueued.push({ name, args }); return 'id' } } }
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'Ping', queueName: 'default' }], queues))
    await tick()
    assert.strictEqual(enqueued.length, 1)
    assert.strictEqual(enqueued[0].name, 'Ping')
  })

  test('deduplicates within the same minute', async () => {
    let count = 0
    const queues = { default: { enqueue: async () => { count++; return 'id' } } }
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'Dedup', queueName: 'default' }], queues))
    await tick()
    await tick()  // second call in same minute key → skipped
    assert.strictEqual(count, 1)
  })

  test('passes args to enqueue', async () => {
    let received = null
    const queues = { default: { enqueue: async (_n, args) => { received = args; return 'id' } } }
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'ArgJob', args: [1, 'two'] }], queues))
    await tick()
    assert.deepStrictEqual(received, [1, 'two'])
  })

  test('defaults to empty args when none provided', async () => {
    let received = null
    const queues = { default: { enqueue: async (_n, args) => { received = args; return 'id' } } }
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'NoArgs' }], queues))
    await tick()
    assert.deepStrictEqual(received, [])
  })

  test('logs warning when queue is not found (does not throw)', () => {
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'OrphanJob', queueName: 'missing' }], {}))
    assert.doesNotThrow(() => tick())
  })

  test('does not enqueue when cron does not match', async () => {
    let count = 0
    const queues = { default: { enqueue: async () => { count++; return 'id' } } }
    // minute=60 never matches
    const tick = captureInterval(() => startScheduler([{ expr: '60 * * * *', jobName: 'Never' }], queues))
    await tick()
    assert.strictEqual(count, 0)
  })

  test('handles enqueue rejection gracefully (does not throw)', async () => {
    const queues = { default: { enqueue: async () => { throw new Error('queue full') } } }
    const tick = captureInterval(() => startScheduler([{ expr: '* * * * *', jobName: 'FailEnqueue' }], queues))
    assert.doesNotThrow(() => tick())
    // Allow async .catch() handler to run
    await new Promise(r => setTimeout(r, 20))
  })

  test('returns a timer handle (clearInterval does not throw)', () => {
    const handle = startScheduler([], {})
    assert.doesNotThrow(() => clearInterval(handle))
  })
})
