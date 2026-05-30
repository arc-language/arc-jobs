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

  test('next fire time is in the future', () => {
    const next = nextFireTime('30 14 * * *')
    assert.ok(next > new Date())
  })

  test('next fire time matches the cron expression', () => {
    const next = nextFireTime('0 9 * * *')
    // Should fire at minute=0, hour=9
    assert.strictEqual(next.getUTCMinutes(), 0)
    assert.strictEqual(next.getUTCHours(), 9)
  })
})
