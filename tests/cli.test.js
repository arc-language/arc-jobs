'use strict'

const { test, describe, afterEach } = require('bun:test')
const assert = require('assert')
const os = require('os')
const path = require('path')
const fs = require('fs')
const { SqliteAdapter } = require('../src/adapters/sqlite')

// Create a real SQLite temp file pre-populated via SqliteAdapter DDL
function makeTempDb() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'arc-jobs-cli-test-'))
  const dbPath = path.join(tmpDir, 'test.db')
  const adapter = new SqliteAdapter({ path: dbPath })  // creates table via DDL
  return { dbPath, adapter, cleanup: () => fs.rmSync(tmpDir, { recursive: true, force: true }) }
}

// ── stats.js ────────────────────────────────────────────────────────────────

describe('cli/stats', () => {
  let cleanup

  afterEach(() => cleanup?.())

  test('prints column headers and per-queue row', async () => {
    const { dbPath, adapter, cleanup: c } = makeTempDb()
    cleanup = c
    await adapter.enqueue('SendEmail', [1])
    await adapter.enqueue('SendEmail', [2])
    const j = await adapter.enqueue('BrokenJob', [])
    await adapter.dequeue()
    await adapter.fail(j, 'boom', 3, 3)

    const stats = require('../src/cli/stats')
    const captured = []
    const orig = console.log
    console.log = (...a) => captured.push(a.join(' '))
    try {
      await stats(['--db', dbPath])
    } finally {
      console.log = orig
    }

    const out = captured.join('\n')
    assert.ok(out.includes('Queue'))
    assert.ok(out.includes('default'))
  })

  test('prints "No jobs found" when table is empty', async () => {
    const { dbPath, cleanup: c } = makeTempDb()
    cleanup = c

    const stats = require('../src/cli/stats')
    const captured = []
    const orig = console.log
    console.log = (...a) => captured.push(a.join(' '))
    try {
      await stats(['--db', dbPath])
    } finally {
      console.log = orig
    }
    assert.ok(captured.join('\n').toLowerCase().includes('no jobs'))
  })
})

// ── replay.js ───────────────────────────────────────────────────────────────

describe('cli/replay', () => {
  let cleanup

  afterEach(() => cleanup?.())

  test('replays all failed jobs and updates status to pending', async () => {
    const { dbPath, adapter, cleanup: c } = makeTempDb()
    cleanup = c

    const id1 = await adapter.enqueue('FailJob', [])
    const id2 = await adapter.enqueue('FailJob', [])
    await adapter.dequeue(); await adapter.dequeue()
    await adapter.fail(id1, 'err', 3, 3)
    await adapter.fail(id2, 'err', 3, 3)

    const replay = require('../src/cli/replay')
    const captured = []
    const orig = console.log
    console.log = (...a) => captured.push(a.join(' '))
    try {
      await replay(['--db', dbPath])
    } finally {
      console.log = orig
    }

    const out = captured.join('\n')
    assert.ok(out.includes('2') || out.toLowerCase().includes('replay'))
    assert.strictEqual(await adapter.size(), 2)
  })

  test('--job filter replays only matching job name', async () => {
    const { dbPath, adapter, cleanup: c } = makeTempDb()
    cleanup = c

    const id1 = await adapter.enqueue('JobA', [])
    const id2 = await adapter.enqueue('JobB', [])
    await adapter.dequeue(); await adapter.dequeue()
    await adapter.fail(id1, 'err', 3, 3)
    await adapter.fail(id2, 'err', 3, 3)

    const replay = require('../src/cli/replay')
    console.log = () => {}
    try {
      await replay(['--db', dbPath, '--job', 'JobA'])
    } finally {
      console.log = console.log  // restore (noop here)
    }

    const pending = await adapter.listActive()
    assert.strictEqual(pending.filter(j => j.status === 'pending').length, 1)
    const failed = await adapter.dead()
    assert.strictEqual(failed.length, 1)
    assert.strictEqual(failed[0].name, 'JobB')
  })

  test('prints "No failed jobs" when queue is clean', async () => {
    const { dbPath, cleanup: c } = makeTempDb()
    cleanup = c

    const replay = require('../src/cli/replay')
    const captured = []
    const orig = console.log
    console.log = (...a) => captured.push(a.join(' '))
    try {
      await replay(['--db', dbPath])
    } finally {
      console.log = orig
    }
    assert.ok(captured.join('\n').toLowerCase().includes('no failed'))
  })
})

// ── monitor.js — render() ───────────────────────────────────────────────────

describe('cli/monitor — render output', () => {
  let cleanup

  afterEach(() => cleanup?.())

  test('render writes queue table to stdout without crashing', async () => {
    const { dbPath, adapter, cleanup: c } = makeTempDb()
    cleanup = c
    await adapter.enqueue('MonJob', [])

    const monitor = require('../src/cli/monitor')
    const written = []
    const logged = []
    const origWrite = process.stdout.write.bind(process.stdout)
    const origLog = console.log

    process.stdout.write = (s) => { written.push(s); return true }
    console.log = (...a) => logged.push(a.join(' '))

    // Intercept setInterval to capture the render callback, then stop it
    let renderCb
    const origInterval = globalThis.setInterval
    globalThis.setInterval = (fn, ms) => {
      renderCb = fn
      return origInterval(() => {}, 99999)
    }

    // Intercept process.on to prevent SIGINT handler from calling process.exit
    const origOn = process.on.bind(process)
    process.on = (event, fn) => {
      if (event === 'SIGINT') return  // skip SIGINT handler registration
      return origOn(event, fn)
    }

    try {
      // Start monitor — it registers the interval and calls render() once
      const p = monitor(['--db', dbPath])
      // Trigger one render manually if it hasn't fired yet
      if (renderCb) renderCb()
      // Resolve monitor by emitting SIGINT after a tick
      await new Promise(r => setTimeout(r, 10))
      process.emit('SIGINT')
      await p.catch(() => {})
    } catch (_) {
      // process.exit() throws in test env — ignore
    } finally {
      process.stdout.write = origWrite
      console.log = origLog
      globalThis.setInterval = origInterval
      process.on = origOn
    }

    const all = [...written, ...logged].join('\n')
    assert.ok(all.length > 0)
  })
})
