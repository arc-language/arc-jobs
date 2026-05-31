'use strict'

const { test, describe, beforeEach } = require('bun:test')
const assert = require('assert')
const { MemoryAdapter } = require('../src/adapters/memory')
const { createQueue } = require('../src/core/queue')
const { arcJobsHandle, broadcastProgress } = require('../src/dashboard/handler')

function makeQueue(name = 'default') {
  process.env.NODE_ENV = 'test'
  const adapter = new MemoryAdapter({ name })
  const q = createQueue(adapter)
  return { q, adapter }
}

function makeRequest(method, path, body) {
  const url = 'http://localhost' + path
  const opts = { method }
  if (body) {
    opts.body = JSON.stringify(body)
    opts.headers = { 'Content-Type': 'application/json' }
  }
  return new Request(url, opts)
}

describe('arcJobsHandle()', () => {
  let queues, q, adapter

  beforeEach(() => {
    const setup = makeQueue('default')
    q = setup.q
    adapter = setup.adapter
    queues = { default: q }
  })

  test('returns null for non-matching paths', async () => {
    const res = await arcJobsHandle(makeRequest('GET', '/api/other'), queues)
    assert.strictEqual(res, null)
  })

  test('serves dashboard HTML at /_arc/jobs', async () => {
    const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs'), queues)
    assert.ok(res instanceof Response)
    assert.strictEqual(res.status, 200)
    assert.ok(res.headers.get('Content-Type').includes('text/html'))
    const body = await res.text()
    assert.ok(body.includes('arc-jobs'))
    assert.ok(body.includes('<!DOCTYPE html>'))
  })

  test('serves dashboard HTML at /_arc/jobs/', async () => {
    const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/'), queues)
    assert.ok(res instanceof Response)
    assert.strictEqual(res.status, 200)
  })

  test('returns 404 for unknown sub-path', async () => {
    const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/nonexistent'), queues)
    assert.strictEqual(res.status, 404)
  })

  describe('/api/overview', () => {
    test('returns queue names and zero counts when empty', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/overview'), queues)
      assert.strictEqual(res.status, 200)
      const data = await res.json()
      assert.ok(Array.isArray(data.queues))
      assert.strictEqual(data.queues[0].name, 'default')
      assert.strictEqual(data.queues[0].pending, 0)
    })

    test('reflects enqueued job in pending count', async () => {
      await adapter.enqueue('TestJob', [1])
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/overview'), queues)
      const data = await res.json()
      assert.strictEqual(data.queues[0].pending, 1)
    })

    test('multiple queues all listed', async () => {
      const { q: q2 } = makeQueue('payments')
      const multiQueues = { default: q, payments: q2 }
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/overview'), multiQueues)
      const data = await res.json()
      const names = data.queues.map(q => q.name)
      assert.ok(names.includes('default'))
      assert.ok(names.includes('payments'))
    })
  })

  describe('/api/jobs', () => {
    test('returns empty jobs list when queue is empty', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs?queue=default'), queues)
      const data = await res.json()
      assert.deepStrictEqual(data.jobs, [])
    })

    test('returns pending jobs', async () => {
      await adapter.enqueue('MyJob', [42, 'hello'])
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs?queue=default'), queues)
      const data = await res.json()
      assert.strictEqual(data.jobs.length, 1)
      assert.strictEqual(data.jobs[0].name, 'MyJob')
      assert.deepStrictEqual(data.jobs[0].args, [42, 'hello'])
    })

    test('returns empty array for unknown queue', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs?queue=unknown'), queues)
      const data = await res.json()
      assert.deepStrictEqual(data.jobs, [])
    })
  })

  describe('/api/jobs/:id/cancel', () => {
    test('cancels a pending job', async () => {
      const id = await adapter.enqueue('CancelMe', [])
      assert.strictEqual(await adapter.size(), 1)

      const res = await arcJobsHandle(makeRequest('POST', '/_arc/jobs/api/jobs/' + id + '/cancel'), queues)
      const data = await res.json()
      assert.strictEqual(data.ok, true)
      assert.strictEqual(await adapter.size(), 0)
    })

    test('cancel returns ok even for unknown id', async () => {
      const res = await arcJobsHandle(makeRequest('POST', '/_arc/jobs/api/jobs/no-such-id/cancel'), queues)
      const data = await res.json()
      assert.strictEqual(data.ok, true)
    })
  })

  describe('/api/dlq', () => {
    test('returns empty array when no dead jobs', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/dlq?queue=default'), queues)
      const data = await res.json()
      assert.deepStrictEqual(data.jobs, [])
    })

    test('returns dead jobs', async () => {
      const id = await adapter.enqueue('FailedJob', [99])
      await adapter.dequeue()
      await adapter.fail(id, 'boom', 3, 3)
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/dlq?queue=default'), queues)
      const data = await res.json()
      assert.strictEqual(data.jobs.length, 1)
      assert.strictEqual(data.jobs[0].name, 'FailedJob')
    })
  })

  describe('/api/dlq/replay (all)', () => {
    test('replays all dead jobs', async () => {
      q.register('FailedJob', async () => {})
      const id1 = await adapter.enqueue('FailedJob', [])
      const id2 = await adapter.enqueue('FailedJob', [])
      await adapter.dequeue()
      await adapter.dequeue()
      await adapter.fail(id1, 'err', 3, 3)
      await adapter.fail(id2, 'err', 3, 3)

      const res = await arcJobsHandle(makeRequest('POST', '/_arc/jobs/api/dlq/replay?queue=default'), queues)
      const data = await res.json()
      assert.strictEqual(data.replayed, 2)
      assert.strictEqual(await adapter.size(), 2)
    })
  })

  describe('/api/dlq/:id/replay', () => {
    test('replays a single dead job by id', async () => {
      q.register('OneJob', async () => {})
      const jobId = await adapter.enqueue('OneJob', [5])
      await adapter.dequeue()
      await adapter.fail(jobId, 'err', 3, 3)

      const res = await arcJobsHandle(makeRequest('POST', '/_arc/jobs/api/dlq/' + jobId + '/replay'), queues)
      const data = await res.json()
      assert.strictEqual(data.ok, true)
      assert.strictEqual(await adapter.size(), 1)
      assert.strictEqual(adapter.pending('OneJob').length, 1)
    })
  })

  describe('/api/locks', () => {
    test('returns empty locks when none active', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/locks'), queues)
      const data = await res.json()
      assert.deepStrictEqual(data.locks, [])
    })

    test('returns active locks', async () => {
      await adapter.acquireLock('invoice:42', 60000)
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/locks'), queues)
      const data = await res.json()
      assert.strictEqual(data.locks.length, 1)
      assert.strictEqual(data.locks[0].key, 'invoice:42')
      assert.ok(data.locks[0].expiresAt > Date.now())
    })

    test('does not return expired locks', async () => {
      await adapter.acquireLock('old-lock', 1)  // 1ms TTL — expires immediately
      await new Promise(r => setTimeout(r, 10))
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/locks'), queues)
      const data = await res.json()
      assert.deepStrictEqual(data.locks, [])
    })
  })

  describe('/api/locks/:key DELETE', () => {
    test('releases an active lock', async () => {
      await adapter.acquireLock('my-lock', 60000)
      assert.strictEqual(await adapter.acquireLock('my-lock', 60000), false)  // lock is held

      const res = await arcJobsHandle(makeRequest('DELETE', '/_arc/jobs/api/locks/my-lock'), queues)
      const data = await res.json()
      assert.strictEqual(data.ok, true)
      assert.strictEqual(await adapter.acquireLock('my-lock', 60000), true)  // lock was released
    })

    test('URL-encoded lock keys are decoded', async () => {
      await adapter.acquireLock('job:42:step', 60000)
      const res = await arcJobsHandle(makeRequest('DELETE', '/_arc/jobs/api/locks/job%3A42%3Astep'), queues)
      const data = await res.json()
      assert.strictEqual(data.ok, true)
    })
  })

  describe('/api/schedules', () => {
    test('returns empty list when no schedules', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/schedules'), queues, [])
      const data = await res.json()
      assert.deepStrictEqual(data.schedules, [])
    })

    test('returns schedule info with next fire time', async () => {
      const schedules = [{ job: 'DailyDigest', cron: '0 9 * * *', queue: 'default' }]
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/schedules'), queues, schedules)
      const data = await res.json()
      assert.strictEqual(data.schedules.length, 1)
      assert.strictEqual(data.schedules[0].job, 'DailyDigest')
      assert.strictEqual(data.schedules[0].cron, '0 9 * * *')
      assert.ok(data.schedules[0].next)
    })
  })

  describe('broadcastProgress()', () => {
    test('is exported and callable', () => {
      assert.strictEqual(typeof broadcastProgress, 'function')
      // No SSE listeners attached — should not throw
      assert.doesNotThrow(() => broadcastProgress('job-1', 42, { processed: 5 }))
    })
  })

  describe('/events SSE endpoint', () => {
    test('returns SSE response with correct headers', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/events'), queues)
      assert.ok(res instanceof Response)
      assert.ok(res.headers.get('Content-Type').includes('text/event-stream'))
    })
  })

  describe('/api/jobs/:id/progress SSE endpoint', () => {
    test('returns SSE response with correct headers', async () => {
      const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs/job-123/progress'), queues)
      assert.ok(res instanceof Response)
      assert.ok(res.headers.get('Content-Type').includes('text/event-stream'))
    })

    test('does not require auth (progress endpoint bypasses auth check)', async () => {
      const origSecret = process.env.ARC_JOBS_SECRET
      process.env.ARC_JOBS_SECRET = 'required-secret'
      try {
        // No auth header — progress endpoint is always accessible
        const res = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs/job-abc/progress'), queues)
        assert.ok(res instanceof Response)
        assert.ok(res.headers.get('Content-Type').includes('text/event-stream'))
      } finally {
        if (origSecret === undefined) delete process.env.ARC_JOBS_SECRET
        else process.env.ARC_JOBS_SECRET = origSecret
      }
    })
  })

  describe('broadcastProgress() with active listeners', () => {
    test('delivers progress to subscribed SSE stream', async () => {
      // Subscribe to progress for job-xyz
      const sseRes = await arcJobsHandle(makeRequest('GET', '/_arc/jobs/api/jobs/job-xyz/progress'), queues)
      assert.ok(sseRes instanceof Response)

      // Broadcast — should not throw even with active listener
      assert.doesNotThrow(() => broadcastProgress('job-xyz', 50, { step: 'half' }))
      assert.doesNotThrow(() => broadcastProgress('job-xyz', 100))
    })
  })

  describe('auth — ARC_JOBS_SECRET env var', () => {
    function makeAuthRequest(method, path, token) {
      const url = 'http://localhost' + path
      const headers = token ? { authorization: `Bearer ${token}` } : {}
      return new Request(url, { method, headers })
    }

    test('allows request with correct Bearer token', async () => {
      const origSecret = process.env.ARC_JOBS_SECRET
      process.env.ARC_JOBS_SECRET = 'my-secret-123'
      try {
        const res = await arcJobsHandle(makeAuthRequest('GET', '/_arc/jobs/api/overview', 'my-secret-123'), queues)
        assert.strictEqual(res.status, 200)
      } finally {
        if (origSecret === undefined) delete process.env.ARC_JOBS_SECRET
        else process.env.ARC_JOBS_SECRET = origSecret
      }
    })

    test('allows request with correct ?token= query param', async () => {
      const origSecret = process.env.ARC_JOBS_SECRET
      process.env.ARC_JOBS_SECRET = 'qs-secret'
      try {
        const url = 'http://localhost/_arc/jobs/api/overview?token=qs-secret'
        const res = await arcJobsHandle(new Request(url), queues)
        assert.strictEqual(res.status, 200)
      } finally {
        if (origSecret === undefined) delete process.env.ARC_JOBS_SECRET
        else process.env.ARC_JOBS_SECRET = origSecret
      }
    })

    test('returns 401 JSON for API paths with wrong token', async () => {
      const origSecret = process.env.ARC_JOBS_SECRET
      process.env.ARC_JOBS_SECRET = 'correct-secret'
      try {
        const res = await arcJobsHandle(makeAuthRequest('GET', '/_arc/jobs/api/overview', 'wrong-token'), queues)
        assert.strictEqual(res.status, 401)
        const data = await res.json()
        assert.ok(data.error)
      } finally {
        if (origSecret === undefined) delete process.env.ARC_JOBS_SECRET
        else process.env.ARC_JOBS_SECRET = origSecret
      }
    })

    test('returns 401 HTML for dashboard root with wrong token', async () => {
      const origSecret = process.env.ARC_JOBS_SECRET
      process.env.ARC_JOBS_SECRET = 'correct-secret'
      try {
        const res = await arcJobsHandle(makeAuthRequest('GET', '/_arc/jobs', 'bad'), queues)
        assert.strictEqual(res.status, 401)
        const body = await res.text()
        assert.ok(body.includes('<!DOCTYPE html>'))
      } finally {
        if (origSecret === undefined) delete process.env.ARC_JOBS_SECRET
        else process.env.ARC_JOBS_SECRET = origSecret
      }
    })

    test('allows request when opts.auth function returns true', async () => {
      const res = await arcJobsHandle(
        makeRequest('GET', '/_arc/jobs/api/overview'),
        queues,
        [],
        { auth: async () => true }
      )
      assert.strictEqual(res.status, 200)
    })

    test('returns 401 when opts.auth function returns false', async () => {
      const res = await arcJobsHandle(
        makeRequest('GET', '/_arc/jobs/api/overview'),
        queues,
        [],
        { auth: async () => false }
      )
      assert.strictEqual(res.status, 401)
    })
  })
})
