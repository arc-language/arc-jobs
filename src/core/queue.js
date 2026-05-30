'use strict'

// createQueue() — wraps an adapter with the public Queue API and job registry.
// Called from compiled server.js: const _queues = { default: createQueue(new SqliteAdapter({...})) }

function createQueue(adapter) {
  const registry = {}

  const queue = {
    // Internal — used by emitted server.js
    _adapter: adapter,
    _registry: registry,

    // Register a job handler (called from emitted server.js)
    register(name, fn, opts = {}) {
      registry[name] = { fn, ...opts }
      adapter._registry = registry
    },

    // Enqueue a job
    async enqueue(name, args, opts = {}) {
      return adapter.enqueue(name, args, opts)
    },

    // Acquire a @unique lock before enqueueing
    async acquireLock(key, ttlMs) {
      return adapter.acquireLock(key, ttlMs)
    },

    async releaseLock(key) {
      return adapter.releaseLock(key)
    },

    // Public API
    async status(id) { return adapter.status(id) },
    async size() { return adapter.size() },
    async dead() { return adapter.dead() },
    async replayDead() { return adapter.replayDead() },
    async drain(timeoutMs) { return adapter.drain(timeoutMs) },
    async cancel(id) {
      // Mark as cancelled — adapter must support status updates
      if (adapter._run) {
        adapter._run(`UPDATE _arc_jobs SET status='cancelled', completed_at=?2 WHERE id=?1`, id, Date.now())
      } else if (adapter._pending) {
        const idx = adapter._pending.findIndex(j => j.id === id)
        if (idx !== -1) adapter._pending.splice(idx, 1)
      }
    },

    // Update job progress (called by job.progress() inside @progress jobs)
    async updateProgress(id, pct, meta = {}) {
      await adapter.updateProgress(id, pct)
      // Broadcast to SSE listeners if broadcaster is set
      if (queue._progressBroadcast) queue._progressBroadcast(id, pct, meta)
    },

    // Start processing loop (called at server startup)
    start(intervalMs) {
      if (adapter.startPoller) adapter.startPoller(registry, intervalMs)
      else if (adapter._registry !== undefined) adapter._registry = registry
    },

    // Test helpers (only available in MemoryAdapter)
    reset: () => adapter.reset?.(),
    flush: () => adapter.flush?.(),
    pending: (name) => adapter.pending?.(name) ?? [],
    completed: (name) => adapter.completed?.(name) ?? [],
    assertEnqueued: (...args) => adapter.assertEnqueued?.(...args),
    assertQueue: (...args) => adapter.assertQueue?.(...args),
  }

  return queue
}

module.exports = { createQueue }
