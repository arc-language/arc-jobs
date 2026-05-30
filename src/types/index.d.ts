/**
 * Options for enqueuing a job.
 */
export interface EnqueueOptions {
  /** Dequeue order within the queue. High-priority jobs run before normal, normal before low. */
  priority?: 'high' | 'normal' | 'low'
  /** Delay before the job becomes eligible to run, in milliseconds. */
  delayMs?: number
  /** Absolute time the job should become eligible to run. */
  at?: Date
  /** Maximum number of attempts before the job moves to the dead letter queue. Default: 3. */
  maxAttempts?: number
  /** Idempotency key — if a pending/running job with this key exists, the new enqueue is skipped. */
  idempotencyKey?: string
  /** TTL for the idempotency lock in milliseconds. Default: 3 600 000 (1 hour). */
  lockTtlMs?: number
}

/**
 * A job record as stored in the queue.
 */
export interface Job {
  id: string
  name: string
  args: unknown[]
  priority: number
  attempts: number
  maxAttempts: number
  scheduledAt?: number
  startedAt?: number
  status?: string
  error?: string
}

/**
 * The current status of a job, returned by `Queue.status(id)`.
 */
export interface JobStatus {
  id: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'unknown'
  /** Progress percentage (0–100) for `@progress` jobs. */
  progress?: number
  error?: string
  createdAt?: number
  startedAt?: number
  completedAt?: number
}

/**
 * The adapter interface all queue backends must implement.
 * Extend `BaseAdapter` rather than implementing this directly.
 */
export interface QueueAdapter {
  enqueue(name: string, args: unknown[], opts?: EnqueueOptions): Promise<string>
  dequeue(): Promise<Job | null>
  complete(id: string): Promise<void>
  fail(id: string, error: string, attempts: number, maxAttempts: number): Promise<void>
  updateProgress(id: string, pct: number): Promise<void>
  status(id: string): Promise<JobStatus>
  size(): Promise<number>
  dead(): Promise<Job[]>
  replayDead(): Promise<number>
  /** Acquire an `@unique` lock. Returns `true` if acquired, `false` if already held. */
  acquireLock(key: string, ttlMs: number): Promise<boolean>
  releaseLock(key: string): Promise<void>
  drain(timeoutMs?: number): Promise<void>
}

/**
 * The public Queue API returned by `createQueue(adapter)`.
 *
 * @example
 * ```ts
 * import { createQueue, SqliteAdapter } from '@arc-lang/arc-jobs'
 * const queue = createQueue(new SqliteAdapter({ path: 'app.db' }))
 * queue.register('SendEmail', async (userId: number) => { ... })
 * queue.start()
 * const id = await queue.enqueue('SendEmail', [42])
 * ```
 */
export interface Queue {
  /** Enqueue a job by name with the given args. Returns the job ID. */
  enqueue(name: string, args: unknown[], opts?: EnqueueOptions): Promise<string>
  /** Acquire an `@unique` lock for the given key and TTL. */
  acquireLock(key: string, ttlMs: number): Promise<boolean>
  releaseLock(key: string): Promise<void>
  /** Get the current status of a job by ID. */
  status(id: string): Promise<JobStatus>
  /** Number of jobs currently pending (not yet running). */
  size(): Promise<number>
  /** Dead letter queue — jobs that exhausted all retries. */
  dead(): Promise<Job[]>
  /** Re-enqueue all dead jobs. Returns the count replayed. */
  replayDead(): Promise<number>
  /** Wait until the queue drains or the timeout elapses. */
  drain(timeoutMs?: number): Promise<void>
  /** Cancel a pending job by ID. */
  cancel(id: string): Promise<void>
  /** Update progress for an `@progress` job (0–100). */
  updateProgress(id: string, pct: number, meta?: Record<string, unknown>): Promise<void>
  /** Start the queue processing loop. Call once at server startup. */
  start(intervalMs?: number): void

  // ── Test helpers (MemoryAdapter only) ────────────────────────────────────
  /** Reset all queues to empty state. Only available with MemoryAdapter. */
  reset(): void
  /** Run all pending jobs synchronously. Only available with MemoryAdapter. */
  flush(): Promise<void>
  /** Return pending jobs, optionally filtered by name. */
  pending(name?: string): Job[]
  /** Return completed jobs, optionally filtered by name. */
  completed(name?: string): Job[]
  /** Assert that a job with the given name and args is pending. Throws if not found. */
  assertEnqueued(name: string, args: unknown[]): void
}

/**
 * A handle returned when a job is enqueued, providing access to its status and progress stream.
 */
export interface JobHandle {
  id: string
  /** SSE URL for real-time progress updates (`@progress` jobs). */
  progressUrl: string
  status(): Promise<JobStatus>
  cancel(): Promise<void>
}

/** Internal registry entry for a job handler. */
export interface JobRegistryEntry {
  fn: (...args: unknown[]) => Promise<void>
  priority?: number
  maxRetries?: number
  timeoutMs?: number
  backoffMs?: number
  concurrency?: number
  schedule?: string
  hasProgress?: boolean
  thenJob?: string
}

/**
 * Abstract base class for queue adapters.
 * Extend this class to implement a custom backend.
 *
 * @example
 * ```ts
 * import { BaseAdapter } from '@arc-lang/arc-jobs'
 * class MyAdapter extends BaseAdapter {
 *   async enqueue(name, args, opts) { ... }
 *   async dequeue() { ... }
 *   // ... implement all abstract methods
 * }
 * ```
 */
export declare class BaseAdapter implements QueueAdapter {
  constructor(opts?: { name?: string; timeout?: number; concurrency?: number })
  enqueue(name: string, args: unknown[], opts?: EnqueueOptions): Promise<string>
  dequeue(): Promise<Job | null>
  complete(id: string): Promise<void>
  fail(id: string, error: string, attempts: number, maxAttempts: number): Promise<void>
  updateProgress(id: string, pct: number): Promise<void>
  status(id: string): Promise<JobStatus>
  size(): Promise<number>
  dead(): Promise<Job[]>
  replayDead(): Promise<number>
  acquireLock(key: string, ttlMs: number): Promise<boolean>
  releaseLock(key: string): Promise<void>
  drain(timeoutMs?: number): Promise<void>
}

/**
 * In-memory queue adapter for development and testing.
 *
 * In `NODE_ENV=test`, jobs are NOT auto-processed — call `flush()` explicitly.
 * Jobs are lost on process restart; not suitable for production.
 *
 * @example
 * ```ts
 * import { createQueue, MemoryAdapter } from '@arc-lang/arc-jobs'
 * process.env.NODE_ENV = 'test'
 * const queue = createQueue(new MemoryAdapter())
 * queue.register('MyJob', async (x) => { ... })
 * await queue.enqueue('MyJob', [42])
 * queue.assertEnqueued('MyJob', [42])
 * await queue.flush()
 * ```
 */
export declare class MemoryAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; timeout?: number; concurrency?: number })
  reset(): void
  flush(): Promise<void>
  pending(name?: string): Job[]
  completed(name?: string): Job[]
  assertEnqueued(name: string, args: unknown[]): void
}

/**
 * SQLite-backed queue adapter — zero infrastructure, persistent, ~15k jobs/sec in WAL mode.
 *
 * Uses `bun:sqlite` on Bun or `better-sqlite3` on Node.js.
 * Reuses Arc's existing `_db` connection when passed via `opts.db`.
 *
 * @example
 * ```ts
 * import { createQueue, SqliteAdapter } from '@arc-lang/arc-jobs'
 * const queue = createQueue(new SqliteAdapter({ path: 'app.db' }))
 * queue.start()
 * ```
 */
export declare class SqliteAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; db?: unknown; path?: string; timeout?: number; concurrency?: number })
  /** Start polling for delayed/scheduled jobs. */
  startPoller(registry: Record<string, JobRegistryEntry>, intervalMs?: number): void
  stopPoller(): void
}

/**
 * Redis-backed queue adapter — high-throughput, 100k+ ops/sec.
 *
 * Uses `Bun.Redis` (built-in) on Bun or `ioredis` on Node.js.
 * Priority queues via sorted sets; `@unique` locks via atomic `SET NX PX`.
 *
 * @example
 * ```ts
 * import { createQueue, RedisAdapter } from '@arc-lang/arc-jobs'
 * const queue = createQueue(new RedisAdapter({ url: process.env.REDIS_URL }))
 * queue.start()
 * ```
 */
export declare class RedisAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; url?: string; timeout?: number; concurrency?: number })
  startPoller(registry: Record<string, JobRegistryEntry>, intervalMs?: number): void
  stopPoller(): void
  disconnect(): Promise<void>
}

/**
 * Wrap a queue adapter with the public Queue API and job registry.
 *
 * @example
 * ```ts
 * const queue = createQueue(new SqliteAdapter({ path: 'app.db' }))
 * queue.register('SendEmail', async (userId: number) => { ... }, { maxRetries: 5 })
 * queue.start()
 * await queue.enqueue('SendEmail', [42])
 * ```
 */
export declare function createQueue(adapter: QueueAdapter): Queue

/**
 * Test whether a 5-field cron expression matches a given date.
 *
 * @param expr - 5-field cron: `min hour dom month dow` (supports `*`, `*/step`, `N-M`, comma lists)
 * @param date - Date to test against
 * @returns `true` if the expression matches the date's minute/hour/day/month/weekday
 *
 * @example
 * ```ts
 * cronMatches('0 9 * * 1', new Date('2026-06-01T09:00:00')) // true — Monday at 9am
 * cronMatches('*/15 * * * *', new Date('2026-06-01T09:15:00')) // true — every 15 min
 * ```
 */
export declare function cronMatches(expr: string, date: Date): boolean

/**
 * Find the next Date on which the given cron expression will fire.
 *
 * Walks forward minute-by-minute up to one year. Returns `null` if no match found.
 *
 * @example
 * ```ts
 * nextFireTime('0 9 * * 1') // next Monday at 09:00 UTC
 * nextFireTime('0 0 1 * *') // first day of next month at midnight
 * ```
 */
export declare function nextFireTime(expr: string): Date | null

/**
 * Start the in-process cron scheduler. Checks expressions every 30 seconds with minute-level dedup.
 *
 * @param schedules - Array of `{ expr, jobName, queueName?, args? }` entries
 * @param queues    - Map of queue name → Queue (from `createQueue`)
 * @returns The `setInterval` handle (call `clearInterval` to stop)
 *
 * @example
 * ```ts
 * startScheduler(
 *   [{ expr: '0 9 * * *', jobName: 'DailyDigest', queueName: 'default' }],
 *   { default: queue }
 * )
 * ```
 */
export declare function startScheduler(
  schedules: Array<{ expr: string; jobName: string; queueName?: string; args?: unknown[] }>,
  queues: Record<string, Queue>
): ReturnType<typeof setInterval>
