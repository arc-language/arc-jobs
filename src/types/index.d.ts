export interface EnqueueOptions {
  priority?: 'high' | 'normal' | 'low'
  delayMs?: number
  at?: Date
  maxAttempts?: number
  idempotencyKey?: string
  lockTtlMs?: number
}

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

export interface JobStatus {
  id: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'unknown'
  progress?: number
  error?: string
  createdAt?: number
  startedAt?: number
  completedAt?: number
}

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
  acquireLock(key: string, ttlMs: number): Promise<boolean>
  releaseLock(key: string): Promise<void>
  drain(timeoutMs?: number): Promise<void>
}

export interface Queue {
  enqueue(name: string, args: unknown[], opts?: EnqueueOptions): Promise<string>
  acquireLock(key: string, ttlMs: number): Promise<boolean>
  releaseLock(key: string): Promise<void>
  status(id: string): Promise<JobStatus>
  size(): Promise<number>
  dead(): Promise<Job[]>
  replayDead(): Promise<number>
  drain(timeoutMs?: number): Promise<void>
  cancel(id: string): Promise<void>
  updateProgress(id: string, pct: number, meta?: Record<string, unknown>): Promise<void>
  start(intervalMs?: number): void
  // Test helpers (MemoryAdapter only)
  reset(): void
  flush(): Promise<void>
  pending(name?: string): Job[]
  completed(name?: string): Job[]
  assertEnqueued(name: string, args: unknown[]): void
}

export interface JobHandle {
  id: string
  progressUrl: string
  status(): Promise<JobStatus>
  cancel(): Promise<void>
}

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

export declare class MemoryAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; timeout?: number; concurrency?: number })
  reset(): void
  flush(): Promise<void>
  pending(name?: string): Job[]
  completed(name?: string): Job[]
  assertEnqueued(name: string, args: unknown[]): void
}

export declare class SqliteAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; db?: unknown; path?: string; timeout?: number; concurrency?: number })
  startPoller(registry: Record<string, JobRegistryEntry>, intervalMs?: number): void
  stopPoller(): void
}

export declare class RedisAdapter extends BaseAdapter {
  constructor(opts?: { name?: string; url?: string; timeout?: number; concurrency?: number })
  startPoller(registry: Record<string, JobRegistryEntry>, intervalMs?: number): void
  stopPoller(): void
  disconnect(): Promise<void>
}

export declare function createQueue(adapter: QueueAdapter): Queue

export declare function cronMatches(expr: string, date: Date): boolean
export declare function nextFireTime(expr: string): Date | null
export declare function startScheduler(
  schedules: Array<{ expr: string; jobName: string; queueName?: string; args?: unknown[] }>,
  queues: Record<string, Queue>
): ReturnType<typeof setInterval>
