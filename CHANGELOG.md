# Changelog

All notable changes to `arc-jobs` will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.1.0] - 2026-05-30

### Added
- `MemoryAdapter` — in-process development and test adapter; synchronous `flush()` in `NODE_ENV=test`
- `SqliteAdapter` — persistent zero-infra queue via `bun:sqlite` (Bun) or `better-sqlite3` (Node.js); ~15k jobs/sec in WAL mode
- `RedisAdapter` — high-throughput queue via `Bun.Redis` or `ioredis`; priority queues via sorted sets; 100k+ ops/sec
- `createQueue(adapter)` — uniform wrapper API over all adapters
- `@unique` lock support: `acquireLock` / `releaseLock` on all adapters; strategies: `skip`, `reject`, `replace`
- `@progress` support: `updateProgress(id, pct)` on all adapters; SSE broadcaster for `/_arc/jobs/:id/progress`
- `@then` job chaining: auto-enqueue next job on successful completion
- `@schedule` cron scheduler: `cronMatches`, `nextFireTime`, `startScheduler`
- Admin dashboard served at `/_arc/jobs` via `arcJobsHandle(req, queues, schedules)`
- Dashboard JSON API: overview stats, active jobs, schedules, locks, dead letter queue, replay, cancel, force-unlock
- Live SSE updates at `/_arc/jobs/events`
- CLI: `arc-jobs init`, `arc-jobs stats`, `arc-jobs replay`, `arc-jobs monitor`
- Full TypeScript definitions in `src/types/index.d.ts`
