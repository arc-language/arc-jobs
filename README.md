# arc-jobs

Production background jobs for [Arc](https://arc-lang.com) — SQLite, Redis, and SQS adapters with scheduling, deduplication, and progress tracking.

**Better than Django Celery because:**
- Zero external broker for development — SQLite queue runs in-process
- Compile-time type-safe — wrong job signatures fail at `arc build`, not in production
- No separate worker process — jobs run inside your server
- `@unique` prevents duplicate jobs (celery-once built-in)
- `@schedule` replaces Celery Beat without a second process

```arc
@queue payments
@priority high
@unique strategy=skip
job ProcessPayment(orderId: Int, amount: Float)
  const order = db.orders.find(orderId)
  stripe.charge(order.userId, amount)

@schedule "0 9 * * *"
job DailyDigest()
  const users = db.users.findMany({ active: true })
  for user in users
    email.send({ to: user.email, subject: "Your digest" })

@progress
job ImportCSV(fileId: Int)
  const rows = db.uploads.find(fileId)
  for i, row in rows
    job.progress(i / rows.length * 100)
    processRow(row)
```

## Install

```bash
bun add arc-jobs
```

Then add queue configuration to `arc.config.json`:

```json
{
  "queues": {
    "default":   { "backend": "sqlite" },
    "payments":  { "backend": "redis", "url": "${REDIS_URL}" },
    "reports":   { "backend": "sqlite", "timeout": 300000 }
  }
}
```

Or run the interactive setup:

```bash
arc-jobs init
```

## Quick Start

### 1. Define jobs in your `.arc` server files

```arc
// server/jobs.arc

// Basic job — uses the "default" queue
job SendWelcomeEmail(userId: Int)
  const user = db.users.find(userId)
  email.send({ to: user.email, subject: "Welcome!" })

// High-priority job on a dedicated Redis queue
@queue payments
@priority high
@retries 5
job ProcessPayment(orderId: Int, amount: Float)
  // ... payment logic

// Prevent duplicate sends while job is running or pending
@queue notifications
@unique timeout=3600000 strategy=skip
job SendInvoice(invoiceId: Int)
  email.send({ to: "..." })

// Schedule without a separate process
@schedule "0 9 * * 1"
job WeeklyReport()
  // ... runs every Monday at 9am
```

### 2. Call jobs from routes

```arc
@route post "/orders" -> Response
  const order = db.orders.create(parseBody(request))
  ProcessPayment(order.id, order.total)   // fire-and-forget
  SendWelcomeEmail.delay(86400000, order.userId)  // delayed 24h
  json(order, 201)
```

### 3. Build and run

```bash
arc build-server .
bun dist/server.js
```

That's it. No broker to set up. Jobs run inside your server process.

---

## Job Annotations Reference

### `@queue <name>`

Route the job to a named queue from `arc.config.json`. Omitting `@queue` uses the `"default"` queue.

```arc
@queue payments
job ProcessPayment(orderId: Int)
  // uses the "payments" queue (Redis, in this example)
```

### `@schedule "<cron>"`

Run the job on a cron schedule. No separate Celery Beat process — the scheduler runs inside your server.

```arc
@schedule "0 9 * * *"     // daily at 9am UTC
@schedule "*/15 * * * *"  // every 15 minutes
@schedule "0 0 1 * *"     // first of every month
job CleanupOldSessions()
  db.sessions.deleteMany({ expiresAt: { lt: now() } })
```

Cron format: `min hour dom month dow` (5-field standard cron).

### `@priority high|normal|low`

Controls dequeue order within a queue. High-priority jobs are processed before normal, normal before low.

```arc
@priority high
job ProcessPayment(orderId: Int)
  // dequeued before normal jobs
```

### `@retries <n>`

Override the default retry count (default: 3). Failed jobs retry with exponential backoff.

```arc
@retries 5
@backoff 2000     // base backoff in ms (doubles each retry)
job SyncInventory(productId: Int)
  // retries up to 5 times: 2s, 4s, 8s, 16s, 32s
```

### `@timeout <ms>`

Override the default job timeout (default: 30,000ms). Jobs that exceed this are treated as failures.

```arc
@timeout 300000   // 5-minute timeout
job GenerateReport(reportId: Int)
  // heavy computation
```

### `@concurrency <n>`

Limit how many instances of this job run simultaneously across all workers.

```arc
@concurrency 2
job ResizeImages(assetId: Int)
  // max 2 running at a time
```

### `@unique`

**celery-once equivalent.** Prevents duplicate jobs when one is already pending or running. Lock is keyed by job name + serialized args. Stale locks (crashed workers) auto-expire.

```arc
@unique                          // default: skip duplicates silently
@unique strategy=skip            // same as above
@unique strategy=reject          // throw JobAlreadyRunning error
@unique strategy=replace         // cancel existing, enqueue new
@unique timeout=3600000          // lock TTL in ms (default: 1 hour)
@unique timeout=300000 strategy=skip
job SendInvoice(invoiceId: Int)
  email.send({ to: "..." })
```

Calling `SendInvoice(42)` twice while the first is pending → second is silently discarded (with `strategy=skip`).

### `@progress`

Enables `job.progress(pct)` inside the job body. Progress streams to `/_arc/jobs/:id/progress` via SSE, which can be consumed by `@live` pages.

```arc
@progress
job ImportCSV(fileId: Int)
  const rows = db.uploads.find(fileId)
  for i, row in rows
    job.progress((i + 1) / rows.length * 100, { processed: i + 1 })
    processRow(row)
```

### `@then <JobName>`

Auto-enqueue another job on success. Validated at compile time — `arc build` fails if `JobName` doesn't exist.

```arc
@then ProcessOrder
job ValidateOrder(orderId: Int)
  // ... validate; if this succeeds ProcessOrder(orderId) is auto-called

job ProcessOrder(orderId: Int)
  // ...
```

---

## Calling Jobs

```arc
// Fire and forget (returns Promise<string> job id)
SendInvoice(invoiceId)

// Delayed execution
SendReminderEmail.delay(86400000, userId)   // delay in ms

// Run at a specific time
DailyDigest.at(new Date("2026-06-01T09:00:00Z"))

// Custom idempotency key
ProcessPayment.unique("order-42-payment", orderId, amount)

// Check job status from a route
@route get "/jobs/:id" -> Response
  const status = await Queue.status(id)
  json(status)
```

---

## Queue Adapters

### SQLite (default — zero ops)

Best for: single-server apps, development, apps with < ~10k jobs/min.

```json
{
  "queues": {
    "default": { "backend": "sqlite" }
  }
}
```

- **~15,000 jobs/sec** in WAL mode
- Zero infrastructure — uses your existing `app.db`
- Persistent across restarts
- Jobs survive server crashes

### Redis (high-throughput)

Best for: high-volume apps, multi-worker deployments, horizontal scaling.

```json
{
  "queues": {
    "default": {
      "backend": "redis",
      "url": "${REDIS_URL}"
    }
  }
}
```

- **100,000+ ops/sec**
- Priority queues via sorted sets
- `@unique` locks via atomic `SET NX PX` (exact celery-once behavior)
- Requires `Bun.Redis` (built-in) or `ioredis` (`bun add ioredis` for Node.js)

### Mixed (recommended for production)

Use SQLite for non-urgent work, Redis for time-critical paths:

```json
{
  "queues": {
    "default":       { "backend": "sqlite" },
    "payments":      { "backend": "redis", "url": "${REDIS_URL}" },
    "notifications": { "backend": "redis", "url": "${REDIS_URL}" }
  }
}
```

---

## Admin Dashboard

A built-in dashboard is served at `/_arc/jobs` when your server is running:

- **Overview**: pending / running / completed / failed counts per queue (live, 2s refresh)
- **Active jobs**: elapsed time, progress bar for `@progress` jobs, cancel button
- **Schedules**: cron expression + next fire time
- **Active locks**: `@unique` locks with remaining TTL, force-unlock button
- **Dead letter queue**: failed jobs with Replay button

---

## CLI

```bash
# Interactive queue setup
arc-jobs init

# Show queue depths and job counts
arc-jobs stats
arc-jobs stats --db path/to/app.db

# Replay dead letter queue
arc-jobs replay
arc-jobs replay --job SendInvoice         # specific job type
arc-jobs replay --db path/to/app.db

# Real-time terminal monitor (refreshes every 2s)
arc-jobs monitor
arc-jobs monitor --db path/to/app.db
```

---

## Testing

In `NODE_ENV=test`, all queues use a synchronous in-memory adapter that does **not** auto-process jobs. Call `Queue.flush()` explicitly to run them.

```javascript
// tests/jobs.test.js
import { Queue, SendInvoice, ProcessPayment } from './dist/server.test.js'

test('SendInvoice is enqueued on order creation', async () => {
  Queue.reset()

  const res = await fetch('http://localhost:3001/orders', {
    method: 'POST',
    body: JSON.stringify({ amount: 99 }),
  })
  assert.strictEqual(res.status, 201)
  Queue.assertEnqueued('SendInvoice', [42])
})

test('SendInvoice executes correctly', async () => {
  Queue.reset()
  await SendInvoice(42)
  await Queue.flush()
  assert.strictEqual(Queue.completed('SendInvoice').length, 1)
  assert.strictEqual(Queue.dead().length, 0)
})

// Test @unique deduplication
test('duplicate SendInvoice calls are deduplicated', async () => {
  Queue.reset()
  await SendInvoice(42)
  await SendInvoice(42)  // duplicate — skipped
  assert.strictEqual(Queue.pending('SendInvoice').length, 1)
})

// Test @then chain
test('@then chain auto-enqueues ProcessOrder', async () => {
  Queue.reset()
  await ValidateOrder(99)
  await Queue.flush()
  assert.strictEqual(Queue.completed('ValidateOrder').length, 1)
  await Queue.flush()  // process the auto-enqueued job
  assert.strictEqual(Queue.completed('ProcessOrder').length, 1)
})
```

Build a test-mode server with `NODE_ENV=test arc build-server .`.

---

## Performance vs Django Celery

| | arc-jobs (SQLite) | arc-jobs (Redis) | Django Celery |
|---|---|---|---|
| **Enqueue latency** | ~0.1ms (local write) | ~0.5ms (local Redis) | ~1-5ms (network hop) |
| **Throughput** | ~15k jobs/sec | ~100k jobs/sec | Millions/min (distributed) |
| **Setup** | Zero — uses app.db | `bun add ioredis` | Redis + worker process + Celery Beat |
| **Worker process** | In-process | In-process | Separate `celery worker` |
| **Scheduler** | In-process setInterval | In-process setInterval | Separate `celery beat` |
| **Type safety** | Compile-time | Compile-time | Runtime (stringly-typed) |
| **`@unique`** | Built-in | Built-in | Requires celery-once |
| **Test mode** | Synchronous flush | Synchronous flush | Needs mock broker |

Arc jobs on Bun outperform Python Celery for I/O-bound work (webhooks, email, API calls) — the common 80% case. Celery wins for CPU-bound tasks (ML, image processing) that benefit from multi-process parallelism.

---

## Cloudflare Workers

On the Cloudflare target, arc-jobs maps to native CF primitives:

| Feature | CF Primitive |
|---|---|
| Queue | CF Queues binding |
| `@schedule` | CF Cron Triggers (in `wrangler.toml`) |
| `@unique` | Durable Objects |
| `@progress` | Durable Objects state |

No configuration needed — `arc build --target cloudflare` handles it automatically.

---

## GitHub Actions CI

```yaml
# .github/workflows/test.yml
name: Test
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v2
      - run: bun install
      - run: bun test packages/arc-jobs/tests/
```

---

## License

MIT
