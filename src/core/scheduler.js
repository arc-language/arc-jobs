'use strict'

// Minimal 5-field cron parser and in-process scheduler.
// Emitted inline into server.js when @schedule jobs exist.
// Zero external dependencies — no node-cron, no separate process.

function cronMatches(expr, d) {
  const fields = expr.trim().split(/\s+/)
  if (fields.length !== 5) return false
  const [min, hr, dom, mon, dow] = fields
  const check = (field, val) => {
    if (field === '*') return true
    if (field.includes('/')) {
      const [base, step] = field.split('/')
      const start = base === '*' ? 0 : +base
      return val >= start && (val - start) % +step === 0
    }
    if (field.includes(',')) return field.split(',').map(Number).includes(val)
    if (field.includes('-')) {
      const [lo, hi] = field.split('-').map(Number); return val >= lo && val <= hi
    }
    return +field === val
  }
  return check(min, d.getMinutes()) && check(hr, d.getHours()) &&
    check(dom, d.getDate()) && check(mon, d.getMonth() + 1) &&
    check(dow, d.getDay())
}

function nextFireTime(expr) {
  const now = new Date()
  const next = new Date(now)
  next.setSeconds(0, 0)
  next.setMinutes(next.getMinutes() + 1)
  // Walk forward up to 1 week (10080 min) — avoids blocking event loop on pathological expressions
  for (let i = 0; i < 10080; i++) {
    if (cronMatches(expr, next)) return next
    next.setMinutes(next.getMinutes() + 1)
  }
  return null
}

function startScheduler(schedules, queues) {
  // Per-schedule dedup map: composite key → last fired minKey
  const lastFiredMin = new Map()
  return setInterval(() => {
    const now = new Date()
    const minKey = Math.floor(now.getTime() / 60000)
    for (const { expr, jobName, queueName, args } of schedules) {
      const schedKey = `${jobName}:${queueName ?? 'default'}:${expr}`
      if (lastFiredMin.get(schedKey) === minKey) continue
      if (cronMatches(expr, now)) {
        lastFiredMin.set(schedKey, minKey)
        const queue = queues[queueName ?? 'default']
        if (queue) {
          queue.enqueue(jobName, args ?? []).catch(e => {
            console.error(JSON.stringify({ ts: now.toISOString(), level: 'error', event: 'schedule_enqueue_failed', job: jobName, error: e?.message }))
          })
          console.log(JSON.stringify({ ts: now.toISOString(), level: 'info', event: 'schedule_fired', job: jobName, cron: expr }))
        } else {
          console.warn(JSON.stringify({ ts: now.toISOString(), level: 'warn', event: 'schedule_no_queue', job: jobName, queue: queueName ?? 'default', msg: 'queue not found — job will not fire' }))
        }
      }
    }
  }, 10000)
}

module.exports = { cronMatches, nextFireTime, startScheduler }
