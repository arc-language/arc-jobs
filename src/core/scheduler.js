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
      const [, step] = field.split('/'); return val % +step === 0
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
  // Walk forward up to 1 year to find next match
  for (let i = 0; i < 525960; i++) {
    if (cronMatches(expr, next)) return next
    next.setMinutes(next.getMinutes() + 1)
  }
  return null
}

function startScheduler(schedules, queues) {
  let lastMin = -1
  return setInterval(() => {
    const now = new Date()
    const thisMin = now.getMinutes()
    if (thisMin === lastMin) return
    lastMin = thisMin
    for (const { expr, jobName, queueName, args } of schedules) {
      if (cronMatches(expr, now)) {
        const queue = queues[queueName ?? 'default']
        if (queue) {
          queue.enqueue(jobName, args ?? []).catch(e => {
            console.error(JSON.stringify({ ts: now.toISOString(), level: 'error', event: 'schedule_enqueue_failed', job: jobName, error: e?.message }))
          })
          console.error(JSON.stringify({ ts: now.toISOString(), level: 'info', event: 'schedule_fired', job: jobName, cron: expr }))
        }
      }
    }
  }, 30000)
}

module.exports = { cronMatches, nextFireTime, startScheduler }
