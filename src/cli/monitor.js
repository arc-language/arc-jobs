'use strict'

const { openDb } = require('./_db')

module.exports = async function monitor(args) {
  const dbPath = args.find((_, i) => args[i - 1] === '--db') ?? 'app.db'
  const db = openDb(dbPath, 'arc-jobs monitor')

  const query = (sql, ...params) => {
    try {
      if (db.query) return db.query(sql).all(...params)
      return db.prepare(sql).all(...params)
    } catch (_) { return [] }
  }

  const clear = () => process.stdout.write('\x1b[2J\x1b[H')
  const cyan = (s) => `\x1b[36m${s}\x1b[0m`
  const red = (s) => `\x1b[31m${s}\x1b[0m`
  const green = (s) => `\x1b[32m${s}\x1b[0m`
  const dim = (s) => `\x1b[2m${s}\x1b[0m`

  function render() {
    clear()
    const now = new Date().toISOString()
    console.log(cyan('arc-jobs monitor') + dim(` - ${now} - Ctrl+C to exit\n`))

    const stats = query(`
      SELECT queue,
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
      FROM _arc_jobs GROUP BY queue
    `)

    console.log('Queue'.padEnd(20) + 'Pending'.padEnd(10) + 'Running'.padEnd(10) + 'Completed'.padEnd(12) + 'Failed')
    console.log('─'.repeat(62))
    for (const r of stats) {
      const failed = r.failed > 0 ? red(String(r.failed)) : '0'
      const running = r.running > 0 ? green(String(r.running)) : '0'
      console.log(r.queue.padEnd(20) + String(r.pending).padEnd(10) + running.padEnd(running.length + (10 - String(r.running).length)) + String(r.completed).padEnd(12) + failed)
    }

    const active = query(`SELECT id, name, queue, started_at, progress FROM _arc_jobs WHERE status='running' ORDER BY started_at ASC LIMIT 10`)
    if (active.length) {
      console.log('\n' + cyan('Active jobs:'))
      for (const job of active) {
        const elapsed = job.started_at ? `${Math.round((Date.now() - job.started_at) / 1000)}s` : '?'
        const prog = job.progress > 0 ? ` [${Math.round(job.progress)}%]` : ''
        console.log(`  ${job.name.padEnd(25)} ${job.queue.padEnd(15)} elapsed: ${elapsed}${prog}`)
      }
    }

    const recent = query(`SELECT name, queue, status, error, completed_at FROM _arc_jobs WHERE status IN ('completed','failed') ORDER BY completed_at DESC LIMIT 5`)
    if (recent.length) {
      console.log('\n' + dim('Recent:'))
      for (const job of recent) {
        const status = job.status === 'failed' ? red('✗') : green('✓')
        const err = job.error ? dim(` — ${job.error.slice(0, 50)}`) : ''
        console.log(`  ${status} ${job.name.padEnd(25)} ${job.queue}${err}`)
      }
    }
  }

  render()
  const timer = setInterval(render, 2000)
  process.on('SIGINT', () => { clearInterval(timer); process.exit(0) })
}
