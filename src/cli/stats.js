'use strict'

const { openDb } = require('./_db')

module.exports = async function stats(args) {
  const dbPath = args.find((_, i) => args[i - 1] === '--db') ?? 'app.db'
  const db = openDb(dbPath, 'arc-jobs stats')

  const query = (sql, ...params) => {
    if (db.query) return db.query(sql).all(...params)
    return db.prepare(sql).all(...params)
  }

  try {
    const rows = query(`
      SELECT queue,
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
      FROM _arc_jobs
      GROUP BY queue
    `)

    if (!rows.length) {
      console.log('No jobs found. Have you run arc-jobs init and used @queue annotations?')
      return
    }

    console.log('\narc-jobs stats\n')
    console.log('Queue'.padEnd(20) + 'Pending'.padEnd(10) + 'Running'.padEnd(10) + 'Completed'.padEnd(12) + 'Failed')
    console.log('-'.repeat(62))
    for (const r of rows) {
      console.log(r.queue.padEnd(20) + String(r.pending).padEnd(10) + String(r.running).padEnd(10) + String(r.completed).padEnd(12) + String(r.failed))
    }
    console.log()
  } catch (_e) {
    console.error('arc-jobs stats: _arc_jobs table not found. Run your server first to create it.')
  }
}
