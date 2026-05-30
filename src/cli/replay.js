'use strict'

module.exports = async function replay(args) {
  const dbPath = args.find((_, i) => args[i - 1] === '--db') ?? 'app.db'
  const jobFilter = args.find((_, i) => args[i - 1] === '--job')

  let db
  try {
    const { Database } = require('bun:sqlite')
    db = new Database(dbPath)
  } catch (_) {
    try {
      const Database = require('better-sqlite3')
      db = new Database(dbPath)
    } catch (_) {
      console.error('arc-jobs replay: could not open database.')
      process.exit(1)
    }
  }

  const run = (sql, ...params) => db.run ? db.run(sql, ...params) : db.prepare(sql).run(...params)
  const query = (sql, ...params) => db.query ? db.query(sql).all(...params) : db.prepare(sql).all(...params)

  const filter = jobFilter ? `AND name = '${jobFilter.replace(/'/g, "''")}'` : ''
  const dead = query(`SELECT id, name, args, error FROM _arc_jobs WHERE status = 'failed' ${filter} ORDER BY completed_at DESC`)

  if (!dead.length) {
    console.log(`No failed jobs found${jobFilter ? ` for '${jobFilter}'` : ''}.`)
    return
  }

  console.log(`\nReplaying ${dead.length} failed job(s)...\n`)
  let replayed = 0
  for (const job of dead) {
    run(`UPDATE _arc_jobs SET status='pending', attempts=0, error=NULL, scheduled_at=?2 WHERE id=?1`, job.id, Date.now())
    console.log(`  ✓ ${job.name} (${job.id.slice(0, 8)}…)  — was: ${job.error?.slice(0, 60) ?? 'unknown error'}`)
    replayed++
  }
  console.log(`\nReplayed ${replayed} job(s). Start your server to process them.`)
}
