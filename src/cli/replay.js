'use strict'

const { openDb } = require('./_db')

module.exports = async function replay(args) {
  const dbPath = args.find((_, i) => args[i - 1] === '--db') ?? 'app.db'
  const jobFilter = args.find((_, i) => args[i - 1] === '--job')
  const db = openDb(dbPath, 'arc-jobs replay')

  const run = (sql, ...params) => db.run ? db.run(sql, ...params) : db.prepare(sql).run(...params)
  const query = (sql, ...params) => db.query ? db.query(sql).all(...params) : db.prepare(sql).all(...params)

  const dead = jobFilter
    ? query(`SELECT id, name, args, error FROM _arc_jobs WHERE status = 'failed' AND name = ?1 ORDER BY completed_at DESC`, jobFilter)
    : query(`SELECT id, name, args, error FROM _arc_jobs WHERE status = 'failed' ORDER BY completed_at DESC`)

  if (!dead.length) {
    console.log(`No failed jobs found${jobFilter ? ` for '${jobFilter}'` : ''}.`)
    return
  }

  console.log(`\nReplaying ${dead.length} failed job(s)...\n`)
  let replayed = 0
  for (const job of dead) {
    run(`UPDATE _arc_jobs SET status='pending', attempts=0, error=NULL, scheduled_at=?2 WHERE id=?1`, job.id, Date.now())
    console.log(`  ✓ ${job.name} (${job.id.slice(0, 8)}…)  - was: ${job.error?.slice(0, 60) ?? 'unknown error'}`)
    replayed++
  }
  console.log(`\nReplayed ${replayed} job(s). Start your server to process them.`)
  if (db.close) db.close()
}
