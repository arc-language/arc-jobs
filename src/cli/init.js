'use strict'

const fs = require('node:fs')
const path = require('node:path')
const readline = require('node:readline')

module.exports = async function init(_args) {
  const configPath = path.resolve('arc.config.json')
  let config = {}
  if (fs.existsSync(configPath)) {
    try { config = JSON.parse(fs.readFileSync(configPath, 'utf8')) } catch (_) {}
  }

  if (config.queues) {
    console.log('arc.config.json already has a "queues" section:')
    console.log(JSON.stringify(config.queues, null, 2))
    return
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  const ask = (q) => new Promise(r => rl.question(q, r))

  console.log('\narc-jobs init — configure your job queues\n')

  const backend = await ask('Default queue backend? [sqlite/redis] (default: sqlite): ')
  const queues = {
    default: { backend: backend.trim() || 'sqlite' }
  }

  if (queues.default.backend === 'sqlite') {
    console.log('✓ Default queue: SQLite (zero-ops, ~15k jobs/sec, single-server)')
  } else {
    const url = await ask('Redis URL? (default: ${REDIS_URL}): ')
    queues.default.url = url.trim() || '${REDIS_URL}'
    console.log('✓ Default queue: Redis (high-throughput, distributed workers)')
  }

  const addMore = await ask('\nAdd more queues? [y/N]: ')
  if (addMore.toLowerCase() === 'y') {
    console.log('\nTip: separate queues let different jobs use different adapters (e.g. payments on Redis, reports on SQLite)\n')
    let adding = true
    while (adding) {
      const name = await ask('Queue name (e.g. payments, notifications): ')
      if (!name.trim()) break
      const qBackend = await ask('Backend? [sqlite/redis] (default: redis): ')
      const entry = { backend: qBackend.trim() || 'redis' }
      if (entry.backend === 'redis') {
        const url = await ask('Redis URL? (default: ${REDIS_URL}): ')
        entry.url = url.trim() || '${REDIS_URL}'
      }
      queues[name.trim()] = entry
      const cont = await ask('Add another? [y/N]: ')
      adding = cont.toLowerCase() === 'y'
    }
  }

  rl.close()

  config.queues = queues
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + '\n')
  console.log('\n✓ arc.config.json updated with queues configuration')
  console.log('\nUsage in .arc files:')
  console.log('  @queue default         (or omit @queue to use default)')
  if (Object.keys(queues).length > 1) {
    const extra = Object.keys(queues).find(k => k !== 'default')
    if (extra) console.log(`  @queue ${extra}`)
  }
  console.log('\nSee: https://arc-lang.com/docs/jobs')
}
