#!/usr/bin/env node
'use strict'

const [,, cmd, ...args] = process.argv

switch (cmd) {
  case 'stats':
    require('../src/cli/stats')(args)
    break
  case 'replay':
    require('../src/cli/replay')(args)
    break
  case 'monitor':
    require('../src/cli/monitor')(args)
    break
  case 'init':
    require('../src/cli/init')(args)
    break
  default:
    console.log(`arc-jobs — background jobs for Arc

Commands:
  arc-jobs init            Scaffold arc.config queues section
  arc-jobs stats           Show queue depths and throughput
  arc-jobs replay          Replay dead letter queue jobs
  arc-jobs monitor         Real-time terminal job monitor

Options:
  --queue <name>           Target a specific queue (default: all)
  --db <path>              SQLite database path (default: app.db)
  --redis <url>            Redis connection URL
`)
}
