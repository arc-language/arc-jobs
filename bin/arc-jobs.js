#!/usr/bin/env node
'use strict'

const [,, cmd, ...args] = process.argv

if (cmd === '--version' || cmd === '-v') {
  const { version } = require('../package.json')
  console.log(version)
  process.exit(0)
}

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
    console.log(`arc-jobs — background jobs for Arc (https://arc-language.dev)

Usage: arc-jobs <command> [options]

Commands:
  init             Scaffold arc.config queues section interactively
  stats            Show queue depths, throughput, and job counts
  replay           Replay dead letter queue jobs
  monitor          Real-time terminal job monitor (2s refresh)

Options:
  --queue <name>   Target a specific queue (default: all)
  --db <path>      SQLite database path (default: app.db)
  --redis <url>    Redis connection URL
  --version, -v    Print version number
  --help, -h       Show this help message

Examples:
  arc-jobs init
  arc-jobs stats --db ./data/app.db
  arc-jobs replay --job SendInvoice
  arc-jobs monitor --queue payments
`)
}
