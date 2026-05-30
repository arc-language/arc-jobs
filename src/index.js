'use strict'

const { BaseAdapter } = require('./adapters/base')
const { MemoryAdapter } = require('./adapters/memory')
const { SqliteAdapter } = require('./adapters/sqlite')
const { RedisAdapter } = require('./adapters/redis')
const { createQueue } = require('./core/queue')
const { cronMatches, nextFireTime, startScheduler } = require('./core/scheduler')
const { arcJobsHandle, broadcastProgress } = require('./dashboard/handler')

module.exports = {
  // Adapters
  BaseAdapter,
  MemoryAdapter,
  SqliteAdapter,
  RedisAdapter,
  // Queue factory
  createQueue,
  // Scheduler utilities
  cronMatches,
  nextFireTime,
  startScheduler,
  // Dashboard
  arcJobsHandle,
  broadcastProgress,
}
