'use strict'

function openDb(dbPath, cliName) {
  try {
    const { Database } = require('bun:sqlite')
    return new Database(dbPath)
  } catch (_) {}
  try {
    const Database = require('better-sqlite3')
    return new Database(dbPath)
  } catch (_) {
    console.error(`${cliName}: could not open database.`)
    process.exit(1)
  }
}

module.exports = { openDb }
