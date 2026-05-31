'use strict'

// Structured JSON logger — shared by scheduler, dashboard, and CLI code.
// Adapter code uses BaseAdapter._log() which prepends queue: this.name automatically.

function log(level, fields) {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, ...fields })
  if (level === 'error') console.error(line)
  else if (level === 'warn') console.warn(line)
  else console.log(line)
}

const errMsg = e => e?.message ?? String(e)

module.exports = { log, errMsg }
