'use strict'

// Tests for Arc compiler integration — verifies parser/checker/emitter
// handle all new job annotations correctly.

const { test, describe } = require('bun:test')
const assert = require('assert')
const { Lexer } = require('../../../src/lexer')
const { Parser } = require('../../../src/parser')
const { Checker } = require('../../../src/checker')

function parse(src) {
  const lexer = new Lexer(src, '<test>')
  const tokens = lexer.tokenize()
  const parser = new Parser(tokens, '<test>')
  return parser.parse()
}

function check(src) {
  const prog = parse(src)
  const checker = new Checker('<test>')
  return checker.check(prog)
}

function getJob(src) {
  const prog = parse(src)
  return prog.declarations.find(d => d.type === 'JobDecl')
}

describe('@queue annotation', () => {
  test('sets queueName on JobDecl', () => {
    const job = getJob(`
@queue payments
job ProcessPayment(orderId: Int)
  const x = 1
`)
    assert.strictEqual(job.queueName, 'payments')
  })

  test('defaults queueName to null when not specified', () => {
    const job = getJob(`
job SendEmail(userId: Int)
  const x = 1
`)
    assert.strictEqual(job.queueName, null)
  })
})

describe('@schedule annotation', () => {
  test('sets schedule on JobDecl', () => {
    const job = getJob(`
@schedule "0 9 * * *"
job DailyDigest()
  const x = 1
`)
    assert.strictEqual(job.schedule, '0 9 * * *')
  })

  test('checker rejects invalid cron expression', () => {
    const { errors } = check(`
@schedule "invalid-cron"
job BadSchedule()
  const x = 1
`)
    assert.ok(errors.some(e => e.message.includes('invalid cron expression')))
  })

  test('checker accepts valid cron expression', () => {
    const { errors } = check(`
@schedule "0 9 * * 1"
job WeeklyJob()
  const x = 1
`)
    assert.strictEqual(errors.filter(e => e.message.includes('cron')).length, 0)
  })

  test('checker warns on @schedule with required params', () => {
    const { warnings } = check(`
@schedule "0 9 * * *"
job ScheduledWithParam(userId: Int)
  const x = 1
`)
    assert.ok(warnings.some(w => w.message.includes('required params')))
  })
})

describe('@priority annotation', () => {
  test('sets priority to high', () => {
    const job = getJob(`
@priority high
job UrgentJob()
  const x = 1
`)
    assert.strictEqual(job.priority, 'high')
  })

  test('sets priority to low', () => {
    const job = getJob(`
@priority low
job BackgroundJob()
  const x = 1
`)
    assert.strictEqual(job.priority, 'low')
  })

  test('defaults to normal when not specified', () => {
    const job = getJob(`
job NormalJob()
  const x = 1
`)
    assert.strictEqual(job.priority, 'normal')
  })

  test('checker rejects invalid priority', () => {
    const { errors } = check(`
@priority ultrafast
job BadPriority()
  const x = 1
`)
    assert.ok(errors.some(e => e.message.includes("@priority must be 'high'")))
  })
})

describe('@retries and @backoff annotations', () => {
  test('sets maxRetries', () => {
    const job = getJob(`
@retries 5
job RetryableJob()
  const x = 1
`)
    assert.strictEqual(job.maxRetries, 5)
  })

  test('sets backoffMs', () => {
    const job = getJob(`
@backoff 2000
job SlowRetryJob()
  const x = 1
`)
    assert.strictEqual(job.backoffMs, 2000)
  })
})

describe('@timeout annotation', () => {
  test('sets timeoutMs', () => {
    const job = getJob(`
@timeout 120000
job LongRunningJob()
  const x = 1
`)
    assert.strictEqual(job.timeoutMs, 120000)
  })
})

describe('@unique annotation', () => {
  test('sets unique=true', () => {
    const job = getJob(`
@unique
job IdempotentJob(invoiceId: Int)
  const x = 1
`)
    assert.strictEqual(job.unique, true)
  })

  test('defaults strategy to skip', () => {
    const job = getJob(`
@unique
job SkipJob()
  const x = 1
`)
    assert.strictEqual(job.uniqueStrategy, 'skip')
  })

  test('sets strategy=reject', () => {
    const job = getJob(`
@unique strategy=reject
job RejectJob()
  const x = 1
`)
    assert.strictEqual(job.uniqueStrategy, 'reject')
  })

  test('sets strategy=replace', () => {
    const job = getJob(`
@unique strategy=replace
job ReplaceJob()
  const x = 1
`)
    assert.strictEqual(job.uniqueStrategy, 'replace')
  })

  test('sets uniqueTimeout', () => {
    const job = getJob(`
@unique timeout=300000
job TimeoutJob()
  const x = 1
`)
    assert.strictEqual(job.uniqueTimeout, 300000)
  })

  test('checker rejects invalid strategy', () => {
    const { errors } = check(`
@unique strategy=invalid
job BadStrategy()
  const x = 1
`)
    assert.ok(errors.some(e => e.message.includes("@unique strategy must be")))
  })
})

describe('@progress annotation', () => {
  test('sets hasProgress=true', () => {
    const job = getJob(`
@progress
job LongJob(fileId: Int)
  const x = 1
`)
    assert.strictEqual(job.hasProgress, true)
  })

  test('hasProgress=false by default', () => {
    const job = getJob(`
job NormalJob()
  const x = 1
`)
    assert.strictEqual(job.hasProgress, false)
  })
})

describe('@then annotation', () => {
  test('sets thenJob', () => {
    const job = getJob(`
@then SendConfirmation
job ProcessOrder(orderId: Int)
  const x = 1

job SendConfirmation(orderId: Int)
  const y = 2
`)
    assert.strictEqual(job.thenJob, 'SendConfirmation')
  })

  test('checker rejects unknown @then target', () => {
    const { errors } = check(`
@then NonExistentJob
job ProcessOrder(orderId: Int)
  const x = 1
`)
    assert.ok(errors.some(e => e.message.includes("unknown job 'NonExistentJob'")))
  })

  test('checker accepts valid @then cross-reference', () => {
    const { errors } = check(`
@then NextStep
job FirstStep(id: Int)
  const x = 1

job NextStep(id: Int)
  const y = 2
`)
    assert.strictEqual(errors.filter(e => e.message.includes('@then')).length, 0)
  })
})

describe('multi-line annotation stacking', () => {
  test('multiple annotations on separate lines all applied', () => {
    const job = getJob(`
@queue payments
@priority high
@retries 5
@unique timeout=3600000 strategy=skip
job FullyAnnotatedJob(orderId: Int)
  const x = 1
`)
    assert.strictEqual(job.queueName, 'payments')
    assert.strictEqual(job.priority, 'high')
    assert.strictEqual(job.maxRetries, 5)
    assert.strictEqual(job.unique, true)
    assert.strictEqual(job.uniqueTimeout, 3600000)
    assert.strictEqual(job.uniqueStrategy, 'skip')
  })

  test('plain job without annotations has correct defaults', () => {
    const job = getJob(`
job PlainJob(userId: Int)
  const x = 1
`)
    assert.strictEqual(job.queueName, null)
    assert.strictEqual(job.priority, 'normal')
    assert.strictEqual(job.unique, false)
    assert.strictEqual(job.hasProgress, false)
    assert.strictEqual(job.thenJob, null)
    assert.strictEqual(job.schedule, null)
  })
})
