# Contributing to arc-jobs

Thank you for your interest in contributing! arc-jobs is part of the [Arc](https://arc-language.dev) ecosystem.

## Setup

```bash
git clone https://github.com/arc-language/arc-jobs.git
cd arc-jobs
bun install
bun test          # all tests should pass
```

## Running Tests

```bash
bun test                                    # all tests
bun test tests/memory-adapter.test.js       # one file
bun test tests/ --watch                     # watch mode
bun test --coverage tests/                  # with coverage
```

## Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feat/<description>` | `feat/sqs-adapter` |
| Bug fix | `fix/<description>` | `fix/sqlite-lock-race` |
| Docs | `docs/<description>` | `docs/redis-examples` |
| Refactor | `refactor/<description>` | `refactor/base-adapter` |

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add SQS adapter
fix: prevent double-processing on SQLite dequeue
docs: add Redis cluster example
test: cover @unique strategy=replace
refactor: extract lock logic into base adapter
```

## Pull Request Checklist

- [ ] Tests added or updated for any changed behaviour
- [ ] `bun test` passes locally
- [ ] No unrelated changes included
- [ ] Commit messages follow conventional format

## Adding a New Adapter

1. Extend `BaseAdapter` in `src/adapters/base.js`
2. Implement all abstract methods (`enqueue`, `dequeue`, `complete`, `fail`, `status`, `size`, `dead`, `replayDead`, `acquireLock`, `releaseLock`)
3. Add tests in `tests/<name>-adapter.test.js` (see `memory-adapter.test.js` as a template)
4. Export from `src/index.js` and add to `src/types/index.d.ts`

## Reporting Bugs

Use the [bug report template](https://github.com/arc-language/arc-jobs/issues/new?template=bug.yml) — please include a minimal reproducible example.

## Questions

Open a [GitHub Discussion](https://github.com/arc-language/arc-jobs/discussions) for questions, ideas, or feedback.
