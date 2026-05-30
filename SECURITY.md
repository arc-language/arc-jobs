# Security Policy

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, use [GitHub's private security advisory feature](https://github.com/arc-language/arc-jobs/security/advisories/new) to report the issue confidentially.

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof of concept
- The arc-jobs version affected
- Any suggested fix (optional but appreciated)

**Response SLA:** We will acknowledge your report within **72 hours** and aim to release a fix within **14 days** for critical issues.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Scope

In scope:
- Remote code execution via job arguments or queue adapter
- Authentication bypass in the `/_arc/jobs` dashboard
- SQL injection in the SQLite adapter
- Lock bypass in `@unique` deduplication

Out of scope:
- Denial of service via high job volume (design your queues appropriately)
- Issues requiring physical access to the server
- Vulnerabilities in optional peer dependencies (`ioredis`, `better-sqlite3`)
