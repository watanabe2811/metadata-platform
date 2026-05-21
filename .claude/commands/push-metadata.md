---
name: push-metadata
description: Push reviewed metadata (connections + lineage events) to the collector API. Step 3 of 3.
argument-hint: [<scan-file>] [--collector-url URL] [--token TOKEN] [--dry-run]
allowed-tools: ["Read", "Bash", "AskUserQuestion", "TodoWrite"]
---

# push-metadata — Step 3: Push to API

Push the connections and lineage events from the reviewed metadata file to the collector API.

## Parse arguments

| Argument | Default | Description |
|---|---|---|
| `<scan-file>` (positional) | `.metadata_scan.json` | File produced by `/review-metadata` |
| `--collector-url URL` | _(ask user)_ | Collector base URL |
| `--token TOKEN` | _(ask user)_ | Bearer token |
| `--dry-run` | false | Print payloads, do not call API |

## Steps

**1. Read the scan file**

Read `<scan-file>`. If not found, print:
```
File not found: <scan-file>
Run /scan-metadata then /review-metadata first.
```
Then stop.

**2. Ask for API credentials (if not provided as flags)**

If `--collector-url` or `--token` were not passed as arguments, use AskUserQuestion:

**Question 1:** "Collector API URL?"
- Options:
  - "http://localhost:8080  (local dev)"
  - "http://metadata:8080  (Docker internal)"
  - "Custom URL"

If user selects "Custom URL", prompt: "Enter the full base URL (e.g. https://metadata.internal):"
(Use the Other/free-text option of AskUserQuestion for this.)

**Question 2:** "Bearer token?"
- Options:
  - "dev-token-change-me  (local dev default)"
  - "Enter token manually"

If user selects "Enter token manually", use the Other/free-text option.

**Question 3:** "Push mode?"
- Options:
  - "Push for real"
  - "Dry-run only — print payloads, do not call API"

**3. Health check**

Before pushing, verify the collector is reachable:

```bash
curl -s -o /dev/null -w "%{http_code}" <collector-url>/health
```

- HTTP 200 → continue
- Anything else → print error and ask user to confirm whether to continue anyway

**4. Push connections**

For each connection in `connections[]`:

Build the payload (exclude `_source_file`):
```json
{
  "logical_name": "...",
  "platform": "...",
  "host": "...",
  "port": ...,
  "service_name": "...",
  "vault_path": "...",
  "classification": "...",
  "owner_team": "...",
  "description": "...",
  "properties": {}
}
```
(Omit null fields.)

In dry-run mode: pretty-print the payload with label `[DRY-RUN] POST /api/v1/connections`.

In live mode:
```bash
curl -s -w "\n%{http_code}" -X POST <collector-url>/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '<payload>'
```

Result interpretation:
| HTTP | Meaning | Action |
|---|---|---|
| 201 | Created | `[OK] <logical_name> — created` |
| 200 | Stub upgraded | `[OK] <logical_name> — stub upgraded` |
| 409 | Already exists | `[SKIP] <logical_name> — already registered` |
| 422 | Validation error | `[FAIL] <logical_name>: <error detail>` — continue |
| other | Server error | `[FAIL] <logical_name>: HTTP <code>` — continue |

**Never stop on a single failure — continue with remaining connections.**

**5. Push lineage events**

For each job in `jobs[]`, push `job.lineage_event`:

In dry-run mode: pretty-print with label `[DRY-RUN] POST /api/v1/lineage`.

In live mode:
```bash
curl -s -w "\n%{http_code}" -X POST <collector-url>/api/v1/lineage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '<lineage_event>'
```

Result interpretation:
| HTTP | Meaning | Action |
|---|---|---|
| 202 | Accepted | `[OK] <job-name>` |
| 400 | Bad event | `[FAIL] <job-name>: <error detail>` — continue |
| 422 | Validation error | `[FAIL] <job-name>: <error detail>` — continue |
| other | Server error | `[FAIL] <job-name>: HTTP <code>` — continue |

**6. Final report**

Print:

```
=== push-metadata complete ===

Collector : <collector-url>
File      : <scan-file>
Mode      : LIVE  (or DRY-RUN)

Connections
  Created  : N
  Upgraded : N
  Skipped  : N  (already existed)
  Failed   : N

Lineage events
  Accepted : N
  Failed   : N

Failed items:
  [connection] <logical_name>: <reason>
  [job]        <name>: <reason>

Next steps:
  - Enrich stub connections at <collector-url>/docs → PUT /api/v1/connections/{name}
  - Run /scan-metadata again after adding more jobs to the project
```

If mode was DRY-RUN, replace the first line with:
```
=== push-metadata complete (DRY-RUN — nothing was pushed) ===
```
