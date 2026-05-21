---
name: search-metadata
description: Search connections and datasets in the metadata collector via API. Supports fuzzy name search, IP/host lookup, platform filter, and table name search.
argument-hint: "<query>" [--type connections|datasets|both] [--host IP] [--platform NAME] [--connection NAME] [--dataset-type TYPE] [--classification LEVEL] [--collector-url URL] [--token TOKEN]
allowed-tools: ["Bash", "AskUserQuestion"]
---

# search-metadata

Search the metadata collector for connections and/or datasets matching **$ARGUMENTS**.

---

## Step 1 — Parse arguments

| Argument | Default | Description |
|---|---|---|
| `"<query>"` (positional, quoted string) | — | Free-text search term |
| `--type connections\|datasets\|both` | `both` | What to search |
| `--host IP` | — | Search connections by IP / hostname (partial match) |
| `--platform NAME` | — | Filter connections by platform (oracle, kafka, ...) |
| `--connection NAME` | — | Filter datasets by connection logical_name |
| `--dataset-type TYPE` | — | Filter datasets by type (table, view, topic, ...) |
| `--classification LEVEL` | — | Filter by classification (public, internal, confidential) |
| `--collector-url URL` | `http://localhost:8080` | Collector base URL |
| `--token TOKEN` | `dev-token-change-me` | Bearer token |
| `--limit N` | `20` | Max results per category |

If no positional query and no filter flags are given, use AskUserQuestion:

**Question 1:** "What do you want to search?"
- Options:
  - "Connection by name or description"
  - "Connection by IP / hostname"
  - "Connection by platform (oracle, kafka, ...)"
  - "Dataset / table by name"

**Question 2 (conditional):** Ask for the search term based on the answer above.

---

## Step 2 — Build and execute API calls

### Search connections

Call when `--type` is `connections` or `both`, or when user chose a connection-related option.

Build the query string from provided flags:

```bash
curl -s -G "http://localhost:8080/api/v1/search/connections" \
  -H "Authorization: Bearer <token>" \
  [--data-urlencode "q=<query>"] \
  [--data-urlencode "host=<host>"] \
  [--data-urlencode "platform=<platform>"] \
  [--data-urlencode "classification=<classification>"] \
  [--data-urlencode "owner_team=<owner_team>"] \
  --data-urlencode "limit=<limit>"
```

Only include parameters that were provided by the user. At least one parameter is required.

### Search datasets

Call when `--type` is `datasets` or `both`, or when user chose dataset search.

```bash
curl -s -G "http://localhost:8080/api/v1/search/datasets" \
  -H "Authorization: Bearer <token>" \
  [--data-urlencode "q=<query>"] \
  [--data-urlencode "connection=<connection>"] \
  [--data-urlencode "dataset_type=<dataset_type>"] \
  [--data-urlencode "classification=<classification>"] \
  --data-urlencode "limit=<limit>"
```

---

## Step 3 — Display results

### Connection results

Format as a table:

```
Connections found: N

  logical_name            platform   host                   port  classification  owner_team
  ──────────────────────  ─────────  ─────────────────────  ────  ──────────────  ──────────
  t24-core-prod           oracle     oracle-t24.bank.local  1521  confidential    core-banking
  kafka-cdc-prod          kafka      —                      —     internal        streaming
  iceberg-warehouse       iceberg    —                      —     internal        data-platform
```

If `score` is present in the response, sort by score descending and add a `score` column.

If no results: print `No connections found matching your query.`

### Dataset results

Format as a table:

```
Datasets found: N

  fqn                                name             type          connection        classification
  ─────────────────────────────────  ───────────────  ────────────  ────────────────  ──────────────
  t24-core-prod.STMT                 STMT             table         t24-core-prod     confidential
  iceberg-warehouse.fact_stmt        fact_stmt        iceberg_table iceberg-warehouse internal
  kafka-cdc-prod.txn-events          txn-events       topic         kafka-cdc-prod    internal
```

If no results: print `No datasets found matching your query.`

---

## Step 4 — Offer follow-up actions

After showing results, offer follow-up options using AskUserQuestion:

**Question:** "What would you like to do next?"
- multiSelect: false
- Options:
  - "View lineage for a dataset (upstream/downstream)"
  - "View jobs touching a connection"
  - "View related connections"
  - "Done"

If user selects "View lineage for a dataset":
- Ask which dataset FQN from the results
- Ask direction: upstream or downstream
- Ask depth (default 3)
- Call: `GET /api/v1/search/datasets/{fqn}/upstream?depth=N` or `/downstream`
- Display nodes as an indented tree:
  ```
  fact_stmt (root)
  └─ depth 1: t24-core-prod.STMT  (via mbbank.dwh.etl.t24.daily_stmt_load)
  └─ depth 1: t24-core-prod.ACCOUNT  (via mbbank.dwh.etl.t24.daily_stmt_load)
  ```

If user selects "View jobs touching a connection":
- Ask which logical_name from the results
- Call: `GET /api/v1/search/connections/{logical_name}/jobs`
- Display jobs table with role (reader/writer/both) and last_seen_at

If user selects "View related connections":
- Ask which logical_name
- Call: `GET /api/v1/search/connections/{logical_name}/related`
- Display related connections with bridging_job_count

---

## Error handling

| HTTP | Action |
|---|---|
| 400 | "No search parameters provided. Use at least one of: q, host, platform, ..." |
| 401 | "Authentication failed — check --token" |
| 404 | "Not found" |
| Connection refused | "Collector not reachable at <url>. Check docker compose or --collector-url" |

Never stop on a soft error — if connections search fails but datasets search would work, still try datasets.
