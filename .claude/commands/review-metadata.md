---
name: review-metadata
description: Review and edit the metadata JSON file before pushing. Step 2 of 3.
argument-hint: [<scan-file>]
allowed-tools: ["Read", "Write", "Bash", "AskUserQuestion", "TodoWrite"]
---

# review-metadata — Step 2: Review & edit metadata

Review the scan result file, allow the user to correct or remove items, then save the final file.

## Parse arguments

| Argument | Default | Description |
|---|---|---|
| `<scan-file>` (positional) | `.metadata_scan.json` in current dir | File produced by `/scan-metadata` |

If the file does not exist, print:
```
File not found: <scan-file>
Run /scan-metadata <project-dir> first.
```
Then stop.

## Steps

**1. Read the scan file**

Read `<scan-file>` and parse the JSON. Show a detailed review table:

### Connections

For each connection, show:
- `logical_name` — is it a valid slug? (only `[a-z0-9._-]`)
- `platform` — is it `unknown`? Flag with ⚠️
- `host` / `port` — present or null?
- `properties` — any required keys missing for this platform?

Platform required-key rules:
| Platform | Required in `properties` |
|---|---|
| `kafka` | `bootstrap_servers` |
| `oracle` | `service_name` |
| `trino` | `catalog` |
| `iceberg` | `warehouse` |

Flag every violation as ⚠️.

### Jobs

For each job:
- `name` — valid slug?
- `job_type` — one of `airflow_task / spark / flink / python / fastapi / trino_query / unknown`?
- `inputs` / `outputs` — namespaces must match a `logical_name` in the connections list; flag mismatches with ⚠️

**2. Ask: what to fix**

Use AskUserQuestion:

**Question 1:** "Which connections do you want to REMOVE from the push list?"
- multiSelect: true
- Options: one per connection (label = logical_name, description = platform + any ⚠️ warnings)
- Include option "None — keep all"

**Question 2:** "Which jobs do you want to REMOVE from the push list?"
- multiSelect: true
- Options: one per job (label = name, description = job_type + inputs → outputs + any ⚠️)
- Include option "None — keep all"

**Question 3:** "Do you want to edit any connection's details?"
- Options:
  - "No, proceed as-is"
  - "Yes, I'll describe changes and you apply them"

If user chooses "Yes, I'll describe changes":
- Ask the user to describe each correction in plain text (e.g. "set logical_name of the first kafka connection to kafka-prod")
- Apply each correction to the in-memory JSON
- Show the modified item back to the user for confirmation

**3. Remove selected items**

Remove connections and jobs the user chose to exclude.

**4. Save the reviewed file**

Write the updated JSON back to `<scan-file>` (overwrite in place).

Print:
```
Review complete.
Connections to push : N
Jobs to push        : N

File updated: <scan-file>
```

**5. Tell the user what to do next**

Print exactly:
```
Next step: /push-metadata <scan-file>
```
