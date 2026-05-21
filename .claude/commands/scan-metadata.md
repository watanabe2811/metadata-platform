---
name: scan-metadata
description: Scan a project directory and generate a metadata JSON file (connections + jobs). Step 1 of 3.
argument-hint: <project-dir> [--namespace NS] [--out FILE]
allowed-tools: ["Bash", "Read", "TodoWrite"]
---

# scan-metadata — Step 1: Scan & generate metadata file

Scan **$ARGUMENTS** to detect database connections and jobs, then save the result to a JSON file.

## Parse arguments

| Argument | Default | Description |
|---|---|---|
| `<project-dir>` (positional) | `.` | Directory to scan |
| `--namespace NS` | project dir name | OpenLineage job namespace for detected jobs |
| `--out FILE` | `.metadata_scan.json` in project-dir | Output file path |

## Steps

**1. Locate the scanner script**

Find `tools/metadata_scanner.py` relative to the metadata-platform repo root.
Use `find` if the path is unknown:
```bash
find ~ -name "metadata_scanner.py" -path "*/tools/*" 2>/dev/null | head -1
```

**2. Run the scanner**

```bash
python <scanner-path> <project-dir> \
  [--namespace <NS>] \
  --out <out-file>
```

Capture stderr separately. Print each WARNING line from stderr to the user.

If the scanner exits non-zero, print the error and stop.

**3. Read the output file**

Read `<out-file>` and print a concise summary:

```
Scan complete: <project-dir>
Namespace   : <namespace>

Connections found: N
  - <logical_name>  [<platform>]  <host>  (from <source_file>)
  ...

Jobs found: N
  - <name>  [<job_type>]  <inputs> → <outputs>  (from <source_file>)
  ...

Warnings: N
  - ...

Output saved to: <out-file>
```

**4. Tell the user what to do next**

Print exactly:
```
Next step: /review-metadata <out-file>
```
