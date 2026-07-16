# Forge CI/CD Platform with Integrated Artifact Registry

## Public URL
- CI Engine:  http://xx.xxx.xxx.xxx:8000
- Registry:   http://xx.xxx.xxx.xxx:8001

## Fresh VPS Setup

```bash
# 1. Install dependencies
sudo apt update
sudo apt install docker.io docker-compose-v2 python3-pip python3-venv git -y
sudo usermod -aG docker ubuntu && newgrp docker

# 2. Clone the repo
git clone https://github.com/Bukunmi0817/Forge.git
cd Forge

# 3. Add Slack webhook to config.yaml
nano config.yaml  # set slack.webhook_url

# 4. Start services
docker compose up --build -d

# 5. Create first auth token
docker exec forge-engine python3 -c "
import sys; sys.path.insert(0, '/app')
from registry.auth import create_token, init_db
init_db('/app/data/tokens.db')
print(create_token('/app/data/tokens.db', 'ci-bot'))
"
```

## Pipeline YAML Schema

```yaml
name: build-lib-http        # required, string
version: 1.0.0              # required, semver MAJOR.MINOR.PATCH

dependencies:               # optional
  - name: lib-core          # required, package name
    version: "^1.0.0"       # required, semver constraint

jobs:                       # required, at least one job
  build:                    # job name
    runtime: alpine:3.18    # required, Docker image
    resources:              # optional
      cpu: 1.0              # CPU cores (default: 1.0)
      memory: 512Mi         # RAM limit (default: 512Mi)
    needs: []               # optional, list of job names this job depends on
    steps:                  # required, at least one step
      - name: test          # optional, step label
        run: "sh ./test.sh" # required, shell command

artifacts:                  # optional
  - name: lib-http          # required, artifact name
    version: 1.0.0          # required, semver
    path: ./out.tar.gz      # required, path to file in workspace
```

Unknown fields produce an error pointing at the offending field.
Missing required fields produce an error naming the missing field.

## How the DAG Scheduler Works

Jobs declare `needs: [other-job]` to express dependencies. This forms a
Directed Acyclic Graph (DAG). We use Kahn's algorithm to schedule jobs:

1. Count incoming edges (dependencies) for each job
2. Add all jobs with 0 dependencies to a ready queue
3. Process the queue level by level — jobs in the same level run in parallel
4. When a job completes, reduce the dependency count of its dependents
5. Any dependent reaching 0 joins the next level's queue
6. If the queue empties before all jobs are processed → cycle detected

Failed jobs mark their dependents as SKIPPED (not failed).
Independent jobs run in parallel up to `max_concurrent_jobs` (config).

## How Isolation Works

Each job runs in a Docker container with:
- **Filesystem**: temp workspace mounted at /workspace, host FS not visible
- **Network**: `forge-isolated` bridge network with `internal: true` — no internet egress
- **CPU**: enforced via Docker `--nano-cpus`
- **Memory**: enforced via Docker `--memory`, OOM produces exit code 137 which we detect and log
- **Timeout**: `asyncio.wait_for` kills container after `default_job_timeout` seconds
- **Privileges**: `no-new-privileges`, all capabilities dropped except CHOWN, DAC_OVERRIDE, SETUID, SETGID

## How the Storage Layer Works

Content-addressable blob storage. Every file is stored by its SHA-256 hash:
- Upload file → compute SHA-256 → store at `blobs/<first-2-chars>/<remaining-62-chars>`
- The (name, version) pair in SQLite points to the hash
- Two files with identical content share one blob
- Splitting the hash into a 2-char prefix + 62-char filename spreads files across 256 subdirectories, preventing filesystem slowdowns with large numbers of files

Metadata lives in SQLite with a UNIQUE(name, version) constraint enforcing immutability.

## How the Resolver Works (and Why It's Deterministic)

1. Collect all constraints for each package (including transitive dependencies)
2. For each package, fetch all available versions from the registry
3. Filter to versions satisfying ALL constraints simultaneously
4. Pick the HIGHEST satisfying version using `max()`
5. Look up its SHA-256 from metadata
6. Output a lockfile sorted alphabetically by package name

Determinism is guaranteed by:
- `sorted(collected.items())` — always processes packages alphabetically
- `max(satisfying)` — always picks the same highest version
- Same registry state + same constraints = identical lockfile, byte-for-byte

## How Log Streaming Works

Logs are written to disk as newline-delimited JSON in real time:
`data/logs/<run_id>/<job_name>.log`

Each line: `{"ts": "...", "job": "build", "line": "Hello"}`

Streaming uses Server-Sent Events (SSE):
- Client connects to `GET /runs/{id}/logs?follow=true`
- Server opens the log file and reads line by line
- New lines yielded as `data: <json>\n\n` SSE events
- When end of file is reached but job is still running: sleep 50ms and retry
- When job finishes: send remaining lines and close stream
- 50MB logs stream without loading into memory — only 64KB in RAM at a time

## How Racing Publishes Are Handled

Two pipelines publishing the same (name, version) simultaneously:
- Both compute checksums and save blobs (idempotent — same content = same hash)
- Both attempt `INSERT INTO artifacts` with UNIQUE(name, version) constraint
- SQLite's write locking ensures only one INSERT succeeds
- The second gets `IntegrityError` → caught → returns 409 Conflict

## Slack Alerts

Events notified:
- Pipeline started: pipeline name, run ID
- Pipeline succeeded: pipeline name, duration
- Pipeline failed: pipeline name, failing job, duration
- Integrity failure: artifact coordinate, expected/actual SHA-256, run ID
- Resolution failure: pipeline name, conflict or cycle details

## Required HTTP API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | /runs | ✅ | Submit pipeline |
| GET | /runs/{id} | | Get run status |
| GET | /runs/{id}/lockfile | | Get resolved lockfile |
| GET | /runs/{id}/logs?follow=true | | Stream logs (SSE) |
| POST | /artifacts/{name}/{version} | ✅ | Upload artifact |
| GET | /artifacts/{name}/{version} | | Download artifact |
| GET | /artifacts/{name}/{version}/meta | | Get metadata |
| GET | /artifacts/{name} | | List versions |
