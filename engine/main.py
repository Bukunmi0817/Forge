import json
import uuid
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from sse_starlette.sse import EventSourceResponse

from engine.parser    import parse_pipeline, ParseError
from engine.scheduler import JobDAG, JobStatus, CycleError
from engine.runner    import JobRunner
from engine.logs      import MultiLogWriter, LogReader
from registry.auth    import verify_token, extract_bearer_token, init_db as init_auth_db

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

ENGINE_CFG   = CONFIG["engine"]
REGISTRY_URL = ENGINE_CFG["registry_url"]
LOGS_PATH    = ENGINE_CFG["logs_path"]
MAX_PARALLEL = ENGINE_CFG.get("max_concurrent_jobs", 4)
TOKENS_DB    = CONFIG["auth"]["tokens_db_path"]
SLACK_URL    = CONFIG.get("slack", {}).get("webhook_url", "")

Path(LOGS_PATH).mkdir(parents=True, exist_ok=True)
init_auth_db(TOKENS_DB)

app   = FastAPI(title="Forge Engine")
RUNS: dict[str, dict] = {}


def require_auth(authorization: str | None) -> str:
    raw = extract_bearer_token(authorization)
    publisher = verify_token(TOKENS_DB, raw)
    if not publisher:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return publisher


async def slack_notify(message: str):
    if not SLACK_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(SLACK_URL, json={"text": message})
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "service": "forge-engine"}


@app.post("/runs")
async def submit_run(
    pipeline:      UploadFile        = File(...),
    authorization: str | None = Header(None),
):
    publisher = require_auth(authorization)
    yaml_text = (await pipeline.read()).decode("utf-8")

    try:
        parsed = parse_pipeline(yaml_text)
    except ParseError as e:
        raise HTTPException(status_code=400, detail={"error": "parse_error", "message": str(e)})

    lockfile = None
    if parsed.dependencies:
        reqs = [{"name": d.name, "version": d.version} for d in parsed.dependencies]
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{REGISTRY_URL}/resolve", json={"requirements": reqs})

        if resp.status_code == 409:
            detail     = resp.json().get("detail", {})
            error_type = detail.get("error", "conflict")
            msg        = detail.get("message", str(detail))
            run_id     = str(uuid.uuid4())
            status     = "conflict_failure" if error_type == "conflict" else "cycle_failure"
            RUNS[run_id] = _make_run(run_id, parsed, status, publisher, error=msg)
            await slack_notify(f":x: *Resolution Failure*\nPipeline: `{parsed.name}`\n{msg}")
            return {"run_id": run_id, "status": status, "error": msg}

        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Registry unavailable")

        lockfile = resp.json().get("lockfile", {})

    dag = JobDAG(parsed.jobs)
    try:
        dag.validate()
        levels = dag.execution_levels()
    except CycleError as e:
        run_id = str(uuid.uuid4())
        RUNS[run_id] = _make_run(run_id, parsed, "cycle_failure", publisher, error=str(e))
        await slack_notify(f":x: *Cycle Failure*\nPipeline: `{parsed.name}`\n{e}")
        return {"run_id": run_id, "status": "cycle_failure", "error": str(e)}

    run_id = str(uuid.uuid4())
    RUNS[run_id] = _make_run(run_id, parsed, "queued", publisher, lockfile=lockfile)

    asyncio.create_task(_execute_run(run_id, parsed, dag, levels, lockfile, publisher))

    await slack_notify(f":rocket: *Pipeline Started*\nPipeline: `{parsed.name}` | Run: `{run_id}`")
    return {"run_id": run_id}


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return {
        "run_id":       run_id,
        "status":       run["status"],
        "jobs":         run["jobs"],
        "lockfile_url": f"/runs/{run_id}/lockfile",
        "created_at":   run["created_at"],
        "started_at":   run.get("started_at"),
        "finished_at":  run.get("finished_at"),
        "error":        run.get("error"),
    }


@app.get("/runs/{run_id}/lockfile")
def get_lockfile(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return {"lockfile": run.get("lockfile") or {}}


@app.get("/runs/{run_id}/logs")
async def stream_logs(run_id: str, follow: bool = False, job: Optional[str] = None):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    reader = LogReader(LOGS_PATH, run_id)

    def is_running():
        r = RUNS.get(run_id)
        return r and r["status"] in ("queued", "running")

    async def event_generator():
        async for event in reader.stream_logs(job, follow, is_running):
            yield event

    return EventSourceResponse(event_generator())


async def _execute_run(run_id, pipeline, dag, levels, lockfile, forge_token):
    run             = RUNS[run_id]
    run["status"]   = "running"
    run["started_at"] = _now()

    log_writer   = MultiLogWriter(LOGS_PATH, run_id)
    final_status = "succeeded"
    failing_job  = None
    semaphore    = asyncio.Semaphore(MAX_PARALLEL)

    try:
        for level_idx, level in enumerate(levels):
            log_writer.write("forge", f"=== Level {level_idx}: {level} ===")
            level_jobs = [j for j in level if dag.nodes[j].status == JobStatus.PENDING]
            if not level_jobs:
                continue

            tasks = []
            for job_name in level_jobs:
                dag.nodes[job_name].status     = JobStatus.RUNNING
                dag.nodes[job_name].started_at = _now()
                run["jobs"][job_name]["status"]     = JobStatus.RUNNING
                run["jobs"][job_name]["started_at"] = _now()
                task = asyncio.create_task(
                    _run_single_job(job_name, pipeline, run_id, lockfile, forge_token, log_writer, semaphore, dag)
                )
                tasks.append((job_name, task))

            for job_name, task in tasks:
                job_status = await task
                dag.nodes[job_name].status      = job_status
                dag.nodes[job_name].finished_at = _now()
                run["jobs"][job_name]["status"]      = job_status
                run["jobs"][job_name]["finished_at"] = _now()

                if job_status in ("failed", "integrity_failure"):
                    final_status = job_status
                    failing_job  = job_name
                    dag.mark_skipped_dependents(job_name)
                    for jname, node in dag.nodes.items():
                        if node.status == JobStatus.SKIPPED:
                            run["jobs"][jname]["status"] = JobStatus.SKIPPED

    except Exception as e:
        final_status = "failed"
        log_writer.write("forge", f"[forge] Unexpected error: {e}")
    finally:
        run["status"]      = final_status
        run["finished_at"] = _now()
        log_writer.close()

        duration = _duration(run["started_at"], run["finished_at"])
        if final_status == "succeeded":
            await slack_notify(f":white_check_mark: *Pipeline Succeeded*\n`{pipeline.name}` | {duration}")
        elif final_status == "integrity_failure":
            await slack_notify(f":warning: *Integrity Failure* <!here>\n`{pipeline.name}` | job: `{failing_job}`")
        else:
            await slack_notify(f":x: *Pipeline Failed*\n`{pipeline.name}` | job: `{failing_job}` | {duration}")


async def _run_single_job(job_name, pipeline, run_id, lockfile, forge_token, log_writer, semaphore, dag):
    async with semaphore:
        runner = JobRunner(
            job=pipeline.jobs[job_name], run_id=run_id,
            lockfile=lockfile or {}, forge_token=forge_token,
            log_writer=log_writer, pipeline=pipeline,
        )
        return await runner.execute()


def _make_run(run_id, pipeline, status, publisher, lockfile=None, error=None) -> dict:
    return {
        "run_id":      run_id,
        "status":      status,
        "pipeline":    pipeline,
        "lockfile":    lockfile,
        "publisher":   publisher,
        "jobs":        {
            name: {"status": JobStatus.PENDING, "started_at": None, "finished_at": None}
            for name in pipeline.jobs
        },
        "created_at":  _now(),
        "started_at":  None,
        "finished_at": None,
        "error":       error,
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _duration(start, end) -> str:
    if not start or not end:
        return "unknown"
    try:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        return f"{int((e-s).total_seconds())}s"
    except Exception:
        return "unknown"
