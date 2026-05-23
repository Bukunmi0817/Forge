import sys
import json
import os
import hashlib
from pathlib import Path

import click
import httpx
import yaml

CREDS_FILE = Path.home() / ".forge" / "credentials.json"


def load_creds() -> dict:
    if not CREDS_FILE.exists():
        click.echo("Not logged in. Run: forge login <url>", err=True)
        sys.exit(1)
    with open(CREDS_FILE) as f:
        return json.load(f)


def save_creds(url: str, token: str):
    CREDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CREDS_FILE, "w") as f:
        json.dump({"url": url.rstrip("/"), "token": token}, f)
    os.chmod(CREDS_FILE, 0o600)


def auth_headers(creds: dict) -> dict:
    return {"Authorization": f"Bearer {creds['token']}"}


@click.group()
def cli():
    """Forge — CI/CD platform and artifact registry CLI"""
    pass


@cli.command()
@click.argument("url")
@click.option("--token", prompt="Token", hide_input=True)
def login(url: str, token: str):
    """Save credentials for a Forge server."""
    try:
        resp = httpx.get(f"{url.rstrip('/')}/health", timeout=10)
        resp.raise_for_status()
    except Exception as e:
        click.echo(f"Cannot reach Forge at {url}: {e}", err=True)
        sys.exit(1)
    save_creds(url, token)
    click.echo(f"Logged in to {url}")


@cli.command("run")
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--watch", is_flag=True, default=False)
def run_pipeline(pipeline: str, watch: bool):
    """Submit a pipeline YAML for execution."""
    creds = load_creds()
    with open(pipeline, "rb") as f:
        pipeline_data = f.read()
    try:
        resp = httpx.post(
            f"{creds['url']}/runs",
            files={"pipeline": ("pipeline.yaml", pipeline_data, "text/yaml")},
            headers=auth_headers(creds),
            timeout=30,
        )
    except Exception as e:
        click.echo(f"Failed to submit: {e}", err=True)
        sys.exit(1)

    if resp.status_code not in (200, 201):
        click.echo(f"Error: {resp.status_code} — {resp.text}", err=True)
        sys.exit(1)

    data   = resp.json()
    run_id = data.get("run_id")
    status = data.get("status")
    click.echo(f"Run submitted: {run_id}")
    click.echo(f"Status: {status}")

    if status in ("conflict_failure", "cycle_failure"):
        click.echo(f"Error: {data.get('error')}", err=True)
        sys.exit(1)

    if watch:
        _stream_logs(creds["url"], run_id, creds, follow=True)


@cli.command("logs")
@click.argument("run_id")
@click.option("--follow", "-f", is_flag=True, default=False)
@click.option("--job", default=None)
def logs(run_id: str, follow: bool, job: str | None):
    """Fetch or follow logs for a run."""
    creds = load_creds()
    _stream_logs(creds["url"], run_id, creds, follow=follow, job=job)


def _stream_logs(engine_url, run_id, creds, follow=True, job=None):
    params  = {}
    if follow: params["follow"] = "true"
    if job:    params["job"]    = job
    headers = {**auth_headers(creds), "Accept": "text/event-stream"}
    try:
        with httpx.stream("GET", f"{engine_url}/runs/{run_id}/logs",
                          params=params, headers=headers, timeout=None) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    try:
                        entry = json.loads(line[6:])
                        ts    = entry.get("ts", "")[:19].replace("T", " ")
                        job_  = entry.get("job", "")
                        text  = entry.get("line", "")
                        click.echo(f"[{ts}] [{job_}] {text}")
                    except json.JSONDecodeError:
                        click.echo(line)
    except KeyboardInterrupt:
        click.echo("\nStopped.")
    except Exception as e:
        click.echo(f"Log stream error: {e}", err=True)


@cli.command("publish")
@click.argument("path", type=click.Path(exists=True))
@click.option("--name",    required=True)
@click.option("--version", required=True)
@click.option("--deps",    default="[]")
def publish(path: str, name: str, version: str, deps: str):
    """Publish an artifact to the registry."""
    creds = load_creds()
    sha   = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha.update(chunk)
    checksum = "sha256:" + sha.hexdigest()
    click.echo(f"Publishing {name}@{version} checksum={checksum}")
    with open(path, "rb") as f:
        resp = httpx.post(
            f"{creds['url']}/artifacts/{name}/{version}",
            files={"file": (os.path.basename(path), f, "application/octet-stream")},
            data={"checksum": checksum, "deps": deps},
            headers=auth_headers(creds),
            timeout=120,
        )
    if resp.status_code == 201:
        click.echo(f"Published {name}@{version}")
    elif resp.status_code == 409:
        click.echo(f"Error: {name}@{version} already exists", err=True)
        sys.exit(1)
    else:
        click.echo(f"Error: {resp.status_code} — {resp.text}", err=True)
        sys.exit(1)


@cli.command("resolve")
@click.argument("pipeline", type=click.Path(exists=True))
def resolve(pipeline: str):
    """Resolve dependencies and print lockfile."""
    creds = load_creds()
    with open(pipeline) as f:
        data = yaml.safe_load(f)
    deps = data.get("dependencies") or []
    if not deps:
        click.echo("No dependencies declared.")
        return
    reqs = [{"name": d["name"], "version": d["version"]} for d in deps]
    resp = httpx.post(f"{creds['url']}/resolve", json={"requirements": reqs}, timeout=30)
    if resp.status_code == 200:
        click.echo(json.dumps(resp.json().get("lockfile", {}), indent=2))
    else:
        detail = resp.json().get("detail", {})
        msg = detail.get("message", str(detail)) if isinstance(detail, dict) else str(detail)
        click.echo(f"Resolution failed: {msg}", err=True)
        sys.exit(1)


@cli.command("ls")
@click.argument("package")
def ls(package: str):
    """List all versions of a package."""
    creds = load_creds()
    resp  = httpx.get(f"{creds['url']}/artifacts/{package}", timeout=10)
    if resp.status_code == 404:
        click.echo(f"Package '{package}' not found")
        return
    resp.raise_for_status()
    for v in resp.json()["versions"]:
        click.echo(f"  {v['version']}  sha256:{v['sha256'][:16]}...  {v['published_at'][:10]}")


@cli.command("token")
@click.argument("name")
@click.option("--db", default="./data/tokens.db")
def create_token(name: str, db: str):
    """Create a new API token (run on the server)."""
    from registry.auth import create_token as _create, init_db as _init
    _init(db)
    try:
        token = _create(db, name)
        click.echo(f"\nToken for '{name}':\n\n  {token}\n\nSave this — shown once only.")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
