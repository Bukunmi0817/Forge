import re
import json
import yaml
from pathlib import Path
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Header, Request
from fastapi.responses import StreamingResponse

from registry.storage  import BlobStore
from registry.metadata import init_db, publish, get_artifact, get_versions
from registry.auth     import init_db as init_auth_db, verify_token, extract_bearer_token
from registry.resolver import DependencyResolver, ConflictError, CycleError

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

DB_PATH      = CONFIG["registry"]["db_path"]
TOKENS_DB    = CONFIG["auth"]["tokens_db_path"]
STORAGE_PATH = CONFIG["registry"]["storage_path"]

app = FastAPI(title="Forge Registry")

init_db(DB_PATH)
init_auth_db(TOKENS_DB)
blob_store = BlobStore(STORAGE_PATH)

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


def require_auth(authorization: str | None) -> str:
    raw = extract_bearer_token(authorization)
    publisher = verify_token(TOKENS_DB, raw)
    if not publisher:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return publisher


def validate_semver(version: str):
    if not SEMVER_RE.match(version):
        raise HTTPException(
            status_code=400,
            detail=f"Version '{version}' is not valid semver (use MAJOR.MINOR.PATCH)"
        )


@app.get("/health")
def health():
    return {"status": "ok", "service": "forge-registry"}


@app.post("/artifacts/{name}/{version}", status_code=201)
async def upload_artifact(
    name:          str,
    version:       str,
    file:          UploadFile = File(...),
    checksum:      str        = Form(...),
    deps:          str        = Form("[]"),
    authorization: str | None = Header(None),
):
    publisher = require_auth(authorization)
    validate_semver(version)
    data = await file.read()

    try:
        deps_list = json.loads(deps)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="deps must be valid JSON")

    try:
        sha256 = blob_store.save(data, declared_sha256=checksum)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        record = publish(
            db_path=DB_PATH, name=name, version=version,
            sha256=sha256, size=len(data), publisher=publisher, deps=deps_list,
        )
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {"message": f"{name}@{version} published", "sha256": sha256}


@app.get("/artifacts/{name}/{version}/meta")
def get_artifact_meta(name: str, version: str):
    meta = get_artifact(DB_PATH, name, version)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"{name}@{version} not found")
    return meta


@app.get("/artifacts/{name}/{version}")
def download_artifact(name: str, version: str):
    meta = get_artifact(DB_PATH, name, version)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"{name}@{version} not found")
    sha256 = meta["sha256"]
    return StreamingResponse(
        blob_store.stream(sha256),
        media_type="application/octet-stream",
        headers={
            "X-Artifact-SHA256":   f"sha256:{sha256}",
            "Content-Disposition": f'attachment; filename="{name}-{version}.tar.gz"',
        },
    )


@app.get("/artifacts/{name}")
def list_versions(name: str):
    versions = get_versions(DB_PATH, name)
    if not versions:
        raise HTTPException(status_code=404, detail=f"Package '{name}' not found")
    return {"name": name, "versions": versions}


@app.post("/resolve")
async def resolve_dependencies(request: Request):
    body = await request.json()
    reqs = body.get("requirements", [])
    resolver = DependencyResolver(DB_PATH)
    try:
        lockfile = resolver.resolve(reqs)
    except CycleError as e:
        raise HTTPException(status_code=409, detail={"error": "cycle", "message": str(e)})
    except ConflictError as e:
        raise HTTPException(status_code=409, detail={"error": "conflict", "message": str(e)})
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"lockfile": lockfile}
