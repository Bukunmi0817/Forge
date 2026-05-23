import os
import asyncio
import hashlib
import shutil
import tempfile
from pathlib import Path

import docker
import httpx
import yaml

from engine.parser    import Pipeline, Job
from engine.logs      import MultiLogWriter
from engine.scheduler import JobStatus

with open("config.yaml") as f:
    _CONFIG = yaml.safe_load(f)

REGISTRY_URL = _CONFIG["engine"]["registry_url"]
JOB_TIMEOUT  = _CONFIG["engine"].get("default_job_timeout", 1800)
FORGE_NETWORK = "forge-isolated"


def ensure_forge_network():
    client = docker.from_env()
    existing = [n.name for n in client.networks.list()]
    if FORGE_NETWORK not in existing:
        client.networks.create(
            FORGE_NETWORK,
            driver="bridge",
            internal=True,
        )


class JobRunner:
    def __init__(self, job: Job, run_id: str, lockfile: dict,
                 forge_token: str, log_writer: MultiLogWriter, pipeline: Pipeline):
        self.job         = job
        self.run_id      = run_id
        self.lockfile    = lockfile
        self.forge_token = forge_token
        self.log_writer  = log_writer
        self.pipeline    = pipeline
        self.client      = docker.from_env()
        self.workspace   = None

    def _log(self, line: str):
        self.log_writer.write(self.job.name, line)

    async def execute(self) -> str:
        self._log(f"[forge] Starting job '{self.job.name}'")
        self._log(f"[forge] Runtime: {self.job.runtime}")
        self._log(f"[forge] CPU: {self.job.resources.cpu} Memory: {self.job.resources.memory_mb}MB")
        self.workspace = tempfile.mkdtemp(prefix=f"forge-{self.run_id}-{self.job.name}-", dir="/tmp")

        try:
            ok = await self._download_deps()
            if not ok:
                return "integrity_failure"

            for step in self.job.steps:
                self._log(f"[forge] Running step: {step.name}")
                status = await self._run_step(step.run)
                if status != JobStatus.SUCCEEDED:
                    return status

            self._log(f"[forge] Job '{self.job.name}' succeeded")
            return JobStatus.SUCCEEDED

        except Exception as e:
            self._log(f"[forge] Job failed with error: {e}")
            return JobStatus.FAILED

        finally:
            if self.workspace and os.path.exists(self.workspace):
                shutil.rmtree(self.workspace, ignore_errors=True)

    async def _download_deps(self) -> bool:
        if not self.lockfile:
            return True

        deps_dir = Path(self.workspace) / "deps"
        deps_dir.mkdir(exist_ok=True)

        for dep_name, info in self.lockfile.items():
            version  = info["version"]
            expected = info["sha256"]
            self._log(f"[forge] Downloading {dep_name}@{version}")

            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    resp = await client.get(f"{REGISTRY_URL}/artifacts/{dep_name}/{version}")
                    resp.raise_for_status()
                    blob = resp.content
            except Exception as e:
                self._log(f"[forge] ERROR: Failed to fetch {dep_name}@{version}: {e}")
                return False

            actual = hashlib.sha256(blob).hexdigest()
            if actual != expected:
                self._log(f"[forge] INTEGRITY FAILURE for {dep_name}@{version}")
                self._log(f"[forge]   Expected: sha256:{expected}")
                self._log(f"[forge]   Actual:   sha256:{actual}")
                return False

            dep_dir = deps_dir / dep_name
            dep_dir.mkdir(exist_ok=True)
            (dep_dir / f"{dep_name}-{version}.tar.gz").write_bytes(blob)
            self._log(f"[forge] Verified {dep_name}@{version}")

        return True

    async def _run_step(self, command: str) -> str:
        memory_bytes = self.job.resources.memory_mb * 1024 * 1024
        environment  = {
            "FORGE_TOKEN": self.forge_token,
            "FORGE_URL":   REGISTRY_URL,
            "HOME":        "/workspace",
            "PATH":        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        }

        container = None
        try:
            ensure_forge_network()

            try:
                self.client.images.pull(self.job.runtime)
            except Exception as e:
                self._log(f"[forge] Warning: could not pull image: {e}")

            container = self.client.containers.create(
                image       = self.job.runtime,
                command     = ["sh", "-c", command],
                working_dir = "/workspace",
                environment = environment,
                mem_limit   = memory_bytes,
                nano_cpus   = int(self.job.resources.cpu * 1e9),
                network     = FORGE_NETWORK,
                volumes     = {self.workspace: {"bind": "/workspace", "mode": "rw"}},
                security_opt= ["no-new-privileges:true"],
                cap_drop    = ["ALL"],
                cap_add     = ["CHOWN", "DAC_OVERRIDE", "SETUID", "SETGID"],
            )

            container.start()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._stream_logs, container)

            try:
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, container.wait),
                    timeout=JOB_TIMEOUT
                )
                exit_code = result.get("StatusCode", 1)
            except asyncio.TimeoutError:
                self._log(f"[forge] Job exceeded timeout of {JOB_TIMEOUT}s — killing")
                container.kill()
                return JobStatus.FAILED

            if exit_code == 137:
                self._log(f"[forge] Job killed by OOM. Limit was {self.job.resources.memory_mb}MB")
                return JobStatus.FAILED

            if exit_code != 0:
                self._log(f"[forge] Step exited with code {exit_code}")
                return JobStatus.FAILED

            return JobStatus.SUCCEEDED

        except docker.errors.ImageNotFound:
            self._log(f"[forge] ERROR: Image '{self.job.runtime}' not found")
            return JobStatus.FAILED
        except Exception as e:
            self._log(f"[forge] Container error: {e}")
            return JobStatus.FAILED
        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    def _stream_logs(self, container):
        try:
            for log_line in container.logs(stream=True, follow=True):
                line = log_line.decode("utf-8", errors="replace").rstrip("\n")
                self._log(line)
        except Exception as e:
            self._log(f"[forge] Log streaming error: {e}")
