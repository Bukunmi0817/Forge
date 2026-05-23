import os
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path


class MultiLogWriter:
    def __init__(self, logs_dir: str, run_id: str):
        self.logs_dir  = Path(logs_dir) / run_id
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._job_files: dict[str, object] = {}
        self._all_file = open(self.logs_dir / "all.log", "a")

    def _get_job_file(self, job_name: str):
        if job_name not in self._job_files:
            path = self.logs_dir / f"{job_name}.log"
            self._job_files[job_name] = open(path, "a")
        return self._job_files[job_name]

    def write(self, job_name: str, line: str):
        ts    = datetime.now(timezone.utc).isoformat()
        entry = json.dumps({"ts": ts, "job": job_name, "line": line})
        payload = entry + "\n"
        jf = self._get_job_file(job_name)
        jf.write(payload)
        jf.flush()
        self._all_file.write(payload)
        self._all_file.flush()

    def close(self, job_name: str | None = None):
        if job_name:
            if job_name in self._job_files:
                self._job_files[job_name].close()
                del self._job_files[job_name]
        else:
            for f in self._job_files.values():
                f.close()
            self._job_files.clear()
            self._all_file.close()


class LogReader:
    def __init__(self, logs_dir: str, run_id: str):
        self.logs_dir = Path(logs_dir)
        self.run_id   = run_id

    def _path(self, job_name: str | None) -> Path:
        if job_name:
            return self.logs_dir / self.run_id / f"{job_name}.log"
        return self.logs_dir / self.run_id / "all.log"

    async def stream_logs(self, job_name: str | None, follow: bool, is_running_fn):
        path = self._path(job_name)

        for _ in range(20):
            if path.exists():
                break
            await asyncio.sleep(0.1)

        if not path.exists():
            if not follow:
                return
            while is_running_fn() and not path.exists():
                await asyncio.sleep(0.5)
            if not path.exists():
                return

        with open(path, "r") as f:
            while True:
                line = f.readline()
                if line:
                    line = line.strip()
                    if line:
                        try:
                            entry = json.loads(line)
                            yield f"data: {json.dumps(entry)}\n\n"
                        except json.JSONDecodeError:
                            pass
                else:
                    if not follow or not is_running_fn():
                        break
                    await asyncio.sleep(0.05)
