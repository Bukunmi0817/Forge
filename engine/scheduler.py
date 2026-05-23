from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional
from engine.parser import Job


class JobStatus:
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    SKIPPED   = "skipped"


@dataclass
class JobNode:
    job: Job
    status: str = JobStatus.PENDING
    started_at: Optional[str]  = None
    finished_at: Optional[str] = None
    error: Optional[str]       = None


class JobDAG:
    def __init__(self, jobs: dict[str, Job]):
        self.jobs  = jobs
        self.nodes = {name: JobNode(job=job) for name, job in jobs.items()}

    def validate(self):
        UNVISITED   = 0
        IN_PROGRESS = 1
        DONE        = 2
        state = {name: UNVISITED for name in self.jobs}

        def dfs(name: str, path: list[str]):
            if state[name] == DONE:
                return
            if state[name] == IN_PROGRESS:
                cycle_start = path.index(name)
                cycle = path[cycle_start:] + [name]
                raise CycleError("Circular dependency: " + " → ".join(cycle))
            state[name] = IN_PROGRESS
            path.append(name)
            for needed in self.jobs[name].needs:
                dfs(needed, path)
            path.pop()
            state[name] = DONE

        for job_name in self.jobs:
            if state[job_name] == UNVISITED:
                dfs(job_name, [])

    def execution_levels(self) -> list[list[str]]:
        in_degree:  dict[str, int]       = {name: 0 for name in self.jobs}
        dependents: dict[str, list[str]] = defaultdict(list)

        for name, job in self.jobs.items():
            for needed in job.needs:
                in_degree[name] += 1
                dependents[needed].append(name)

        queue = deque(name for name, deg in in_degree.items() if deg == 0)
        levels: list[list[str]] = []
        processed = 0

        while queue:
            level_size = len(queue)
            level: list[str] = []
            for _ in range(level_size):
                name = queue.popleft()
                level.append(name)
                processed += 1
                for dep_name in dependents[name]:
                    in_degree[dep_name] -= 1
                    if in_degree[dep_name] == 0:
                        queue.append(dep_name)
            levels.append(level)

        if processed != len(self.jobs):
            unprocessed = [n for n in self.jobs if in_degree[n] > 0]
            raise CycleError(f"Cycle detected involving: {', '.join(unprocessed)}")

        return levels

    def mark_skipped_dependents(self, failed_job: str):
        dependents: dict[str, list[str]] = defaultdict(list)
        for name, job in self.jobs.items():
            for needed in job.needs:
                dependents[needed].append(name)

        queue = deque([failed_job])
        visited = {failed_job}

        while queue:
            current = queue.popleft()
            for dep_name in dependents[current]:
                if dep_name not in visited:
                    visited.add(dep_name)
                    if self.nodes[dep_name].status == JobStatus.PENDING:
                        self.nodes[dep_name].status = JobStatus.SKIPPED
                    queue.append(dep_name)


class CycleError(Exception):
    pass
