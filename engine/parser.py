import yaml
import re
from dataclasses import dataclass, field


@dataclass
class ResourceSpec:
    cpu: float
    memory_mb: int


@dataclass
class Step:
    name: str
    run: str


@dataclass
class Job:
    name: str
    runtime: str
    steps: list[Step]
    resources: ResourceSpec
    needs: list[str]


@dataclass
class ArtifactSpec:
    name: str
    version: str
    path: str


@dataclass
class Dependency:
    name: str
    version: str


@dataclass
class Pipeline:
    name: str
    version: str
    dependencies: list[Dependency]
    jobs: dict[str, Job]
    artifacts: list[ArtifactSpec]
    raw_yaml: str


VALID_TOP_LEVEL      = {"name", "version", "dependencies", "jobs", "artifacts"}
VALID_JOB_FIELDS     = {"runtime", "resources", "steps", "needs"}
VALID_STEP_FIELDS    = {"name", "run"}
VALID_RESOURCE_FIELDS= {"cpu", "memory"}
VALID_DEP_FIELDS     = {"name", "version"}
VALID_ARTIFACT_FIELDS= {"name", "version", "path"}
SEMVER_RE            = re.compile(r"^\d+\.\d+\.\d+$")


def parse_pipeline(yaml_text: str) -> Pipeline:
    try:
        data = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        raise ParseError(f"YAML syntax error: {e}")

    if not isinstance(data, dict):
        raise ParseError("Pipeline must be a YAML mapping at the top level")

    _check_unknown_fields(data, VALID_TOP_LEVEL, "pipeline")

    name    = _require_str(data, "name",    "pipeline")
    version = _require_str(data, "version", "pipeline")

    if not SEMVER_RE.match(version):
        raise ParseError(f"pipeline.version '{version}' must be MAJOR.MINOR.PATCH")

    dependencies = []
    for i, dep in enumerate(data.get("dependencies") or []):
        if not isinstance(dep, dict):
            raise ParseError(f"dependencies[{i}] must be a mapping")
        _check_unknown_fields(dep, VALID_DEP_FIELDS, f"dependencies[{i}]")
        dependencies.append(Dependency(
            name    = _require_str(dep, "name",    f"dependencies[{i}]"),
            version = _require_str(dep, "version", f"dependencies[{i}]"),
        ))

    raw_jobs = data.get("jobs")
    if not raw_jobs or not isinstance(raw_jobs, dict):
        raise ParseError("'jobs' is required and must be a mapping")

    jobs: dict[str, Job] = {}
    for job_name, job_data in raw_jobs.items():
        if not isinstance(job_data, dict):
            raise ParseError(f"jobs.{job_name} must be a mapping")
        _check_unknown_fields(job_data, VALID_JOB_FIELDS, f"jobs.{job_name}")

        runtime   = _require_str(job_data, "runtime", f"jobs.{job_name}")
        resources = _parse_resources(job_data.get("resources") or {}, f"jobs.{job_name}.resources")
        needs_raw = job_data.get("needs") or []
        if not isinstance(needs_raw, list):
            raise ParseError(f"jobs.{job_name}.needs must be a list")

        steps_raw = job_data.get("steps") or []
        if not isinstance(steps_raw, list) or len(steps_raw) == 0:
            raise ParseError(f"jobs.{job_name}.steps is required and must be non-empty")

        steps = []
        for i, step_data in enumerate(steps_raw):
            if not isinstance(step_data, dict):
                raise ParseError(f"jobs.{job_name}.steps[{i}] must be a mapping")
            _check_unknown_fields(step_data, VALID_STEP_FIELDS, f"jobs.{job_name}.steps[{i}]")
            steps.append(Step(
                name = step_data.get("name") or f"step-{i}",
                run  = _require_str(step_data, "run", f"jobs.{job_name}.steps[{i}]"),
            ))

        jobs[job_name] = Job(
            name=job_name, runtime=runtime, steps=steps,
            resources=resources, needs=[str(n) for n in needs_raw],
        )

    for job_name, job in jobs.items():
        for needed in job.needs:
            if needed not in jobs:
                raise ParseError(f"jobs.{job_name}.needs references unknown job '{needed}'")

    artifacts = []
    for i, art in enumerate(data.get("artifacts") or []):
        if not isinstance(art, dict):
            raise ParseError(f"artifacts[{i}] must be a mapping")
        _check_unknown_fields(art, VALID_ARTIFACT_FIELDS, f"artifacts[{i}]")
        art_version = _require_str(art, "version", f"artifacts[{i}]")
        if not SEMVER_RE.match(art_version):
            raise ParseError(f"artifacts[{i}].version '{art_version}' must be valid semver")
        artifacts.append(ArtifactSpec(
            name    = _require_str(art, "name",    f"artifacts[{i}]"),
            version = art_version,
            path    = _require_str(art, "path",    f"artifacts[{i}]"),
        ))

    return Pipeline(name=name, version=version, dependencies=dependencies,
                    jobs=jobs, artifacts=artifacts, raw_yaml=yaml_text)


def _require_str(d: dict, key: str, context: str) -> str:
    if key not in d:
        raise ParseError(f"'{context}.{key}' is required but missing")
    return str(d[key])


def _check_unknown_fields(d: dict, valid: set, context: str):
    unknown = set(d.keys()) - valid
    if unknown:
        raise ParseError(
            f"Unknown field(s) in {context}: {', '.join(sorted(unknown))}. "
            f"Valid: {', '.join(sorted(valid))}"
        )


def _parse_memory(s: str) -> int:
    s = str(s).strip()
    if s.endswith("Gi"): return int(float(s[:-2]) * 1024)
    elif s.endswith("Mi"): return int(s[:-2])
    elif s.endswith("G"):  return int(float(s[:-1]) * 1024)
    elif s.endswith("M"):  return int(s[:-1])
    else:
        try: return int(s) // (1024 * 1024)
        except ValueError: raise ParseError(f"Cannot parse memory: '{s}'")


def _parse_resources(d: dict, context: str) -> ResourceSpec:
    _check_unknown_fields(d, VALID_RESOURCE_FIELDS, context)
    try:
        cpu = float(d.get("cpu", 1.0))
    except (ValueError, TypeError):
        raise ParseError(f"{context}.cpu must be a number")
    return ResourceSpec(cpu=cpu, memory_mb=_parse_memory(str(d.get("memory", "512Mi"))))


class ParseError(Exception):
    pass
