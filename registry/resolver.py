import re
from typing import NamedTuple


class Version(NamedTuple):
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def parse_version(version_str: str) -> Version:
    version_str = version_str.strip()
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", version_str)
    if not match:
        raise ValueError(
            f"'{version_str}' is not valid semver. Use MAJOR.MINOR.PATCH"
        )
    return Version(int(match.group(1)), int(match.group(2)), int(match.group(3)))


class Constraint:
    def __init__(self, constraint_str: str):
        self.raw = constraint_str.strip()
        self._conditions: list[tuple[str, Version]] = []
        self._parse(self.raw)

    def _parse(self, s: str):
        s = s.strip()
        if s.startswith("^"):
            base = parse_version(s[1:])
            self._conditions = [
                (">=", base),
                ("<",  Version(base.major + 1, 0, 0)),
            ]
        elif s.startswith("~"):
            base = parse_version(s[1:])
            self._conditions = [
                (">=", base),
                ("<",  Version(base.major, base.minor + 1, 0)),
            ]
        elif re.match(r"^[><=!]", s):
            parts = s.split()
            for part in parts:
                m = re.fullmatch(r"(>=|<=|>|<|==|!=|=)(\d+\.\d+\.\d+)", part)
                if not m:
                    raise ValueError(f"Cannot parse constraint part: '{part}'")
                self._conditions.append((m.group(1), parse_version(m.group(2))))
        else:
            base = parse_version(s)
            self._conditions = [("=", base)]

    def matches(self, version: Version) -> bool:
        for op, bound in self._conditions:
            if op in ("=", "=="):
                if version != bound: return False
            elif op == ">=":
                if version < bound:  return False
            elif op == ">":
                if version <= bound: return False
            elif op == "<=":
                if version > bound:  return False
            elif op == "<":
                if version >= bound: return False
            elif op == "!=":
                if version == bound: return False
        return True


class DependencyResolver:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def resolve(self, requirements: list[dict]) -> dict:
        collected: dict[str, list[Constraint]] = {}
        visited: set[str] = set()

        def walk(reqs: list[dict], path: list[str]):
            for req in reqs:
                name    = req["name"]
                version = req["version"]
                if name in path:
                    cycle_str = " → ".join(path + [name])
                    raise CycleError(f"Circular dependency: {cycle_str}")
                if name not in collected:
                    collected[name] = []
                collected[name].append(Constraint(version))
                if name not in visited:
                    visited.add(name)
                    self._walk_transitive(name, path + [name], collected, visited)

        walk(requirements, [])

        from registry.metadata import get_all_versions_of, get_artifact
        lockfile = {}

        for name, constraints in sorted(collected.items()):
            available_raw = get_all_versions_of(self.db_path, name)
            if not available_raw:
                raise LookupError(f"Package '{name}' not found in registry")

            available = []
            for v in available_raw:
                try:
                    available.append(parse_version(v))
                except ValueError:
                    pass

            satisfying = [v for v in available if all(c.matches(v) for c in constraints)]

            if not satisfying:
                constraint_strs = [c.raw for c in constraints]
                raise ConflictError(
                    f"No version of '{name}' satisfies: "
                    + ", ".join(constraint_strs)
                    + f". Available: {[str(v) for v in sorted(available)]}"
                )

            best = max(satisfying)
            meta = get_artifact(self.db_path, name, str(best))
            if meta is None:
                raise LookupError(f"Metadata not found for {name}@{best}")

            lockfile[name] = {
                "version": str(best),
                "sha256":  meta["sha256"],
                "deps":    meta["deps"],
            }

        return lockfile

    def _walk_transitive(self, name, path, collected, visited):
        from registry.metadata import get_all_versions_of, get_artifact
        for ver_str in get_all_versions_of(self.db_path, name):
            meta = get_artifact(self.db_path, name, ver_str)
            if not meta:
                continue
            for dep in meta.get("deps", []):
                dep_name = dep["name"]
                dep_ver  = dep["version"]
                if dep_name not in collected:
                    collected[dep_name] = []
                collected[dep_name].append(Constraint(dep_ver))
                if dep_name not in visited:
                    visited.add(dep_name)
                    self._walk_transitive(dep_name, path + [dep_name], collected, visited)


class ConflictError(Exception):
    pass

class CycleError(Exception):
    pass
