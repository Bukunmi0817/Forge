import os
import json
import sqlite3
from datetime import datetime


def get_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_db(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS artifacts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT    NOT NULL,
            version      TEXT    NOT NULL,
            sha256       TEXT    NOT NULL,
            size         INTEGER NOT NULL,
            publisher    TEXT    NOT NULL,
            published_at TEXT    NOT NULL,
            deps         TEXT    NOT NULL DEFAULT '[]',
            UNIQUE(name, version)
        )
    """)
    conn.commit()
    conn.close()


def publish(db_path, name, version, sha256, size, publisher, deps) -> dict:
    now = datetime.utcnow().isoformat() + "Z"
    deps_json = json.dumps(deps)
    conn = get_db(db_path)
    try:
        conn.execute(
            """INSERT INTO artifacts
               (name, version, sha256, size, publisher, published_at, deps)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, version, sha256, size, publisher, now, deps_json)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise FileExistsError(f"{name}@{version} already exists (immutable)")
    conn.close()
    return {"name": name, "version": version, "sha256": sha256,
            "size": size, "publisher": publisher, "published_at": now, "deps": deps}


def get_artifact(db_path: str, name: str, version: str) -> dict | None:
    conn = get_db(db_path)
    row = conn.execute(
        "SELECT * FROM artifacts WHERE name=? AND version=?",
        (name, version)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return _row_to_dict(row)


def get_versions(db_path: str, name: str) -> list[dict]:
    conn = get_db(db_path)
    rows = conn.execute(
        "SELECT * FROM artifacts WHERE name=? ORDER BY published_at DESC", (name,)
    ).fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def get_all_versions_of(db_path: str, name: str) -> list[str]:
    conn = get_db(db_path)
    rows = conn.execute(
        "SELECT version FROM artifacts WHERE name=?", (name,)
    ).fetchall()
    conn.close()
    return [r["version"] for r in rows]


def _row_to_dict(row: sqlite3.Row) -> dict:
    return {
        "name":         row["name"],
        "version":      row["version"],
        "sha256":       row["sha256"],
        "size":         row["size"],
        "publisher":    row["publisher"],
        "published_at": row["published_at"],
        "deps":         json.loads(row["deps"]),
    }
