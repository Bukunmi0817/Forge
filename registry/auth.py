import os
import sqlite3
import secrets
import bcrypt
from datetime import datetime


def get_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_db(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL UNIQUE,
            hash        TEXT NOT NULL,
            created_at  TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def create_token(db_path: str, name: str) -> str:
    raw_token = "forge-" + secrets.token_hex(32)
    hashed = bcrypt.hashpw(raw_token.encode(), bcrypt.gensalt(rounds=12))
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO tokens (name, hash, created_at) VALUES (?, ?, ?)",
            (name, hashed.decode(), datetime.utcnow().isoformat())
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise ValueError(f"A token named '{name}' already exists.")
    conn.close()
    return raw_token


def verify_token(db_path: str, raw_token: str) -> str | None:
    if not raw_token:
        return None
    conn = get_db(db_path)
    rows = conn.execute("SELECT name, hash FROM tokens").fetchall()
    conn.close()
    for row in rows:
        try:
            if bcrypt.checkpw(raw_token.encode(), row["hash"].encode()):
                return row["name"]
        except Exception:
            continue
    return None


def extract_bearer_token(authorization_header: str | None) -> str | None:
    if not authorization_header:
        return None
    parts = authorization_header.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1]
