import os
import hashlib
from pathlib import Path

CHUNK_SIZE = 64 * 1024

class BlobStore:
    def __init__(self, storage_path: str):
        self.root = Path(storage_path)
        self.root.mkdir(parents=True, exist_ok=True)

    def _blob_path(self, sha256_hex: str) -> Path:
        prefix = sha256_hex[:2]
        rest   = sha256_hex[2:]
        return self.root / prefix / rest

    def exists(self, sha256_hex: str) -> bool:
        return self._blob_path(sha256_hex).exists()

    def save(self, data: bytes, declared_sha256: str | None = None) -> str:
        actual_hex = hashlib.sha256(data).hexdigest()
        if declared_sha256 is not None:
            declared_hex = declared_sha256.removeprefix("sha256:")
            if declared_hex != actual_hex:
                raise ValueError(
                    f"Checksum mismatch: "
                    f"declared=sha256:{declared_hex}, "
                    f"actual=sha256:{actual_hex}"
                )
        path = self._blob_path(actual_hex)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return actual_hex

    def get(self, sha256_hex: str) -> bytes:
        path = self._blob_path(sha256_hex)
        if not path.exists():
            raise FileNotFoundError(f"Blob sha256:{sha256_hex} not found")
        return path.read_bytes()

    def stream(self, sha256_hex: str):
        path = self._blob_path(sha256_hex)
        if not path.exists():
            raise FileNotFoundError(f"Blob sha256:{sha256_hex} not found")
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    def verify(self, sha256_hex: str) -> bool:
        path = self._blob_path(sha256_hex)
        if not path.exists():
            return False
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest() == sha256_hex

    def size(self, sha256_hex: str) -> int:
        path = self._blob_path(sha256_hex)
        if not path.exists():
            raise FileNotFoundError(f"Blob sha256:{sha256_hex} not found")
        return path.stat().st_size
