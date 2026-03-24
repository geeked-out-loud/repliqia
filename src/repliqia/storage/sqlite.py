"""SQLite-backed persistent storage for Repliqia."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List, Optional

from repliqia.clock import VectorClock
from repliqia.storage.store import StorageBackend, Version, VersionMetadata


class SQLiteBackend(StorageBackend):
    """SQLite-backed persistent storage."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        """Initialize SQLite backend.
        
        Args:
            db_path: Path to SQLite database file. Use ":memory:" for in-memory.
        """
        self.db_path = str(db_path)
        # For in-memory databases, keep a persistent connection
        if self.db_path == ":memory:":
            self._memory_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        else:
            self._memory_conn = None
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection (persistent for in-memory, new for files)."""
        if self._memory_conn is not None:
            return self._memory_conn
        return sqlite3.connect(self.db_path)

    def _init_schema(self) -> None:
        """Create schema if not exists."""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    vector_clock TEXT NOT NULL,
                    author TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    created_at REAL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_key ON versions(key)")
            conn.commit()
        finally:
            # Only close if it's a file-based connection
            if self._memory_conn is None:
                conn.close()

    def put(self, key: str, version: Version) -> None:
        """Store a new version as a sibling."""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO versions (key, value, vector_clock, author, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    key,
                    json.dumps(version.value),
                    json.dumps(version.metadata.vector_clock.to_dict()),
                    version.metadata.author,
                    version.metadata.timestamp,
                ),
            )
            conn.commit()
        finally:
            if self._memory_conn is None:
                conn.close()

    def get(self, key: str) -> List[Version]:
        """Get all versions of a key."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT value, vector_clock, author, timestamp FROM versions WHERE key = ?",
                (key,),
            )
            rows = cursor.fetchall()
        finally:
            if self._memory_conn is None:
                conn.close()

        versions = []
        for value_json, clock_json, author, timestamp in rows:
            ver = Version(
                key=key,
                value=json.loads(value_json),
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict(json.loads(clock_json)),
                    author=author,
                    timestamp=timestamp,
                ),
            )
            versions.append(ver)
        return versions

    def get_latest(self, key: str) -> Optional[Version]:
        """Get first sibling (caller decides strategy)."""
        versions = self.get(key)
        return versions[0] if versions else None

    def remove(self, key: str) -> None:
        """Delete all versions of a key."""
        conn = self._get_connection()
        try:
            conn.execute("DELETE FROM versions WHERE key = ?", (key,))
            conn.commit()
        finally:
            if self._memory_conn is None:
                conn.close()

    def keys(self) -> List[str]:
        """List all unique keys."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT DISTINCT key FROM versions ORDER BY key"
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            if self._memory_conn is None:
                conn.close()

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT 1 FROM versions WHERE key = ? LIMIT 1", (key,)
            )
            return cursor.fetchone() is not None
        finally:
            if self._memory_conn is None:
                conn.close()

    def clear(self) -> None:
        """Clear all storage."""
        conn = self._get_connection()
        try:
            conn.execute("DELETE FROM versions")
            conn.commit()
        finally:
            if self._memory_conn is None:
                conn.close()

    def stats(self) -> dict:
        """Get storage statistics."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT COUNT(DISTINCT key), COUNT(*) FROM versions"
            )
            unique_keys, total_versions = cursor.fetchone()
        finally:
            if self._memory_conn is None:
                conn.close()

        return {
            "unique_keys": unique_keys or 0,
            "total_versions": total_versions or 0,
            "db_path": self.db_path,
        }
