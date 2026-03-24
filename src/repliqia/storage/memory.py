"""In-memory JSON storage backend for testing and demonstration."""

from __future__ import annotations

from typing import Dict, List, Optional

from repliqia.storage.store import StorageBackend, Version


class JSONBackend(StorageBackend):
    """In-memory JSON storage backend (for testing/demo)."""

    def __init__(self) -> None:
        """Initialize empty JSON store."""
        # key -> list of versions (siblings)
        self._store: Dict[str, List[Version]] = {}

    def put(self, key: str, version: Version) -> None:
        """Store a new version as a sibling."""
        if key not in self._store:
            self._store[key] = []
        self._store[key].append(version)

    def get(self, key: str) -> List[Version]:
        """Get all versions of a key."""
        return self._store.get(key, [])

    def get_latest(self, key: str) -> Optional[Version]:
        """Get first sibling (caller decides conflict resolution)."""
        versions = self.get(key)
        return versions[0] if versions else None

    def remove(self, key: str) -> None:
        """Delete all versions of a key."""
        if key in self._store:
            del self._store[key]

    def keys(self) -> List[str]:
        """List all keys."""
        return list(self._store.keys())

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self._store

    def clear(self) -> None:
        """Clear all storage."""
        self._store.clear()

    def to_dict(self) -> dict:
        """Export full store to dict (for serialization)."""
        return {
            k: [v.to_dict() for v in versions] for k, versions in self._store.items()
        }

    @staticmethod
    def from_dict(data: dict) -> JSONBackend:
        """Import from dict."""
        backend = JSONBackend()
        for key, version_dicts in data.items():
            backend._store[key] = [Version.from_dict(vd) for vd in version_dicts]
        return backend
