"""Tests for storage backends."""

import pytest
import tempfile
from pathlib import Path

from repliqia.clock import VectorClock
from repliqia.storage import (
    SQLiteBackend,
    StorageBackend,
    Version,
    VersionMetadata,
)


class TestStorageBackendInterface:
    """Generic tests for any storage backend (abstract interface)."""

    @pytest.fixture(params=["sqlite_memory", "sqlite_file"])
    def backend(self, request: pytest.FixtureRequest) -> StorageBackend:
        """Provide different backend implementations."""
        if request.param == "sqlite_memory":
            return SQLiteBackend(":memory:")
        else:  # sqlite_file
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                path = f.name
            return SQLiteBackend(path)

    @staticmethod
    def _make_version(
        key: str, value: dict, author: str, clock: dict
    ) -> Version:
        """Helper: create a Version."""
        return Version(
            key=key,
            value=value,
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict(clock),
                author=author,
                timestamp=0.0,
            ),
        )

    def test_empty_backend(self, backend: StorageBackend) -> None:
        """Empty backend has no keys."""
        assert backend.keys() == []
        assert not backend.exists("any_key")

    def test_put_and_get(self, backend: StorageBackend) -> None:
        """Put a version, then get it."""
        ver = self._make_version("user_1", {"name": "Alice"}, "A", {"A": 1})
        backend.put("user_1", ver)

        versions = backend.get("user_1")
        assert len(versions) == 1
        assert versions[0].value == {"name": "Alice"}
        assert versions[0].metadata.author == "A"

    def test_get_nonexistent_key(self, backend: StorageBackend) -> None:
        """Get nonexistent key returns empty list."""
        assert backend.get("nonexistent") == []

    def test_exists(self, backend: StorageBackend) -> None:
        """Check key existence."""
        ver = self._make_version("key1", {"x": 1}, "A", {"A": 1})
        backend.put("key1", ver)

        assert backend.exists("key1")
        assert not backend.exists("key2")

    def test_multi_version_siblings(self, backend: StorageBackend) -> None:
        """Multiple versions on same key stored as siblings."""
        ver1 = self._make_version("key", {"v": 1}, "A", {"A": 1})
        ver2 = self._make_version("key", {"v": 2}, "B", {"B": 1})

        backend.put("key", ver1)
        backend.put("key", ver2)

        versions = backend.get("key")
        assert len(versions) == 2
        assert versions[0].value == {"v": 1}
        assert versions[1].value == {"v": 2}

    def test_get_latest_returns_first_sibling(self, backend: StorageBackend) -> None:
        """get_latest returns first sibling (caller decides strategy)."""
        ver1 = self._make_version("key", {"v": 1}, "A", {"A": 1})
        ver2 = self._make_version("key", {"v": 2}, "B", {"B": 1})

        backend.put("key", ver1)
        backend.put("key", ver2)

        latest = backend.get_latest("key")
        assert latest is not None
        assert latest.value == {"v": 1}  # First one

    def test_get_latest_nonexistent(self, backend: StorageBackend) -> None:
        """get_latest on nonexistent key returns None."""
        assert backend.get_latest("nonexistent") is None

    def test_remove_key(self, backend: StorageBackend) -> None:
        """Remove deletes all versions of a key."""
        ver1 = self._make_version("key", {"v": 1}, "A", {"A": 1})
        ver2 = self._make_version("key", {"v": 2}, "B", {"B": 1})

        backend.put("key", ver1)
        backend.put("key", ver2)
        assert len(backend.get("key")) == 2

        backend.remove("key")
        assert backend.get("key") == []
        assert not backend.exists("key")

    def test_keys_listing(self, backend: StorageBackend) -> None:
        """keys() returns all unique keys."""
        backend.put("key1", self._make_version("key1", {"a": 1}, "A", {"A": 1}))
        backend.put("key2", self._make_version("key2", {"b": 2}, "B", {"B": 1}))
        backend.put("key1", self._make_version("key1", {"a": 2}, "B", {"B": 1}))

        keys = sorted(backend.keys())
        assert keys == ["key1", "key2"]

    def test_clear_all(self, backend: StorageBackend) -> None:
        """clear() removes all data."""
        backend.put("key1", self._make_version("key1", {"a": 1}, "A", {"A": 1}))
        backend.put("key2", self._make_version("key2", {"b": 2}, "B", {"B": 1}))

        backend.clear()
        assert backend.keys() == []
        assert not backend.exists("key1")
        assert not backend.exists("key2")

    def test_vector_clock_preserved(self, backend: StorageBackend) -> None:
        """Vector clocks are preserved across put/get."""
        clock = {"A": 3, "B": 2, "C": 1}
        ver = Version(
            key="key",
            value={"x": 100},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict(clock),
                author="A",
                timestamp=12345.0,
            ),
        )
        backend.put("key", ver)

        retrieved = backend.get("key")[0]
        assert retrieved.metadata.vector_clock.to_dict() == clock
        assert retrieved.metadata.author == "A"
        assert retrieved.metadata.timestamp == 12345.0

    def test_complex_values(self, backend: StorageBackend) -> None:
        """Store complex nested JSON values."""
        complex_value = {
            "user": {
                "name": "Alice",
                "tags": ["admin", "user"],
                "metadata": {"created": 1234567890, "active": True},
            }
        }
        ver = Version(
            key="complex_key",
            value=complex_value,
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 1}),
                author="A",
                timestamp=0.0,
            ),
        )
        backend.put("complex_key", ver)

        retrieved = backend.get("complex_key")[0]
        assert retrieved.value == complex_value




class TestSQLiteBackend:
    """SQLite-specific tests."""

    def test_persistent_file(self) -> None:
        """Data persists across backend instantiations."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # Write
            backend1 = SQLiteBackend(db_path)
            backend1.put(
                "key",
                Version(
                    key="key",
                    value={"x": 1},
                    metadata=VersionMetadata(
                        vector_clock=VectorClock.from_dict({"A": 1}),
                        author="A",
                        timestamp=0.0,
                    ),
                ),
            )

            # Read from new instance
            backend2 = SQLiteBackend(db_path)
            versions = backend2.get("key")
            assert len(versions) == 1
            assert versions[0].value == {"x": 1}
        finally:
            Path(db_path).unlink()

    def test_stats(self) -> None:
        """stats() reports correct counts."""
        backend = SQLiteBackend(":memory:")
        assert backend.stats()["unique_keys"] == 0
        assert backend.stats()["total_versions"] == 0

        backend.put(
            "key1",
            Version(
                key="key1",
                value={"a": 1},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"A": 1}),
                    author="A",
                    timestamp=0.0,
                ),
            ),
        )
        backend.put(
            "key1",
            Version(
                key="key1",
                value={"a": 2},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"B": 1}),
                    author="B",
                    timestamp=0.0,
                ),
            ),
        )
        backend.put(
            "key2",
            Version(
                key="key2",
                value={"b": 1},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"A": 1}),
                    author="A",
                    timestamp=0.0,
                ),
            ),
        )

        stats = backend.stats()
        assert stats["unique_keys"] == 2
        assert stats["total_versions"] == 3


class TestDynamoScenarios:
    """Real-world Dynamo-style scenarios."""

    def test_scenario_concurrent_writes(self) -> None:
        """Two nodes write concurrently, both versions stored."""
        backend = SQLiteBackend(":memory:")

        # Node A writes
        backend.put(
            "item_1",
            Version(
                key="item_1",
                value={"status": "active"},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"A": 1}),
                    author="A",
                    timestamp=100.0,
                ),
            ),
        )

        # Node B writes concurrently
        backend.put(
            "item_1",
            Version(
                key="item_1",
                value={"status": "inactive"},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"B": 1}),
                    author="B",
                    timestamp=101.0,
                ),
            ),
        )

        # Both versions stored
        versions = backend.get("item_1")
        assert len(versions) == 2
        assert any(v.value["status"] == "active" for v in versions)
        assert any(v.value["status"] == "inactive" for v in versions)

    def test_scenario_read_repair_via_siblings(self) -> None:
        """Sibling detection enables read repair during sync."""
        backend = SQLiteBackend(":memory:")

        # Store multiple versions (typical after network partition)
        v1 = Version(
            key="data",
            value={"count": 5},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 1, "B": 0}),
                author="A",
                timestamp=100.0,
            ),
        )
        v2 = Version(
            key="data",
            value={"count": 3},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 0, "B": 1}),
                author="B",
                timestamp=101.0,
            ),
        )

        backend.put("data", v1)
        backend.put("data", v2)

        siblings = backend.get("data")
        assert len(siblings) == 2
        # During sync, these clocks would be compared to detect concurrency
        clocks = [s.metadata.vector_clock for s in siblings]
        assert clocks[0].compare(clocks[1]) == "concurrent"
