"""Tests for core Node implementation."""

import pytest
import time
from repliqia.core import Node
from repliqia.clock import VectorClock
from repliqia.storage import JSONBackend, SQLiteBackend, Version, VersionMetadata


class TestNodeBasics:
    """Basic node operations."""

    def test_node_initialization(self) -> None:
        """Create a node with default settings."""
        node = Node("A")
        assert node.node_id == "A"
        assert node.N == 3
        assert node.R == 1
        assert node.W == 1

    def test_node_custom_quorum(self) -> None:
        """Create a node with custom quorum parameters."""
        node = Node("B", N=5, R=2, W=3)
        assert node.N == 5
        assert node.R == 2
        assert node.W == 3

    def test_node_with_sqlite_storage(self) -> None:
        """Create node with SQLite backend."""
        backend = SQLiteBackend(":memory:")
        node = Node("C", storage=backend)
        assert node.storage is backend


class TestNodeVectorClock:
    """Vector clock management in nodes."""

    def test_tick_increments_clock(self) -> None:
        """Each tick increments node's counter."""
        node = Node("A")
        assert node.get_clock().to_dict() == {}  # Empty initially
        
        node.tick()
        assert node.get_clock().to_dict() == {"A": 1}
        
        node.tick()
        assert node.get_clock().to_dict() == {"A": 2}

    def test_advance_clock_merges_and_ticks(self) -> None:
        """advance_clock merges peer's clock and increments self."""
        node_a = Node("A")
        peer_clock = VectorClock.from_dict({"B": 3, "C": 1})
        
        node_a.advance_clock(peer_clock)
        
        clock_dict = node_a.get_clock().to_dict()
        assert clock_dict["A"] == 1  # Ticked
        assert clock_dict["B"] == 3  # Learned from peer
        assert clock_dict["C"] == 1  # Learned from peer

    def test_seen_nodes_tracking(self) -> None:
        """Node tracks all node IDs it learns about."""
        node = Node("A")
        assert node._seen_nodes == {"A"}
        
        node.advance_clock(VectorClock.from_dict({"B": 1}))
        assert "B" in node._seen_nodes
        
        node.advance_clock(VectorClock.from_dict({"C": 2}))
        assert "C" in node._seen_nodes


class TestNodeReadWrite:
    """Local read/write operations."""

    def test_put_creates_version_with_clock(self) -> None:
        """Put increments clock and stores version."""
        node = Node("A")
        version = node.put("key1", {"x": 1})
        
        assert version.key == "key1"
        assert version.value == {"x": 1}
        assert version.metadata.author == "A"
        assert version.metadata.vector_clock.to_dict() == {"A": 1}

    def test_multiple_puts_increment_clock(self) -> None:
        """Multiple puts increment clock monotonically."""
        node = Node("A")
        
        v1 = node.put("key1", {"v": 1})
        v2 = node.put("key2", {"v": 2})
        v3 = node.put("key1", {"v": 3})  # Key1 again
        
        assert v1.metadata.vector_clock.to_dict() == {"A": 1}
        assert v2.metadata.vector_clock.to_dict() == {"A": 2}
        assert v3.metadata.vector_clock.to_dict() == {"A": 3}

    def test_get_returns_versions(self) -> None:
        """Get returns all versions stored."""
        node = Node("A")
        node.put("key", {"v": 1})
        node.put("key", {"v": 2})
        
        versions = node.get("key")
        assert len(versions) == 2
        assert versions[0].value == {"v": 1}
        assert versions[1].value == {"v": 2}

    def test_get_nonexistent_returns_empty(self) -> None:
        """Get on nonexistent key returns empty list."""
        node = Node("A")
        assert node.get("missing") == []

    def test_get_latest_returns_first_sibling(self) -> None:
        """get_latest returns first version (convenience)."""
        node = Node("A")
        node.put("key", {"v": 1})
        node.put("key", {"v": 2})
        
        latest = node.get_latest("key")
        assert latest is not None
        assert latest.value == {"v": 1}

    def test_get_latest_nonexistent_returns_none(self) -> None:
        """get_latest on missing key returns None."""
        node = Node("A")
        assert node.get_latest("missing") is None


class TestNodeMerge:
    """Merge operations and conflict handling."""

    def test_merge_new_version(self) -> None:
        """Merge a version we don't have."""
        node_a = Node("A")
        node_a.put("key", {"version": 1})
        
        # Create an incoming version from another node
        incoming = Version(
            key="key",
            value={"version": 2},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=time.time(),
            ),
        )
        
        node_a.merge([incoming])
        
        versions = node_a.get("key")
        assert len(versions) == 2  # Both stored as siblings

    def test_merge_duplicate_ignored(self) -> None:
        """Merging same version twice doesn't duplicate."""
        node_a = Node("A")
        node_a.put("key", {"v": 1})
        
        first_version = node_a.get("key")[0]
        
        # Try to merge the same version
        node_a.merge([first_version])
        
        assert len(node_a.get("key")) == 1

    def test_merge_concurrent_creates_sibling(self) -> None:
        """Concurrent versions stored as siblings (conflict)."""
        node_a = Node("A")
        node_a.put("key", {"version": "A"})
        
        # Peer B wrote concurrently
        incoming_b = Version(
            key="key",
            value={"version": "B"},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=time.time(),
            ),
        )
        
        node_a.merge([incoming_b])
        
        versions = node_a.get("key")
        assert len(versions) == 2
        clocks = [v.metadata.vector_clock for v in versions]
        assert clocks[0].compare(clocks[1]) == "concurrent"

    def test_merge_causal_descendant_replaces(self) -> None:
        """If incoming is a descendant, it replaces all ancestors."""
        node_a = Node("A")
        
        # We have A's initial write
        old_version = node_a.put("key", {"status": "initial"})
        
        # Incoming is a descendant (knows about A:1)
        incoming = Version(
            key="key",
            value={"status": "updated"},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 1, "B": 1}),
                author="B",
                timestamp=time.time(),
            ),
        )
        
        node_a.merge([incoming])
        
        versions = node_a.get("key")
        # Should have only the newer one
        assert len(versions) == 1
        assert versions[0].value == {"status": "updated"}

    def test_merge_multiple_from_peer(self) -> None:
        """Merge multiple versions in one call (sequential from peer)."""
        node_a = Node("A")
        
        incoming_versions = [
            Version(
                key="key",
                value={"v": i},
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict({"B": i}),
                    author="B",
                    timestamp=time.time(),
                ),
            )
            for i in [1, 2]
        ]
        
        node_a.merge(incoming_versions)
        
        # Should only keep the latest one ({"B": 2} is after {"B": 1})
        versions = node_a.get("key")
        assert len(versions) == 1
        assert versions[0].value["v"] == 2

    def test_merge_advances_local_clock(self) -> None:
        """merge() updates local vector clock."""
        node_a = Node("A")
        node_a.tick()  # A:1
        
        incoming = Version(
            key="key",
            value={"v": 1},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 5, "C": 3}),
                author="B",
                timestamp=time.time(),
            ),
        )
        
        node_a.merge([incoming])
        
        clock = node_a.get_clock().to_dict()
        assert clock["A"] >= 2  # Ticked after merge
        assert clock["B"] == 5  # Learned from B
        assert clock["C"] == 3  # Learned from C


class TestNodeState:
    """Node state inspection."""

    def test_get_state_empty(self) -> None:
        """State of empty node."""
        node = Node("A", N=3, R=1, W=2)
        state = node.get_state()
        
        assert state["node_id"] == "A"
        assert state["vector_clock"] == {}  # Empty until first tick
        assert state["seen_nodes"] == ["A"]
        assert state["quorum"] == {"N": 3, "R": 1, "W": 2}
        assert state["storage"]["keys"] == 0
        assert state["storage"]["total_versions"] == 0

    def test_get_state_with_data(self) -> None:
        """State with stored data."""
        node = Node("A")
        node.put("key1", {"x": 1})
        node.put("key2", {"y": 2})
        node.put("key1", {"x": 2})  # 2 versions of key1
        
        state = node.get_state()
        
        assert state["storage"]["keys"] == 2
        assert state["storage"]["total_versions"] == 3


class TestDynamoScenarios:
    """Real-world Dynamo-style scenarios."""

    def test_scenario_single_node_write_then_read(self) -> None:
        """Simple write then read on one node."""
        node = Node("A")
        
        node.put("user_1", {"name": "Alice"})
        versions = node.get("user_1")
        
        assert len(versions) == 1
        assert versions[0].value["name"] == "Alice"
        assert versions[0].metadata.author == "A"

    def test_scenario_concurrent_writes_create_conflict(self) -> None:
        """Two nodes write concurrently, conflict on merge."""
        node_a = Node("A")
        node_b = Node("B")
        
        # Both write same key independently
        version_a = node_a.put("item", {"status": "active"})
        version_b = node_b.put("item", {"status": "inactive"})
        
        # A learns B's write
        node_a.merge([version_b])
        
        # Should have both versions as siblings
        versions = node_a.get("item")
        assert len(versions) == 2
        
        clocks = [v.metadata.vector_clock for v in versions]
        assert clocks[0].compare(clocks[1]) == "concurrent"

    def test_scenario_read_repair_via_merge(self) -> None:
        """After network partition, merge brings nodes in sync."""
        node_a = Node("A")
        node_b = Node("B")
        
        # A and B both have initial value
        a_version = node_a.put("key", {"version": 0})
        
        # A advances after partition
        node_a.put("key", {"version": 1})
        
        # B's older version (hasn't seen A's second write)
        b_versions = [a_version]
        
        # Partition heals, B receives A's newer version
        b_newer = node_a.get("key")[-1]  # Latest from A
        node_b.merge([b_newer])
        
        # B should now see the newer version
        assert node_b.get_latest("key").value["version"] == 1

    def test_scenario_multi_node_causality(self) -> None:
        """Three nodes with causal ordering."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")
        
        # A writes first
        v1 = node_a.put("data", {"step": 1})
        
        # B learns from A, writes
        node_b.merge([v1])
        v2 = node_b.put("data", {"step": 2})
        
        # C learns from B (transitively learns A's causality)
        node_c.merge([v2])
        
        # C's clock should reflect all three nodes
        clock_c = node_c.get_clock().to_dict()
        assert "A" in clock_c
        assert "B" in clock_c


class TestNodeWithSQLiteBackend:
    """Tests with persistence."""

    def test_node_persists_via_sqlite(self) -> None:
        """Node state persists across backend reuse."""
        db = SQLiteBackend(":memory:")
        node = Node("A", storage=db)
        
        node.put("key", {"persistent": True})
        stored = node.get("key")
        
        assert len(stored) == 1
        assert stored[0].value["persistent"] is True
