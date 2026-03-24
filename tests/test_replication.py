"""Tests for replication engine (PeerSync)."""

import pytest
import time
from repliqia.core import Node
from repliqia.clock import VectorClock
from repliqia.replication import PeerSync, ConflictView, SyncResult
from repliqia.storage import Version, VersionMetadata


class TestConflictView:
    """ConflictView representation."""

    def test_conflict_view_creation(self) -> None:
        """Create a ConflictView."""
        v1 = Version(
            key="key",
            value={"v": 1},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 1}),
                author="A",
                timestamp=0.0,
            ),
        )
        v2 = Version(
            key="key",
            value={"v": 2},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=0.0,
            ),
        )

        conflict = ConflictView(key="key", versions=[v1, v2])
        assert conflict.key == "key"
        assert len(conflict.versions) == 2

    def test_conflict_view_to_dict(self) -> None:
        """Export ConflictView to dict."""
        v1 = Version(
            key="key",
            value={"status": "A"},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"A": 1}),
                author="A",
                timestamp=100.0,
            ),
        )
        v2 = Version(
            key="key",
            value={"status": "B"},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=101.0,
            ),
        )

        conflict = ConflictView(key="key", versions=[v1, v2])
        data = conflict.to_dict()

        assert data["key"] == "key"
        assert len(data["versions"]) == 2
        assert data["versions"][0]["author"] == "A"
        assert data["versions"][1]["author"] == "B"


class TestSyncResultBasics:
    """SyncResult representation."""

    def test_sync_result_creation(self) -> None:
        """Create a SyncResult."""
        result = SyncResult(
            source_node_id="A",
            dest_node_id="B",
            keys_synced=5,
            new_versions_exchanged=3,
        )
        assert result.source_node_id == "A"
        assert result.dest_node_id == "B"
        assert result.keys_synced == 5
        assert result.new_versions_exchanged == 3

    def test_sync_result_to_dict(self) -> None:
        """Export SyncResult to dict."""
        result = SyncResult(
            source_node_id="A",
            dest_node_id="B",
            keys_synced=2,
            new_versions_exchanged=1,
            already_in_sync=False,
        )
        data = result.to_dict()

        assert data["source"] == "A"
        assert data["destination"] == "B"
        assert data["keys_synced"] == 2
        assert data["new_versions"] == 1


class TestPeerSyncBasics:
    """Basic PeerSync operations."""

    def test_peersync_initialization(self) -> None:
        """Create a PeerSync."""
        node_a = Node("A")
        node_b = Node("B")
        sync = PeerSync(node_a, node_b)

        assert sync.node_a.node_id == "A"
        assert sync.node_b.node_id == "B"

    def test_sync_empty_nodes(self) -> None:
        """Sync two empty nodes."""
        node_a = Node("A")
        node_b = Node("B")
        sync = PeerSync(node_a, node_b)

        result = sync.sync()

        assert result.keys_synced == 0
        assert result.new_versions_exchanged == 0
        assert result.already_in_sync is True
        assert result.conflicts_detected == []


class TestPeerSyncBasicSync:
    """Basic sync scenarios."""

    def test_sync_single_version_a_to_b(self) -> None:
        """A has data, sync brings it to B."""
        node_a = Node("A")
        node_b = Node("B")

        # A writes
        node_a.put("key1", {"data": "from_A"})

        sync = PeerSync(node_a, node_b)
        result = sync.sync()

        # B should now have the version
        versions_b = node_b.storage.get("key1")
        assert len(versions_b) == 1
        assert versions_b[0].value["data"] == "from_A"

    def test_sync_multiple_keys(self) -> None:
        """Sync multiple keys between nodes."""
        node_a = Node("A")
        node_b = Node("B")

        node_a.put("key1", {"v": 1})
        node_a.put("key2", {"v": 2})
        node_b.put("key3", {"v": 3})

        sync = PeerSync(node_a, node_b)
        result = sync.sync()

        assert result.keys_synced == 3
        assert node_b.storage.exists("key1")
        assert node_b.storage.exists("key2")
        assert node_a.storage.exists("key3")

    def test_sync_specific_key(self) -> None:
        """Sync only a specific key."""
        node_a = Node("A")
        node_b = Node("B")

        node_a.put("key1", {"sync": True})
        node_a.put("key2", {"sync": False})

        sync = PeerSync(node_a, node_b)
        result = sync.sync(key="key1")

        assert result.keys_synced == 1
        assert node_b.storage.exists("key1")
        assert not node_b.storage.exists("key2")

    def test_sync_already_in_sync(self) -> None:
        """Sync when nodes already have same data."""
        node_a = Node("A")
        node_b = Node("B")

        v = node_a.put("key", {"data": 1})
        node_b.merge([v])

        sync = PeerSync(node_a, node_b)
        result = sync.sync()

        assert result.already_in_sync is True
        assert result.new_versions_exchanged == 0


class TestPeerSyncConflicts:
    """Conflict detection during sync."""

    def test_sync_detects_concurrent_writes(self) -> None:
        """Sync detects conflicts from concurrent writes."""
        node_a = Node("A")
        node_b = Node("B")

        # Both write same key independently
        node_a.put("item", {"status": "active"})
        node_b.put("item", {"status": "inactive"})

        sync = PeerSync(node_a, node_b)
        result = sync.sync()

        # Should detect conflict
        assert len(result.conflicts_detected) > 0
        assert result.conflicts_detected[0].key == "item"
        assert len(result.conflicts_detected[0].versions) == 2

    def test_conflict_visualization(self) -> None:
        """Conflict can be visualized for CLI/UI."""
        node_a = Node("A")
        node_b = Node("B")

        v_a = node_a.put("data", {"choice": "A"})
        v_b = node_b.put("data", {"choice": "B"})

        sync = PeerSync(node_a, node_b)
        sync.sync()

        conflicts = sync.get_conflicts()
        assert len(conflicts) == 1

        # Export for visualization
        conflict_dict = conflicts[0].to_dict()
        assert "versions" in conflict_dict
        assert len(conflict_dict["versions"]) == 2

    def test_merge_resolves_conflicts(self) -> None:
        """If one version is a descendant, merge resolves (no conflict)."""
        node_a = Node("A")
        node_b = Node("B")

        # A writes first
        v1 = node_a.put("key", {"v": 1})

        # B learns A's version and writes descendant
        node_b.merge([v1])
        node_b.put("key", {"v": 2})

        # Sync
        sync = PeerSync(node_a, node_b)
        result = sync.sync()

        # No conflict because B's version is descended from A's
        conflicted_keys = [c.key for c in result.conflicts_detected]
        assert "key" not in conflicted_keys


class TestPeerSyncDivergence:
    """Divergence measurement."""

    def test_divergence_no_divergence(self) -> None:
        """Nodes in perfect sync show no divergence."""
        node_a = Node("A")
        node_b = Node("B")

        v = node_a.put("key", {"data": 1})
        node_b.merge([v])

        sync = PeerSync(node_a, node_b)
        sync.sync()

        divergence = sync.get_divergence()
        assert divergence["keys_only_in_a"] == 0
        assert divergence["keys_only_in_b"] == 0
        assert divergence["version_differences"] == 0

    def test_divergence_partial(self) -> None:
        """Measure partial divergence."""
        node_a = Node("A")
        node_b = Node("B")

        node_a.put("key_a", {"a": 1})
        node_b.put("key_b", {"b": 2})

        sync = PeerSync(node_a, node_b)

        divergence = sync.get_divergence()
        assert divergence["keys_only_in_a"] == 1
        assert divergence["keys_only_in_b"] == 1

    def test_divergence_with_conflicts(self) -> None:
        """Divergence includes conflicts."""
        node_a = Node("A")
        node_b = Node("B")

        node_a.put("key", {"choice": "A"})
        node_b.put("key", {"choice": "B"})

        sync = PeerSync(node_a, node_b)
        sync.sync()

        divergence = sync.get_divergence()
        assert divergence["conflict_count"] == 1


class TestPeerSyncVisualization:
    """Visualization/introspection methods."""

    def test_visualize_state(self) -> None:
        """Get complete state for visualization."""
        node_a = Node("A", N=3, R=1, W=2)
        node_b = Node("B", N=3, R=1, W=2)

        node_a.put("key1", {"v": 1})
        node_b.put("key2", {"v": 2})

        sync = PeerSync(node_a, node_b)
        state = sync.visualize_state()

        assert "node_a" in state
        assert "node_b" in state
        assert "divergence" in state
        assert "conflicts" in state

        # Check node states
        assert state["node_a"]["node_id"] == "A"
        assert state["node_b"]["node_id"] == "B"


class TestDynamoScenarios:
    """Real-world Dynamo-like scenarios."""

    def test_scenario_network_partition_recovery(self) -> None:
        """Two nodes partition, then recover."""
        node_a = Node("A")
        node_b = Node("B")

        # Initially in sync
        v1 = node_a.put("data", {"version": 1})
        node_b.merge([v1])

        # Partition: A and B diverge
        a_version = node_a.put("data", {"version": 2})
        b_version = node_b.put("data", {"version": 3})

        # Nodes are now out-of-sync with concurrent versions
        sync = PeerSync(node_a, node_b)
        divergence_before = sync.get_divergence()
        assert divergence_before["version_differences"] > 0

        # Partition heals: sync
        result = sync.sync()
        assert len(result.conflicts_detected) > 0

        # After sync, both nodes have all versions (may include redundant ones)
        final_versions = node_a.storage.get("data")
        assert len(final_versions) >= 2  # At least the conflicted versions

    def test_scenario_anti_entropy_repair(self) -> None:
        """Background sync repairs missing replicas."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")

        # A writes, but B and C miss it (network issue)
        node_a.put("key", {"value": "stored_on_A"})

        # Later, anti-entropy (background sync) runs
        sync_ab = PeerSync(node_a, node_b)
        sync_ab.sync()

        # Now also sync B to C
        sync_bc = PeerSync(node_b, node_c)
        sync_bc.sync()

        # C should now have the data (transitively via B)
        assert node_c.storage.exists("key")
        assert node_c.storage.get("key")[0].value == {"value": "stored_on_A"}

    def test_scenario_multi_node_conflict_resolution(self) -> None:
        """Three nodes with conflicts, needs multi-step resolution."""
        nodes = {
            node.node_id: node
            for node in [Node("A"), Node("B"), Node("C")]
        }

        # Each node writes independently
        nodes["A"].put("decision", {"node": "A", "value": 1})
        nodes["B"].put("decision", {"node": "B", "value": 2})
        nodes["C"].put("decision", {"node": "C", "value": 3})

        # Full mesh sync
        sync_ab = PeerSync(nodes["A"], nodes["B"])
        sync_ab.sync()
        sync_bc = PeerSync(nodes["B"], nodes["C"])
        sync_bc.sync()
        sync_ca = PeerSync(nodes["C"], nodes["A"])
        sync_ca.sync()

        # All should have all versions as siblings (conflicts)
        for node_id, node in nodes.items():
            versions = node.storage.get("decision")
            assert len(versions) == 3, f"Node {node_id} should have all 3 versions"


class TestReplicationWithEdgeCases:
    """Edge cases in replication."""

    def test_sync_preserves_metadata(self) -> None:
        """Metadata (clock, author, timestamp) preserved in sync."""
        node_a = Node("A")
        node_b = Node("B")

        v_original = node_a.put("key", {"data": "test"})
        original_clock = v_original.metadata.vector_clock.to_dict()
        original_author = v_original.metadata.author

        sync = PeerSync(node_a, node_b)
        sync.sync()

        v_synced = node_b.storage.get("key")[0]
        assert v_synced.metadata.vector_clock.to_dict() == original_clock
        assert v_synced.metadata.author == original_author

    def test_sync_handles_empty_keys_gracefully(self) -> None:
        """Sync with no data on either side."""
        node_a = Node("A")
        node_b = Node("B")

        sync = PeerSync(node_a, node_b)
        result = sync.sync(key="nonexistent")

        assert result.keys_synced == 1
        assert result.new_versions_exchanged == 0
