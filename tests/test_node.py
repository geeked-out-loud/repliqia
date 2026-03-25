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
        """Get returns all versions stored (siblings from conflicts).
        
        Note: Sequential writes from same node don't create siblings.
        This test demonstrates the get() API returns siblings when conflicts exist.
        """
        node = Node("A")
        node_b = Node("B")
        
        # A writes
        va = node.put("key", {"v": 1})
        # B writes concurrently (different node)
        vb = node_b.put("key", {"v": 2})
        # A learns B's concurrent write
        node.merge([vb])
        
        versions = node.get("key")
        assert len(versions) == 2  # Both stored as siblings (concurrent)
        values = sorted([v.value["v"] for v in versions])
        assert values == [1, 2]

    def test_get_nonexistent_returns_empty(self) -> None:
        """Get on nonexistent key returns empty list."""
        node = Node("A")
        assert node.get("missing") == []

    def test_get_latest_returns_first_sibling(self) -> None:
        """get_latest returns first sibling when conflicts exist."""
        node = Node("A")
        node_b = Node("B")
        
        # Create a conflict from two different nodes
        va = node.put("key", {"v": 1})
        vb = node_b.put("key", {"v": 2})
        
        # Merge to create sibling
        node.merge([vb])
        
        latest = node.get_latest("key")
        assert latest is not None
        # Returns first sibling (same node's write)
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
        node_a = Node("A")
        node_b = Node("B")
        
        node_a.put("key1", {"x": 1})
        node_a.put("key2", {"y": 2})
        
        # Create concurrent write to key1 (from different node)
        vb = node_b.put("key1", {"x": 2})
        node_a.merge([vb])  # Now key1 has 2 versions
        
        state = node_a.get_state()
        
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


class TestConflictResolution:
    """Comprehensive conflict detection and resolution tests.
    
    Scenarios:
    1. Same node, ordered writes → no conflict (replacement)
    2. Different nodes, concurrent writes → real conflict
    3. Same value across nodes → no conflict (deduplication)
    4. Causal chain → no conflict (keep latest)
    5. Three-way concurrent → 3-way conflict
    """

    # ========== Scenario 1: Same Node Sequential Writes ==========

    def test_same_node_sequential_writes_no_conflict(self) -> None:
        """Same node writing same key twice shows NO conflict.
        
        Expected: Only the newer value exists (old is discarded).
        This is the core fix for the bug.
        """
        node = Node("A")
        
        # First write
        v1 = node.put("user:1", {"name": "Alice", "age": 30})
        assert v1.metadata.vector_clock.to_dict() == {"A": 1}
        
        # Second write to same key
        v2 = node.put("user:1", {"name": "Bob", "age": 35})
        assert v2.metadata.vector_clock.to_dict() == {"A": 2}
        
        # Should only have the newest version
        versions = node.get("user:1")
        assert len(versions) == 1, "Expected 1 version, got conflict"
        assert versions[0].value["name"] == "Bob"
        assert versions[0].metadata.vector_clock.to_dict() == {"A": 2}

    def test_same_node_multiple_sequential_updates(self) -> None:
        """Multiple sequential updates on same key from same node."""
        node = Node("A")
        
        # Write 1
        node.put("counter", {"value": 1})
        # Write 2
        node.put("counter", {"value": 2})
        # Write 3
        node.put("counter", {"value": 3})
        
        # Only latest should remain
        versions = node.get("counter")
        assert len(versions) == 1
        assert versions[0].value["value"] == 3

    def test_same_node_sequential_then_different_key(self) -> None:
        """Sequential writes to same key, then write to different key."""
        node = Node("A")
        
        node.put("key1", {"v": 1})  # A:1
        node.put("key1", {"v": 2})  # A:2
        node.put("key2", {"v": 1})  # A:3
        
        # key1 should have only v2
        versions_k1 = node.get("key1")
        assert len(versions_k1) == 1
        assert versions_k1[0].value["v"] == 2
        
        # key2 should have v1
        versions_k2 = node.get("key2")
        assert len(versions_k2) == 1
        assert versions_k2[0].value["v"] == 1

    def test_same_node_identical_values_deduplicated(self) -> None:
        """Writing identical values twice from same node - keeps only newest.
        
        New clock {A:2} dominates old {A:1}, so old is discarded.
        Even though values are identical, the domination is based on clock.
        """
        node = Node("A")
        
        # Write same value twice
        v1 = node.put("setting", {"dark_mode": True})
        v2 = node.put("setting", {"dark_mode": True})
        
        # Second write's clock dominates first, so only newest is kept
        versions = node.get("setting")
        assert len(versions) == 1
        assert versions[0].value["dark_mode"] is True
        assert versions[0].metadata.vector_clock.to_dict() == {"A": 2}

    # ========== Scenario 2: Different Nodes - Concurrent Writes ==========

    def test_different_nodes_concurrent_writes_create_conflict(self) -> None:
        """Different nodes writing concurrently → real conflict preserved."""
        node_a = Node("A")
        node_b = Node("B")
        
        # A writes
        va = node_a.put("key", {"node": "A", "value": 1})
        # B writes (concurrently, doesn't know about A's write)
        vb = node_b.put("key", {"node": "B", "value": 1})
        
        # A learns B's write
        node_a.merge([vb])
        
        # Should have BOTH versions as conflict
        versions = node_a.get("key")
        assert len(versions) == 2
        
        clocks = [v.metadata.vector_clock for v in versions]
        assert clocks[0].compare(clocks[1]) == "concurrent"

    def test_three_way_concurrent_conflict(self) -> None:
        """Three nodes writing concurrently → 3-way conflict."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")
        
        # All write concurrently to same key
        va = node_a.put("item", {"writer": "A"})
        vb = node_b.put("item", {"writer": "B"})
        vc = node_c.put("item", {"writer": "C"})
        
        # A learns B and C's writes
        node_a.merge([vb, vc])
        
        # Should have all 3 versions
        versions = node_a.get("item")
        assert len(versions) == 3
        
        writers = {v.value["writer"] for v in versions}
        assert writers == {"A", "B", "C"}

    def test_two_writers_interleaved_causality(self) -> None:
        """A writes, B merges then writes, A merges → causal order, no conflict."""
        node_a = Node("A")
        node_b = Node("B")
        
        # A writes first
        va = node_a.put("data", {"author": "A", "version": 1})
        assert va.metadata.vector_clock.to_dict() == {"A": 1}
        
        # B merges A's write and builds on it
        node_b.merge([va])  # B's clock becomes {A:1, B:1} after merge+tick
        vb = node_b.put("data", {"author": "B", "version": 2})
        clock_b = vb.metadata.vector_clock.to_dict()
        assert clock_b["A"] == 1  # Knows about A:1
        assert clock_b["B"] == 2  # B's own writes: 1 from merge+tick, 1 from put+tick
        
        # A learns B's causal update
        node_a.merge([vb])
        
        # A should only have B's version (it dominates A's older one)
        versions = node_a.get("data")
        assert len(versions) == 1
        assert versions[0].value["author"] == "B"
        assert versions[0].metadata.vector_clock.to_dict() == {"A": 1, "B": 2}

    # ========== Scenario 3: Same Value Deduplication ==========

    def test_same_value_from_different_nodes_deduplication(self) -> None:
        """Two nodes write same value concurrently → both stored (different authors)."""
        node_a = Node("A")
        node_b = Node("B")
        
        # Both write identical value independently
        va = node_a.put("config", {"mode": "read-only"})
        vb = node_b.put("config", {"mode": "read-only"})
        
        # A learns B's write
        node_a.merge([vb])
        
        # Even though values are identical, clocks are concurrent
        # So we keep both (though apps may deduplicate by value)
        versions = node_a.get("config")
        assert len(versions) == 2
        
        # Both have same value
        assert all(v.value["mode"] == "read-only" for v in versions)

    def test_same_node_then_different_node_same_value(self) -> None:
        """Same node writes value, then different node writes same value."""
        node_a = Node("A")
        node_b = Node("B")
        
        # A writes
        va = node_a.put("key", {"status": "active"})
        
        # B writes same value (doesn't know about A)
        vb = node_b.put("key", {"status": "active"})
        
        # A learns B's concurrent write
        node_a.merge([vb])
        
        # Both versions exist (concurrent, even though identical)
        versions = node_a.get("key")
        assert len(versions) == 2

    # ========== Scenario 4: Causal Chains (Not Concurrent) ==========

    def test_causal_chain_replaces_ancestor(self) -> None:
        """Causal chain: A→B→C written sequentially, latest C dominates."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")
        
        # A writes first
        v1 = node_a.put("chain", {"step": 1})
        
        # B merges and adds to chain
        node_b.merge([v1])
        v2 = node_b.put("chain", {"step": 2})
        
        # C merges B's version and adds further
        node_c.merge([v2])
        v3 = node_c.put("chain", {"step": 3})
        
        # Now B merges C's latest
        node_b.merge([v3])
        
        # B should only have C's version (it dominates B's earlier version)
        versions = node_b.get("chain")
        assert len(versions) == 1
        assert versions[0].value["step"] == 3

    def test_three_node_linear_causality(self) -> None:
        """Linear causal chain: A (step 1) → B (step 2) → C (step 3)."""
        node_a = Node("A")
        node_b = Node("B")  
        node_c = Node("C")
        
        # Step 1: A writes
        v1 = node_a.put("counter", {"count": 1})
        
        # Step 2: B merges A and increments
        node_b.merge([v1])
        v2 = node_b.put("counter", {"count": 2})
        assert v2.metadata.vector_clock.compare(v1.metadata.vector_clock) == "after"
        
        # Step 3: C merges B and increments
        node_c.merge([v2])
        v3 = node_c.put("counter", {"count": 3})
        assert v3.metadata.vector_clock.compare(v2.metadata.vector_clock) == "after"
        
        # Verify full causal chain at C
        clock_c = node_c.get_clock().to_dict()
        assert clock_c["A"] == 1
        assert clock_c["B"] == 2  # B incremented during merge and put
        assert clock_c["C"] == 2  # C incremented during merge and put

    # ========== Scenario 5: Complex Multi-Node Patterns ==========

    def test_diamond_topology_conflict(self) -> None:
        """Diamond: A writes, B and C diverge independently, D learns both."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")
        node_d = Node("D")
        
        # A writes base version
        va = node_a.put("key", {"version": 1, "author": "A"})
        
        # B merges A and diverges
        node_b.merge([va])
        vb = node_b.put("key", {"version": 2, "author": "B"})
        
        # C merges A and diverges differently
        node_c.merge([va])
        vc = node_c.put("key", {"version": 3, "author": "C"})
        
        # Now they're concurrent
        assert vb.metadata.vector_clock.compare(vc.metadata.vector_clock) == "concurrent"
        
        # D merges both concurrent versions
        node_d.merge([vb, vc])
        
        # D should see 2-way conflict
        versions = node_d.get("key")
        assert len(versions) == 2

    def test_merge_resolves_partial_causality(self) -> None:
        """Partial causality: B writes after A, C concurrent. Merging B then C."""
        node_a = Node("A")
        node_b = Node("B")
        node_c = Node("C")
        
        va = node_a.put("key", {"seen": ["A"]})
        
        # B sees A
        node_b.merge([va])
        vb = node_b.put("key", {"seen": ["A", "B"]})
        
        # C doesn't see A yet (concurrent writes)
        vc = node_c.put("key", {"seen": ["C"]})
        
        # A merges both: B dominates A, C concurrent with B
        node_a.merge([vb, vc])
        
        versions = node_a.get("key")
        assert len(versions) == 2  # B and C are concurrent
        
        seen_values = [v.value["seen"] for v in versions]
        assert ["A", "B"] in seen_values
        assert ["C"] in seen_values

    # ========== Scenario 6: Edge Cases ==========

    def test_empty_key_list_then_write(self) -> None:
        """Writing to non-existent key (empty history)."""
        node = Node("A")
        
        v = node.put("new_key", {"initial": True})
        
        versions = node.get("new_key")
        assert len(versions) == 1
        assert versions[0].value["initial"] is True

    def test_write_after_merge_of_same_key(self) -> None:
        """Write independently, then merge (creates real concurrent write)."""
        node_a = Node("A")
        node_b = Node("B")

        # Both write independently to same key (don't know about each other)
        va = node_a.put("key", {"writer": "A", "clock": 1})
        vb = node_b.put("key", {"writer": "B", "clock": 1})

        # Now A learns about B's independent write
        node_a.merge([vb])

        # Should have both as concurrent (neither knows about the other's write)
        versions = node_a.get("key")
        assert len(versions) == 2
        
        # Both clocks are concurrent
        clocks = [v.metadata.vector_clock for v in versions]
        assert clocks[0].compare(clocks[1]) == "concurrent"
    def test_merge_old_version_after_newer_local_write(self) -> None:
        """Write locally, then merge an older version from peer (should ignore)."""
        node_a = Node("A")
        node_b = Node("B")
        
        # B's old write first
        v1_old = Version(
            key="key",
            value={"version": 1},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=time.time(),
            ),
        )
        
        # A writes locally (newer)
        va = node_a.put("key", {"version": 2})
        # A's clock is now {"A": 1}
        
        # Now A receives B's old version (not causally related)
        # B:1 vs A:1 are concurrent
        node_a.merge([v1_old])
        
        # Should have both as conflict (concurrent)
        versions = node_a.get("key")
        assert len(versions) == 2

    def test_get_state_reflects_conflict_state(self) -> None:
        """Node state correctly reports multi-version situation."""
        node_a = Node("A")
        node_b = Node("B")
        
        # Create concurrent writes
        va = node_a.put("key", {"node": "A"})
        vb = node_b.put("key", {"node": "B"})
        
        node_a.merge([vb])
        
        state = node_a.get_state()
        assert state["storage"]["total_versions"] == 2
        assert state["storage"]["keys"] == 1

    # ========== Scenario 7: Integration with Vector Clock Semantics ==========

    def test_clock_semantic_dominated_not_stored(self) -> None:
        """Verify vector clock domination correctly filters old versions."""
        node = Node("A")
        
        # Write 1: creates {A: 1}
        v1 = node.put("key", {"value": "first"})
        assert v1.metadata.vector_clock.to_dict() == {"A": 1}
        
        # Write 2: creates {A: 2}
        v2 = node.put("key", {"value": "second"})
        assert v2.metadata.vector_clock.to_dict() == {"A": 2}
        
        # {A: 2} dominates {A: 1} semantically
        clock1 = v1.metadata.vector_clock
        clock2 = v2.metadata.vector_clock
        assert clock1.compare(clock2) == "before"  # 1 is before 2
        
        # Old version should be removed
        versions = node.get("key")
        assert len(versions) == 1
        assert versions[0].value["value"] == "second"

    def test_clock_semantic_concurrent_preserved(self) -> None:
        """Verify concurrent versions (different nodes) are preserved."""
        node = Node("A")
        
        # A writes
        va = node.put("key", {"author": "A"})
        clock_a = va.metadata.vector_clock
        
        # Simulate B's concurrent write
        vb = Version(
            key="key",
            value={"author": "B"},
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict({"B": 1}),
                author="B",
                timestamp=time.time(),
            ),
        )
        clock_b = vb.metadata.vector_clock
        
        # Verify they're concurrent
        assert clock_a.compare(clock_b) == "concurrent"
        
        # Merge B's version
        node.merge([vb])
        
        # Both should exist
        versions = node.get("key")
        assert len(versions) == 2

    def test_idempotent_operations(self) -> None:
        """Writing multiple identical values creates dominated versions."""
        node = Node("A")
        
        # Write same thing 5 times
        value = {"data": "immutable"}
        for _ in range(5):
            node.put("key", value)
        
        # Should only have the latest (clock 5)
        versions = node.get("key")
        assert len(versions) == 1
        assert versions[0].metadata.vector_clock.to_dict() == {"A": 5}

