"""Tests for vector clock implementation."""

import pytest

from repliqia.clock import VectorClock


class TestVectorClockBasics:
    """Test basic VectorClock operations."""

    def test_empty_clock(self) -> None:
        """Empty clock should be falsy."""
        vc = VectorClock()
        assert not vc
        assert vc.data == {}

    def test_tick(self) -> None:
        """Ticking increments node counter."""
        vc = VectorClock()
        vc1 = vc.tick("A")
        assert vc1.data == {"A": 1}
        assert vc.data == {}  # Original unchanged (immutable)

        vc2 = vc1.tick("A")
        assert vc2.data == {"A": 2}

        vc3 = vc2.tick("B")
        assert vc3.data == {"A": 2, "B": 1}

    def test_merge(self) -> None:
        """Merge takes element-wise max."""
        a = VectorClock.from_dict({"A": 2, "B": 1})
        b = VectorClock.from_dict({"A": 1, "B": 2})
        merged = a.merge(b)
        assert merged.data == {"A": 2, "B": 2}

    def test_merge_with_new_node(self) -> None:
        """Merge can introduce new nodes."""
        a = VectorClock.from_dict({"A": 1})
        b = VectorClock.from_dict({"B": 1})
        merged = a.merge(b)
        assert merged.data == {"A": 1, "B": 1}


class TestVectorClockComparison:
    """Test causality comparison logic."""

    def test_compare_equal(self) -> None:
        """Equal clocks compare as equal."""
        a = VectorClock.from_dict({"A": 1, "B": 2})
        b = VectorClock.from_dict({"A": 1, "B": 2})
        assert a.compare(b) == "equal"

    def test_compare_before(self) -> None:
        """Older clock is before newer."""
        old = VectorClock.from_dict({"A": 1})
        new = VectorClock.from_dict({"A": 2})
        assert old.compare(new) == "before"

    def test_compare_after(self) -> None:
        """Newer clock is after older."""
        new = VectorClock.from_dict({"A": 2})
        old = VectorClock.from_dict({"A": 1})
        assert new.compare(old) == "after"

    def test_compare_concurrent_same_node(self) -> None:
        """Clocks with different node paths are concurrent."""
        a = VectorClock.from_dict({"A": 1, "B": 0})
        b = VectorClock.from_dict({"A": 0, "B": 1})
        assert a.compare(b) == "concurrent"
        assert b.compare(a) == "concurrent"

    def test_compare_concurrent_multi_node(self) -> None:
        """Complex concurrency: A ahead on one axis, behind on another."""
        a = VectorClock.from_dict({"A": 2, "B": 1})
        b = VectorClock.from_dict({"A": 1, "B": 2})
        assert a.compare(b) == "concurrent"

    def test_compare_causal_chain(self) -> None:
        """A -> (merge B) should show A's later state is after B."""
        a1 = VectorClock.from_dict({"A": 1})
        b = VectorClock.from_dict({"B": 1})
        a2 = a1.merge(b).tick("A")  # A learned about B, then ticked
        assert a2.data == {"A": 2, "B": 1}
        assert a2.compare(b) == "after"


class TestVectorClockRelationships:
    """Test helper comparison functions."""

    def test_is_causal_descendant_of_true(self) -> None:
        """Test causal descendant relationship."""
        parent = VectorClock.from_dict({"A": 1, "B": 0})
        child = VectorClock.from_dict({"A": 2, "B": 1})
        assert child.is_causal_descendant_of(parent)

    def test_is_causal_descendant_of_false(self) -> None:
        """Non-descendants return False."""
        a = VectorClock.from_dict({"A": 2})
        b = VectorClock.from_dict({"B": 1})
        assert not a.is_causal_descendant_of(b)
        assert not b.is_causal_descendant_of(a)


class TestVectorClockSerialization:
    """Test dict conversion."""

    def test_to_from_dict(self) -> None:
        """Round-trip through dict."""
        original = VectorClock.from_dict({"A": 3, "B": 2, "C": 1})
        d = original.to_dict()
        restored = VectorClock.from_dict(d)
        assert original == restored


class TestVectorClockStringRepresentation:
    """Test string formatting."""

    def test_repr(self) -> None:
        """String representation is compact."""
        vc = VectorClock.from_dict({"A": 1, "B": 2})
        assert repr(vc) == "{A:1, B:2}"

    def test_str(self) -> None:
        """str() calls repr()."""
        vc = VectorClock.from_dict({"A": 1})
        assert str(vc) == "{A:1}"


class TestVectorClockHashing:
    """Test that VectorClock can be used in sets/dicts."""

    def test_hash_consistency(self) -> None:
        """Equal clocks have same hash."""
        a = VectorClock.from_dict({"A": 1, "B": 2})
        b = VectorClock.from_dict({"A": 1, "B": 2})
        assert hash(a) == hash(b)

    def test_set_membership(self) -> None:
        """VectorClock works in sets."""
        a = VectorClock.from_dict({"A": 1})
        b = VectorClock.from_dict({"A": 1})
        s = {a, b}
        assert len(s) == 1  # Deduplicated

    def test_dict_key(self) -> None:
        """VectorClock works as dict key."""
        vc = VectorClock.from_dict({"A": 1})
        d = {vc: "value"}
        assert d[vc] == "value"


class TestDynamoScenarios:
    """Real-world Dynamo-style scenarios."""

    def test_scenario_sequential_write(self) -> None:
        """Node A writes, then B reads and writes."""
        a_write1 = VectorClock.from_dict({"A": 1})
        b_write1 = a_write1.merge(VectorClock()).tick("B")  # B learns A's version
        assert b_write1.data == {"A": 1, "B": 1}
        assert b_write1.compare(a_write1) == "after"

    def test_scenario_concurrent_writes(self) -> None:
        """A and B write independently (concurrent)."""
        a_write = VectorClock.from_dict({"A": 1})
        b_write = VectorClock.from_dict({"B": 1})
        assert a_write.compare(b_write) == "concurrent"

    def test_scenario_merge_conflict(self) -> None:
        """Two nodes merge their clocks after concurrent writes."""
        a_clock = VectorClock.from_dict({"A": 2})
        b_clock = VectorClock.from_dict({"B": 1})
        # After sync, both learn each other's history
        merged_a = a_clock.merge(b_clock)  # A now sees {A:2, B:1}
        merged_b = b_clock.merge(a_clock)  # B now sees {A:2, B:1}
        assert merged_a == merged_b == VectorClock.from_dict({"A": 2, "B": 1})
