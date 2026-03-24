"""Tests for REST API server module."""

import json
from typing import Any, Dict

import pytest

from repliqia.api import create_app
from repliqia.clock import VectorClock
from repliqia.core import Node
from repliqia.storage import JSONBackend, Version, VersionMetadata


@pytest.fixture
def backend():
    """Create fresh JSON backend for each test."""
    return JSONBackend()


@pytest.fixture
def node(backend):
    """Create a Node with test configuration."""
    return Node(node_id="node-1", storage=backend, N=3, R=1, W=1)


@pytest.fixture
def app(node):
    """Create Flask test app."""
    return create_app(node)


@pytest.fixture
def client(app):
    """Create Flask test client."""
    return app.test_client()


class TestKeyValueOperations:
    """Test PUT/GET/DELETE operations."""

    def test_put_creates_key(self, client):
        """PUT should create a new key-value pair."""
        response = client.put(
            "/kvstore/user:1", json={"value": {"name": "Alice", "age": 30}}
        )
        assert response.status_code == 201
        data = response.get_json()
        assert data["key"] == "user:1"
        assert data["value"]["name"] == "Alice"
        assert "clock" in data
        assert "quorum" in data

    def test_put_includes_vector_clock(self, client):
        """PUT response should include vector clock."""
        response = client.put("/kvstore/key-1", json={"value": {"x": 1}})
        data = response.get_json()
        assert isinstance(data["clock"], dict)
        assert "node-1" in data["clock"]

    def test_put_includes_quorum_info(self, client):
        """PUT response should include quorum parameters."""
        response = client.put("/kvstore/key-1", json={"value": {"x": 1}})
        data = response.get_json()
        quorum = data["quorum"]
        assert quorum["N"] == 3
        assert quorum["R"] == 1
        assert quorum["W"] == 1
        assert quorum["satisfied"] is True
        assert quorum["acks"] == 1

    def test_get_returns_value(self, client):
        """GET should return stored value."""
        client.put("/kvstore/name", json={"value": "Alice"})
        response = client.get("/kvstore/name")
        assert response.status_code == 200
        data = response.get_json()
        assert data["key"] == "name"
        assert data["value"] == "Alice"
        assert data["conflict"] is False

    def test_get_nonexistent_key_returns_404(self, client):
        """GET for missing key should return 404."""
        response = client.get("/kvstore/missing")
        assert response.status_code == 404
        data = response.get_json()
        assert "error" in data

    def test_delete_removes_key(self, client):
        """DELETE should remove a key."""
        client.put("/kvstore/temp", json={"value": {"x": 1}})
        response = client.delete("/kvstore/temp")
        assert response.status_code == 204

    def test_delete_makes_key_unretrievable(self, client):
        """After DELETE, GET should return 404."""
        client.put("/kvstore/temp", json={"value": {"x": 1}})
        client.delete("/kvstore/temp")
        response = client.get("/kvstore/temp")
        assert response.status_code == 404


class TestConflictDetection:
    """Test conflict detection and visualization (D012)."""

    def test_concurrent_versions_detected_as_conflict(self, client, node):
        """When versions are concurrent, GET should return conflict."""
        # Create version 1
        client.put("/kvstore/count", json={"value": 1})

        # Manually create concurrent version (simulate network partition with different node)
        v2_meta = VersionMetadata(
            vector_clock=VectorClock({"node-2": 1}),  # Concurrent to node-1: 1
            author="node-2",
            timestamp="2024-01-01T12:00:00Z",
        )
        v2 = Version(key="count", value=2, metadata=v2_meta)
        node.storage.put("count", v2)

        # Now GET should show conflict
        response = client.get("/kvstore/count")
        assert response.status_code == 200
        data = response.get_json()
        assert data["conflict"] is True
        assert data["sibling_count"] == 2
        assert len(data["siblings"]) == 2

    def test_conflict_response_includes_all_siblings(self, client, node):
        """Conflict response should list all sibling versions."""
        client.put("/kvstore/item", json={"value": {"v": "a"}})
        v2_meta = VersionMetadata(
            vector_clock=VectorClock({"node-2": 1}),  # Concurrent to node-1: 1
            author="node-2",
            timestamp="2024-01-01T12:00:00Z",
        )
        v2 = Version(key="item", value={"v": "b"}, metadata=v2_meta)
        node.storage.put("item", v2)

        response = client.get("/kvstore/item")
        data = response.get_json()
        assert len(data["siblings"]) == 2
        values = [s["value"]["v"] for s in data["siblings"]]
        assert "a" in values
        assert "b" in values

    def test_conflicts_endpoint_lists_concurrent_keys(self, client, node):
        """GET /conflicts should list all conflicting keys."""
        # Create version 1
        client.put("/kvstore/key-a", json={"value": 1})

        # Create concurrent version with different clock (simulates node-2 write)
        meta_a2 = VersionMetadata(
            vector_clock=VectorClock({"node-2": 1}),  # Concurrent to node-1: 1
            author="node-2",
            timestamp="2024-01-01T12:00:00Z",
        )
        v_a2 = Version(key="key-a", value=2, metadata=meta_a2)
        node.storage.put("key-a", v_a2)

        # Create another conflicting key
        client.put("/kvstore/key-b", json={"value": "x"})

        # Create concurrent version for key-b
        meta_b2 = VersionMetadata(
            vector_clock=VectorClock({"node-3": 1}),  # Concurrent to node-1: 2
            author="node-3",
            timestamp="2024-01-01T12:00:00Z",
        )
        v_b2 = Version(key="key-b", value="y", metadata=meta_b2)
        node.storage.put("key-b", v_b2)

        response = client.get("/conflicts")
        assert response.status_code == 200
        data = response.get_json()
        assert data["conflict_count"] == 2
        assert len(data["conflicts"]) == 2

    def test_conflicts_endpoint_empty_when_no_conflicts(self, client):
        """GET /conflicts should return empty list when no conflicts."""
        client.put("/kvstore/x", json={"value": 1})
        client.put("/kvstore/y", json={"value": 2})

        response = client.get("/conflicts")
        assert response.status_code == 200
        data = response.get_json()
        assert data["conflict_count"] == 0
        assert len(data["conflicts"]) == 0


class TestNodeInspection:
    """Test node state and inspection endpoints."""

    def test_node_state_endpoint(self, client, node):
        """GET /node/state should return node's complete state."""
        client.put("/kvstore/x", json={"value": 1})
        response = client.get("/node/state")
        assert response.status_code == 200
        data = response.get_json()
        assert "vector_clock" in data
        assert "node_id" in data
        assert data["node_id"] == "node-1"

    def test_node_clock_endpoint(self, client, node):
        """GET /node/clock should return current vector clock."""
        client.put("/kvstore/x", json={"value": 1})
        response = client.get("/node/clock")
        assert response.status_code == 200
        data = response.get_json()
        assert "clock" in data
        assert "node_id" in data
        assert isinstance(data["clock"], dict)
        assert "node-1" in data["clock"]

    def test_node_info_endpoint(self, client):
        """GET /node/info should return node configuration."""
        response = client.get("/node/info")
        assert response.status_code == 200
        data = response.get_json()
        assert data["node_id"] == "node-1"
        quorum = data["quorum"]
        assert quorum["N"] == 3
        assert quorum["R"] == 1
        assert quorum["W"] == 1
        assert data["consistency_model"] == "eventual"


class TestQuorumOperations:
    """Test quorum enforcement (D011)."""

    def test_write_quorum_w1_immediately_satisfied(self, client):
        """W=1 writes should be immediately satisfied."""
        response = client.put("/kvstore/key", json={"value": {"x": 1}})
        data = response.get_json()
        assert data["quorum"]["W"] == 1
        assert data["quorum"]["satisfied"] is True

    def test_read_quorum_includes_r_parameter(self, client):
        """GET should include R (read quorum) in response."""
        client.put("/kvstore/key", json={"value": {"x": 1}})
        response = client.get("/kvstore/key")
        data = response.get_json()
        assert "quorum" in data
        assert data["quorum"]["R"] == 1

    def test_multiple_writes_increment_clock(self, client, node):
        """Successive PUT requests should advance clock."""
        response1 = client.put("/kvstore/counter", json={"value": 1})
        clock1 = response1.get_json()["clock"]["node-1"]

        response2 = client.put("/kvstore/counter", json={"value": 2})
        clock2 = response2.get_json()["clock"]["node-1"]

        assert clock2 > clock1

    def test_quorum_w2_requires_coordination(self, app):
        """W>1 should require multi-node coordination."""
        from repliqia.core import Node
        from repliqia.storage import JSONBackend

        backend = JSONBackend()
        node = Node(node_id="node-1", storage=backend, N=3, R=1, W=2)
        app = create_app(node)
        client = app.test_client()

        response = client.put("/kvstore/key", json={"value": {"x": 1}})
        # W=2 not satisfied yet (only 1 ack from self)
        data = response.get_json()
        assert data["quorum"]["W"] == 2
        # Status code 202 (Accepted, pending quorum)
        assert response.status_code == 202


class TestSyncOperations:
    """Test bidirectional sync endpoints."""

    def test_sync_endpoint_accepts_post(self, client):
        """POST /sync/<peer> should accept sync requests."""
        response = client.post("/sync/node-2", json={"versions": []})
        assert response.status_code == 200
        data = response.get_json()
        assert data["peer"] == "node-2"
        assert data["synced"] is True

    def test_sync_endpoint_tracks_exchanges(self, client):
        """Sync response should track exchanged versions."""
        response = client.post(
            "/sync/node-2",
            json={"versions": [{"key": "x", "value": 1}, {"key": "y", "value": 2}]},
        )
        data = response.get_json()
        assert data["versions_exchanged"] == 2


class TestHealthAndErrors:
    """Test health checks and error handling."""

    def test_health_endpoint_returns_ok(self, client):
        """GET /health should return 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "healthy"

    def test_health_includes_node_id(self, client):
        """Health response should include node ID."""
        response = client.get("/health")
        data = response.get_json()
        assert data["node"] == "node-1"

    def test_nonexistent_endpoint_returns_404(self, client):
        """Request to unknown endpoint should return 404."""
        response = client.get("/unknown/path")
        assert response.status_code == 404
        data = response.get_json()
        assert "error" in data


class TestIntegration:
    """Integration tests combining multiple operations."""

    def test_write_then_read_returns_same_value(self, client):
        """Write followed by read should return same value."""
        put_response = client.put(
            "/kvstore/name", json={"value": {"first": "Carol", "last": "Smith"}}
        )
        put_data = put_response.get_json()

        get_response = client.get("/kvstore/name")
        get_data = get_response.get_json()

        assert put_data["value"] == get_data["value"]

    def test_multiple_keys_stored_independently(self, client):
        """Multiple keys should be stored independently."""
        client.put("/kvstore/user:1", json={"value": {"name": "Alice"}})
        client.put("/kvstore/user:2", json={"value": {"name": "Bob"}})

        response1 = client.get("/kvstore/user:1")
        response2 = client.get("/kvstore/user:2")

        assert response1.get_json()["value"]["name"] == "Alice"
        assert response2.get_json()["value"]["name"] == "Bob"

    def test_clock_consistency_across_operations(self, client, node):
        """Vector clock should advance consistently."""
        client.put("/kvstore/a", json={"value": 1})
        clock_after_1 = VectorClock(node.get_clock().to_dict())

        client.put("/kvstore/b", json={"value": 2})
        clock_after_2 = VectorClock(node.get_clock().to_dict())

        # Second clock should be after first
        assert (
            clock_after_2.compare(clock_after_1) == "after"
            or clock_after_2.compare(clock_after_1) == "equal"
        )

    def test_full_workflow_write_sync_read(self, client, app, node):
        """Full workflow: write locally, sync, read should work."""
        # Write locally
        client.put("/kvstore/item:1", json={"value": {"qty": 100}})

        # Check health
        health = client.get("/health")
        assert health.status_code == 200

        # Sync with peer (simulated)
        sync_response = client.post("/sync/node-2", json={"versions": []})
        assert sync_response.status_code == 200

        # Read back
        get_response = client.get("/kvstore/item:1")
        assert get_response.status_code == 200
        assert get_response.get_json()["value"]["qty"] == 100

    def test_put_get_delete_lifecycle(self, client):
        """Standard key lifecycle: create, read, delete."""
        # Create
        put_response = client.put("/kvstore/temp", json={"value": "data"})
        assert put_response.status_code == 201

        # Read
        get_response = client.get("/kvstore/temp")
        assert get_response.status_code == 200
        assert get_response.get_json()["value"] == "data"

        # Delete
        delete_response = client.delete("/kvstore/temp")
        assert delete_response.status_code == 204

        # Verify gone
        get_response = client.get("/kvstore/temp")
        assert get_response.status_code == 404


class TestResponseFormats:
    """Test response format consistency."""

    def test_put_response_json_structure(self, client):
        """PUT response should have specific JSON structure."""
        response = client.put("/kvstore/key", json={"value": {"x": 1}})
        data = response.get_json()
        assert "key" in data
        assert "value" in data
        assert "clock" in data
        assert "author" in data
        assert "quorum" in data

    def test_get_response_json_structure(self, client):
        """GET response should have specific JSON structure."""
        client.put("/kvstore/key", json={"value": {"x": 1}})
        response = client.get("/kvstore/key")
        data = response.get_json()
        assert "key" in data
        assert "value" in data
        assert "clock" in data
        assert "author" in data
        assert "conflict" in data
        assert "quorum" in data

    def test_conflict_response_json_structure(self, client, node):
        """Conflict response should have all sibling details."""
        client.put("/kvstore/key", json={"value": 1})
        meta = VersionMetadata(
            vector_clock=VectorClock({"node-2": 1}),  # Concurrent to node-1: 1
            author="node-2",
            timestamp="2024-01-01T12:00:00Z",
        )
        v2 = Version(key="key", value=2, metadata=meta)
        node.storage.put("key", v2)

        response = client.get("/kvstore/key")
        data = response.get_json()
        assert data["conflict"] is True
        assert "siblings" in data
        for sibling in data["siblings"]:
            assert "value" in sibling
            assert "clock" in sibling
            assert "author" in sibling
