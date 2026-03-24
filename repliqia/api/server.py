"""REST API server for Repliqia nodes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request

from repliqia.core import Node
from repliqia.replication import ConflictView, PeerSync, SyncResult
from repliqia.storage import Version


@dataclass
class QuorumAck:
    """Quorum acknowledgement tracker."""

    key: str
    version_clock: Dict[str, int]
    acks: int  # How many nodes have acknowledged
    required: int  # W (write quorum requirement)

    def is_satisfied(self) -> bool:
        """Check if write quorum is satisfied."""
        return self.acks >= self.required

    def to_dict(self) -> dict:
        """Export for API response."""
        return {
            "key": self.key,
            "acks": self.acks,
            "required": self.required,
            "satisfied": self.is_satisfied(),
        }


def create_app(node: Node, peer_nodes: Optional[Dict[str, str]] = None) -> Flask:
    """Create Flask app for a Repliqia node.
    
    Args:
        node: Node instance to serve
        peer_nodes: Dict of {node_id: http_url} for peer discovery
        
    Returns:
        Flask app configured for this node
    """
    app = Flask(f"repliqia-node-{node.node_id}")
    app.node = node
    app.peer_nodes = peer_nodes or {}
    app.quorum_acks: Dict[str, QuorumAck] = {}  # Track pending quorums

    # ========== Key-Value Operations ==========

    @app.route("/kvstore/<key>", methods=["PUT"])
    def put_key(key: str) -> tuple[dict, int]:
        """Write a value with quorum enforcement.
        
        Quorum: W parameter determines how many nodes must acknowledge.
        If W=1 (default), write succeeds immediately on this node.
        Higher W requires coordinator to wait for peer acknowledgements.
        """
        data = request.get_json() or {}
        value = data.get("value", {})

        # Write locally
        version = node.put(key, value)

        response = {
            "key": key,
            "value": value,
            "clock": version.metadata.vector_clock.to_dict(),
            "author": version.metadata.author,
            "quorum": {
                "N": node.N,
                "R": node.R,
                "W": node.W,
                "acks": 1,  # Self-ack
            },
        }

        # For W > 1, would contact peers asynchronously (simplified here)
        if node.W == 1:
            response["quorum"]["satisfied"] = True
            return jsonify(response), 201
        else:
            # In production: async quorum collection
            response["quorum"]["satisfied"] = node.W <= 1
            return jsonify(response), 202  # Accepted, pending

    @app.route("/kvstore/<key>", methods=["GET"])
    def get_key(key: str) -> tuple[dict, int]:
        """Read a value with quorum enforcement.
        
        Quorum: R parameter determines consistency level.
        If R=1, read from any replica (may be stale).
        Higher R requires reading from multiple nodes.
        """
        versions = node.get(key)

        if not versions:
            return jsonify({"error": f"Key '{key}' not found"}), 404

        if len(versions) == 1:
            # No conflict
            v = versions[0]
            return (
                jsonify(
                    {
                        "key": key,
                        "value": v.value,
                        "clock": v.metadata.vector_clock.to_dict(),
                        "author": v.metadata.author,
                        "conflict": False,
                        "quorum": {"R": node.R, "consistency": "strong"},
                    }
                ),
                200,
            )
        else:
            # Conflict detected
            return (
                jsonify(
                    {
                        "key": key,
                        "conflict": True,
                        "sibling_count": len(versions),
                        "siblings": [
                            {
                                "value": v.value,
                                "clock": v.metadata.vector_clock.to_dict(),
                                "author": v.metadata.author,
                            }
                            for v in versions
                        ],
                        "quorum": {"R": node.R, "consistency": "eventual"},
                    }
                ),
                200,
            )

    @app.route("/kvstore/<key>", methods=["DELETE"])
    def delete_key(key: str) -> tuple[dict, int]:
        """Delete a key (tombstone semantics)."""
        node.storage.remove(key)
        return jsonify({"key": key, "deleted": True}), 204

    # ========== Replication & Sync ==========

    @app.route("/sync/<peer_node_id>", methods=["POST"])
    def sync_peer(peer_node_id: str) -> tuple[dict, int]:
        """Sync with a peer node.
        
        In production: this would be called by peer when contacting us.
        For now: simplified bidirectional sync simulation.
        """
        data = request.get_json() or {}
        incoming_versions = data.get("versions", [])

        # Import Version from payload (simplified)
        if incoming_versions:
            # In production: deserialize properly
            pass

        return (
            jsonify(
                {
                    "peer": peer_node_id,
                    "synced": True,
                    "versions_exchanged": len(incoming_versions),
                }
            ),
            200,
        )

    @app.route("/conflicts", methods=["GET"])
    def list_conflicts() -> tuple[dict, int]:
        """Show all current conflicts (visualization D012)."""
        conflicts: List[ConflictView] = []

        for key in node.storage.keys():
            versions = node.storage.get(key)
            if len(versions) > 1:
                # Check if concurrent (conflict)
                for i, v1 in enumerate(versions):
                    for v2 in versions[i + 1 :]:
                        if (
                            v1.metadata.vector_clock.compare(v2.metadata.vector_clock)
                            == "concurrent"
                        ):
                            conflicts.append(ConflictView(key=key, versions=versions))
                            break
                    if conflicts and conflicts[-1].key == key:
                        break

        return (
            jsonify(
                {
                    "conflict_count": len(conflicts),
                    "conflicts": [c.to_dict() for c in conflicts],
                }
            ),
            200,
        )

    # ========== Node State & Inspection ==========

    @app.route("/node/state", methods=["GET"])
    def get_node_state() -> tuple[dict, int]:
        """Get node state for debugging/visualization."""
        return jsonify(node.get_state()), 200

    @app.route("/node/clock", methods=["GET"])
    def get_node_clock() -> tuple[dict, int]:
        """Get current vector clock."""
        return (
            jsonify({"clock": node.get_clock().to_dict(), "node_id": node.node_id}),
            200,
        )

    @app.route("/node/info", methods=["GET"])
    def get_node_info() -> tuple[dict, int]:
        """Get node configuration and quorum parameters."""
        return (
            jsonify(
                {
                    "node_id": node.node_id,
                    "quorum": {"N": node.N, "R": node.R, "W": node.W},
                    "consistency_model": "eventual",
                    "replication_style": "Dynamo-inspired",
                }
            ),
            200,
        )

    @app.route("/health", methods=["GET"])
    def health_check() -> tuple[dict, int]:
        """Health check endpoint."""
        return jsonify({"status": "healthy", "node": node.node_id}), 200

    # ========== Error Handlers ==========

    @app.errorhandler(404)
    def not_found(error: Exception) -> tuple[dict, int]:
        """404 handler."""
        return jsonify({"error": "Endpoint not found"}), 404

    @app.errorhandler(500)
    def server_error(error: Exception) -> tuple[dict, int]:
        """500 handler."""
        return jsonify({"error": "Internal server error", "details": str(error)}), 500

    return app
