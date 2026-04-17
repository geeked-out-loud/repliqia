"""REST API server for Repliqia nodes."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from flask import Flask, jsonify, request

from repliqia.clock import VectorClock
from repliqia.core import Node
from repliqia.replication import ConflictView
from repliqia.storage import SQLiteBackend, Version, VersionMetadata


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
    quorum_acks: Dict[str, QuorumAck] = {}  # Track pending quorums
    app.quorum_acks = quorum_acks

    def _version_identity(version: Version) -> str:
        """Stable identity for deduplication by vector clock."""
        return json.dumps(version.metadata.vector_clock.to_dict(), sort_keys=True)

    def _serialize_version(version: Version) -> dict[str, Any]:
        """Serialize a version for peer transport."""
        return version.to_dict()

    def _serialize_versions(versions: Iterable[Version]) -> list[dict[str, Any]]:
        """Serialize version list for JSON transport."""
        return [_serialize_version(version) for version in versions]

    def _deserialize_version(payload: dict[str, Any]) -> Optional[Version]:
        """Deserialize version payload received from a peer."""
        try:
            metadata = payload["metadata"]
            return Version(
                key=payload["key"],
                value=payload["value"],
                metadata=VersionMetadata(
                    vector_clock=VectorClock.from_dict(metadata["vector_clock"]),
                    author=metadata["author"],
                    timestamp=metadata.get("timestamp", 0.0),
                ),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _collect_local_versions(
        key: Optional[str] = None, keys: Optional[Iterable[str]] = None
    ) -> list[Version]:
        """Collect local versions, optionally filtered by key(s)."""
        if key is not None:
            return node.get(key)

        collected: list[Version] = []
        if keys is None:
            source_keys = node.storage.keys()
        else:
            source_keys = sorted(set(keys))

        for current_key in source_keys:
            collected.extend(node.get(current_key))
        return collected

    def _merge_versions(incoming_versions: list[Version]) -> int:
        """Merge incoming versions grouped by key and return new-version count."""
        if not incoming_versions:
            return 0

        grouped: dict[str, list[Version]] = defaultdict(list)
        for version in incoming_versions:
            grouped[version.key].append(version)

        new_versions = 0
        for incoming_key, versions_for_key in grouped.items():
            before = {_version_identity(version) for version in node.get(incoming_key)}
            node.merge(versions_for_key)
            after = {_version_identity(version) for version in node.get(incoming_key)}
            new_versions += len(after - before)

        return new_versions

    def _has_concurrent_versions(versions: list[Version]) -> bool:
        """Return True when at least one pair is concurrent."""
        for i, first in enumerate(versions):
            for second in versions[i + 1 :]:
                if first.metadata.vector_clock.compare(second.metadata.vector_clock) == "concurrent":
                    return True
        return False

    def _pick_latest_non_conflicting(versions: list[Version]) -> Version:
        """Pick a latest version from an ordered set (no concurrency)."""
        for candidate in versions:
            if all(
                other.metadata.vector_clock.compare(candidate.metadata.vector_clock)
                in {"before", "equal"}
                for other in versions
            ):
                return candidate
        return versions[0]

    def _collect_conflicts(keys: Optional[Iterable[str]] = None) -> list[ConflictView]:
        """Collect conflicts for all keys or a filtered subset."""
        if keys is None:
            keys_to_check = node.storage.keys()
        else:
            keys_to_check = sorted(set(keys))

        conflicts: list[ConflictView] = []
        for current_key in keys_to_check:
            versions = node.storage.get(current_key)
            if len(versions) > 1 and _has_concurrent_versions(versions):
                conflicts.append(ConflictView(key=current_key, versions=versions))
        return conflicts

    def _replicate_for_write(key: str, versions: list[Version]) -> tuple[int, list[dict[str, Any]]]:
        """Replicate new write to peers until W acknowledgements are reached."""
        acks = 1  # local write acknowledgement
        peer_results: list[dict[str, Any]] = []

        if node.W <= 1:
            return acks, peer_results

        payload = {
            "origin_node_id": node.node_id,
            "key": key,
            "versions": _serialize_versions(versions),
            "return_versions": False,
        }

        for peer_id, peer_url in app.peer_nodes.items():
            if acks >= node.W:
                break

            endpoint = f"{peer_url.rstrip('/')}/sync/{node.node_id}"
            try:
                response = requests.post(endpoint, json=payload, timeout=3.0)
                ok = 200 <= response.status_code < 300
                result: dict[str, Any] = {
                    "peer": peer_id,
                    "ok": ok,
                    "status": response.status_code,
                }
                if ok:
                    acks += 1
                else:
                    result["error"] = response.text[:200]
            except requests.RequestException as exc:
                result = {
                    "peer": peer_id,
                    "ok": False,
                    "error": str(exc),
                }

            peer_results.append(result)

        return acks, peer_results

    def _read_with_quorum(key: str) -> tuple[list[Version], int, list[dict[str, Any]]]:
        """Read key locally + peers until R responses, then run read-repair."""
        versions_by_clock: dict[str, Version] = {
            _version_identity(version): version for version in node.get(key)
        }
        read_acks = 1  # local read attempt
        peer_results: list[dict[str, Any]] = []

        if node.R > 1:
            for peer_id, peer_url in app.peer_nodes.items():
                if read_acks >= node.R:
                    break

                endpoint = f"{peer_url.rstrip('/')}/internal/versions"
                try:
                    response = requests.get(endpoint, params={"key": key}, timeout=3.0)
                    ok = response.status_code == 200
                    peer_result: dict[str, Any] = {
                        "peer": peer_id,
                        "ok": ok,
                        "status": response.status_code,
                    }

                    if ok:
                        payload = response.json() if response.content else {}
                        incoming_payload = payload.get("versions", [])
                        valid_count = 0
                        invalid_count = 0

                        for raw_version in incoming_payload:
                            incoming_version = _deserialize_version(raw_version)
                            if incoming_version is None:
                                invalid_count += 1
                                continue

                            valid_count += 1
                            versions_by_clock.setdefault(
                                _version_identity(incoming_version), incoming_version
                            )

                        peer_result["versions_returned"] = valid_count
                        if invalid_count:
                            peer_result["invalid_versions"] = invalid_count

                        read_acks += 1
                    else:
                        peer_result["error"] = response.text[:200]
                except requests.RequestException as exc:
                    peer_result = {
                        "peer": peer_id,
                        "ok": False,
                        "error": str(exc),
                    }

                peer_results.append(peer_result)

        if versions_by_clock:
            _merge_versions(list(versions_by_clock.values()))

        return node.get(key), read_acks, peer_results

    def _orchestrate_sync(peer_node_id: str, key: Optional[str]) -> tuple[dict, int]:
        """Coordinator mode: push local versions to peer and pull peer versions back."""
        peer_url = app.peer_nodes.get(peer_node_id)
        if not peer_url:
            # Backward-compatible no-op for manual calls in single-node mode.
            return {
                "peer": peer_node_id,
                "synced": True,
                "mode": "noop",
                "versions_exchanged": 0,
                "warning": "Peer URL is not configured",
            }, 200

        outbound_versions = _collect_local_versions(key=key)
        payload: dict[str, Any] = {
            "origin_node_id": node.node_id,
            "versions": _serialize_versions(outbound_versions),
            "return_versions": True,
        }
        if key is not None:
            payload["key"] = key

        endpoint = f"{peer_url.rstrip('/')}/sync/{node.node_id}"
        try:
            peer_response = requests.post(endpoint, json=payload, timeout=5.0)
        except requests.RequestException as exc:
            return {
                "peer": peer_node_id,
                "synced": False,
                "mode": "coordinator",
                "error": str(exc),
            }, 502

        if not (200 <= peer_response.status_code < 300):
            return {
                "peer": peer_node_id,
                "synced": False,
                "mode": "coordinator",
                "status": peer_response.status_code,
                "error": peer_response.text[:300],
            }, 502

        peer_payload = peer_response.json() if peer_response.content else {}
        inbound_payload = peer_payload.get("versions", [])
        inbound_versions: list[Version] = []
        invalid_count = 0

        for raw_version in inbound_payload:
            incoming_version = _deserialize_version(raw_version)
            if incoming_version is None:
                invalid_count += 1
                continue
            inbound_versions.append(incoming_version)

        merged_versions = _merge_versions(inbound_versions)
        involved_keys = {version.key for version in outbound_versions}
        involved_keys.update(version.key for version in inbound_versions)
        conflicts = _collect_conflicts(involved_keys if involved_keys else None)

        return {
            "peer": peer_node_id,
            "synced": True,
            "mode": "coordinator",
            "pushed_versions": len(outbound_versions),
            "pulled_versions": len(inbound_versions),
            "merged_versions": merged_versions,
            "invalid_versions": invalid_count,
            "versions_exchanged": len(outbound_versions) + len(inbound_versions),
            "conflicts": [conflict.to_dict() for conflict in conflicts],
        }, 200

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

        response: dict[str, Any] = {
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

        acks, peer_results = _replicate_for_write(key=key, versions=[version])
        quorum_ack = QuorumAck(
            key=key,
            version_clock=version.metadata.vector_clock.to_dict(),
            acks=acks,
            required=node.W,
        )

        response["quorum"]["acks"] = acks
        response["quorum"]["satisfied"] = quorum_ack.is_satisfied()
        response["quorum"]["peer_results"] = peer_results
        
        # Include node state snapshot for frontend
        response["node_state"] = node.get_state()
        
        # Include node state snapshot for frontend
        response["node_state"] = node.get_state()

        status_code = 201 if quorum_ack.is_satisfied() else 202
        return jsonify(response), status_code

    @app.route("/kvstore/<key>", methods=["GET"])
    def get_key(key: str) -> tuple[dict, int]:
        """Read a value with quorum enforcement.
        
        Quorum: R parameter determines consistency level.
        If R=1, read from any replica (may be stale).
        Higher R requires reading from multiple nodes.
        """
        versions, read_acks, peer_results = _read_with_quorum(key)
        quorum_info = {
            "R": node.R,
            "required": node.R,
            "acks": read_acks,
            "satisfied": read_acks >= node.R,
            "peer_results": peer_results,
        }
        
        if not versions:
            return jsonify({
                "error": f"Key '{key}' not found",
                "quorum": quorum_info,
                "node_state": node.get_state()
            }), 404

        has_conflict = len(versions) > 1 and _has_concurrent_versions(versions)

        if not has_conflict:
            # Return latest single value when versions are causally ordered.
            v = versions[0] if len(versions) == 1 else _pick_latest_non_conflicting(versions)
            return (
                jsonify(
                    {
                        "key": key,
                        "value": v.value,
                        "clock": v.metadata.vector_clock.to_dict(),
                        "author": v.metadata.author,
                        "conflict": False,
                        "node_state": node.get_state(),
                        "quorum": {
                            **quorum_info,
                            "consistency": "strong"
                            if quorum_info["satisfied"]
                            else "eventual",
                        },
                    }
                ),
                200,
            )
        else:
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
                        "node_state": node.get_state(),
                        "quorum": {
                            **quorum_info,
                            "consistency": "eventual",
                        },
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

    @app.route("/internal/versions", methods=["GET"])
    def internal_versions() -> tuple[dict, int]:
        """Internal endpoint for peer reads and anti-entropy sync."""
        key = request.args.get("key")
        versions = _collect_local_versions(key=key)
        return (
            jsonify(
                {
                    "node_id": node.node_id,
                    "key": key,
                    "count": len(versions),
                    "versions": _serialize_versions(versions),
                }
            ),
            200,
        )

    @app.route("/sync/<peer_node_id>", methods=["POST"])
    def sync_peer(peer_node_id: str) -> tuple[dict, int]:
        """Sync with a peer node.
        
        Two modes:
        - Inbound mode: peer sends concrete versions to merge.
        - Coordinator mode: if no versions are provided, this node pushes local
          versions to configured peer and pulls peer versions back.
        """
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}

        key = payload.get("key") or request.args.get("key")
        incoming_payload = payload.get("versions", [])
        if not isinstance(incoming_payload, list):
            incoming_payload = []

        return_versions = bool(payload.get("return_versions", False))

        if incoming_payload:
            incoming_versions: list[Version] = []
            invalid_versions = 0
            for raw_version in incoming_payload:
                if not isinstance(raw_version, dict):
                    invalid_versions += 1
                    continue

                deserialized = _deserialize_version(raw_version)
                if deserialized is None:
                    invalid_versions += 1
                    continue

                incoming_versions.append(deserialized)

            touched_keys = {version.key for version in incoming_versions}
            merged_versions = _merge_versions(incoming_versions)
            response: dict[str, Any] = {
                "peer": peer_node_id,
                "synced": True,
                "mode": "inbound",
                "received_versions": len(incoming_payload),
                "merged_versions": merged_versions,
                "invalid_versions": invalid_versions,
                # Keep this field backward-compatible for existing clients/tests.
                "versions_exchanged": len(incoming_payload),
            }

            if return_versions:
                local_versions = _collect_local_versions(
                    key=key,
                    keys=touched_keys if key is None else None,
                )
                response["versions"] = _serialize_versions(local_versions)

            conflicts = _collect_conflicts(keys=touched_keys if touched_keys else None)
            if conflicts:
                response["conflicts"] = [conflict.to_dict() for conflict in conflicts]

            return jsonify(response), 200

        orchestrated_response, status_code = _orchestrate_sync(peer_node_id=peer_node_id, key=key)
        return jsonify(orchestrated_response), status_code

    @app.route("/conflicts", methods=["GET"])
    def list_conflicts() -> tuple[dict, int]:
        """Show all current conflicts (visualization D012)."""
        conflicts = _collect_conflicts()

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


def _parse_peers(raw_peers: str) -> dict[str, str]:
    """Parse peers from CSV format: NODE=http://host:port."""
    peers: dict[str, str] = {}
    if not raw_peers:
        return peers

    for item in raw_peers.split(","):
        token = item.strip()
        if not token or "=" not in token:
            continue

        node_id, url = token.split("=", 1)
        node_id = node_id.strip().upper()
        url = url.strip().rstrip("/")
        if node_id and url:
            peers[node_id] = url

    return peers


def main() -> None:
    """CLI entrypoint for running a node API server process."""
    parser = argparse.ArgumentParser(description="Run a Repliqia node API server")
    parser.add_argument("--node", required=True, help="Node ID, e.g. A")
    parser.add_argument("--port", type=int, required=True, help="Port for the API server")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    # Storage is always SQLite (JSON backend removed)
    parser.add_argument("--db-dir", type=str, help="Directory for database files (optional)")
    parser.add_argument("--n", type=int, default=3, help="Replication factor N")
    parser.add_argument("--r", type=int, default=2, help="Read quorum R")
    parser.add_argument("--w", type=int, default=2, help="Write quorum W")
    parser.add_argument(
        "--peers",
        default="",
        help="Optional peers in CSV format: B=http://localhost:5002,C=http://localhost:5003",
    )
    args = parser.parse_args()

    node_id = args.node.upper()
    
    # Determine database path
    if args.db_dir:
        db_dir = Path(args.db_dir)
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"repliqia_{node_id}.db"
    else:
        db_path = Path(f"repliqia_{node_id}.db")
    
    backend = SQLiteBackend(str(db_path))

    node = Node(node_id=node_id, storage=backend, N=args.n, R=args.r, W=args.w)
    app = create_app(node, peer_nodes=_parse_peers(args.peers))
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
