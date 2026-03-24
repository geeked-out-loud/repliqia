"""Replication engine for Repliqia - handles peer synchronization and conflict detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from repliqia.clock import VectorClock
from repliqia.core import Node
from repliqia.storage import Version


@dataclass
class ConflictView:
    """Represents a conflict for visualization and debugging.
    
    Shows all concurrent versions of a key along with their vector clocks.
    Used by CLI and UI to display conflicts to users.
    """

    key: str
    versions: List[Version]
    vector_clocks: List[Dict[str, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Extract vector clocks from versions."""
        if not self.vector_clocks and self.versions:
            self.vector_clocks = [
                v.metadata.vector_clock.to_dict() for v in self.versions
            ]

    def to_dict(self) -> dict:
        """Export for JSON serialization (for CLI/UI)."""
        return {
            "key": self.key,
            "conflict_count": len(self.versions),
            "versions": [
                {
                    "value": v.value,
                    "author": v.metadata.author,
                    "clock": v.metadata.vector_clock.to_dict(),
                    "timestamp": v.metadata.timestamp,
                }
                for v in self.versions
            ],
        }


@dataclass
class SyncResult:
    """Result of a sync operation between two nodes."""

    source_node_id: str
    dest_node_id: str
    keys_synced: int
    new_versions_exchanged: int
    conflicts_detected: List[ConflictView] = field(default_factory=list)
    already_in_sync: bool = False

    def to_dict(self) -> dict:
        """Export for reporting/visualization."""
        return {
            "source": self.source_node_id,
            "destination": self.dest_node_id,
            "keys_synced": self.keys_synced,
            "new_versions": self.new_versions_exchanged,
            "conflicts": [c.to_dict() for c in self.conflicts_detected],
            "in_sync": self.already_in_sync,
        }


class PeerSync:
    """Orchestrates synchronization between two nodes.
    
    Handles:
    - Metadata exchange (which versions each node has)
    - Conflict detection via vector clocks
    - Merging replicas between nodes
    - Conflict visualization for diagnosis
    
    Real-world: Implements anti-entropy repair (background sync process in Dynamo).
    """

    def __init__(self, node_a: Node, node_b: Node) -> None:
        """Initialize sync engine for two nodes.
        
        Args:
            node_a: First node
            node_b: Second node
        """
        self.node_a = node_a
        self.node_b = node_b

    # ========== Synchronization ==========

    def sync(self, key: Optional[str] = None) -> SyncResult:
        """Sync nodes, optionally for a specific key.
        
        Bidirectional: A→B then B→A to ensure both see all versions.
        Returns conflict information for visualization.
        
        Args:
            key: If provided, sync only this key. Otherwise sync all keys.
            
        Returns:
            SyncResult with conflict details
        """
        # Determine keys to sync
        if key:
            keys_to_sync = [key]
        else:
            # Sync all keys from both nodes
            keys_to_sync = list(
                set(self.node_a.storage.keys() + self.node_b.storage.keys())
            )

        new_versions_count = 0
        conflicts = []

        for key in keys_to_sync:
            # Get versions from both nodes
            versions_a = self.node_a.storage.get(key)
            versions_b = self.node_b.storage.get(key)

            # A → B: sync A's versions to B
            for version in versions_a:
                if not any(
                    v.metadata.vector_clock.to_dict()
                    == version.metadata.vector_clock.to_dict()
                    for v in versions_b
                ):
                    new_versions_count += 1
                    self.node_b.merge([version])

            # B → A: sync B's versions to A
            for version in versions_b:
                if not any(
                    v.metadata.vector_clock.to_dict()
                    == version.metadata.vector_clock.to_dict()
                    for v in versions_a
                ):
                    new_versions_count += 1
                    self.node_a.merge([version])

            # Detect conflicts after sync
            final_versions = self.node_a.storage.get(key)
            if len(final_versions) > 1:
                # Check if concurrent (conflict)
                if self._has_concurrent_versions(final_versions):
                    conflicts.append(ConflictView(key=key, versions=final_versions))

        already_in_sync = new_versions_count == 0

        return SyncResult(
            source_node_id=self.node_a.node_id,
            dest_node_id=self.node_b.node_id,
            keys_synced=len(keys_to_sync),
            new_versions_exchanged=new_versions_count,
            conflicts_detected=conflicts,
            already_in_sync=already_in_sync,
        )

    # ========== Conflict Detection & Visualization ==========

    def get_conflicts(self) -> List[ConflictView]:
        """Find all current conflicts across both nodes.
        
        Returns:
            List of ConflictView objects for each conflicted key
        """
        conflicts = []
        all_keys = set(self.node_a.storage.keys() + self.node_b.storage.keys())

        for key in all_keys:
            all_versions = self.node_a.storage.get(key)
            if len(all_versions) > 1 and self._has_concurrent_versions(all_versions):
                conflicts.append(ConflictView(key=key, versions=all_versions))

        return conflicts

    def get_divergence(self) -> dict:
        """Measure how far nodes have diverged.
        
        Returns statistics on differences between A and B.
        """
        keys_a = set(self.node_a.storage.keys())
        keys_b = set(self.node_b.storage.keys())
        all_keys = keys_a | keys_b

        only_in_a = keys_a - keys_b
        only_in_b = keys_b - keys_a
        in_both = keys_a & keys_b

        # Count version differences
        import json
        version_diffs = 0
        for key in in_both:
            versions_a = self.node_a.storage.get(key)
            versions_b = self.node_b.storage.get(key)

            # Count versions that differ via clock (use JSON strings as hashable)
            clocks_a = {
                json.dumps(v.metadata.vector_clock.to_dict(), sort_keys=True)
                for v in versions_a
            }
            clocks_b = {
                json.dumps(v.metadata.vector_clock.to_dict(), sort_keys=True)
                for v in versions_b
            }

            version_diffs += len(clocks_a ^ clocks_b)  # Symmetric difference

        return {
            "total_keys": len(all_keys),
            "keys_only_in_a": len(only_in_a),
            "keys_only_in_b": len(only_in_b),
            "keys_in_both": len(in_both),
            "version_differences": version_diffs,
            "conflict_count": len(self.get_conflicts()),
        }

    def visualize_state(self) -> dict:
        """Generate detailed state for visualization/debugging.
        
        Returns complete picture of both nodes and conflicts.
        """
        return {
            "node_a": self.node_a.get_state(),
            "node_b": self.node_b.get_state(),
            "divergence": self.get_divergence(),
            "conflicts": [c.to_dict() for c in self.get_conflicts()],
        }

    # ========== Helpers ==========

    @staticmethod
    def _has_concurrent_versions(versions: List[Version]) -> bool:
        """Check if any pair of versions is concurrent.
        
        Args:
            versions: List of versions to check
            
        Returns:
            True if at least one pair is concurrent (conflict exists)
        """
        for i, v1 in enumerate(versions):
            for v2 in versions[i + 1 :]:
                if v1.metadata.vector_clock.compare(v2.metadata.vector_clock) == "concurrent":
                    return True
        return False
