"""Core node implementation for Repliqia."""

from __future__ import annotations

import time
from typing import List, Optional

from repliqia.clock import VectorClock
from repliqia.storage import JSONBackend, StorageBackend, Version, VersionMetadata


class Node:
    """A single node in the Repliqia distributed system.
    
    Each node:
    - Maintains a vector clock to track causality
    - Stores versions locally via a storage backend
    - Supports quorum-based reads/writes (N, R, W parameters)
    - Merges replicas from peer nodes
    """

    def __init__(
        self,
        node_id: str,
        storage: Optional[StorageBackend] = None,
        N: int = 3,
        R: int = 1,
        W: int = 1,
    ) -> None:
        """Initialize a node.
        
        Args:
            node_id: Unique identifier for this node (e.g., "A", "B", "C")
            storage: Storage backend (defaults to JSONBackend for demos)
            N: Replication factor (how many nodes should hold replicas)
            R: Read quorum (how many nodes must agree on read)
            W: Write quorum (how many nodes must acknowledge write)
        """
        self.node_id = node_id
        self.storage = storage or JSONBackend()
        
        # Quorum parameters (Dynamo-style)
        self.N = N  # Replication factor
        self.R = R  # Read quorum
        self.W = W  # Write quorum
        
        # Local vector clock (tracks this node's events)
        self._clock = VectorClock()
        
        # Track all node IDs we've seen (for expanding clock)
        self._seen_nodes = {node_id}

    # ========== Vector Clock Management ==========
    
    def tick(self) -> None:
        """Increment this node's vector clock.
        
        Called on every local write operation.
        """
        self._clock = self._clock.tick(self.node_id)

    def advance_clock(self, incoming_clock: VectorClock) -> None:
        """Learn from a peer's vector clock via merge.
        
        Used when receiving replicated data. Updates local clock to reflect
        the full causal history.
        
        Args:
            incoming_clock: Vector clock from a peer node
        """
        self._clock = self._clock.merge(incoming_clock)
        self._clock = self._clock.tick(self.node_id)  # Increment self after learning
        
        # Track new node IDs we discover
        for node_id in incoming_clock.to_dict():
            self._seen_nodes.add(node_id)

    def get_clock(self) -> VectorClock:
        """Get current vector clock (read-only)."""
        return VectorClock.from_dict(self._clock.to_dict())

    # ========== Local Read/Write Operations ==========

    def put(self, key: str, value: dict) -> Version:
        """Write a value locally.
        
        Increments vector clock, stores version with metadata.
        
        Args:
            key: Key to write
            value: JSON-serializable value
            
        Returns:
            The Version object that was stored
        """
        self.tick()  # Increment vector clock for this write
        
        version = Version(
            key=key,
            value=value,
            metadata=VersionMetadata(
                vector_clock=self.get_clock(),
                author=self.node_id,
                timestamp=time.time(),
            ),
        )
        
        self.storage.put(key, version)
        return version

    def get(self, key: str) -> List[Version]:
        """Read all versions of a key.
        
        Returns ALL versions (siblings) if conflicts exist.
        Caller decides conflict resolution strategy.
        
        Args:
            key: Key to read
            
        Returns:
            List of all versions (empty if key doesn't exist)
        """
        return self.storage.get(key)

    def get_latest(self, key: str) -> Optional[Version]:
        """Read one version (convenience).
        
        Returns the first sibling. For conflict resolution,
        prefer get() + manual strategy.
        
        Args:
            key: Key to read
            
        Returns:
            First version or None if key doesn't exist
        """
        return self.storage.get_latest(key)

    # ========== Merge & Replication ==========

    def merge(self, incoming_versions: List[Version]) -> None:
        """Merge replicated versions from a peer.
        
        For each incoming version:
        - If we don't have it: store it (new replica)
        - If we have it: check vector clock relationships
          - If incoming is newer than all our versions: replace them
          - If our versions include descendants of incoming: ignore incoming
          - If concurrent: store as sibling (conflict)
        
        Args:
            incoming_versions: Versions from a peer (usually for one key)
        """
        if not incoming_versions:
            return

        key = incoming_versions[0].key
        
        for incoming in incoming_versions:
            incoming_clock = incoming.metadata.vector_clock
            
            # Track discovery of new nodes
            for node_id in incoming_clock.to_dict():
                self._seen_nodes.add(node_id)
            
            # Refresh local versions for each iteration
            local_versions = self.storage.get(key)
            
            # Check if we already have this exact version
            if any(
                v.metadata.vector_clock.to_dict() == incoming_clock.to_dict()
                for v in local_versions
            ):
                continue  # Already have it
            
            if not local_versions:
                # No local versions, just store incoming
                self.storage.put(key, incoming)
            else:
                # Compare incoming against all local versions
                comparisons = [
                    (v, v.metadata.vector_clock.compare(incoming_clock))
                    for v in local_versions
                ]
                
                # Check relationships
                has_descendant = any(result == "after" for _, result in comparisons)
                has_ancestor = any(result == "before" for _, result in comparisons)
                has_concurrent = any(result == "concurrent" for _, result in comparisons)
                has_equal = any(result == "equal" for _, result in comparisons)
                
                if has_descendant:
                    # We have a descendant of incoming → keep ours, ignore incoming
                    pass
                elif has_equal:
                    # Already have it (shouldn't reach here due to earlier check)
                    pass
                elif has_ancestor and not has_concurrent:
                    # All our versions are ancestors of incoming → replace them
                    self.storage.remove(key)
                    self.storage.put(key, incoming)
                else:
                    # Concurrent or mix → store as sibling
                    self.storage.put(key, incoming)
            
            # Update our clock to reflect the causality we just learned
            self.advance_clock(incoming_clock)

    # ========== Inspection & Debugging ==========

    def get_state(self) -> dict:
        """Inspect node state for debugging.
        
        Returns:
            Dict with node_id, clock, storage stats, quorum params
        """
        keys = self.storage.keys()
        total_siblings = sum(len(self.storage.get(k)) for k in keys)
        
        return {
            "node_id": self.node_id,
            "vector_clock": self._clock.to_dict(),
            "seen_nodes": sorted(self._seen_nodes),
            "quorum": {"N": self.N, "R": self.R, "W": self.W},
            "storage": {
                "keys": len(keys),
                "total_versions": total_siblings,
            },
        }
