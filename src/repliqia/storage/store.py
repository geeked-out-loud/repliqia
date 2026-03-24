"""Abstract storage interface and types for Repliqia."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from repliqia.clock import VectorClock


@dataclass
class VersionMetadata:
    """Metadata for a stored version."""

    vector_clock: VectorClock
    author: str
    timestamp: float


@dataclass
class Version:
    """A versioned value with its metadata."""

    key: str
    value: dict
    metadata: VersionMetadata

    def to_dict(self) -> dict:
        """Export as dict for serialization."""
        return {
            "key": self.key,
            "value": self.value,
            "metadata": {
                "vector_clock": self.metadata.vector_clock.to_dict(),
                "author": self.metadata.author,
                "timestamp": self.metadata.timestamp,
            },
        }

    @staticmethod
    def from_dict(data: dict) -> Version:
        """Import from dict."""
        return Version(
            key=data["key"],
            value=data["value"],
            metadata=VersionMetadata(
                vector_clock=VectorClock.from_dict(data["metadata"]["vector_clock"]),
                author=data["metadata"]["author"],
                timestamp=data["metadata"]["timestamp"],
            ),
        )


class StorageBackend(ABC):
    """Abstract storage backend interface."""

    @abstractmethod
    def put(self, key: str, version: Version) -> None:
        """Store a new version of a key.
        
        For multi-version systems, this creates a sibling if conflicts exist.
        """
        pass

    @abstractmethod
    def get(self, key: str) -> List[Version]:
        """Get all versions (siblings) of a key.
        
        Returns:
            List of all versions. Empty list if key doesn't exist.
        """
        pass

    @abstractmethod
    def get_latest(self, key: str) -> Optional[Version]:
        """Get a single latest version (convenience method).
        
        For conflict resolution, returns first sibling (caller picks strategy).
        """
        pass

    @abstractmethod
    def remove(self, key: str) -> None:
        """Delete all versions of a key."""
        pass

    @abstractmethod
    def keys(self) -> List[str]:
        """List all keys in storage."""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists in storage."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all storage."""
        pass
