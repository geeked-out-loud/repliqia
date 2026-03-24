"""Vector clock implementation for causal ordering in distributed systems."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, OrderedDict


@dataclass
class VectorClock:
    """
    Logical clock vector for a distributed node.
    
    Maps node_id -> counter to track causality relationships.
    Two VectorClocks can be compared to determine event ordering:
      - before: A happened before B
      - after: A happened after B
      - equal: same causal state
      - concurrent: neither ordered (conflict)
    """

    data: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize: convert empty dict to empty."""
        if not self.data:
            self.data = {}

    def tick(self, node_id: str) -> VectorClock:
        """Increment counter for this node, return new clock (immutable semantics)."""
        new_data = self.data.copy()
        new_data[node_id] = new_data.get(node_id, 0) + 1
        return VectorClock(new_data)

    def merge(self, other: VectorClock) -> VectorClock:
        """
        Merge with another clock via element-wise max.
        Represents learning peer's event history.
        """
        new_data = self.data.copy()
        for node_id, count in other.data.items():
            new_data[node_id] = max(new_data.get(node_id, 0), count)
        return VectorClock(new_data)

    def compare(self, other: VectorClock) -> str:
        """
        Partial-order comparison against another clock.
        
        Returns:
          - "before": self happened-before other (self < other)
          - "after": self happened-after other (self > other)
          - "equal": same causal state (self == other)
          - "concurrent": neither ordered (incomparable)
        """
        all_keys = set(self.data.keys()) | set(other.data.keys())

        self_le_other = all(self.data.get(k, 0) <= other.data.get(k, 0) for k in all_keys)
        self_ge_other = all(self.data.get(k, 0) >= other.data.get(k, 0) for k in all_keys)

        if self_le_other and self_ge_other:
            return "equal"
        if self_le_other and any(self.data.get(k, 0) < other.data.get(k, 0) for k in all_keys):
            return "before"
        if self_ge_other and any(self.data.get(k, 0) > other.data.get(k, 0) for k in all_keys):
            return "after"
        return "concurrent"

    def is_causal_descendant_of(self, other: VectorClock) -> bool:
        """Returns True if self might causally depend on other (self >= other element-wise)."""
        all_keys = set(self.data.keys()) | set(other.data.keys())
        return all(self.data.get(k, 0) >= other.data.get(k, 0) for k in all_keys)

    def to_dict(self) -> Dict[str, int]:
        """Export as plain dict."""
        return self.data.copy()

    @staticmethod
    def from_dict(data: Dict[str, int]) -> VectorClock:
        """Create from plain dict."""
        return VectorClock(data)

    def __repr__(self) -> str:
        items = ", ".join(f"{k}:{v}" for k, v in sorted(self.data.items()))
        return "{" + items + "}"

    def __str__(self) -> str:
        return repr(self)

    def __bool__(self) -> bool:
        """Empty clock is falsy."""
        return bool(self.data)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.data == other.data

    def __hash__(self) -> int:
        """Hash based on items for use in sets/dicts."""
        return hash(tuple(sorted(self.data.items())))
