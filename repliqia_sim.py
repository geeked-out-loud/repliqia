"""
Repliqia: A minimal vector-clock-based distributed version control simulator.

Workflow pipeline (from PDF):
  Local Edit → Update Vector Clock → Store Version + Metadata 
  → Peer Sync/Metadata Exchange → Compare Vector Clocks → Apply/Conflict
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

VectorClock = Dict[str, int]

@dataclass
class Version:
    """File version: content + vector clock metadata + author."""
    content: str
    clock: VectorClock
    author: str

class Node:
    """Peer node: maintains local repo, tracks versions via vector clocks, syncs with peers."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: VectorClock = {node_id: 0}
        self.repo: Dict[str, Version] = {}

    def _clock_tick(self) -> None:
        """Increment local clock for this node."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1

    def edit(self, filename: str, content: str) -> None:
        """Stage 1 & 2: Local edit → Update vector clock → Store version + metadata."""
        self._clock_tick()
        self.repo[filename] = Version(  
            content=content, clock=self.clock.copy(), author=self.node_id
        )
        print(f"[{self.node_id}] edit {filename!r}: {content!r} @ clock={_fmt_clock(self.clock)}")

    def sync_with(self, peer: "Node", filename: str) -> None:
        """Stages 3-6: Peer sync → metadata exchange → compare clocks → apply/conflict."""
        mine = self.repo.get(filename)
        theirs = peer.repo.get(filename)

        print(f"\n→ SYNC {self.node_id} ↔ {peer.node_id} on {filename!r}")

        decision = _decide_merge(mine, theirs)

        if decision == "apply_theirs":
            self.repo[filename] = Version(
                content=theirs.content, clock=theirs.clock.copy(), author=theirs.author
            )
            self._merge_clock(theirs.clock)
            print(f"  {self.node_id} applied {peer.node_id}'s version")

        elif decision == "apply_mine":
            peer.repo[filename] = Version(
                content=mine.content, clock=mine.clock.copy(), author=mine.author
            )
            peer._merge_clock(mine.clock)
            print(f"  {peer.node_id} applied {self.node_id}'s version")

        elif decision == "conflict":
            # Concurrent edits: store conflict markers on both sides.
            self.repo[f"{filename}@{peer.node_id}"] = Version(
                content=theirs.content, clock=theirs.clock.copy(), author=theirs.author
            )
            peer.repo[f"{filename}@{self.node_id}"] = Version(
                content=mine.content, clock=mine.clock.copy(), author=mine.author
            )
            self._merge_clock(theirs.clock)
            peer._merge_clock(mine.clock)
            print(f"  ⚠ CONFLICT: {self.node_id} and {peer.node_id} both edited {filename!r}")

        else:  # no_change
            print(f"  (already in sync)")

    def _merge_clock(self, incoming: VectorClock) -> None:
        """Learn peer's view: element-wise max to merge causal histories."""
        for node, value in incoming.items():
            self.clock[node] = max(self.clock.get(node, 0), value)

    def show(self) -> None:
        """Display node's full repository state."""
        print(f"\n📦 Node {self.node_id} @ clock={_fmt_clock(self.clock)}")
        if not self.repo:
            print("  (empty)")
            return
        for name in sorted(self.repo):
            v = self.repo[name]
            print(f"  {name}: {v.content!r} [by {v.author} @ {_fmt_clock(v.clock)}]")

def _fmt_clock(clock: VectorClock) -> str:
    """Format vector clock as compact string."""
    return "{" + ", ".join(f"{k}:{clock[k]}" for k in sorted(clock)) + "}"

def _compare_clocks(a: VectorClock, b: VectorClock) -> str:
    """
    Partial-order compare: returns "before", "after", "equal", or "concurrent".
    Uses lexicographic vector comparison to determine causal relationships.
    """
    keys = set(a) | set(b)
    a_le = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    a_ge = all(a.get(k, 0) >= b.get(k, 0) for k in keys)
    a_lt = a_le and any(a.get(k, 0) < b.get(k, 0) for k in keys)
    a_gt = a_ge and any(a.get(k, 0) > b.get(k, 0) for k in keys)

    if a_le and a_ge:
        return "equal"
    if a_le and a_lt:
        return "before"
    if a_ge and a_gt:
        return "after"
    return "concurrent"

def _decide_merge(mine: Optional[Version], theirs: Optional[Version]) -> str:
    """
    Decide merge outcome via vector clock comparison:
    - apply_mine: ours happened-after theirs
    - apply_theirs: theirs happened-after ours
    - conflict: concurrent edits (neither causal)
    - no_change: both empty or both identical
    """
    if mine is None and theirs is None:
        return "no_change"
    if mine is None:
        return "apply_theirs"
    if theirs is None:
        return "apply_mine"

    rel = _compare_clocks(mine.clock, theirs.clock)
    if rel == "before":
        return "apply_theirs"
    if rel == "after":
        return "apply_mine"
    if rel == "equal":
        return "no_change"
    return "conflict"

def main() -> None:
    """Demo: three scenarios from the PDF implementation plan."""
    print("\n" + "=" * 70)
    print("Repliqia: Vector-Clock-Based Distributed Version Control")
    print("=" * 70)

    nodeA = Node("A")
    nodeB = Node("B")

    # Scenario 1: Sequential update (causal ordering).
    print("\n[Scenario 1] Sequential update (no conflict)")
    print("-" * 50)
    nodeA.edit("notes.txt", "First task: setup repo")
    nodeB.sync_with(nodeA, "notes.txt")

    # Scenario 2: Concurrent edits (conflict detected).
    print("\n[Scenario 2] Concurrent edits (both offline)")
    print("-" * 50)
    nodeA.edit("notes.txt", "Second task: add vector clocks")
    nodeB.edit("notes.txt", "Second task: fix bug in sync")
    nodeA.sync_with(nodeB, "notes.txt")

    # Scenario 3: Node reconnection (new file propagation).
    print("\n[Scenario 3] Node reconnection (new file)")
    print("-" * 50)
    nodeB.edit("schema.txt", "Define metadata structure")
    nodeA.sync_with(nodeB, "schema.txt")

    # Final state: show both repositories.
    print("\n" + "=" * 70)
    print("FINAL STATE")
    print("=" * 70)
    nodeA.show()
    nodeB.show()

if __name__ == "__main__":
    main()