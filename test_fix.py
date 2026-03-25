#!/usr/bin/env python
"""Test the conflict resolution fix."""

from repliqia.core import Node

node = Node("test-node")

# Write 1
v1 = node.put("user:1", {"name": "Alice", "age": 30})
print(f"Write 1: clock = {v1.metadata.vector_clock.to_dict()}")

# Read - should have 1 version
versions = node.get("user:1")
print(f"After write 1: {len(versions)} version(s)")

# Write 2
v2 = node.put("user:1", {"name": "Bob", "age": 35})
print(f"Write 2: clock = {v2.metadata.vector_clock.to_dict()}")

# Read - should STILL have 1 version (no conflict!)
versions = node.get("user:1")
print(f"After write 2: {len(versions)} version(s)")
print(f"Value: {versions[0].value}")
print(f"Clock: {versions[0].metadata.vector_clock.to_dict()}")

if len(versions) == 1 and versions[0].value["name"] == "Bob":
    print("\n✅ FIX VERIFIED: Same-node sequential writes correctly replace, no false conflict!")
else:
    print("\n❌ FIX FAILED")
