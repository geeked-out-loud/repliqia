"""Click CLI commands for Repliqia node operations."""

from __future__ import annotations

import json
from typing import Optional

import click

from repliqia.core import Node
from repliqia.storage import JSONBackend, SQLiteBackend


@click.group()
@click.version_option()
def cli() -> None:
    """Repliqia: Vector-clock-based distributed key-value store CLI."""
    pass


class NodeContext:
    """Context for managing node state across commands."""

    def __init__(self) -> None:
        """Initialize context."""
        self.node: Optional[Node] = None

    def get_node(self, node_id: str, storage_type: str = "json") -> Node:
        """Get or create node instance."""
        if self.node is None:
            if storage_type == "sqlite":
                backend = SQLiteBackend(f"repliqia_{node_id}.db")
            else:
                backend = JSONBackend()

            self.node = Node(node_id=node_id, storage=backend, N=3, R=1, W=1)

        return self.node


# Global context
ctx = NodeContext()


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
@click.option("--n", default=3, help="N: replicas")
@click.option("--r", default=1, help="R: read quorum")
@click.option("--w", default=1, help="W: write quorum")
def repl(node_id: str, storage: str, n: int, r: int, w: int) -> None:
    """Start interactive REPL mode for node operations."""
    click.echo(f"🚀 Starting Repliqia node: {node_id} (N={n}, R={r}, W={w})")
    click.echo("Type 'help' for commands, 'quit' to exit\n")

    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend, N=n, R=r, W=w)
    ctx.node = node

    while True:
        try:
            cmd = click.prompt(f"{node_id} >").strip()

            if not cmd:
                continue
            if cmd == "quit" or cmd == "exit":
                click.echo("Goodbye!")
                break
            if cmd == "help":
                show_repl_help()
                continue

            # Parse and execute command
            parts = cmd.split()
            if not parts:
                continue

            execute_repl_command(node, parts)

        except KeyboardInterrupt:
            click.echo("\nGoodbye!")
            break
        except Exception as e:
            click.secho(f"Error: {e}", fg="red")


def execute_repl_command(node: Node, parts: list[str]) -> None:
    """Execute a single REPL command."""
    cmd = parts[0]

    if cmd == "put" and len(parts) >= 3:
        key = parts[1]
        value = " ".join(parts[2:])
        try:
            value_obj = json.loads(value)
        except json.JSONDecodeError:
            value_obj = value

        version = node.put(key, value_obj)
        click.secho(f"✓ Put '{key}'", fg="green")
        click.echo(f"  Clock: {version.metadata.vector_clock.to_dict()}")

    elif cmd == "get" and len(parts) >= 2:
        key = parts[1]
        versions = node.get(key)
        if not versions:
            click.secho(f"✗ Key '{key}' not found", fg="red")
        elif len(versions) == 1:
            v = versions[0]
            click.secho(f"✓ Get '{key}'", fg="green")
            click.echo(f"  Value: {json.dumps(v.value)}")
            click.echo(f"  Clock: {v.metadata.vector_clock.to_dict()}")
            click.echo(f"  Author: {v.metadata.author}")
        else:
            click.secho(f"⚠ Conflict detected: {len(versions)} versions", fg="yellow")
            for i, v in enumerate(versions):
                click.echo(f"  [{i}] {json.dumps(v.value)}")
                click.echo(f"      Clock: {v.metadata.vector_clock.to_dict()}")
                click.echo(f"      Author: {v.metadata.author}")

    elif cmd == "delete" and len(parts) >= 2:
        key = parts[1]
        node.storage.remove(key)
        click.secho(f"✓ Deleted '{key}'", fg="green")

    elif cmd == "show":
        state = node.get_state()
        click.secho(f"Node: {state['node_id']}", fg="cyan", bold=True)
        click.echo(f"Clock: {state['vector_clock']}")
        click.echo(f"Seen nodes: {', '.join(state['seen_nodes'])}")
        click.echo(f"Quorum: N={state['quorum']['N']}, R={state['quorum']['R']}, W={state['quorum']['W']}")
        click.echo(f"Storage: {state['storage']['keys']} keys, {state['storage']['total_versions']} versions")

    elif cmd == "conflicts":
        conflicts = []
        for key in node.storage.keys():
            versions = node.storage.get(key)
            if len(versions) > 1:
                for i, v1 in enumerate(versions):
                    for v2 in versions[i + 1 :]:
                        if v1.metadata.vector_clock.compare(v2.metadata.vector_clock) == "concurrent":
                            conflicts.append((key, versions))
                            break
                    if conflicts and conflicts[-1][0] == key:
                        break

        if not conflicts:
            click.echo("No conflicts detected")
            return

        click.secho(f"Found {len(conflicts)} conflicting keys:", fg="yellow", bold=True)
        for key, versions in conflicts:
            click.echo(f"\nKey: {key}")
            for i, v in enumerate(versions):
                click.echo(f"  Version {i + 1}:")
                click.echo(f"    Value: {json.dumps(v.value)}")
                click.echo(f"    Clock: {v.metadata.vector_clock.to_dict()}")
                click.echo(f"    Author: {v.metadata.author}")

    elif cmd == "clock":
        clock = node.get_clock()
        click.echo(f"Vector Clock: {clock.to_dict()}")

    elif cmd == "keys":
        keys = node.storage.keys()
        if not keys:
            click.echo("No keys stored")
        else:
            for key in keys:
                versions = node.storage.get(key)
                status = "✓" if len(versions) == 1 else "⚠"
                click.echo(f"{status} {key} ({len(versions)} version{'s' if len(versions) > 1 else ''})")

    elif cmd == "clear":
        if click.confirm("Clear all storage?", default=False):
            node.storage.clear()
            click.secho("✓ Storage cleared", fg="green")

    elif cmd == "help":
        show_repl_help()

    else:
        click.secho(f"Unknown command: {cmd}", fg="red")


def show_repl_help() -> None:
    """Display REPL help."""
    click.echo("""
Available commands:
  put <key> <value>    - Write a key-value pair
  get <key>            - Read a value (shows conflicts if any)
  delete <key>         - Delete a key
  show                 - Display node state
  conflicts            - Show all concurrent version conflicts
  clock                - Display current vector clock
  keys                 - List all keys
  clear                - Clear all storage
  help                 - Show this help
  quit/exit            - Exit REPL

Examples:
  > put user:1 {"name":"Alice"}
  > get user:1
  > show
  > conflicts
""")


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
@click.argument("key")
@click.argument("value")
def put(node_id: str, storage: str, key: str, value: str) -> None:
    """Write a key-value pair."""
    try:
        value_obj = json.loads(value)
    except json.JSONDecodeError:
        value_obj = value

    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)
    version = node.put(key, value_obj)

    click.secho(f"✓ Put '{key}' = {json.dumps(value_obj)}", fg="green")
    click.echo(f"  Clock: {version.metadata.vector_clock.to_dict()}")
    click.echo(f"  Author: {version.metadata.author}")


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
@click.argument("key")
def get(node_id: str, storage: str, key: str) -> None:
    """Read a value from storage."""
    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)
    versions = node.get(key)

    if not versions:
        click.secho(f"✗ Key '{key}' not found", fg="red")
        return

    if len(versions) == 1:
        v = versions[0]
        click.secho(f"✓ Get '{key}'", fg="green")
        click.echo(f"  Value: {json.dumps(v.value)}")
        click.echo(f"  Clock: {v.metadata.vector_clock.to_dict()}")
        click.echo(f"  Author: {v.metadata.author}")
    else:
        click.secho(f"⚠ Conflict detected: {len(versions)} versions", fg="yellow")
        for i, v in enumerate(versions):
            click.echo(f"\n  Version {i + 1}:")
            click.echo(f"    Value: {json.dumps(v.value)}")
            click.echo(f"    Clock: {v.metadata.vector_clock.to_dict()}")
            click.echo(f"    Author: {v.metadata.author}")


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
@click.argument("key")
def delete(node_id: str, storage: str, key: str) -> None:
    """Delete a key from storage."""
    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)
    node.storage.remove(key)
    click.secho(f"✓ Deleted '{key}'", fg="green")


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
def show(node_id: str, storage: str) -> None:
    """Display node state and statistics."""
    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)
    state = node.get_state()

    click.secho(f"╔═══ Repliqia Node ═══════════════════╗", fg="cyan", bold=True)
    click.echo(f"║ ID:       {state['node_id']:<26}║")
    click.echo(f"║ Quorum:   N={state['quorum']['N']} R={state['quorum']['R']} W={state['quorum']['W']:<18}║")
    clock_str = str(state['vector_clock'])[:20]
    click.echo(f"║ Clock:    {clock_str:<26}║")
    click.echo(f"║ Nodes:    {', '.join(state['seen_nodes']):<26}║")
    click.echo(f"║ Storage:  {state['storage']['keys']} keys, {state['storage']['total_versions']} versions│")
    click.secho(f"╚════════════════════════════════════════╝", fg="cyan", bold=True)


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
def conflicts(node_id: str, storage: str) -> None:
    """Show all conflicting keys and their concurrent versions."""
    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)

    conflicts_list = []
    for key in node.storage.keys():
        versions = node.storage.get(key)
        if len(versions) > 1:
            for i, v1 in enumerate(versions):
                for v2 in versions[i + 1 :]:
                    if v1.metadata.vector_clock.compare(v2.metadata.vector_clock) == "concurrent":
                        conflicts_list.append((key, versions))
                        break
                if conflicts_list and conflicts_list[-1][0] == key:
                    break

    if not conflicts_list:
        click.echo("No conflicts detected ✓")
        return

    click.secho(f"Found {len(conflicts_list)} conflicting key(s):", fg="yellow", bold=True)

    for key, versions in conflicts_list:
        click.echo(f"\n📌 Key: {key}")
        for i, v in enumerate(versions):
            click.echo(f"   ├─ Version {i + 1}:")
            click.echo(f"   │  ├─ Value: {json.dumps(v.value)}")
            click.echo(f"   │  ├─ Clock: {v.metadata.vector_clock.to_dict()}")
            click.echo(f"   │  └─ Author: {v.metadata.author}")


@cli.command()
@click.option("--id", "node_id", default="node-1", help="Node identifier")
@click.option("--storage", default="json", type=click.Choice(["json", "sqlite"]), help="Storage backend")
def clock(node_id: str, storage: str) -> None:
    """Display current vector clock state."""
    if storage == "sqlite":
        backend = SQLiteBackend(f"repliqia_{node_id}.db")
    else:
        backend = JSONBackend()

    node = Node(node_id=node_id, storage=backend)
    vc = node.get_clock()

    click.secho("Vector Clock:", fg="cyan", bold=True)
    for nid, count in sorted(vc.to_dict().items()):
        bar = "▓" * count + "░" * (5 - count)
        click.echo(f"  {nid}:  {bar}  {count}")


if __name__ == "__main__":
    cli()
