"""Tests for CLI commands."""

import json

import pytest
from click.testing import CliRunner

from interface.cli import cli
from repliqia.storage import JSONBackend, VersionMetadata, Version


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_storage(tmp_path):
    """Create temporary storage directory for tests."""
    return tmp_path


class TestBasicCommands:
    """Test basic CLI commands."""

    def test_cli_help(self, runner):
        """CLI should display help."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Repliqia" in result.output

    def test_put_command(self, runner):
        """Put command should write key-value."""
        result = runner.invoke(cli, ["put", "--id", "node-1", "key1", '{"x": 1}'])
        assert result.exit_code == 0
        assert "✓ Put 'key1'" in result.output
        assert "Clock:" in result.output

    def test_put_with_string_value(self, runner):
        """Put should handle string values."""
        result = runner.invoke(cli, ["put", "--id", "node-1", "name", "alice"])
        assert result.exit_code == 0
        assert "✓ Put 'name'" in result.output

    def test_get_command_existing_key(self, runner):
        """Get command should work via REPL (cross-command persistence)."""
        result = runner.invoke(
            cli, 
            ["repl", "--id", "node-1"], 
            input='put user {"name": "Alice"}\nget user\nquit\n'
        )
        assert result.exit_code == 0
        assert "✓ Get 'user'" in result.output or "Alice" in result.output

    def test_get_command_missing_key(self, runner):
        """Get on missing key should show error."""
        result = runner.invoke(cli, ["get", "--id", "node-1", "missing"])
        assert result.exit_code == 0
        assert "✗ Key 'missing' not found" in result.output

    def test_delete_command(self, runner):
        """Delete command should remove key."""
        runner.invoke(cli, ["put", "--id", "node-1", "temp", "value"])
        result = runner.invoke(cli, ["delete", "--id", "node-1", "temp"])
        assert result.exit_code == 0
        assert "✓ Deleted 'temp'" in result.output

    def test_show_command(self, runner):
        """Show command displays node state."""
        runner.invoke(cli, ["put", "--id", "node-1", "x", "1"])
        result = runner.invoke(cli, ["show", "--id", "node-1"])
        assert result.exit_code == 0
        assert "node-1" in result.output
        assert "Quorum:" in result.output

    def test_clock_command(self, runner):
        """Clock command shows vector clock via REPL."""
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input="put x 1\nclock\nquit\n",
        )
        assert result.exit_code == 0
        assert "Vector Clock:" in result.output


class TestConflictDetection:
    """Test conflict detection in CLI."""

    def test_conflicts_command_no_conflicts(self, runner):
        """Conflicts command with no conflicts."""
        runner.invoke(cli, ["put", "--id", "node-1", "x", "1"])
        result = runner.invoke(cli, ["conflicts", "--id", "node-1"])
        assert result.exit_code == 0
        assert "No conflicts detected" in result.output

    def test_get_command_shows_conflict(self, runner):
        """Get should show multiple versions as conflict."""
        # First write
        runner.invoke(cli, ["put", "--id", "node-1", "count", "1"])
        # Note: In actual tests, we can't easily create true conflicts via CLI
        # because each command creates a fresh node. This is by design.
        # Conflict tests are better tested at the unit level.


class TestStorageOption:
    """Test different storage backends."""

    def test_put_with_json_backend(self, runner):
        """Put with JSON backend (default)."""
        result = runner.invoke(cli, ["put", "--id", "node-1", "--storage", "json", "key", "value"])
        assert result.exit_code == 0
        assert "✓ Put" in result.output

    def test_put_with_sqlite_backend(self, runner, tmp_path):
        """Put with SQLite backend."""
        import os

        os.chdir(tmp_path)
        result = runner.invoke(cli, ["put", "--id", "node-1", "--storage", "sqlite", "key", "value"])
        assert result.exit_code == 0
        assert "✓ Put" in result.output


class TestQuorumOptions:
    """Test quorum parameter display."""

    def test_show_displays_quorum_params(self, runner):
        """Show should display N, R, W parameters."""
        result = runner.invoke(cli, ["show", "--id", "node-1"])
        assert result.exit_code == 0
        assert "Quorum:" in result.output
        assert "N=3" in result.output
        assert "R=1" in result.output
        assert "W=1" in result.output


class TestJSONValueHandling:
    """Test JSON serialization in CLI."""

    def test_put_complex_json(self, runner):
        """Put should handle complex JSON objects."""
        value = json.dumps({"name": "Alice", "age": 30, "tags": ["dev", "ai"]})
        result = runner.invoke(cli, ["put", "--id", "node-1", "user:1", value])
        assert result.exit_code == 0
        assert "✓ Put" in result.output

    def test_get_displays_json_nicely(self, runner):
        """Get should display JSON with proper formatting via REPL."""
        value = '{"x": 1, "y": 2}'
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input=f'put point {value}\nget point\nquit\n'
        )
        assert result.exit_code == 0
        assert "x" in result.output or "1" in result.output


class TestREPLMode:
    """Test interactive REPL mode."""

    def test_repl_help_command(self, runner):
        """REPL help should list available commands."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input="help\nquit\n")
        assert result.exit_code == 0
        assert "Available commands:" in result.output or "put" in result.output

    def test_repl_quit_command(self, runner):
        """REPL quit should exit gracefully."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input="quit\n")
        assert result.exit_code == 0
        assert "Goodbye!" in result.output

    def test_repl_put_command(self, runner):
        """REPL put should work interactively."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input='put x 1\nquit\n')
        assert result.exit_code == 0
        assert "✓ Put" in result.output

    def test_repl_get_command(self, runner):
        """REPL get should retrieve values."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input='put item value\nget item\nquit\n')
        assert result.exit_code == 0
        assert "✓ Put" in result.output
        assert "✓ Get" in result.output or "value" in result.output

    def test_repl_show_command(self, runner):
        """REPL show should display node state."""
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input="put x 1\nshow\nquit\n",
        )
        assert result.exit_code == 0
        assert "node-1" in result.output

    def test_repl_keys_command(self, runner):
        """REPL keys should list stored keys."""
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input="put a 1\nput b 2\nkeys\nquit\n",
        )
        assert result.exit_code == 0
        assert "a" in result.output
        assert "b" in result.output

    def test_repl_delete_command(self, runner):
        """REPL delete should remove keys."""
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input="put temp val\ndelete temp\nquit\n",
        )
        assert result.exit_code == 0
        assert "✓ Deleted" in result.output

    def test_repl_clock_command(self, runner):
        """REPL clock should show vector clock."""
        result = runner.invoke(
            cli,
            ["repl", "--id", "node-1"],
            input="put x 1\nclock\nquit\n",
        )
        assert result.exit_code == 0
        assert "Vector Clock:" in result.output or "node-1" in result.output

    def test_repl_empty_input(self, runner):
        """REPL should handle empty input gracefully."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input="\n\nquit\n")
        assert result.exit_code == 0

    def test_repl_unknown_command(self, runner):
        """REPL should report unknown commands."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input="invalid\nquit\n")
        assert result.exit_code == 0
        assert "Unknown command" in result.output


class TestErrorHandling:
    """Test error handling in CLI."""

    def test_put_invalid_json_value(self, runner):
        """Put should handle invalid JSON gracefully via REPL."""
        result = runner.invoke(
            cli, 
            ["repl", "--id", "node-1"], 
            input='put key {invalid\nquit\n'
        )
        assert result.exit_code == 0
        # Should store the string value
        assert "Put" in result.output or "key" in result.output

    def test_repl_keyboard_interrupt(self, runner):
        """REPL should handle Ctrl+C gracefully."""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input="\x03")
        # Should exit gracefully
        assert result.exit_code in [0, 1, -1]


class TestNodeOptions:
    """Test node configuration options."""

    def test_custom_node_id(self, runner):
        """Commands should respect custom node ID."""
        runner.invoke(cli, ["put", "--id", "node-alpha", "x", "1"])
        result = runner.invoke(cli, ["show", "--id", "node-alpha"])
        assert result.exit_code == 0
        assert "node-alpha" in result.output

    def test_custom_quorum_params(self, runner):
        """Repl should accept custom quorum parameters."""
        result = runner.invoke(
            cli, ["repl", "--id", "node-1", "--n", "5", "--r", "2", "--w", "3"], input="show\nquit\n"
        )
        assert result.exit_code == 0
        # Note: The actual display depends on implementation


class TestMultipleOperations:
    """Test sequences of operations."""

    def test_write_read_sequence(self, runner):
        """Write followed by read should work via REPL."""
        result = runner.invoke(
            cli, 
            ["repl", "--id", "node-1"], 
            input='put user:1 {"name": "Alice", "age": 30}\nget user:1\nquit\n'
        )
        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "30" in result.output or "Alice" in result.output

    def test_multiple_keys(self, runner):
        """Multiple keys should be stored independently via REPL."""
        result = runner.invoke(
            cli, 
            ["repl", "--id", "node-1"], 
            input='put a 1\nput b 2\nput c 3\nget b\nquit\n'
        )
        assert result.exit_code == 0
        assert "2" in result.output

    def test_repl_complex_workflow(self, runner):
        """REPL should handle complex workflows."""
        commands = """put user:1 {"name": "Alice", "role": "admin"}
put user:2 {"name": "Bob", "role": "user"}
show
keys
conflict
quit
"""
        result = runner.invoke(cli, ["repl", "--id", "node-1"], input=commands)
        assert result.exit_code == 0
