"""
Orchestrator Server for Repliqia Dashboard

Manages:
- Node process lifecycle (start, stop, restart)
- Port assignment (5001+)
- Request proxying to node servers
- WebSocket broadcasting for frontend events
- Demo reset functionality
"""

import os
import signal
import subprocess
import time
import shutil
from pathlib import Path
from datetime import datetime
import sys
import json
import requests
from urllib.parse import quote_plus

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sock import Sock
from threading import Lock

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
sock = Sock(app)

# ============================================================================
# GLOBALS
# ============================================================================

nodes = {}  # { "A": { "process": Popen|None, "port": 5001, "status": "online"|"offline" } }
next_port = 5001
clients = set()  # WebSocket clients
nodes_lock = Lock()

PROJECT_ROOT = Path(__file__).parent.parent  # Up from repliqia/ to root
REPLIQIA_ROOT = Path(__file__).parent  # repliqia/ folder
DATA_DIR = REPLIQIA_ROOT / "data"  # repliqia/data/


# ============================================================================
# UTILITIES
# ============================================================================

def get_next_port():
    """Allocate next available port, starting from 5001."""
    global next_port
    port = next_port
    next_port += 1
    return port


def broadcast(event: dict):
    """Send event to all connected WebSocket clients, removing dead connections."""
    dead_clients = set()
    for client in clients:
        try:
            client.send(json.dumps(event))
        except Exception:
            dead_clients.add(client)
    clients.difference_update(dead_clients)


def format_timestamp():
    """Return current time as HH:MM:SS.mmm"""
    now = datetime.now()
    return now.strftime("%H:%M:%S.%f")[:-3]


def terminate_process(process: subprocess.Popen | None) -> None:
    """Terminate a process gracefully, then force kill if needed."""
    if process is None:
        return

    try:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
    except Exception:
        pass


def process_output(process: subprocess.Popen | None) -> tuple[str, str]:
    """Collect captured stdout/stderr for an exited process."""
    if process is None or process.poll() is None:
        return "", ""

    try:
        stdout, stderr = process.communicate(timeout=0.2)
    except Exception:
        return "", ""

    stdout_text = (stdout or "").strip()
    stderr_text = (stderr or "").strip()
    return stdout_text[-2000:], stderr_text[-2000:]


def wait_for_node_ready(process: subprocess.Popen, port: int, attempts: int = 20) -> bool:
    """Wait until node health endpoint is reachable or process exits."""
    for _ in range(attempts):
        if process.poll() is not None:
            return False

        try:
            resp = requests.get(f"http://localhost:{port}/health", timeout=0.5)
            if resp.ok:
                return True
        except Exception:
            time.sleep(0.25)

    return False


# ============================================================================
# NODE LIFECYCLE
# ============================================================================

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok"}), 200


@app.route("/nodes/start", methods=["POST"])
def start_node():
    """
    Start a new replica node.
    
    Body: {
      "node_id": "A",
      "n": 3,
      "r": 2,
      "w": 2
    }
    """
    data = request.get_json() or {}
    node_id = data.get("node_id", "").upper()
    n = data.get("n", 3)
    r = data.get("r", 2)
    w = data.get("w", 2)

    try:
        n = int(n)
        r = int(r)
        w = int(w)
    except (TypeError, ValueError):
        return jsonify({"error": "n, r, and w must be integers"}), 400
    
    if not node_id or not node_id.isalpha():
        return jsonify({"error": "invalid node_id"}), 400
    
    with nodes_lock:
        if node_id in nodes:
            return jsonify({"error": f"node {node_id} already exists"}), 409
        
        port = get_next_port()
        
        # Create node-specific data directory
        node_data_dir = DATA_DIR / node_id
        node_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Spawn subprocess
        cmd = [
            "uv",
            "run",
            "python",
            "-m",
            "repliqia.api.server",
            "--node", node_id,
            "--port", str(port),
            "--db-dir", str(node_data_dir),
            "--n", str(n),
            "--r", str(r),
            "--w", str(w),
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(REPLIQIA_ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            return jsonify({"error": f"failed to start: {str(e)}"}), 500

        if not wait_for_node_ready(process, port):
            stdout_text, stderr_text = process_output(process)
            exit_code = process.poll()
            terminate_process(process)
            return (
                jsonify(
                    {
                        "error": f"node {node_id} failed to start",
                        "exit_code": exit_code,
                        "stdout": stdout_text,
                        "stderr": stderr_text,
                    }
                ),
                500,
            )
        
        nodes[node_id] = {
            "process": process,
            "port": port,
            "status": "online",
            "n": n,
            "r": r,
            "w": w,
        }
        
        broadcast({
            "type": "node_online",
            "node_id": node_id,
            "port": port,
            "timestamp": format_timestamp(),
        })
        
        return jsonify({
            "node_id": node_id,
            "port": port,
            "status": "online",
        }), 201


@app.route("/nodes/<node_id>/stop", methods=["POST"])
def stop_node(node_id):
    """Stop a running node by terminating its process."""
    node_id = node_id.upper()
    
    with nodes_lock:
        if node_id not in nodes:
            return jsonify({"error": f"node {node_id} not found"}), 404
        
        node = nodes[node_id]
        if node["process"] is not None:
            terminate_process(node["process"])
        
        node["status"] = "offline"
        node["process"] = None
        
        broadcast({
            "type": "node_offline",
            "node_id": node_id,
            "timestamp": format_timestamp(),
        })
        
        return jsonify({
            "node_id": node_id,
            "status": "offline",
        })


@app.route("/nodes/<node_id>/restart", methods=["POST"])
def restart_node(node_id):
    """Restart a node (stop then start on same port)."""
    node_id = node_id.upper()
    
    with nodes_lock:
        if node_id not in nodes:
            return jsonify({"error": f"node {node_id} not found"}), 404
        
        node = nodes[node_id]
        port = node["port"]
        n = node.get("n", 3)
        r = node.get("r", 2)
        w = node.get("w", 2)
        
        # Stop existing process
        if node["process"] is not None:
            terminate_process(node["process"])
        
        # Start new process on same port
        node_data_dir = DATA_DIR / node_id
        node_data_dir.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            "uv",
            "run",
            "python",
            "-m",
            "repliqia.api.server",
            "--node", node_id,
            "--port", str(port),
            "--db-dir", str(node_data_dir),
            "--n", str(n),
            "--r", str(r),
            "--w", str(w),
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(REPLIQIA_ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            node["status"] = "offline"
            node["process"] = None
            return jsonify({"error": f"failed to restart: {str(e)}"}), 500

        if not wait_for_node_ready(process, port):
            stdout_text, stderr_text = process_output(process)
            exit_code = process.poll()
            terminate_process(process)
            node["status"] = "offline"
            node["process"] = None
            return (
                jsonify(
                    {
                        "error": f"node {node_id} failed to restart",
                        "exit_code": exit_code,
                        "stdout": stdout_text,
                        "stderr": stderr_text,
                    }
                ),
                500,
            )
        
        node["process"] = process
        node["status"] = "online"
        
        broadcast({
            "type": "node_online",
            "node_id": node_id,
            "port": port,
            "timestamp": format_timestamp(),
        })
        
        return jsonify({
            "node_id": node_id,
            "port": port,
            "status": "online",
        })


@app.route("/nodes", methods=["GET"])
def get_nodes():
    """List all nodes with status and port."""
    with nodes_lock:
        for node in nodes.values():
            process = node.get("process")
            if node.get("status") == "online" and process is not None and process.poll() is not None:
                node["status"] = "offline"
                node["process"] = None

        return jsonify([
            {
                "node_id": node_id,
                "port": node["port"],
                "status": node["status"],
            }
            for node_id, node in nodes.items()
        ])


@app.route("/nodes/<node_id>", methods=["DELETE"])
def delete_node(node_id):
    """Stop and remove a node from registry."""
    node_id = node_id.upper()
    
    with nodes_lock:
        if node_id not in nodes:
            return jsonify({"error": f"node {node_id} not found"}), 404
        
        node = nodes[node_id]
        if node["process"] is not None:
            terminate_process(node["process"])
        
        del nodes[node_id]
        
        broadcast({
            "type": "node_offline",
            "node_id": node_id,
            "timestamp": format_timestamp(),
        })
        
        return jsonify({"deleted": True})


# ============================================================================
# PROXY ENDPOINTS
# ============================================================================

def proxy_request(node_id: str, path: str, method: str, body=None):
    """
    Forward a request to a node server.
    
    Returns: (response_dict, status_code)
    """
    node_id = node_id.upper()
    
    with nodes_lock:
        if node_id not in nodes:
            return {"error": f"node {node_id} not found"}, 404
        
        node = nodes[node_id]

        process = node.get("process")
        if node["status"] == "online" and process is not None and process.poll() is not None:
            stdout_text, stderr_text = process_output(process)
            node["status"] = "offline"
            node["process"] = None
            return {
                "error": "node process exited",
                "exit_code": process.returncode,
                "stdout": stdout_text,
                "stderr": stderr_text,
            }, 503

        if node["status"] == "offline":
            return {"error": "node offline"}, 503
        
        port = node["port"]
    
    url = f"http://localhost:{port}/{path.lstrip('/')}"
    
    try:
        if method == "GET":
            resp = requests.get(url, timeout=5)
        elif method == "POST":
            resp = requests.post(url, json=body, timeout=5)
        elif method == "PUT":
            resp = requests.put(url, json=body, timeout=5)
        elif method == "DELETE":
            resp = requests.delete(url, timeout=5)
        else:
            return {"error": f"unsupported method: {method}"}, 400
        
        try:
            response_data = resp.json()
        except Exception:
            response_data = resp.text
        
        return response_data, resp.status_code
    
    except Exception as e:
        return {"error": f"proxy failed: {str(e)}"}, 503


@app.route("/proxy/<node_id>/kvstore/<key>", methods=["GET", "PUT", "DELETE"])
def proxy_kvstore(node_id, key):
    """Proxy to node kvstore endpoints."""
    path = f"kvstore/{key}"
    body = request.get_json() if request.method in ["PUT", "POST"] else None
    
    response_data, status_code = proxy_request(node_id, path, request.method, body)
    
    if status_code in [200, 201, 204]:
        operation_event = {
            "type": "operation",
            "event_type": "operation_completed",
            "node_id": node_id.upper(),
            "method": request.method,
            "path": f"/kvstore/{key}",
            "status_code": status_code,
            "timestamp": format_timestamp(),
        }
        
        if isinstance(response_data, dict):
            operation_event["response"] = response_data
            if "clock" in response_data:
                operation_event["clock"] = response_data["clock"]
            if "quorum" in response_data:
                operation_event["quorum"] = response_data["quorum"]
        
        broadcast(operation_event)
    
    return jsonify(response_data), status_code


@app.route("/proxy/<node_id>/sync/<peer_id>", methods=["POST"])
def proxy_sync(node_id, peer_id):
    """Synchronize two nodes by mediating version exchange through the orchestrator."""
    source_node_id = node_id.upper()
    dest_node_id = peer_id.upper()

    if source_node_id == dest_node_id:
        return jsonify({"error": "source and peer must be different nodes"}), 400

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}

    sync_key = payload.get("key")
    internal_versions_path = "internal/versions"
    if sync_key:
        internal_versions_path = f"{internal_versions_path}?key={quote_plus(str(sync_key))}"

    # Step 1: pull source versions
    source_versions_response, source_status = proxy_request(
        source_node_id,
        internal_versions_path,
        "GET",
    )
    if source_status != 200 or not isinstance(source_versions_response, dict):
        return (
            jsonify(
                {
                    "error": "failed to collect source versions",
                    "source": source_node_id,
                    "peer": dest_node_id,
                    "details": source_versions_response,
                }
            ),
            source_status,
        )

    outbound_versions = source_versions_response.get("versions", [])
    if not isinstance(outbound_versions, list):
        outbound_versions = []

    # Step 2: push to destination and ask for destination versions in return
    outbound_payload = {
        "origin_node_id": source_node_id,
        "versions": outbound_versions,
        "return_versions": True,
    }
    if sync_key:
        outbound_payload["key"] = sync_key

    destination_response, destination_status = proxy_request(
        dest_node_id,
        f"sync/{source_node_id}",
        "POST",
        outbound_payload,
    )
    if not (200 <= destination_status < 300) or not isinstance(destination_response, dict):
        return (
            jsonify(
                {
                    "error": "failed to push versions to peer",
                    "source": source_node_id,
                    "peer": dest_node_id,
                    "details": destination_response,
                }
            ),
            destination_status,
        )

    inbound_versions = destination_response.get("versions", [])
    if not isinstance(inbound_versions, list):
        inbound_versions = []

    # Step 3: merge destination versions back into source
    merged_back_versions = 0
    if inbound_versions:
        inbound_payload = {
            "origin_node_id": dest_node_id,
            "versions": inbound_versions,
            "return_versions": False,
        }
        if sync_key:
            inbound_payload["key"] = sync_key

        back_merge_response, back_merge_status = proxy_request(
            source_node_id,
            f"sync/{dest_node_id}",
            "POST",
            inbound_payload,
        )
        if not (200 <= back_merge_status < 300):
            return (
                jsonify(
                    {
                        "error": "failed to merge peer versions back into source",
                        "source": source_node_id,
                        "peer": dest_node_id,
                        "details": back_merge_response,
                    }
                ),
                back_merge_status,
            )

        if isinstance(back_merge_response, dict):
            merged_back_versions = int(back_merge_response.get("merged_versions", 0) or 0)

    conflicts_response, conflicts_status = proxy_request(source_node_id, "conflicts", "GET")
    conflicts = []
    if conflicts_status == 200 and isinstance(conflicts_response, dict):
        conflicts = conflicts_response.get("conflicts", []) or []

    response_data = {
        "source": source_node_id,
        "peer": dest_node_id,
        "synced": True,
        "mode": "mediated",
        "pushed_versions": len(outbound_versions),
        "pulled_versions": len(inbound_versions),
        "merged_back_versions": merged_back_versions,
        "versions_exchanged": len(outbound_versions) + len(inbound_versions),
        "conflicts": conflicts,
    }

    broadcast(
        {
            "type": "operation",
            "event_type": "operation_completed",
            "node_id": source_node_id,
            "method": "SYNC",
            "path": f"/sync/{dest_node_id}",
            "status_code": 200,
            "response": response_data,
            "timestamp": format_timestamp(),
        }
    )

    return jsonify(response_data), 200


@app.route("/proxy/<node_id>/conflicts", methods=["GET"])
def proxy_conflicts(node_id):
    """Proxy conflicts endpoint."""
    path = "conflicts"
    response_data, status_code = proxy_request(node_id, path, request.method)
    return jsonify(response_data), status_code


@app.route("/proxy/<node_id>/node/state", methods=["GET"])
def proxy_node_state(node_id):
    """Proxy to node state endpoint."""
    response_data, status_code = proxy_request(node_id, "node/state", "GET")
    return jsonify(response_data), status_code


@app.route("/proxy/<node_id>/node/clock", methods=["GET"])
def proxy_node_clock(node_id):
    """Proxy to node clock endpoint."""
    response_data, status_code = proxy_request(node_id, "node/clock", "GET")
    return jsonify(response_data), status_code


# ============================================================================
# DEMO & MANAGEMENT
# ============================================================================

@app.route("/demo/reset", methods=["POST"])
def demo_reset():
    """Reset all nodes and clear all backend data state."""
    global next_port, nodes
    
    with nodes_lock:
        # Stop all processes
        for node_id, node in nodes.items():
            if node["process"] is not None:
                terminate_process(node["process"])
        
        nodes.clear()
        next_port = 5001

    removed_db_files = 0
    removed_data_entries = 0

    # Fully wipe backend data directory contents (nodes, dbs, sidecar files, folders).
    if DATA_DIR.exists():
        try:
            removed_data_entries = sum(1 for _ in DATA_DIR.rglob("*"))
            removed_db_files += sum(
                1
                for p in DATA_DIR.rglob("*")
                if p.is_file() and (p.suffix == ".db" or ".db-" in p.name)
            )
            shutil.rmtree(DATA_DIR)
        except Exception:
            pass

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Backward-compatible cleanup for legacy root-level database files
    for pattern in ("repliqia_*.db", "repliqia_*.db-*"):
        for db_file in PROJECT_ROOT.glob(pattern):
            if not db_file.is_file():
                continue
            try:
                db_file.unlink()
                removed_db_files += 1
            except Exception:
                pass
    
    broadcast({
        "type": "system",
        "message": "Demo reset - all nodes and backend data cleared",
        "timestamp": format_timestamp(),
    })

    return jsonify(
        {
            "reset": True,
            "removed_db_files": removed_db_files,
            "removed_data_entries": removed_data_entries,
            "nodes_cleared": True,
        }
    )


# ============================================================================
# WEBSOCKET
# ============================================================================

@sock.route("/ws")
def websocket(ws):
    """Handle WebSocket connections for event broadcast."""
    clients.add(ws)
    print(f"[WS] Client connected from {request.remote_addr}. Total clients: {len(clients)}")
    
    try:
        # Keep connection alive
        while True:
            try:
                data = ws.receive()
                if data is None:
                    break
            except Exception as e:
                print(f"[WS] Receive error: {e}")
                break
    except Exception as e:
        print(f"[WS] Connection error: {e}")
    finally:
        clients.discard(ws)
        print(f"[WS] Client disconnected. Total clients: {len(clients)}")


# ============================================================================
# STARTUP
# ============================================================================

if __name__ == "__main__":
    print("Orchestrator starting on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
