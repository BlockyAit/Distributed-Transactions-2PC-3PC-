#!/usr/bin/env python3
"""
Lab 4 Coordinator (2PC/3PC) with WAL persistence and retries
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
from typing import Dict, Any, List, Tuple

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8000
PARTICIPANTS: List[str] = []
TIMEOUT_S: float = 2.0
WAL_PATH: str = "/tmp/coordinator.wal"

TX: Dict[str, Dict[str, Any]] = {}

# ---------------------------
# JSON helpers
# ---------------------------
def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

# ---------------------------
# HTTP helper
# ---------------------------
def post_json(url: str, payload: dict, timeout: float = TIMEOUT_S) -> Tuple[int, dict]:
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.status, jload(resp.read())

# ---------------------------
# WAL helpers
# ---------------------------
def wal_append(line: str) -> None:
    """Append a line to coordinator WAL."""
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")

def replay_wal() -> None:
    """Replay WAL on startup to restore TX states."""
    try:
        with open(WAL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) < 2:
                    continue
                txid, state = parts[0], parts[1]
                with lock:
                    TX.setdefault(txid, {
                        "txid": txid,
                        "state": state,
                        "votes": {},
                        "decision": None,
                        "participants": list(PARTICIPANTS)
                    })
    except FileNotFoundError:
        pass

# ---------------------------
# Two-Phase Commit (2PC)
# ---------------------------
def two_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {
            "txid": txid, "protocol": "2PC", "state": "PREPARE_SENT",
            "op": op, "votes": {}, "decision": None,
            "participants": list(PARTICIPANTS), "ts": time.time()
        }

    wal_append(f"{txid} PREPARE_SENT {op}")

    votes = {}
    all_yes = True
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/prepare", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"
    with lock:
        TX[txid]["votes"] = votes
        TX[txid]["decision"] = decision
        TX[txid]["state"] = f"{decision}_SENT"

    wal_append(f"{txid} {decision}_SENT {votes}")

    endpoint = "/commit" if decision == "COMMIT" else "/abort"
    for p in PARTICIPANTS:
        retries = 3
        for i in range(retries):
            try:
                post_json(p.rstrip("/") + endpoint, {"txid": txid})
                break
            except Exception:
                time.sleep(0.5)
                if i == retries - 1:
                    print(f"[{NODE_ID}] Failed to send {endpoint} to {p} after {retries} retries")

    with lock:
        TX[txid]["state"] = "DONE"
    wal_append(f"{txid} DONE")

    return {"ok": True, "txid": txid, "protocol": "2PC", "decision": decision, "votes": votes}

# ---------------------------
# Three-Phase Commit (3PC)
# ---------------------------
def three_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {
            "txid": txid, "protocol": "3PC", "state": "CAN_COMMIT_SENT",
            "op": op, "votes": {}, "decision": None,
            "participants": list(PARTICIPANTS), "ts": time.time()
        }

    wal_append(f"{txid} CAN_COMMIT_SENT {op}")

    votes = {}
    all_yes = True
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/can_commit", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    with lock:
        TX[txid]["votes"] = votes

    if not all_yes:
        with lock:
            TX[txid]["decision"] = "ABORT"
            TX[txid]["state"] = "ABORT_SENT"
        wal_append(f"{txid} ABORT_SENT {votes}")
        for p in PARTICIPANTS:
            retries = 3
            for i in range(retries):
                try:
                    post_json(p.rstrip("/") + "/abort", {"txid": txid})
                    break
                except Exception:
                    time.sleep(0.5)
                    if i == retries - 1:
                        print(f"[{NODE_ID}] Failed to send /abort to {p} after {retries} retries")
        with lock:
            TX[txid]["state"] = "DONE"
        wal_append(f"{txid} DONE")
        return {"ok": True, "txid": txid, "protocol": "3PC", "decision": "ABORT", "votes": votes}

    with lock:
        TX[txid]["decision"] = "PRECOMMIT"
        TX[txid]["state"] = "PRECOMMIT_SENT"
    wal_append(f"{txid} PRECOMMIT_SENT")

    for p in PARTICIPANTS:
        retries = 3
        for i in range(retries):
            try:
                post_json(p.rstrip("/") + "/precommit", {"txid": txid})
                break
            except Exception:
                time.sleep(0.5)
                if i == retries - 1:
                    print(f"[{NODE_ID}] Failed to send /precommit to {p} after {retries} retries")

    time.sleep(2)  # make PRECOMMIT observable

    with lock:
        TX[txid]["decision"] = "COMMIT"
        TX[txid]["state"] = "DOCOMMIT_SENT"
    wal_append(f"{txid} DOCOMMIT_SENT")

    for p in PARTICIPANTS:
        retries = 3
        for i in range(retries):
            try:
                post_json(p.rstrip("/") + "/commit", {"txid": txid})
                break
            except Exception:
                time.sleep(0.5)
                if i == retries - 1:
                    print(f"[{NODE_ID}] Failed to send /commit to {p} after {retries} retries")

    with lock:
        TX[txid]["state"] = "DONE"
    wal_append(f"{txid} DONE")

    return {"ok": True, "txid": txid, "protocol": "3PC", "decision": "COMMIT", "votes": votes}

# ---------------------------
# HTTP Handler
# ---------------------------
class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {"ok": True, "node": NODE_ID, "port": PORT, "participants": PARTICIPANTS, "tx": TX})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = jload(raw)
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        if self.path == "/tx/start":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            protocol = str(body.get("protocol", "2PC")).upper()

            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return
            if protocol not in ("2PC", "3PC"):
                self._send(400, {"ok": False, "error": "protocol must be 2PC or 3PC"})
                return

            if protocol == "2PC":
                result = two_pc(txid, op)
            else:
                result = three_pc(txid, op)

            self._send(200, result)
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return

# ---------------------------
# Main
# ---------------------------
def main():
    global NODE_ID, PORT, PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True, help="Comma-separated participant base URLs (http://IP:PORT)")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    PARTICIPANTS = [p.strip() for p in args.participants.split(",") if p.strip()]

    # Replay WAL to restore transactions after crash
    replay_wal()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[{NODE_ID}] Coordinator listening on {args.host}:{args.port} participants={PARTICIPANTS}")
    server.serve_forever()

if __name__ == "__main__":
    main()
