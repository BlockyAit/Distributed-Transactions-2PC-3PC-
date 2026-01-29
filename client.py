#!/usr/bin/env python3
"""
Lab 4 Starter — Client for Coordinator (2PC/3PC)

Examples:
  python3 client.py --coord http://<COORD-IP>:8000 status
  python3 client.py --coord http://<COORD-IP>:8000 start TX1 2PC SET x 5
  python3 client.py --coord http://<COORD-IP>:8000 start TX2 3PC SET y 9
"""

from urllib import request
import argparse
import json
import sys

def jdump(obj): return json.dumps(obj).encode("utf-8")
def jload(b): return json.loads(b.decode("utf-8"))

def post_json(url: str, payload: dict, timeout: float = 2.0):
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.status, jload(resp.read())

def get_json(url: str, timeout: float = 2.0):
    with request.urlopen(url, timeout=timeout) as resp:
        return resp.status, jload(resp.read())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coord", required=True)
    ap.add_argument("cmd", choices=["start", "status"])
    ap.add_argument("txid", nargs="?")
    ap.add_argument("protocol", nargs="?")
    ap.add_argument("optype", nargs="?")
    ap.add_argument("key", nargs="?")
    ap.add_argument("value", nargs="?")
    args = ap.parse_args()

    base = args.coord.rstrip("/")

    if args.cmd == "status":
        s, obj = get_json(base + "/status")
        print(s)
        print(json.dumps(obj, indent=2))
        return

    if args.cmd == "start":
        if not (args.txid and args.protocol and args.optype and args.key):
            print("Usage: start <TXID> <2PC|3PC> SET <key> <value>")
            sys.exit(2)
        if args.optype.upper() != "SET":
            print("Only SET supported in starter.")
            sys.exit(2)

        payload = {
            "txid": args.txid,
            "protocol": args.protocol,
            "op": {"type":"SET", "key": args.key, "value": args.value if args.value is not None else ""}
        }
        s, obj = post_json(base + "/tx/start", payload)
        print(s)
        print(json.dumps(obj, indent=2))
        return

if __name__ == "__main__":
    main()
