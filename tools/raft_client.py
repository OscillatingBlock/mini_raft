#!/usr/bin/env python3
"""Minimal Raft client: speaks length-prefixed JSON Codec over TCP.

Protocol (see src/codec.rs, src/network.rs):
 1. connect to a node's client_listener_address
 2. server sends {"HelloType": {"id": N}} first
 3. client sends {"HelloClientType": {"id": <u32>}}
 4. client sends {"ClientRequestType": {"command": {"Set": {"key": k, "value": v}},
                                       "client_id": <u32>, "request_id": <u32>}}
 5. leader replies {"ClientResponseType": {"SetSuccess": {"request_id": N}}}
    followers currently drop non-leader requests (timeout -> try next node).
"""
import argparse
import json
import random
import socket
import struct
import sys


def send_msg(sock, obj):
    payload = json.dumps(obj).encode()
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed during recv")
        buf += chunk
    return buf


def recv_msg(sock):
    hdr = recv_exact(sock, 4)
    (ln,) = struct.unpack(">I", hdr)
    payload = recv_exact(sock, ln)
    return json.loads(payload)


def try_node(addr, key, value, client_id, request_id, timeout=4.0):
    host, port = addr.rsplit(":", 1)
    sock = socket.create_connection((host, int(port)), timeout=timeout)
    sock.settimeout(timeout)
    try:
        hello = recv_msg(sock)  # server HelloType first
        # print(f"[{addr}] hello: {hello}", file=sys.stderr)
        send_msg(sock, {"HelloClientType": {"id": client_id}})
        send_msg(
            sock,
            {
                "ClientRequestType": {
                    "command": {"Set": {"key": key, "value": value}},
                    "client_id": client_id,
                    "request_id": request_id,
                }
            },
        )
        resp = recv_msg(sock)
        return resp
    finally:
        sock.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True, help="comma-separated client addrs")
    ap.add_argument("--key", required=True)
    ap.add_argument("--value", required=True)
    ap.add_argument("--client-id", type=int, default=random.randint(1, 100000))
    ap.add_argument("--request-id", type=int, default=random.randint(1, 100000))
    args = ap.parse_args()

    nodes = [n.strip() for n in args.nodes.split(",")]
    last_err = None
    for addr in nodes:
        try:
            resp = try_node(addr, args.key, args.value, args.client_id, args.request_id)
            print(f"[{addr}] -> {json.dumps(resp)}")
            # success if SetSuccess with matching request_id
            try:
                inner = resp.get("ClientResponseType", {})
                if "SetSuccess" in inner and inner["SetSuccess"].get("request_id") == args.request_id:
                    print(f"SUCCESS via {addr}")
                    return 0
            except Exception:
                pass
            last_err = f"unexpected response from {addr}"
        except Exception as e:
            print(f"[{addr}] failed: {e}", file=sys.stderr)
            last_err = str(e)
    print(f"FAILED: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
