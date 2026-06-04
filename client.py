from __future__ import annotations
import argparse
import json
import sys
import httpx

DEFAULT_URL = "http://localhost:8001"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI client for the Distributed Key-Value Store"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"Base URL of the KV node (default: {DEFAULT_URL})",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    put_parser = subparsers.add_parser("put", help="Store a key-value pair")
    put_parser.add_argument("key", help="Key to store")
    put_parser.add_argument("value", help="Value to store")
    get_parser = subparsers.add_parser("get", help="Retrieve a value")
    get_parser.add_argument("key", help="Key to look up")
    del_parser = subparsers.add_parser("delete", help="Delete a key")
    del_parser.add_argument("key", help="Key to delete")
    subparsers.add_parser("health", help="Check node health")
    subparsers.add_parser("metrics", help="View node metrics")
    subparsers.add_parser("cluster", help="View cluster peer status")
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    base = args.url.rstrip("/")
    try:
        with httpx.Client(timeout=5.0) as client:
            if args.command == "put":
                resp = client.put(f"{base}/kv/{args.key}", json={"value": args.value})
            elif args.command == "get":
                resp = client.get(f"{base}/kv/{args.key}")
            elif args.command == "delete":
                resp = client.delete(f"{base}/kv/{args.key}")
            elif args.command == "health":
                resp = client.get(f"{base}/health")
            elif args.command == "metrics":
                resp = client.get(f"{base}/metrics")
            elif args.command == "cluster":
                resp = client.get(f"{base}/cluster")
            else:
                parser.print_help()
                sys.exit(1)
            if resp.status_code >= 400:
                print(f"❌ Error ({resp.status_code}):")
                try:
                    print(json.dumps(resp.json(), indent=2))
                except Exception:
                    print(resp.text)
                sys.exit(1)
            else:
                print(json.dumps(resp.json(), indent=2))
    except httpx.ConnectError:
        print(f"❌ Cannot connect to {base} — is the node running?")
        sys.exit(1)


if __name__ == "__main__":
    main()
