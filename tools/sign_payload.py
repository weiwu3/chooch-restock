#!/usr/bin/env python3
"""Generate the authentication headers used by the assessment webhook."""

import argparse
import hashlib
import hmac
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("payload", type=Path)
    parser.add_argument("secret")
    parser.add_argument("--timestamp", type=int, default=None)
    args = parser.parse_args()

    timestamp = args.timestamp if args.timestamp is not None else int(time.time())
    body = args.payload.read_bytes()
    message = str(timestamp).encode("ascii") + b"." + body
    digest = hmac.new(args.secret.encode("utf-8"), message, hashlib.sha256).hexdigest()

    print(f"X-Webhook-Timestamp: {timestamp}")
    print(f"X-Webhook-Signature: v1={digest}")


if __name__ == "__main__":
    main()

