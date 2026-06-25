#!/usr/bin/env python3
"""
Run the performanceV3 rebuild pipeline in order, replacing the manual Postman steps.

Pipeline (same order you run by hand):
    1. transform-daily-range?start=X&end=Y   raw landing JSON  -> daily parquets
    2. build-index                            all parquets      -> file_index.parquet
    3. refresh                                last 10 work days -> 10-day summary cache
    4. refresh-users                          all parquets      -> per-user caches

Runs against your LOCAL Functions host by default (http://localhost:7071). Start
it first in the project folder:
    func start                # or: func host start

Usage (PowerShell), in another terminal:
    python tools/run_performanceV3.py --start 2026-05-01 --end 2026-06-13

The local host does NOT enforce function keys, so no key is needed locally. The
function still reads/writes the real Azure Blob Storage + Key Vault, so make sure
you are signed in for DefaultAzureCredential (e.g. `az login`) before starting it.

Other options:
    --end            defaults to today (UTC) if omitted
    --force          re-transform days that already have a parquet (passed to step 1)
    --base-url       defaults to http://localhost:7071 (use the azurewebsites.net
                     URL only if you ever want to hit the deployed app)
    --key            function key — only needed if --base-url is the online app
                     (overrides FUNCTION_KEY env var)
    --only           run only some steps, comma-separated
                     (transform,index,refresh,users) — e.g. --only index,refresh
    --timeout        per-request timeout in seconds (default 600)
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_BASE_URL = "http://localhost:7071"  # local `func start` host
ONLINE_BASE_URL = "https://functionapp-python-pdf.azurewebsites.net"
ROUTE = "/api/performancev3/"

# (key, action, builds-query-params) — order matters: transform must run first.
STEPS = [
    ("transform", "transform-daily-range"),
    ("index", "build-index"),
    ("refresh", "refresh"),
    ("users", "refresh-users"),
]


def _valid_date(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}', use YYYY-MM-DD")


def call(base_url, action, key, params, timeout):
    """POST one action and return (http_status, parsed_json_or_text)."""
    url = base_url.rstrip("/") + ROUTE + action
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {"x-functions-key": key} if key else {}
    req = urllib.request.Request(url, data=b"", method="POST", headers=headers)
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        status = e.code
    except urllib.error.URLError as e:
        return None, f"connection error: {e.reason}", time.time() - started
    try:
        body = json.loads(body)
    except json.JSONDecodeError:
        pass
    return status, body, time.time() - started


def summarize(action, body):
    """Pull the interesting fields out of each action's JSON response."""
    if not isinstance(body, dict):
        return str(body)[:300]
    # success responses for these actions don't carry a 'status' key; if one is
    # present it means no_data / skipped — surface its message instead of Nones
    if body.get("status") in ("no_data", "skipped"):
        return f"{body['status']}: {body.get('message', '')}"
    if action == "transform-daily-range":
        return (f"range={body.get('range')} success={body.get('success')} "
                f"skipped={body.get('skipped')} no_data={body.get('no_data')} "
                f"total_days={body.get('total_days')}")
    if action == "build-index":
        return (f"declarations_indexed={body.get('total_declarations_indexed')} "
                f"source_files={body.get('source_files')}")
    if action == "refresh":
        return f"days_processed={body.get('days_processed')} users={body.get('users')}"
    if action == "refresh-users":
        return (f"processed={body.get('processed')} failed={body.get('failed')} "
                f"discovered={body.get('total_users_discovered')}")
    return json.dumps(body)[:300]


def main():
    # API responses may contain non-ASCII (e.g. a "→" in the range field); make
    # stdout/stderr tolerate it on Windows consoles instead of crashing.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    p = argparse.ArgumentParser(description="Run the performanceV3 rebuild pipeline.")
    p.add_argument("--start", type=_valid_date, help="first day to transform (YYYY-MM-DD)")
    p.add_argument("--end", type=_valid_date, default=dt.datetime.utcnow().date(),
                   help="last day to transform (YYYY-MM-DD, default: today UTC)")
    p.add_argument("--force", action="store_true", help="re-transform days that already have a parquet")
    p.add_argument("--key", default=os.environ.get("FUNCTION_KEY"),
                   help="function key — only needed when targeting the online app")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL,
                   help=f"default {DEFAULT_BASE_URL}; online app is {ONLINE_BASE_URL}")
    p.add_argument("--only", help="comma-separated subset of: transform,index,refresh,users")
    p.add_argument("--timeout", type=int, default=1800,
                   help="per-request timeout seconds (default 1800; refresh-users is heavy)")
    args = p.parse_args()

    is_local = urllib.parse.urlparse(args.base_url).hostname in ("localhost", "127.0.0.1")
    if not is_local and not args.key:
        sys.exit("ERROR: targeting a remote host but no function key. Set FUNCTION_KEY or pass --key.")

    selected = set(s.strip() for s in args.only.split(",")) if args.only else {k for k, _ in STEPS}
    unknown = selected - {k for k, _ in STEPS}
    if unknown:
        sys.exit(f"ERROR: unknown --only step(s): {', '.join(unknown)}")

    if "transform" in selected:
        if not args.start:
            sys.exit("ERROR: --start is required when running the transform step.")
        if args.start > args.end:
            sys.exit(f"ERROR: --start ({args.start}) is after --end ({args.end}).")

    print(f"Target : {args.base_url}{ROUTE}  ({'local' if is_local else 'REMOTE'})")
    print(f"Steps  : {', '.join(k for k, _ in STEPS if k in selected)}")
    if "transform" in selected:
        print(f"Range  : {args.start} -> {args.end}  (force={args.force})")
    print("=" * 70)

    for key, action in STEPS:
        if key not in selected:
            continue
        params = {}
        if action == "transform-daily-range":
            params = {"start": args.start.isoformat(), "end": args.end.isoformat()}
            if args.force:
                params["force"] = "true"

        print(f"\n> {action} ...", flush=True)
        status, body, elapsed = call(args.base_url, action, args.key, params, args.timeout)

        if status is None:
            sys.exit(f"[FAIL] {action}: {body} (after {elapsed:.0f}s)\nAborting - later steps depend on this one.")
        if status != 200:
            detail = body.get("error") if isinstance(body, dict) else body
            sys.exit(f"[FAIL] {action}: HTTP {status} - {detail} (after {elapsed:.0f}s)\n"
                     f"Aborting - later steps depend on this one.")

        print(f"[OK] {action}: HTTP 200 in {elapsed:.0f}s - {summarize(action, body)}")

    print("\n" + "=" * 70)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
