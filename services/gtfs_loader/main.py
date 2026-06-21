"""CLI entrypoint for the static GTFS loader.

Usage::

    uv run python -m services.gtfs_loader.main            # use cached ZIP if present
    uv run python -m services.gtfs_loader.main --force    # force re-download

This is a one-shot batch job (run on demand / daily), not a long-running
service — so there is no health server or signal handling, unlike the ingester.
"""

from __future__ import annotations

import argparse
import logging
import sys

from services.gtfs_loader.config import GtfsLoaderConfig, GtfsLoaderConfigError
from services.gtfs_loader.loader import load


def main(argv: list[str] | None = None) -> int:
    """Run the loader. Returns a process exit code."""
    parser = argparse.ArgumentParser(description="Load static GTFS (bus subset) into Postgres.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download the GTFS ZIP even if a cached copy exists.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    try:
        config = GtfsLoaderConfig.from_env()
    except GtfsLoaderConfigError as exc:
        print(f"FATAL GTFS loader configuration error: {exc}", file=sys.stderr)
        return 1

    counts = load(config, force_download=args.force)

    total = sum(counts.values())
    print("\nGTFS load summary:")
    for table, n in counts.items():
        print(f"  {table:24} {n:>10,}")
    print(f"  {'TOTAL':24} {total:>10,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
