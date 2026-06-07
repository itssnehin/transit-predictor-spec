"""Phase 0 smoke test: poll TransLink vehicle positions once and report the count.

No Kafka, no persistence. This exists to confirm the live GTFS-Realtime feed is
reachable and decodable before the full ingester (Phase 1, see specs/INGESTER.md)
is built. Fetch, parse, and count are kept separate so the decode logic is unit
testable without a network call.
"""

from __future__ import annotations

import os
import sys

import httpx
from google.transit import gtfs_realtime_pb2

DEFAULT_VEHICLE_POSITIONS_URL = (
    "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions"
)
USER_AGENT = "transit-predictor/0.1 (snehin.kukreja@gmail.com)"
REQUEST_TIMEOUT_S = 15.0


def fetch_feed(url: str, *, timeout: float = REQUEST_TIMEOUT_S) -> bytes:
    """Fetch the raw protobuf bytes for a GTFS-Realtime feed.

    Args:
        url: Full feed URL.
        timeout: Per-request timeout in seconds.

    Returns:
        The raw, protobuf-encoded response body.

    Raises:
        httpx.HTTPError: On connection failure, timeout, or non-2xx status.
    """
    response = httpx.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    return response.content


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Decode raw protobuf bytes into a GTFS-Realtime ``FeedMessage``."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    return feed


def count_vehicle_entities(feed: gtfs_realtime_pb2.FeedMessage) -> int:
    """Count feed entities that carry a vehicle position."""
    return sum(1 for entity in feed.entity if entity.HasField("vehicle"))


def main() -> int:
    """Poll the vehicle positions feed once and print the record count.

    Returns:
        Process exit code: 0 on success, 1 if the feed could not be fetched.
    """
    url = os.environ.get("TRANSLINK_VEHICLE_POSITIONS_URL", DEFAULT_VEHICLE_POSITIONS_URL)
    try:
        raw = fetch_feed(url)
    except httpx.HTTPError as exc:
        print(f"Failed to fetch feed from {url}: {exc}", file=sys.stderr)
        return 1

    feed = parse_feed(raw)
    count = count_vehicle_entities(feed)
    print(f"Fetched: {url}")
    print(f"Feed timestamp: {feed.header.timestamp}")
    print(f"Vehicle position records: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
