"""Unit tests for ArrivalPipeline: raw Kafka message dicts -> arrival events.

Exercises the exact payload shapes the ingester produces (protobuf JSON:
string uint64 timestamps, omitted optional fields) against a fake schedule
repository. No Spark, no Postgres.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from services.stream_processor.ground_truth import (
    ScheduledStop,
    TripSchedule,
    parse_gtfs_time,
)
from services.stream_processor.pipeline import ArrivalPipeline

SERVICE_DATE = date(2026, 6, 1)  # a Monday
WATERMARK = timedelta(minutes=30)


def _schedule(trip_id: str = "T1") -> TripSchedule:
    stops = tuple(
        ScheduledStop(
            stop_id=f"S{seq}",
            stop_sequence=seq,
            scheduled_arrival_ts=parse_gtfs_time(SERVICE_DATE, f"08:{(seq - 1) * 5:02d}:00"),
        )
        for seq in range(1, 5)
    )
    return TripSchedule(
        trip_id=trip_id, route_id="199", service_date=SERVICE_DATE, stops=stops
    )


class _FakeRepo:
    def __init__(self, schedules: dict[str, TripSchedule]) -> None:
        self._schedules = schedules
        self.get_calls = 0

    def get(self, trip_id: str, observed_at: datetime) -> TripSchedule | None:
        self.get_calls += 1
        return self._schedules.get(trip_id)


def _pipeline(trip_id: str = "T1") -> tuple[ArrivalPipeline, _FakeRepo]:
    repo = _FakeRepo({trip_id: _schedule(trip_id)})
    return ArrivalPipeline(repo, watermark=WATERMARK), repo  # type: ignore[arg-type]


def _epoch(schedule: TripSchedule, seq: int, *, offset_s: int = 0) -> int:
    stop = schedule.stop_at(seq)
    assert stop is not None
    return int(stop.scheduled_arrival_ts.timestamp()) + offset_s


def _vp(
    trip_id: str,
    seq: int | None,
    status: str | None,
    epoch: int | None,
) -> dict:
    """Build a vehicle_positions Kafka message as the ingester produces it."""
    payload: dict = {"trip": {"trip_id": trip_id}, "vehicle": {"id": "BUS123"}}
    if seq is not None:
        payload["current_stop_sequence"] = seq
    if status is not None:
        payload["current_status"] = status
    if epoch is not None:
        payload["timestamp"] = str(epoch)  # protobuf uint64 -> JSON string
    return {
        "envelope": {
            "source_feed": "vehicle_positions",
            "ingested_at": "2026-06-01T00:00:00+00:00",
            "ingester_version": "0.1.0",
            "feed_timestamp": epoch or 0,
        },
        "payload": payload,
    }


def _cancel(trip_id: str, epoch: int) -> dict:
    return {
        "envelope": {
            "source_feed": "trip_updates",
            "ingested_at": "2026-06-01T00:00:00+00:00",
            "ingester_version": "0.1.0",
            "feed_timestamp": epoch,
        },
        "payload": {
            "trip": {"trip_id": trip_id, "schedule_relationship": "CANCELED"},
            "timestamp": str(epoch),
        },
    }


def _mid_trip(schedule: TripSchedule) -> datetime:
    return schedule.stops[0].scheduled_arrival_ts + timedelta(minutes=5)


def test_position_record_produces_arrival_event() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    epoch = _epoch(schedule, 1, offset_s=90)

    events = pipeline.process_records(
        [_vp("T1", 1, "STOPPED_AT", epoch)], now=_mid_trip(schedule)
    )

    assert len(events) == 1
    assert events[0].trip_id == "T1"
    assert events[0].observed_delay_s == 90
    assert events[0].observed_delay_imputed is False


def test_missing_status_defaults_to_in_transit_to() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    # GTFS-RT: omitted current_status means IN_TRANSIT_TO. Heading to stop 3
    # with no status field imputes stops 1 and 2 but does not record stop 3.
    events = pipeline.process_records(
        [_vp("T1", 3, None, _epoch(schedule, 3))], now=_mid_trip(schedule)
    )

    assert [(e.stop_sequence, e.observed_delay_imputed) for e in events] == [
        (1, True),
        (2, True),
    ]


def test_state_persists_across_micro_batches() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    now = _mid_trip(schedule)

    first = pipeline.process_records(
        [_vp("T1", 1, "STOPPED_AT", _epoch(schedule, 1))], now=now
    )
    second = pipeline.process_records(
        [_vp("T1", 2, "STOPPED_AT", _epoch(schedule, 2))], now=now
    )

    assert [e.stop_sequence for e in first] == [1]
    # Batch 2 emits only stop 2 — stop 1 is remembered, not re-imputed.
    assert [e.stop_sequence for e in second] == [2]


def test_non_bus_trip_is_skipped() -> None:
    pipeline, repo = _pipeline()
    schedule = _schedule()

    events = pipeline.process_records(
        [_vp("TRAIN-1", 1, "STOPPED_AT", _epoch(schedule, 1))],
        now=_mid_trip(schedule),
    )

    assert events == []
    assert pipeline.active_trips == 0


def test_cancellation_suppresses_later_positions() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    now = _mid_trip(schedule)

    pipeline.process_records([_cancel("T1", _epoch(schedule, 1))], now=now)
    events = pipeline.process_records(
        [_vp("T1", 2, "STOPPED_AT", _epoch(schedule, 2))], now=now
    )

    assert events == []


def test_state_evicted_after_scheduled_end_plus_watermark() -> None:
    pipeline, repo = _pipeline()
    schedule = _schedule()
    now = _mid_trip(schedule)

    pipeline.process_records(
        [_vp("T1", 1, "STOPPED_AT", _epoch(schedule, 1))], now=now
    )
    assert pipeline.active_trips == 1

    scheduled_end = schedule.stops[-1].scheduled_arrival_ts
    after_expiry = scheduled_end + WATERMARK + timedelta(minutes=1)
    pipeline.process_records([], now=after_expiry)

    assert pipeline.active_trips == 0


def test_malformed_records_are_skipped() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    now = _mid_trip(schedule)
    no_trip = _vp("T1", 1, "STOPPED_AT", _epoch(schedule, 1))
    no_trip["payload"].pop("trip")
    no_timestamp = _vp("T1", 1, "STOPPED_AT", None)
    no_timestamp["envelope"]["feed_timestamp"] = 0
    empty: dict = {"envelope": {}, "payload": {}}

    events = pipeline.process_records([no_trip, no_timestamp, empty], now=now)

    assert events == []


def test_payload_timestamp_falls_back_to_feed_timestamp() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    epoch = _epoch(schedule, 1, offset_s=60)
    record = _vp("T1", 1, "STOPPED_AT", None)
    record["envelope"]["feed_timestamp"] = epoch

    events = pipeline.process_records([record], now=_mid_trip(schedule))

    assert len(events) == 1
    assert events[0].observed_delay_s == 60


def test_event_to_row_matches_arrival_columns() -> None:
    from services.stream_processor.pipeline import ARRIVAL_COLUMNS, event_to_row

    pipeline, _ = _pipeline()
    schedule = _schedule()
    events = pipeline.process_records(
        [_vp("T1", 1, "STOPPED_AT", _epoch(schedule, 1))], now=_mid_trip(schedule)
    )

    row = event_to_row(events[0])

    assert len(row) == len(ARRIVAL_COLUMNS)
    as_dict = dict(zip(ARRIVAL_COLUMNS, row, strict=True))
    assert as_dict["trip_id"] == "T1"
    assert as_dict["stop_sequence"] == 1
    # dt partition = UTC date of observed arrival. Scheduled 08:00 Brisbane on
    # 2026-06-01 is 22:00 UTC on 2026-05-31 — the partition must say 05-31.
    assert as_dict["dt"] == "2026-05-31"


def test_alert_records_are_ignored() -> None:
    pipeline, _ = _pipeline()
    schedule = _schedule()
    alert = {
        "envelope": {"source_feed": "alerts", "feed_timestamp": _epoch(schedule, 1)},
        "payload": {"header_text": {}},
    }

    events = pipeline.process_records([alert], now=_mid_trip(schedule))

    assert events == []
    assert pipeline.active_trips == 0
