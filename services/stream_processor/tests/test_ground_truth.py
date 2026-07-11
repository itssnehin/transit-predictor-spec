"""Unit tests for the ground-truth derivation algorithm.

Pure Python — no Spark, no containers. Each test is a hand-crafted trip
scenario from the edge-case catalogue in specs/STREAM_PROCESSOR.md and
docs/DATA.md.

Fixture geography: a 4-stop trip on route "199" scheduled at 08:00, 08:05,
08:10, 08:15 Brisbane time on Monday 2026-06-01.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from services.stream_processor.ground_truth import (
    BRISBANE_TZ,
    IN_TRANSIT_TO,
    INCOMING_AT,
    STOPPED_AT,
    Observation,
    ScheduledStop,
    TripSchedule,
    TripTracker,
    derive_arrivals,
    parse_gtfs_time,
)

SERVICE_DATE = date(2026, 6, 1)  # a Monday


def _schedule(n_stops: int = 4) -> TripSchedule:
    stops = tuple(
        ScheduledStop(
            stop_id=f"S{seq}",
            stop_sequence=seq,
            scheduled_arrival_ts=parse_gtfs_time(SERVICE_DATE, f"08:{(seq - 1) * 5:02d}:00"),
        )
        for seq in range(1, n_stops + 1)
    )
    return TripSchedule(
        trip_id="T1", route_id="199", service_date=SERVICE_DATE, stops=stops
    )


def _obs(
    sequence: int | None,
    status: str | None,
    at: datetime,
) -> Observation:
    return Observation(
        current_stop_sequence=sequence,
        current_status=status,
        timestamp=at,
        ingested_at=at + timedelta(seconds=2),
    )


def _sched_ts(schedule: TripSchedule, seq: int) -> datetime:
    stop = schedule.stop_at(seq)
    assert stop is not None
    return stop.scheduled_arrival_ts


# ---------------------------------------------------------------------------
# GTFS time parsing
# ---------------------------------------------------------------------------


def test_parse_gtfs_time_is_brisbane_local() -> None:
    ts = parse_gtfs_time(SERVICE_DATE, "08:00:00")
    # Brisbane is UTC+10 (no DST): 08:00 local == 22:00 UTC the previous day.
    assert ts == datetime(2026, 5, 31, 22, 0, 0, tzinfo=UTC)


def test_parse_gtfs_time_past_midnight() -> None:
    ts = parse_gtfs_time(SERVICE_DATE, "25:30:00")
    local = ts.astimezone(BRISBANE_TZ)
    # 25:30 on the 2026-06-01 service day = 01:30 on 2026-06-02 wall clock.
    assert (local.year, local.month, local.day, local.hour, local.minute) == (2026, 6, 2, 1, 30)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_all_stops_observed() -> None:
    schedule = _schedule()
    observations = [
        _obs(seq, STOPPED_AT, _sched_ts(schedule, seq) + timedelta(seconds=120))
        for seq in (1, 2, 3, 4)
    ]

    events = derive_arrivals(schedule, observations)

    assert [e.stop_sequence for e in events] == [1, 2, 3, 4]
    assert all(e.observed_delay_s == 120 for e in events)
    assert all(e.observed_delay_imputed is False for e in events)
    assert [e.stop_id for e in events] == ["S1", "S2", "S3", "S4"]


def test_long_dwell_uses_first_stopped_at() -> None:
    schedule = _schedule()
    first = _sched_ts(schedule, 1) + timedelta(seconds=60)
    tracker = TripTracker(schedule=schedule)

    events = tracker.observe(_obs(1, STOPPED_AT, first))
    events += tracker.observe(_obs(1, STOPPED_AT, first + timedelta(seconds=45)))

    assert len(events) == 1
    assert events[0].observed_delay_s == 60  # the first record's time won


def test_early_arrival_gives_negative_delay() -> None:
    schedule = _schedule()
    events = derive_arrivals(
        schedule, [_obs(1, STOPPED_AT, _sched_ts(schedule, 1) - timedelta(seconds=90))]
    )
    assert events[0].observed_delay_s == -90


# ---------------------------------------------------------------------------
# Imputation (skipped / unobserved stops)
# ---------------------------------------------------------------------------


def test_in_transit_to_imputes_earlier_unrecorded_stops() -> None:
    schedule = _schedule()
    at = _sched_ts(schedule, 3)
    events = derive_arrivals(schedule, [_obs(3, IN_TRANSIT_TO, at)])

    # Heading to stop 3 with nothing recorded: stops 1 and 2 were passed.
    assert [(e.stop_sequence, e.observed_delay_imputed) for e in events] == [
        (1, True),
        (2, True),
    ]
    assert all(e.observed_arrival_ts == at for e in events)


def test_stopped_at_jump_imputes_intermediates_and_records_real_arrival() -> None:
    schedule = _schedule()
    t1 = _sched_ts(schedule, 1) + timedelta(seconds=30)
    t3 = _sched_ts(schedule, 3) + timedelta(seconds=30)

    events = derive_arrivals(
        schedule,
        [_obs(1, STOPPED_AT, t1), _obs(3, STOPPED_AT, t3)],
    )

    by_seq = {e.stop_sequence: e for e in events}
    assert set(by_seq) == {1, 2, 3}
    assert by_seq[1].observed_delay_imputed is False
    assert by_seq[2].observed_delay_imputed is True  # skipped, inferred
    assert by_seq[3].observed_delay_imputed is False


def test_incoming_at_does_not_record_the_target_stop() -> None:
    schedule = _schedule()
    events = derive_arrivals(
        schedule, [_obs(2, INCOMING_AT, _sched_ts(schedule, 2))]
    )
    # Approaching stop 2 proves stop 1 was passed, but 2 itself is not arrived.
    assert [e.stop_sequence for e in events] == [1]
    assert events[0].observed_delay_imputed is True


# ---------------------------------------------------------------------------
# Edge cases: backtracking, cancellation, malformed records
# ---------------------------------------------------------------------------


def test_backtrack_warns_and_stays_idempotent(caplog) -> None:
    schedule = _schedule()
    tracker = TripTracker(schedule=schedule)
    t = _sched_ts(schedule, 3)

    tracker.observe(_obs(3, IN_TRANSIT_TO, t))  # imputes 1, 2
    with caplog.at_level(logging.WARNING):
        repeat = tracker.observe(_obs(2, IN_TRANSIT_TO, t + timedelta(seconds=10)))

    assert repeat == []  # stop 1 already recorded; nothing re-emitted
    assert any("Backtrack" in r.message for r in caplog.records)


def test_cancelled_trip_emits_nothing_further() -> None:
    schedule = _schedule()
    tracker = TripTracker(schedule=schedule)
    t1 = _sched_ts(schedule, 1)

    before = tracker.observe(_obs(1, STOPPED_AT, t1))
    tracker.cancel()
    after = tracker.observe(_obs(2, STOPPED_AT, _sched_ts(schedule, 2)))

    assert len(before) == 1
    assert after == []
    assert tracker.finalize() == []


def test_unknown_status_is_ignored(caplog) -> None:
    schedule = _schedule()
    with caplog.at_level(logging.WARNING):
        events = derive_arrivals(
            schedule, [_obs(1, "TELEPORTING", _sched_ts(schedule, 1))]
        )
    assert events == []
    assert any("Unknown current_status" in r.message for r in caplog.records)


def test_missing_sequence_or_status_is_skipped() -> None:
    schedule = _schedule()
    t = _sched_ts(schedule, 1)
    assert derive_arrivals(schedule, [_obs(None, STOPPED_AT, t)]) == []
    assert derive_arrivals(schedule, [_obs(1, None, t)]) == []


def test_stopped_at_unscheduled_sequence_warns_but_imputes_earlier(caplog) -> None:
    schedule = _schedule(n_stops=4)
    with caplog.at_level(logging.WARNING):
        events = derive_arrivals(
            schedule, [_obs(9, STOPPED_AT, _sched_ts(schedule, 4))]
        )
    # Sequence 9 is not in the schedule: no real arrival for it, but all four
    # scheduled stops are before it and get imputed.
    assert [e.stop_sequence for e in events] == [1, 2, 3, 4]
    assert all(e.observed_delay_imputed for e in events)
    assert any("unscheduled sequence" in r.message for r in caplog.records)


def test_did_not_observe_tail_stops_emit_nothing() -> None:
    schedule = _schedule()
    # Vehicle seen at stop 1, then silence: stops 2-4 have no pass evidence.
    events = derive_arrivals(
        schedule, [_obs(1, STOPPED_AT, _sched_ts(schedule, 1))]
    )
    assert [e.stop_sequence for e in events] == [1]


# ---------------------------------------------------------------------------
# Event construction details
# ---------------------------------------------------------------------------


def test_event_ids_are_deterministic_across_reprocessing() -> None:
    schedule = _schedule()
    obs = [_obs(1, STOPPED_AT, _sched_ts(schedule, 1))]

    first = derive_arrivals(schedule, obs)
    second = derive_arrivals(schedule, obs)  # simulated Kafka replay

    assert first[0].event_id == second[0].event_id


def test_temporal_features_use_brisbane_local_time() -> None:
    schedule = _schedule()
    events = derive_arrivals(
        schedule, [_obs(1, STOPPED_AT, _sched_ts(schedule, 1))]
    )
    # Scheduled 08:00 Monday Brisbane — not the 22:00-Sunday it is in UTC.
    assert events[0].day_of_week == 0
    assert events[0].hour_of_day == 8


def test_derive_arrivals_sorts_observations_by_timestamp() -> None:
    schedule = _schedule()
    t1 = _sched_ts(schedule, 1)
    t2 = _sched_ts(schedule, 2)

    # Delivered out of order (micro-batch boundary effect).
    events = derive_arrivals(
        schedule,
        [_obs(2, STOPPED_AT, t2), _obs(1, STOPPED_AT, t1)],
    )

    by_seq = {e.stop_sequence: e for e in events}
    # Stop 1 was processed first after sorting, so it is a real observation,
    # not an imputation triggered by the stop-2 record.
    assert by_seq[1].observed_delay_imputed is False
    assert by_seq[2].observed_delay_imputed is False
