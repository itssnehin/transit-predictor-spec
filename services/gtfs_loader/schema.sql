-- Static GTFS schema (bus subset) for the Transit Predictor.
--
-- Loaded by services/gtfs_loader. The stream processor (Phase 2) joins live
-- vehicle positions against gtfs_stop_times to derive scheduled arrival times.
--
-- Design notes:
--   * We DROP + CREATE on every load. The static GTFS is refreshed ~weekly by
--     TransLink; a full reload is simplest and the dataset is small once
--     filtered to buses. No foreign-key constraints — this is an analytical
--     load, and FKs would only slow the bulk COPY and complicate load order.
--   * arrival_time / departure_time are TEXT, not TIME. GTFS permits values
--     past 24:00:00 (e.g. "25:30:00" = 01:30 the next calendar day, still part
--     of the *service* day). A TIME column would reject those. The stream
--     processor parses them relative to the trip's service date.

DROP TABLE IF EXISTS gtfs_stop_times;
DROP TABLE IF EXISTS gtfs_trips;
DROP TABLE IF EXISTS gtfs_routes;
DROP TABLE IF EXISTS gtfs_stops;
DROP TABLE IF EXISTS gtfs_calendar;
DROP TABLE IF EXISTS gtfs_calendar_dates;

CREATE TABLE gtfs_routes (
    route_id         TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name  TEXT,
    route_type       INTEGER NOT NULL
);

CREATE TABLE gtfs_trips (
    trip_id       TEXT PRIMARY KEY,
    route_id      TEXT NOT NULL,
    service_id    TEXT NOT NULL,
    trip_headsign TEXT,
    direction_id  SMALLINT
);

CREATE TABLE gtfs_stops (
    stop_id   TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_lat  DOUBLE PRECISION,
    stop_lon  DOUBLE PRECISION
);

CREATE TABLE gtfs_stop_times (
    trip_id        TEXT NOT NULL,
    stop_sequence  INTEGER NOT NULL,
    stop_id        TEXT NOT NULL,
    arrival_time   TEXT,
    departure_time TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE gtfs_calendar (
    service_id TEXT PRIMARY KEY,
    monday     SMALLINT,
    tuesday    SMALLINT,
    wednesday  SMALLINT,
    thursday   SMALLINT,
    friday     SMALLINT,
    saturday   SMALLINT,
    sunday     SMALLINT,
    start_date TEXT,
    end_date   TEXT
);

CREATE TABLE gtfs_calendar_dates (
    service_id     TEXT NOT NULL,
    date           TEXT NOT NULL,
    exception_type SMALLINT NOT NULL,
    PRIMARY KEY (service_id, date)
);

-- Indexes for the stream processor's hot-path lookups:
--   "give me every scheduled stop for trip X, in order"
--   "give me every trip that calls at stop Y"
CREATE INDEX idx_stop_times_trip ON gtfs_stop_times (trip_id);
CREATE INDEX idx_stop_times_stop ON gtfs_stop_times (stop_id);
CREATE INDEX idx_trips_route     ON gtfs_trips (route_id);
CREATE INDEX idx_trips_service   ON gtfs_trips (service_id);
