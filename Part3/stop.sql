DROP TABLE IF EXISTS trip CASCADE;
DROP TABLE IF EXISTS breadcrumb;
DROP TABLE IF EXISTS stop_events;
DROP VIEW IF EXISTS trip_full_view;

-- 1. Trip table
CREATE TABLE trip (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT,
    vehicle_id TEXT,
    service_key TEXT,
    direction TEXT
);

-- 2. Breadcrumb table
CREATE TABLE breadcrumb (
    trip_id TEXT,
    tstamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    speed FLOAT
);

-- 3. Stop Events table
CREATE TABLE stop_events (
    vehicle_number TEXT,
    trip_id TEXT,
    stop_time TEXT,
    leave_time TEXT,
    arrive_time TEXT,
    route_id TEXT,
    direction TEXT,
    service_key TEXT,
    maximum_speed TEXT,
    train_mileage TEXT,
    dwell TEXT,
    location_id TEXT,
    lift TEXT,
    ons TEXT,
    offs TEXT,
    estimated_load TEXT
);

-- 4. SQL VIEW to integrate all data
CREATE OR REPLACE VIEW trip_full_view AS
SELECT
    t.trip_id,
    t.vehicle_id,
    t.route_id,
    t.service_key,
    t.direction,

    -- Breadcrumb data
    b.tstamp,
    b.latitude,
    b.longitude,
    b.speed,

    -- Stop Events data
    s.stop_time,
    s.leave_time,
    s.arrive_time,
    s.location_id,
    s.dwell,
    s.ons,
    s.offs,
    s.estimated_load,
    s.maximum_speed,
    s.train_mileage,
    s.lift

FROM trip t
LEFT JOIN breadcrumb b ON t.trip_id = b.trip_id
LEFT JOIN stop_events s ON t.trip_id = s.trip_id;
