-- Draft schema (Workshop #1). Derived from docs/er_diagram.mmd; implemented and tested in Workshop #2.
-- All timestamps are stored in UTC (timestamptz).

CREATE TABLE source (
    source_id    text PRIMARY KEY,           -- nasa_glc, openmeteo_historical, openmeteo_forecast, nasa_lhasa
    name         text NOT NULL,
    url          text NOT NULL,
    format       text NOT NULL,
    license      text NOT NULL
);

CREATE TABLE ingestion_run (
    run_id        bigserial PRIMARY KEY,
    mode          text NOT NULL CHECK (mode IN ('historical', 'incremental')),
    source_id     text NOT NULL REFERENCES source,
    started       timestamptz NOT NULL DEFAULT now(),
    finished      timestamptz,
    status        text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    rows_in       integer DEFAULT 0,
    rows_new      integer DEFAULT 0,
    rows_updated  integer DEFAULT 0,
    rows_rejected integer DEFAULT 0
);

CREATE TABLE ingestion_state (
    source_id     text NOT NULL REFERENCES source,
    partition_key text NOT NULL,             -- e.g. grid cell + year, or 'all'
    watermark     text NOT NULL,             -- ETag, date, hour or issue time (see config/sources.yaml)
    last_success  timestamptz NOT NULL,
    PRIMARY KEY (source_id, partition_key)
);

CREATE TABLE event (
    event_id      text PRIMARY KEY,          -- '<source>:<natural key>'
    source        text NOT NULL REFERENCES source,
    event_type    text NOT NULL CHECK (event_type IN ('landslide', 'rainfall_daily', 'rainfall_hourly', 'landslide_nowcast')),
    "timestamp"   timestamptz NOT NULL,
    latitude      double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude     double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    severity      double precision,
    severity_unit text NOT NULL,
    raw_reference text NOT NULL,
    content_hash  text NOT NULL,
    loaded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX event_type_time_idx ON event (event_type, "timestamp");

CREATE TABLE event_group (
    group_id   bigint NOT NULL,
    event_id   text NOT NULL REFERENCES event,
    match_rule text NOT NULL,                -- e.g. 'glc_same_day_30km'
    PRIMARY KEY (group_id, event_id)
);

CREATE TABLE rejected_record (
    id        bigserial PRIMARY KEY,
    run_id    bigint NOT NULL REFERENCES ingestion_run,
    source    text NOT NULL,
    reason    text NOT NULL,
    payload   jsonb NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE analysis_cell (
    cell_id    text PRIMARY KEY,             -- 0.25 degree cell, e.g. 'c_+05.00_-075.50'
    min_lat    double precision NOT NULL,
    min_lon    double precision NOT NULL,
    max_lat    double precision NOT NULL,
    max_lon    double precision NOT NULL,
    centre_lat double precision NOT NULL,
    centre_lon double precision NOT NULL,
    department text
);

CREATE TABLE landslide (
    event_id              text PRIMARY KEY REFERENCES event,
    glc_event_id          integer NOT NULL UNIQUE,   -- duplicate key of the source
    title                 text,
    location_description  text,
    location_accuracy_km  double precision,          -- 'exact' -> 0, 'unknown' -> NULL
    time_known            boolean NOT NULL,
    landslide_category    text,
    landslide_trigger     text,
    landslide_size        text,
    landslide_setting     text,
    fatality_count        integer,
    injury_count          integer,
    country_name          text,
    admin_division_name   text,
    source_name           text,
    source_link           text,
    last_edited_date      timestamptz
);

CREATE TABLE rainfall_observation (
    event_id            text PRIMARY KEY REFERENCES event,
    cell_lat            double precision NOT NULL,
    cell_lon            double precision NOT NULL,
    elevation_m         double precision,
    period              text NOT NULL CHECK (period IN ('daily', 'hourly')),
    precipitation_mm    double precision NOT NULL CHECK (precipitation_mm >= 0),
    precipitation_hours double precision,
    soil_moisture_m3m3  double precision,
    is_forecast         boolean NOT NULL DEFAULT false,
    dataset             text NOT NULL CHECK (dataset IN ('era5_archive', 'forecast'))
);

CREATE TABLE lhasa_nowcast (
    event_id      text PRIMARY KEY REFERENCES event,
    issue_time    timestamptz NOT NULL,
    file_name     text NOT NULL,
    cell_id       text NOT NULL REFERENCES analysis_cell,
    hazard_max    double precision CHECK (hazard_max BETWEEN 0 AND 1),
    hazard_mean   double precision CHECK (hazard_mean BETWEEN 0 AND 1),
    hazard_p90    double precision CHECK (hazard_p90 BETWEEN 0 AND 1),
    valid_pixels  integer NOT NULL,
    etag          text,
    last_modified timestamptz,
    UNIQUE (issue_time, cell_id)
);
