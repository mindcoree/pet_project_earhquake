CREATE SCHEMA stg;
CREATE SCHEMA ods;
CREATE SCHEMA dm;

-- STG: без PK и ограничений для быстрой и надежной загрузки сырых данных.
CREATE TABLE stg.earthquake_raw
(
    loaded_at         timestamp default now(),
    time              text,
    latitude          text,
    longitude         text,
    depth             text,
    mag               text,
    mag_type          text,
    nst               text,
    gap               text,
    dmin              text,
    rms               text,
    net               text,
    id                text,
    updated           text,
    place             text,
    type              text,
    horizontal_error  text,
    depth_error       text,
    mag_error         text,
    mag_nst           text,
    status            text,
    location_source   text,
    mag_source        text
);

CREATE TABLE ods.earthquakes (
    id                VARCHAR(50) PRIMARY KEY,
    event_time        TIMESTAMP WITH TIME ZONE,
    updated_at        TIMESTAMP WITH TIME ZONE,
    latitude          NUMERIC(12, 8),
    longitude         NUMERIC(12, 8),
    depth             NUMERIC(10, 4),
    mag               NUMERIC(5, 2),
    mag_type          VARCHAR(20),
    nst               INTEGER,
    gap               NUMERIC(6, 2),
    dmin              NUMERIC(8, 4),
    rms               NUMERIC(6, 3),
    horizontal_error  NUMERIC(10, 5),
    depth_error       NUMERIC(10, 5),
    mag_error         NUMERIC(10, 5),
    mag_nst           INTEGER,
    net               VARCHAR(10),
    place             TEXT,
    event_type        VARCHAR(50),
    status            VARCHAR(20),
    location_source   VARCHAR(10),
    mag_source        VARCHAR(10),
    region            TEXT,
    ods_loaded_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dm.dim_region (
    region_id     SERIAL PRIMARY KEY,
    region_name   TEXT NOT NULL UNIQUE
);

CREATE TABLE dm.dim_time (
    time_id      SERIAL PRIMARY KEY,
    event_time   TIMESTAMPTZ NOT NULL UNIQUE,
    year         INT,
    month        INT,
    day          INT,
    hour         INT,
    quarter      INT,
    day_of_week  VARCHAR(10),
    is_weekend   BOOLEAN
);

CREATE TABLE dm.dim_location (
    location_id  SERIAL PRIMARY KEY,
    region_id    INT REFERENCES dm.dim_region(region_id),
    latitude     NUMERIC(12,8),
    longitude    NUMERIC(12,8),
    place        TEXT,
    UNIQUE(latitude, longitude, place)
);

CREATE TABLE dm.dim_magnitude (
    mag_type_id   SERIAL PRIMARY KEY,
    mag_type      VARCHAR(20) NOT NULL,
    mag_source    VARCHAR(10) NOT NULL,
    UNIQUE (mag_type, mag_source)
);

CREATE TABLE dm.dim_network (
    network_id       SERIAL PRIMARY KEY,
    net              VARCHAR(10) NOT NULL,
    location_source  VARCHAR(10) NOT NULL,
    UNIQUE (net, location_source)
);

CREATE TABLE dm.dim_status (
    status_id  SERIAL PRIMARY KEY,
    status     VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE dm.dim_event_type (
    event_type_id  SERIAL PRIMARY KEY,
    event_type     VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dm.fact_earthquakes (
    id               VARCHAR(50) PRIMARY KEY,
    time_id          INT REFERENCES dm.dim_time(time_id),
    location_id      INT REFERENCES dm.dim_location(location_id),
    mag_type_id      INT REFERENCES dm.dim_magnitude(mag_type_id),
    network_id       INT REFERENCES dm.dim_network(network_id),
    status_id        INT REFERENCES dm.dim_status(status_id),
    event_type_id    INT REFERENCES dm.dim_event_type(event_type_id),
    mag              NUMERIC(5,2),
    depth            NUMERIC(10,4),
    nst              INT,
    gap              NUMERIC(6,2),
    dmin             NUMERIC(8,4),
    rms              NUMERIC(6,3),
    horizontal_error NUMERIC(10,5),
    depth_error      NUMERIC(10,5),
    mag_error        NUMERIC(10,5),
    mag_nst          INT,
    dwh_loaded_at    TIMESTAMP DEFAULT NOW()
);
