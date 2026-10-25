CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS silver.trips (
 trip_id TEXT PRIMARY KEY,
 trip_start TIMESTAMP NOT NULL,
 trip_end TIMESTAMP NOT NULL,
 trip_date DATE NOT NULL,
 trip_seconds DOUBLE PRECISION NOT NULL CHECK (trip_seconds >= 0),
 trip_miles DOUBLE PRECISION NOT NULL CHECK (trip_miles >= 0),
 fare DOUBLE PRECISION NOT NULL CHECK (fare >= 0),
 tips DOUBLE PRECISION,
 trip_total DOUBLE PRECISION NOT NULL CHECK (trip_total >= 0),
 pickup_community_area INTEGER,
 dropoff_community_area INTEGER,
 payment_type TEXT,
 company TEXT,
 source_run_id TEXT NOT NULL,
 updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS silver_trips_date_idx ON silver.trips(trip_date);

CREATE TABLE IF NOT EXISTS gold.dim_date (
 date_key INTEGER PRIMARY KEY, full_date DATE UNIQUE NOT NULL,
 year INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL, day_of_week INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.dim_location (
 location_key INTEGER PRIMARY KEY, community_area INTEGER UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
 payment_type_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, payment_type TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.dim_company (
 company_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, company TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.fact_trips (
 trip_id TEXT PRIMARY KEY,
 date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
 pickup_location_key INTEGER REFERENCES gold.dim_location(location_key),
 dropoff_location_key INTEGER REFERENCES gold.dim_location(location_key),
 payment_type_key INTEGER REFERENCES gold.dim_payment_type(payment_type_key),
 company_key INTEGER REFERENCES gold.dim_company(company_key),
 trip_seconds DOUBLE PRECISION NOT NULL,
 trip_miles DOUBLE PRECISION NOT NULL,
 fare DOUBLE PRECISION NOT NULL,
 tips DOUBLE PRECISION,
 trip_total DOUBLE PRECISION NOT NULL,
 source_run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fact_trips_date_idx ON gold.fact_trips(date_key);
CREATE INDEX IF NOT EXISTS fact_trips_pickup_idx ON gold.fact_trips(pickup_location_key);

CREATE TABLE IF NOT EXISTS data_quality.check_results (
 id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, stage TEXT NOT NULL,
 check_name TEXT NOT NULL, success BOOLEAN NOT NULL,
 observed_value TEXT, details JSONB, checked_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
 run_id TEXT NOT NULL, task_name TEXT NOT NULL, status TEXT NOT NULL,
 input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
 started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ DEFAULT now(),
 duration_seconds DOUBLE PRECISION, error_message TEXT,
 PRIMARY KEY (run_id,task_name)
);

CREATE OR REPLACE VIEW gold.v_daily_revenue AS
 SELECT d.full_date AS trip_date, count(*) AS trip_count,
 round(sum(f.trip_total)::numeric,2) AS total_revenue,
 round(avg(f.trip_total)::numeric,2) AS avg_ticket
 FROM gold.fact_trips f JOIN gold.dim_date d USING(date_key) GROUP BY d.full_date;
CREATE OR REPLACE VIEW gold.v_trip_statistics AS
 SELECT d.full_date AS trip_date, count(*) AS trip_count,
 round((avg(f.trip_seconds)/60)::numeric,2) AS avg_duration_minutes,
 round(avg(f.trip_miles)::numeric,2) AS avg_distance_miles,
 round(avg(f.fare)::numeric,2) AS avg_fare
 FROM gold.fact_trips f JOIN gold.dim_date d USING(date_key) GROUP BY d.full_date;
CREATE OR REPLACE VIEW gold.v_pickup_zones AS
 SELECT l.community_area AS pickup_community_area, count(*) AS trip_count,
 round(sum(f.trip_total)::numeric,2) AS total_revenue
 FROM gold.fact_trips f JOIN gold.dim_location l ON l.location_key=f.pickup_location_key
 GROUP BY l.community_area;
CREATE OR REPLACE VIEW gold.v_payment_methods AS
 SELECT p.payment_type, count(*) AS trip_count, round(sum(f.trip_total)::numeric,2) AS total_revenue
 FROM gold.fact_trips f JOIN gold.dim_payment_type p USING(payment_type_key) GROUP BY p.payment_type;
