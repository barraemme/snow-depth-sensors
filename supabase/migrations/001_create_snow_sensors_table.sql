-- Snow sensors table for storing daily snow depth measurements
-- Run this in your Supabase SQL Editor or as a migration

CREATE TABLE IF NOT EXISTS snow_sensors (
    id BIGSERIAL PRIMARY KEY,
    station_code TEXT NOT NULL,
    station_name TEXT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    elevation DOUBLE PRECISION,
    hs DOUBLE PRECISION NOT NULL,  -- Snow depth in cm
    hn DOUBLE PRECISION,           -- New snow in cm (24h)
    measurement_date DATE NOT NULL,
    source TEXT NOT NULL,          -- 'alto_adige' or 'trentino'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicate entries
    UNIQUE(station_code, measurement_date, source)
);

-- Index for efficient date-based queries
CREATE INDEX IF NOT EXISTS idx_snow_sensors_date ON snow_sensors(measurement_date);

-- Index for spatial queries (filtering by bounding box)
CREATE INDEX IF NOT EXISTS idx_snow_sensors_lat ON snow_sensors(latitude);
CREATE INDEX IF NOT EXISTS idx_snow_sensors_lon ON snow_sensors(longitude);

-- Index for source filtering
CREATE INDEX IF NOT EXISTS idx_snow_sensors_source ON snow_sensors(source);

-- Enable Row Level Security (optional, for public read access)
ALTER TABLE snow_sensors ENABLE ROW LEVEL SECURITY;

-- Policy for public read access (using anon key)
CREATE POLICY "Allow public read access" ON snow_sensors
    FOR SELECT
    USING (true);

-- Policy for authenticated insert/update (using service key)
CREATE POLICY "Allow service role insert" ON snow_sensors
    FOR INSERT
    WITH CHECK (true);

CREATE POLICY "Allow service role update" ON snow_sensors
    FOR UPDATE
    USING (true);

-- Comment on table
COMMENT ON TABLE snow_sensors IS 'Daily snow depth measurements from Alto Adige and Trentino weather stations';
COMMENT ON COLUMN snow_sensors.hs IS 'Snow depth (Höhe Schnee) in centimeters';
COMMENT ON COLUMN snow_sensors.hn IS 'New snow in last 24 hours in centimeters';
