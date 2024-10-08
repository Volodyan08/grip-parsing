import sys
import glob
import os
import numpy as np
from datetime import datetime

from grib_processing import parse_grib_files, calculate_wind_speed
from geometry_processing import filter_region, smooth_data, generate_wind_zones
from database import save_geojson_to_db

# Define constants
WIND_SPEED_THRESHOLDS = [x for x in range(20, 130, 5)]  # Wind speed thresholds in miles per hour
GAUSSIAN_SIGMA = 1  # Standard deviation for Gaussian kernel
USA_BOUNDS = {'lat_min': 24.5, 'lat_max': 49.5, 'lon_min': -125, 'lon_max': -66.5}  # Approximate bounds

import os
import psycopg2


def prepare_db():
    # SQL to create the table if it does not exist
    create_table = """
        CREATE TABLE IF NOT EXISTS storm_events (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        type VARCHAR(50),
        min_wind_speed INTEGER,
        max_wind_speed INTEGER,
        coordinates GEOMETRY(MULTIPOLYGON, 4326),
        geographical_name VARCHAR(255)
    );
    """

    # SQL to add the column if it does not exist
    add_column_table = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='storms' AND column_name='storm_event_id') THEN
            ALTER TABLE storms
            ADD COLUMN storm_event_id INTEGER;
            ALTER TABLE storms
            ADD CONSTRAINT fk_storm_event
              FOREIGN KEY (storm_event_id)
              REFERENCES storm_events(id);
        END IF;
    END $$;
    """

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Execute the table creation SQL
    cursor.execute(create_table)

    # Execute the column addition SQL
    cursor.execute(add_column_table)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


def main():
    prepare_db()
    # Get command-line arguments
    grib_file_path = sys.argv[1]
    date = sys.argv[2]
    time_stamp = sys.argv[3]

    print("="*90)
    print(f"Args: file_path - {grib_file_path}, date - {date}, timestamp - {time_stamp}")
    print("="*90)

    # Check if the GRIB2 file exists
    if not os.path.exists(grib_file_path):
        print(f"GRIB2 file {grib_file_path} not found.")
        return

    grib_files = [grib_file_path]

    # Parse GRIB2 files
    u, v, lats, lons = parse_grib_files(grib_files)

    # Adjust lons from 0°-360° to -180° to +180°
    lons = np.where(lons > 180, lons - 360, lons)

    # Calculate wind speed magnitude
    wind_speed = calculate_wind_speed(u, v)

    # Filter by the USA region
    wind_speed_filtered, lats_filtered, lons_filtered = filter_region(
        wind_speed, lats, lons, USA_BOUNDS)

    # Smooth the data
    wind_speed_smooth = smooth_data(wind_speed_filtered, GAUSSIAN_SIGMA)

    # Check wind speed data ranges
    print(f"Wind Speed Smooth Min: {np.nanmin(wind_speed_smooth)} mph")
    print(f"Wind Speed Smooth Max: {np.nanmax(wind_speed_smooth)} mph")

    # Generate wind zones
    features = generate_wind_zones(
        wind_speed_smooth, lats_filtered, lons_filtered, WIND_SPEED_THRESHOLDS)

    # Save to PostgreSQL database
    save_geojson_to_db(features, date, grib_file_path, time_stamp)
    print("Wind zone polygons have been saved to the database.")


if __name__ == "__main__":
    main()
