import psycopg2
from psycopg2.extras import execute_values
import os
from shapely.geometry import shape, mapping, MultiPolygon, Polygon
from shapely.ops import unary_union
import json
from datetime import datetime
from save_storm_events import save_storm_event


def save_geojson_to_db(features, data_date, file_path, time_stamp):
    # Database connection parameters
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )
    cursor = conn.cursor()

    data_datetime = datetime.strptime(f"{data_date} {time_stamp}", "%Y%m%d %H")
    original_date = data_datetime.strftime("%d.%m.%Y")
    filename = os.path.basename(file_path)
    updated_at = datetime.now()

    for feature in features:
        min_speed = feature['properties']['min_speed']
        max_speed = feature['properties']['max_speed']
        polygon_geom = shape(feature['geometry'])

        # Ensure the polygon is valid
        if not polygon_geom.is_valid:
            polygon_geom = polygon_geom.buffer(0)

        # Convert Polygon to WKT
        geom_wkt = polygon_geom.wkt

        # Save storm event and get storm_event_id
        storm_event_id = save_storm_event(cursor, data_datetime, 'wind', min_speed, max_speed, polygon_geom)

        # Insert into storms table
        insert_query = """
        INSERT INTO storms (type, original_date, min, max, file_name, date, coordinates, updated_at, storm_event_id)
        VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)
        """
        data = (
            'wind',
            original_date,
            min_speed,
            max_speed,
            filename,
            data_datetime,
            geom_wkt,
            updated_at,
            storm_event_id
        )
        cursor.execute(insert_query, data)

    conn.commit()
    cursor.close()
    conn.close()
