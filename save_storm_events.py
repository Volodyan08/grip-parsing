import requests
from shapely.geometry import MultiPolygon, Polygon


def save_storm_event(cursor, data_datetime, event_type, min_speed, max_speed, polygon_geom):
    # Ensure the polygon is valid
    if not polygon_geom.is_valid:
        polygon_geom = polygon_geom.buffer(0)

    # Convert Polygon to MultiPolygon if necessary
    if isinstance(polygon_geom, Polygon):
        polygon_geom = MultiPolygon([polygon_geom])

    # Convert geometry to WKT
    geom_wkt = polygon_geom.wkt

    # Get the geographical name using Nominatim
    try:
        geographical_name = get_geographical_name(polygon_geom)
    except:
        geographical_name = "geographical_name"
    # Check for existing storm event based on spatial overlap
    select_query = """
    SELECT id
    FROM storm_events
    WHERE type = %s AND min_wind_speed = %s AND max_wind_speed = %s
    AND ST_Intersects(coordinates, ST_GeomFromText(%s, 4326))
    """
    cursor.execute(select_query, (event_type, min_speed, max_speed, geom_wkt))
    result = cursor.fetchone()

    if result:
        storm_event_id = result[0]

        # Merge geometries
        update_query = """
        UPDATE storm_events
        SET coordinates = ST_Multi(ST_Union(coordinates, ST_GeomFromText(%s, 4326))),
            updated_at = NOW()
        WHERE id = %s
        """
        cursor.execute(update_query, (geom_wkt, storm_event_id))
    else:
        # Insert a new storm event
        insert_query = """
        INSERT INTO storm_events (date, type, min_wind_speed, max_wind_speed, coordinates, geographical_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s, ST_Multi(ST_GeomFromText(%s, 4326)), %s, NOW(), NOW())
        RETURNING id
        """
        data = (
            data_datetime,
            event_type,
            min_speed,
            max_speed,
            geom_wkt,
            geographical_name
        )
        cursor.execute(insert_query, data)
        storm_event_id = cursor.fetchone()[0]

    return storm_event_id


def get_geographical_name(polygon_geom):
    """
    Get the geographical name of the largest city or state within the storm event polygon
    using Nominatim reverse geocoding service.
    """
    # Calculate the centroid of the polygon
    centroid = polygon_geom.centroid
    lat = centroid.y
    lon = centroid.x

    # Nominatim API endpoint
    url = 'https://nominatim.openstreetmap.org/reverse'

    # Parameters for the API request
    params = {
        'format': 'json',
        'lat': lat,
        'lon': lon,
        'zoom': 10,  # Adjust zoom level as needed
        'addressdetails': 1,
    }

    # Include a valid User-Agent header as required by Nominatim's usage policy
    headers = {
        'User-Agent': 'YourAppName/1.0 (your.email@example.com)'
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extract the geographical name from the response
        address = data.get('address', {})
        city = address.get('city')
        state = address.get('state')
        country = address.get('country')

        if city:
            return city
        elif state:
            return state
        elif country:
            return country
        else:
            return 'Unknown'
    except requests.RequestException as e:
        print(f"Error fetching geographical name: {e}")
        return 'Unknown'
