import pygrib
import numpy as np


def parse_grib_files(grib_files):
    """
    Parse GRIB2 files to extract U and V wind components and latitude/longitude grids.
    """
    u_components = []
    v_components = []
    lats = None
    lons = None

    for file in grib_files:
        grbs = pygrib.open(file)
        u_grb = grbs.select(name='10 metre U wind component')[0]
        v_grb = grbs.select(name='10 metre V wind component')[0]

        u_data = u_grb.values
        v_data = v_grb.values

        u_components.append(u_data)
        v_components.append(v_data)

        if lats is None or lons is None:
            lats, lons = u_grb.latlons()

        grbs.close()

    # Average over time if multiple files
    u_mean = np.mean(u_components, axis=0)
    v_mean = np.mean(v_components, axis=0)

    return u_mean, v_mean, lats, lons


def calculate_wind_speed(u, v):
    """
    Calculate wind speed magnitude from U and V wind components.
    """
    wind_speed = np.sqrt(u**2 + v**2)
    # Convert m/s to miles per hour
    wind_speed_mph = wind_speed * 2.23694
    return wind_speed_mph
