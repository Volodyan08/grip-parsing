import numpy as np
from scipy.ndimage import gaussian_filter
from shapely.geometry import Polygon, mapping
import matplotlib.pyplot as plt


def filter_region(data, lats, lons, bounds):
    """
    Filter data to the specified geographic bounds.
    """
    lat_mask = (lats >= bounds['lat_min']) & (lats <= bounds['lat_max'])
    lon_mask = (lons >= bounds['lon_min']) & (lons <= bounds['lon_max'])
    region_mask = lat_mask & lon_mask

    filtered_data = data.copy()
    filtered_data[~region_mask] = np.nan

    filtered_lats = lats.copy()
    filtered_lats[~region_mask] = np.nan

    filtered_lons = lons.copy()
    filtered_lons[~region_mask] = np.nan

    return filtered_data, filtered_lats, filtered_lons


def smooth_data(data, sigma):
    """
    Apply Gaussian smoothing to the data.
    """
    # Temporarily replace NaNs with zeros
    data_filled = np.nan_to_num(data, nan=0.0)
    smoothed = gaussian_filter(data_filled, sigma=sigma)
    # Restore NaNs where original data was NaN
    smoothed[np.isnan(data)] = np.nan
    return smoothed


def generate_wind_zones(wind_speed, lats, lons, thresholds):
    """
    Generate wind zone polygons based on wind speed thresholds.
    Returns a list of individual polygon features.
    """
    zones = []
    for i in range(len(thresholds)-1):
        lower = thresholds[i]
        upper = thresholds[i+1]
        level = (lower + upper) / 2  # Midpoint for the level

        # Create contour
        cs = plt.contour(lons, lats, wind_speed, levels=[level])
        for collection in cs.collections:
            for contour_path in collection.get_paths():
                # Extract the polygon coordinates
                coordinates = contour_path.to_polygons()
                for coord in coordinates:
                    if len(coord) >= 3:
                        # Create a polygon from the contour coordinates
                        poly = Polygon(coord)
                        if poly.is_valid:
                            zones.append({
                                'type': 'Feature',
                                'properties': {
                                    'wind_speed_range': f'{lower}-{upper} mph',
                                    'min_speed': lower,
                                    'max_speed': upper
                                },
                                'geometry': mapping(poly)
                            })
        plt.clf()  # Clear the figure for the next contour

    return zones
