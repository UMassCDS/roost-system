import geopy
from geopy import distance
import numpy as np
from roosts.utils.nexrad_util import NEXRAD_LOCATIONS


def euclid_distance(p, q):
    return np.sqrt(np.sum((p - q) * (p - q), axis=1))


def is_box_nested(box1, box2):
    # check if box1 is nested in box2
    if (
        box1[0] >= box2[0]
        and box1[1] >= box2[1]
        and box1[2] <= box2[2]
        and box1[3] <= box2[3]
    ):
        return True
    return False


def geo_dist_km(coor1, coor2):
    return distance.distance(coor1, coor2).km


def cart2pol(x, y):
    dis = np.sqrt(x**2 + y**2)
    angle = np.arctan2(y, x)
    return angle, dis


def rad2deg(angle):
    return angle * 180.0 / np.pi


def pol2cmp(angle):
    bearing = rad2deg(np.pi / 2 - angle)
    bearing = np.mod(bearing, 360)
    return bearing


def get_roost_coor(roost_xy, station_xy, station_name, distance_per_pixel):
    """
    Convert from image coordinates to geographic coordinates

    Args:
        roost_xy: image coordinates of roost center
        station_xy: image coordinates of station
        station_name: name of station, e.g., KDOX
        distance_per_pixel: geographic distance per pixel, unit: meter

    Return:
        longitude, latitide of roost center
    """
    station_lat, station_lon = (
        NEXRAD_LOCATIONS[station_name]["lat"],
        NEXRAD_LOCATIONS[station_name]["lon"],
    )
    angle, dis = cart2pol(roost_xy[0] - station_xy[0], -roost_xy[1] + station_xy[1])
    bearing = pol2cmp(angle)
    origin = geopy.Point(station_lat, station_lon)
    des = distance.distance(kilometers=dis * distance_per_pixel / 1000.0).destination(
        origin, bearing
    )
    return des[1], des[0]  # in order of lon and lat
