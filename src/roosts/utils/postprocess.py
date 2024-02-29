import os
import numpy as np
import csv
import pickle
from sklearn.neighbors import NearestNeighbors
from roosts.utils.geo_util import geo_dist_km, get_roost_coor
from roosts.utils.time_util import scan_key_to_utc_time
from tqdm import tqdm


class Postprocess:
    """
    Remove the false positives including wind farm and rain
    Populate the detections with geographic information (convert image coordinates to geographic coordinates)
    """

    def __init__(
        self,
        imsize=600,
        geosize=300000,  # by default, the image size represents 300km
        sun_activity=None,
        nms=True,
        clean_windfarm=True,
        clean_rain=True,
    ):
        self.imsize = imsize
        self.geosize = geosize
        assert sun_activity in ["sunrise", "sunset"]
        self.sun_activity = sun_activity
        self.nms = nms
        self.clean_windfarm = clean_windfarm
        self.clean_rain = clean_rain

        if self.clean_windfarm:
            here = os.path.dirname(os.path.realpath(__file__))
            windfarm_database = os.path.join(here, "uswtdb_v1_3_20190107.csv")
            self.windfarm_ball_tree = self._build_ball_tree(windfarm_database)

    #################### Wind Farm ####################
    """ 
    load wind farm map if there is one wind farm inside the bbox, 
    then regard the bbox as wind farm
    """

    def _load_wind_farm_dataset(self, windfarm_database):
        wind_farm_coors = []
        if not os.path.exists(windfarm_database):
            print("Cannot find the wind farm database.")
            sys.exit(1)
        with open(windfarm_database, "r") as f:
            reader = csv.reader(f)
            wind_farm_list = list(reader)
        for entity in wind_farm_list[1:]:
            lon = float(entity[-2])
            lat = float(entity[-1])
            wind_farm_coors.append((lat, lon))
        return wind_farm_coors

    def _build_ball_tree(self, windfarm_database):
        ball_tree_path = os.path.join(
            os.path.dirname(windfarm_database), "uswtdb_ball_tree.pkl"
        )
        if os.path.exists(ball_tree_path):
            with open(ball_tree_path, "rb") as f:
                nbrs = pickle.load(f)
            return nbrs
        wind_farm_coors = self._load_wind_farm_dataset(windfarm_database)
        nbrs = NearestNeighbors(
            n_neighbors=1, algorithm="ball_tree", metric=geo_dist_km
        )
        nbrs.fit(wind_farm_coors)
        with open(ball_tree_path, "wb") as f:
            pickle.dump(nbrs, f)
        return nbrs

    def _is_there_wind_farm(self, det):
        """efficient to search nearest neightbor"""
        X = [[float(det[1]), float(det[0])]]  # in order of lat and lon
        r = float(det[2]) / 1000.0
        distances, indices = self.windfarm_ball_tree.kneighbors(X)
        if distances[0, 0] < r:
            return True
        else:
            return False

    #################### Precipitation ####################
    def _is_there_rain(self, box, dualpol):
        # bbox
        x = int(box[0])
        y = int(box[1])
        r = int(box[2])

        # load dualpol
        dualpol_threshold = 0.95
        NAN, BIRD, RAIN = 1, 2, 3

        dualpol_pred = BIRD * np.ones(dualpol.shape)  # 2: bird
        dualpol_pred[np.isnan(dualpol)] = NAN  # 1: nan
        # make area with nan value smaller than dualpol_threshold
        dualpol[np.isnan(dualpol)] = dualpol_threshold - 1
        dualpol_pred[dualpol > dualpol_threshold] = RAIN  # 3: rain

        # statistic
        rain_count = np.sum(dualpol_pred[y - r : y + r, x - r : x + r] == RAIN)
        bird_count = np.sum(dualpol_pred[y - r : y + r, x - r : x + r] == BIRD)

        if rain_count > bird_count:
            return True
        else:
            return False

    #################### Post-processing ####################
    def filter_untracked_dets(self, detections, tracks):
        det_dict = {}
        for det in detections:
            det_dict[det["det_ID"]] = det
        new_detections = []
        for track in tracks:
            for det_ID in track["det_IDs"]:
                new_detections.append(det_dict[det_ID])
        return new_detections

    def geo_converter(self, detections):
        # convert image coordinates to geometric coordinates
        for det in detections:
            roost_xy = det["im_bbox"][:2]  # image coordinate of roost center
            # the following step is critical to get correct geographic coordinates
            station_xy = (
                self.imsize / 2.0,
                self.imsize / 2.0,
            )  # image coordinate of radar station
            station_name = det["scanname"][:4]
            distance_per_pixel = self.geosize / self.imsize
            roost_lon, roost_lat = get_roost_coor(
                roost_xy, station_xy, station_name, distance_per_pixel
            )
            geo_radius = det["im_bbox"][2] * distance_per_pixel
            det["geo_bbox"] = [roost_lon, roost_lat, geo_radius]
        return detections

    def add_sun_activity_time(self, detections, sun_activity_time):
        for det in detections:
            sun_activity_offset = (
                scan_key_to_utc_time(det["scanname"]) - sun_activity_time
            )
            det[f"from_{self.sun_activity}"] = sun_activity_offset.total_seconds() / 60
        return (
            detections  # the data should have been modified in place but just be safe
        )

    def annotate_detections(self, detections, tracks, npz_files, sun_activity_time):
        scan_dict = {}
        for npz_file in npz_files:
            scanname = os.path.splitext(os.path.basename(npz_file))[0]
            scan_dict[scanname] = npz_file

        # filter detections, only annotate tracked detections
        detections = self.filter_untracked_dets(detections, tracks)
        # convert image coordinates to geometric coordinates
        detections = self.geo_converter(detections)
        # populate detections with mins from sunrise/sunset time
        detections = self.add_sun_activity_time(detections, sun_activity_time)

        if self.nms:
            # use only tracks after NMS
            tracks = [t for t in tracks if not t["NMS_suppressed"]]

        # clean up the windfarms using windfarm database
        det_dict = {}
        for det in detections:
            det["rain"] = False  # not considered rain
            det["windfarm"] = False  # not considered windfarm
            det_dict[det["det_ID"]] = det
        if self.clean_windfarm:
            for track in tqdm(tracks, desc="Cleaning windfarm"):
                # only check reliable tracks, otherwise it is too time-consuming
                if np.sum(track["det_or_pred"]) >= 3:
                    # only check the first detection
                    head_det_ID = track["det_IDs"][0]
                    bbox = det_dict[head_det_ID]["geo_bbox"]
                    flag = self._is_there_wind_farm(bbox)
                else:
                    flag = False
                track["is_windfarm"] = flag
                for det_ID in track["det_IDs"]:
                    det_dict[det_ID]["windfarm"] = flag

        # clean up rain using dualpol
        if self.clean_rain:
            dualpol_data = {}
            for scanname, npz_file in scan_dict.items():
                radar_data = np.load(npz_file)
                if "dualpol_array" in radar_data.files:
                    dualpol = radar_data["dualpol_array"]
                    dualpol = dualpol[1, 0, ::-1, :]
                # use correlation coefficient at the lowest elevation
                # flip the y axis, from geographical (big y means North) to image (big y means lower)
                else:
                    dualpol = None
                dualpol_data[scanname] = dualpol

            for track in tqdm(tracks, desc="Cleaning rain"):
                # heuristics: a rain track is typically long, to speed up the system:
                """
                if np.sum(track["det_or_pred"]) < 3:
                    for det_ID in track["det_IDs"]:
                        # assume these detections are not rain
                        det_dict[det_ID]["rain"] = False
                    continue
                """
                rain_flags = []
                # only check the first 3 dets:
                for det_i in range(min(3, len(track["det_IDs"]))):
                    # only check the first detection
                    det_ID = track["det_IDs"][det_i]
                    bbox = det_dict[det_ID]["im_bbox"]
                    scanname = det_dict[det_ID]["scanname"]
                    dualpol = dualpol_data[scanname]
                    if dualpol is not None:
                        flag = self._is_there_rain(bbox, dualpol)
                    else:
                        flag = False
                    rain_flags.append(flag)
                track["is_rain"] = max(rain_flags)
                for det_ID in track["det_IDs"]:
                    # as long as there is one rain, the track is rain
                    det_dict[det_ID]["rain"] = max(rain_flags)

            """
            for det in tqdm(detections, desc="Cleaning rain"):
                scanname = det["scanname"]
                bbox = det["im_bbox"]
                dualpol = dualpol_data[scanname]
                if dualpol is not None:
                    det["rain"] = self._is_there_rain(bbox, dualpol)
                else:
                    det["rain"] = False
            """
            del dualpol_data

        return detections, tracks


if __name__ == "__main__":
    detections = [
        {
            "scanname": "KDOX20111010_071218_V06",
            "det_ID": 1,
            "det_score": 0.99,
            "im_bbox": [10, 10, 10],
        },
        {
            "scanname": "KDOX20111010_071218_V06",
            "det_ID": 2,
            "det_score": 0.40,
            "im_bbox": [50, 50, 10],
        },
        {
            "scanname": "KDOX20111010_072218_V06",
            "det_ID": 3,
            "det_score": 0.99,
            "im_bbox": [12, 12, 15],
        },
        {
            "scanname": "KDOX20111010_072218_V06",
            "det_ID": 4,
            "det_score": 0.79,
            "im_bbox": [55, 55, 20],
        },
        {
            "scanname": "KDOX20111010_073218_V06",
            "det_ID": 3,
            "det_score": 0.85,
            "im_bbox": [16, 16, 25],
        },
        {
            "scanname": "KDOX20111010_073218_V06",
            "det_ID": 4,
            "det_score": 0.79,
            "im_bbox": [56, 56, 35],
        },
        {
            "scanname": "KDOX20111010_073218_V06",
            "det_ID": 4,
            "det_score": 0.79,
            "im_bbox": [30, 30, 20],
        },
    ]
    postprocess = Postprocess()
    dets = postprocess.annotate_detections(detections, [])
