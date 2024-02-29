import os
import numpy as np
from wsrlib import pyart, radar2mat
import matplotlib.pyplot as plt
import matplotlib.colors as pltc
from matplotlib import image
from tqdm import tqdm


# rendering settings
ARRAY_Y_DIRECTION = "xy"  # default radar direction, y is first dim (row), large y is north, row 0 is south
ARRAY_R_MAX = 150000.0
ARRAY_DIM = 600
ARRAY_ATTRIBUTES = ["reflectivity", "velocity", "spectrum_width"]
ARRAY_ELEVATIONS = [0.5, 1.5, 2.5, 3.5, 4.5]
ARRAY_RENDER_CONFIG = {
    "ydirection": ARRAY_Y_DIRECTION,
    "fields": ARRAY_ATTRIBUTES,
    "coords": "cartesian",
    "r_min": 2125.0,  # default: first range bin of WSR-88D
    "r_max": ARRAY_R_MAX,  # 459875.0 default: last range bin
    "r_res": 250,  # default: super-res gate spacing
    "az_res": 0.5,  # default: super-res azimuth resolution
    "dim": ARRAY_DIM,  # num pixels on a side in Cartesian rendering
    "sweeps": None,
    "elevs": ARRAY_ELEVATIONS,
    "use_ground_range": True,
    "interp_method": "nearest",
}
DUALPOL_DIM = 600
DUALPOL_ATTRIBUTES = [
    "differential_reflectivity",
    "cross_correlation_ratio",
    "differential_phase",
]
DUALPOL_ELEVATIONS = [0.5, 1.5, 2.5, 3.5, 4.5]
DUALPOL_RENDER_CONFIG = {
    "ydirection": ARRAY_Y_DIRECTION,
    "fields": DUALPOL_ATTRIBUTES,
    "coords": "cartesian",
    "r_min": 2125.0,  # default: first range bin of WSR-88D
    "r_max": ARRAY_R_MAX,  # default 459875.0: last range bin
    "r_res": 250,  # default: super-res gate spacing
    "az_res": 0.5,  # default: super-res azimuth resolution
    "dim": DUALPOL_DIM,  # num pixels on a side in Cartesian rendering
    "sweeps": None,
    "elevs": DUALPOL_ELEVATIONS,
    "use_ground_range": True,
    "interp_method": "nearest",
}


# visualization settings
COLOR_ARRAY = [
    "#006400",  # for not scaled boxes
    "#FF00FF",  # scaled to RCNN then to user factor 0.7429 which is sheldon average
    "#800080",
    "#FFA500",
    "#FFFF00",
]
NORMALIZERS = {
    "reflectivity": pltc.Normalize(vmin=-5, vmax=35),
    "velocity": pltc.Normalize(vmin=-15, vmax=15),
    "spectrum_width": pltc.Normalize(vmin=0, vmax=10),
    "differential_reflectivity": pltc.Normalize(vmin=-4, vmax=8),
    "differential_phase": pltc.Normalize(vmin=0, vmax=250),
    "cross_correlation_ratio": pltc.Normalize(vmin=0, vmax=1.1),
}

ODIM_FIELD_NAMES = {
    "DBZH": "reflectivity",
    "TH": "total_power",
    "RHOHV": "cross_correlation_ratio",
    "WRADH": "spectrum_width",
    "PHIDP": "differential_phase",
    "ZDR": "differential_reflectivity",
    "KDP": "specific_differential_phase",
    "VRADH": "velocity",
}


class Renderer:
    def __init__(
        self,
        download_dir,
        npz_dir,
        ui_img_dir,
        array_render_config=ARRAY_RENDER_CONFIG,
        dualpol_render_config=DUALPOL_RENDER_CONFIG,
        is_canadian_data=False,
    ):
        self.download_dir = download_dir
        self.npz_dir = npz_dir

        self.dz05_imgdir = os.path.join(ui_img_dir, "dz05")
        self.vr05_imgdir = os.path.join(ui_img_dir, "vr05")
        self.imgdirs = {
            ("reflectivity", 0.5): self.dz05_imgdir,
            ("velocity", 0.5): self.vr05_imgdir,
        }

        self.array_render_config = array_render_config
        self.dualpol_render_config = dualpol_render_config
        self.is_canadian_data = is_canadian_data

    def render(self, keys, logger, force_rendering=False):
        """
        Extract radar products from raw radar scans, save them to npz files,
        render dz05 and vr05 as png for UI.
        Radar scans are typically deleted after this.
        """

        npz_files = []  # the list of arrays for the detector to load and process
        scan_names = []  # the list of all scans for the tracker to know
        img_files = []  # the list of dz05 images for visualization

        for key in tqdm(keys, desc="Rendering"):
            key_splits = key.split("/")
            utc_year = key_splits[-5]
            utc_month = key_splits[-4]
            utc_date = key_splits[-3]
            utc_station = key_splits[-2]
            utc_date_station_prefix = os.path.join(
                utc_year, utc_month, utc_date, utc_station
            )
            scan = os.path.splitext(key_splits[-1])[0]

            npz_dir = os.path.join(self.npz_dir, utc_date_station_prefix)
            dz05_imgdir = os.path.join(self.dz05_imgdir, utc_date_station_prefix)
            vr05_imgdir = os.path.join(self.vr05_imgdir, utc_date_station_prefix)
            os.makedirs(npz_dir, exist_ok=True)
            os.makedirs(dz05_imgdir, exist_ok=True)
            os.makedirs(vr05_imgdir, exist_ok=True)

            npz_path = os.path.join(npz_dir, f"{scan}.npz")
            dz05_path = os.path.join(dz05_imgdir, f"{scan}.png")

            if (
                os.path.exists(npz_path)
                and os.path.exists(dz05_path)
                and not force_rendering
            ):
                npz_files.append(npz_path)
                scan_names.append(scan)
                img_files.append(dz05_path)
                continue

            arrays = {}

            try:
                if self.is_canadian_data:
                    radar = pyart.aux_io.read_odim_h5(
                        os.path.join(self.download_dir, key),
                        field_names=ODIM_FIELD_NAMES,
                    )
                else:
                    radar = pyart.io.read_nexrad_archive(
                        os.path.join(self.download_dir, key)
                    )
            except Exception as ex:
                logger.error("[Scan Loading Failure] scan %s - %s" % (scan, str(ex)))
                continue

            try:
                data, _, _, y, x = radar2mat(radar, **self.array_render_config)
                logger.info("[Array Rendering Success] scan %s" % scan)
                arrays["array"] = data
            except Exception as ex:
                logger.error("[Array Rendering Failure] scan %s - %s" % (scan, str(ex)))

            try:
                data, _, _, y, x = radar2mat(radar, **self.dualpol_render_config)
                logger.info("[Dualpol Rendering Success] scan %s" % scan)
                arrays["dualpol_array"] = data
            except Exception as ex:
                logger.error(
                    "[Dualpol Rendering Failure] scan %s - %s" % (scan, str(ex))
                )

            if "array" in arrays:
                np.savez_compressed(npz_path, **arrays)
                self.render_img(
                    arrays["array"], utc_date_station_prefix, scan
                )  # render dz05 and vr05 as png for UI
                npz_files.append(npz_path)
                scan_names.append(scan)
                img_files.append(dz05_path)

        return npz_files, scan_names, img_files

    def render_img(self, array, utc_date_station_prefix, scan):
        attributes = self.array_render_config["fields"]
        elevations = self.array_render_config["elevs"]
        for attr, elev in self.imgdirs:
            cm = plt.get_cmap(pyart.config.get_field_colormap(attr))
            rgb = cm(
                NORMALIZERS[attr](
                    array[attributes.index(attr), elevations.index(elev), :, :]
                )
            )
            rgb = rgb[::-1, :, :3]
            # flip the y axis, from geographical (big y means North) to image (big y means lower)
            # omit the fourth alpha dimension, NAN are black but not white
            image.imsave(
                os.path.join(
                    self.imgdirs[(attr, elev)], utc_date_station_prefix, f"{scan}.png"
                ),
                rgb,
            )
