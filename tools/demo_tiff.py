"""
In order that this works, rename scan files to the format of SSSSYYYYMMDD_HHMMSS.
"""


import argparse
import time
import os
import torch
import warnings
import copy

print(f"torch.get_num_threads: {torch.get_num_threads()}", flush=True)
warnings.filterwarnings("ignore")
from roosts.detection.detector import Detector
from roosts.tracking.tracker import Tracker
from roosts.utils.visualizer import Visualizer
from matplotlib import image
import matplotlib.pyplot as plt
from wsrlib import pyart
import numpy as np
from geotiff import GeoTiff

here = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser()
parser.add_argument(
    "--sun_activity",
    type=str,
    default="sunrise",
    help="time window around sunrise or sunset",
)
parser.add_argument(
    "--data_root",
    type=str,
    help="directory for all outputs",
    default=f"{here}/../roosts_data",
)
parser.add_argument("--model_version", type=str, default="v2")
args = parser.parse_args()
print(args, flush=True)

######################### CONFIG #########################
# detection model config
DET_CFG = {
    "ckpt_path": f"{here}/../checkpoints/3.2_exp07_resnet101-FPN_detptr_anc10.pth",
    "imsize": 1200,
    "anchor_sizes": [
        [16, 18, 20, 22, 24, 26, 28, 30, 32],
        [32, 36, 40, 44, 48, 52, 56, 60, 64],
        [64, 72, 80, 88, 96, 104, 112, 120, 128],
        [128, 144, 160, 176, 192, 208, 224, 240, 256],
        [256, 288, 320, 352, 384, 416, 448, 480, 512],
    ],
    "nms_thresh": 0.3,
    "score_thresh": 0.05,
    "config_file": "COCO-Detection/faster_rcnn_R_101_FPN_3x.yaml",
    "use_gpu": torch.cuda.is_available(),
    "version": args.model_version,
}

# tracker model config
TRACK_CFG = {
    "imsize": 1200,  # not 600
}

PP_CFG = {
    "width": 1200,
    "height": 1200,
    "sun_activity": args.sun_activity,
}

# directories
DIRS = {
    "array_dir": os.path.join(args.data_root, "arrays"),  # rendered tiff file
    "vis_det_dir": os.path.join(
        args.data_root, "vis_dets"
    ),  # vis of detections from detection model
    "vis_NMS_MERGE_track_dir": os.path.join(
        args.data_root, "vis_NMS_MERGE_tracks"
    ),  # vis of tracks after NMS & merge
    "ui_img_dir": os.path.join(args.data_root, "ui", "img"),
}

######################### Init #########################
detector = Detector(**DET_CFG)
tracker = Tracker(**TRACK_CFG)
visualizer = Visualizer(**PP_CFG)
os.makedirs(os.path.join(DIRS["vis_det_dir"]), exist_ok=True)
os.makedirs(os.path.join(DIRS["vis_NMS_MERGE_track_dir"]), exist_ok=True)
os.makedirs(os.path.join(DIRS["ui_img_dir"], "dz05"), exist_ok=True)

######################### Run #########################
tiff_files_by_day = {}
scans_by_day = {}
img_paths_by_day = {}
for tiff_file in os.listdir(DIRS["array_dir"]):
    if not tiff_file.endswith(".tiff"):
        continue
    day = tiff_file[4:12]
    if day not in tiff_files_by_day:
        tiff_files_by_day[day] = []
        scans_by_day[day] = []
        img_paths_by_day[day] = []
    tiff_files_by_day[day].append(os.path.join(DIRS["array_dir"], tiff_file))
    scans_by_day[day].append(tiff_file[:-5])
    img_path = os.path.join(DIRS["ui_img_dir"], "dz05", f"{tiff_file[:-5]}.png")
    img_paths_by_day[day].append(img_path)

    # render the first channel as images
    if not os.path.exists(img_path):
        cm = plt.get_cmap(pyart.config.get_field_colormap("reflectivity"))
        rgb = np.array(
            GeoTiff(os.path.join(DIRS["array_dir"], tiff_file), crs_code=4326).read()
        )[:, :, 0]
        rgb = cm(rgb)[:, :, :3]
        image.imsave(img_path, rgb)

for day in sorted(list(tiff_files_by_day.keys())):
    tiff_files_by_day[day] = sorted(tiff_files_by_day[day])
    scans_by_day[day] = sorted(scans_by_day[day])
    img_paths_by_day[day] = sorted(img_paths_by_day[day])

    process_start_time = time.time()

    detections = detector.run(tiff_files_by_day[day], file_type="tiff")
    tracked_detections, tracks = tracker.tracking(
        scans_by_day[day], copy.deepcopy(detections)
    )
    tracks = [t for t in tracks if not t["NMS_suppressed"]]

    """ visualize detections under multiple thresholds of detection score"""
    visualizer.draw_dets_multi_thresh(
        img_paths_by_day[day], copy.deepcopy(detections), DIRS["vis_det_dir"]
    )

    """ visualize results after NMS and merging on tracks"""
    visualizer.draw_tracks_multi_thresh(
        img_paths_by_day[day],
        copy.deepcopy(tracked_detections),
        copy.deepcopy(tracks),
        DIRS["vis_NMS_MERGE_track_dir"],
    )

    process_end_time = time.time()
    print(f"Total time elapse: {process_end_time - process_start_time}\n", flush=True)
