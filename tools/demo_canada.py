import argparse, time, os, torch, warnings
from datetime import timedelta
print(f"torch.get_num_threads: {torch.get_num_threads()}", flush=True)
warnings.filterwarnings("ignore")

from roosts.system import RoostSystem
from roosts.utils.time_util import get_days_list, get_sun_activity_time
from roosts.utils.s3_util import get_station_day_scan_keys

here = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser()
parser.add_argument('--station', type=str, required=True, help="a single station name, eg. KDOX")
parser.add_argument('--start', type=str, required=True, help="the first local date to process, eg. 20101001")
parser.add_argument('--end', type=str, required=True, help="the last local date to process, eg. 20101001")
parser.add_argument('--sun_activity', type=str, default="sunrise", help="time window around sunrise or sunset")
parser.add_argument('--min_before', type=int, default=30,
                    help="process scans at most these minutes before the selected sun activity")
parser.add_argument('--min_after', type=int, default=90,
                    help="process scans at most these minutes after the selected sun activity")
parser.add_argument('--data_root', type=str, help="directory for all outputs",
                    default=f"{here}/../roosts_data")
parser.add_argument('--just_render', action='store_true', help="just download and render, no detection and tracking")
parser.add_argument('--model_version', type=str, default="v2")
parser.add_argument('--gif_vis', action='store_true', help="generate gif visualization")
parser.add_argument('--aws_access_key_id', type=str, default=None)
parser.add_argument('--aws_secret_access_key', type=str, default=None)
args = parser.parse_args()
assert args.sun_activity in ["sunrise", "sunset"]
print(args, flush=True)

######################### CONFIG #########################
if args.model_version == "v2":
    ckpt_path = f"{here}/../checkpoints/3.2_exp07_resnet101-FPN_detptr_anc10.pth"
elif args.model_version == "v3":
    ckpt_path = f"{here}/../checkpoints/v3.pth"

# detection model config
DET_CFG = {
    "ckpt_path":        ckpt_path,
    "imsize":           1100 if args.model_version == "v3" else 1200,
    "anchor_sizes":     [[16, 18, 20, 22, 24, 26, 28, 30, 32],
                         [32, 36, 40, 44, 48, 52, 56, 60, 64],
                         [64, 72, 80, 88, 96, 104, 112, 120, 128],
                         [128, 144, 160, 176, 192, 208, 224, 240, 256],
                         [256, 288, 320, 352, 384, 416, 448, 480, 512]],
    "nms_thresh":       0.3,
    "score_thresh":     0.05,
    "config_file":      "COCO-Detection/faster_rcnn_R_101_FPN_3x.yaml",
    "use_gpu":          torch.cuda.is_available(),
    "version":          args.model_version,
}

# postprocessing config
PP_CFG = {
    "imsize":           600,
    "geosize":          300000,
    "sun_activity":     args.sun_activity,
    "clean_windfarm":   True,
    "clean_rain":       True,
}

# directories
DIRS = {
    "scan_dir":                   os.path.join(args.data_root, 'scans'),  # raw scans downloaded from AWS
    "npz_dir":                    os.path.join(args.data_root, 'arrays'), # rendered npz file
    "log_root_dir":               os.path.join(args.data_root, 'logs'),
    "vis_det_dir":                os.path.join(args.data_root, 'vis_dets'), # vis of detections from detection model
    "vis_NMS_MERGE_track_dir":    os.path.join(args.data_root, 'vis_NMS_MERGE_tracks'), # vis of tracks after NMS & merge
    "ui_img_dir":                 os.path.join(args.data_root, 'ui', 'img'),
    "scan_and_track_dir":         os.path.join(args.data_root, 'ui', "scans_and_tracks"),
}

######################### Run #########################
roost_system = RoostSystem(args, DET_CFG, PP_CFG, DIRS)

days = get_days_list(args.start, args.end) # timestamps that indicate the beginning of dates, no time zone info
print("Total number of days: %d" % len(days), flush=True)
for day_idx, day in enumerate(days):
    process_start_time = time.time()

    date_string = day.strftime('%Y%m%d')  # yyyymmdd
    print(f"-------------------- Day {day_idx+1}: {date_string} --------------------\n", flush=True)

    sun_activity_time = get_sun_activity_time(
        args.station,
        day, # must not have tzinfo
        args.sun_activity
    )  # utc timestamp (with utc tzinfo) for the local sun activity after the beginning of the local date
    start_time = sun_activity_time - timedelta(minutes=args.min_before)
    end_time = sun_activity_time + timedelta(minutes=args.min_after)
    keys = get_station_day_scan_keys(
        start_time,
        end_time,
        args.station,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
    )  # aws keys which uses UTC time: yyyy/mm/dd/ssss/ssssyyyymmdd_hhmmss*
    keys = sorted(list(set(keys)))

    roost_system.run_day_station(day, sun_activity_time, keys, process_start_time)

