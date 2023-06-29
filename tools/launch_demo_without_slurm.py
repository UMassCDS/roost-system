import argparse, time, os, torch, warnings
from datetime import timedelta
print(f"torch.get_num_threads: {torch.get_num_threads()}", flush=True)
warnings.filterwarnings("ignore")

from roosts.system import RoostSystem
from roosts.utils.time_util import get_days_list, get_sun_activity_time
from roosts.utils.s3_util import get_station_day_scan_keys
from roosts.utils.azure_blob_util import get_station_date_scan_keys as get_keys_canadian
from yacs.config import CfgNode as CN

here = os.path.dirname(os.path.realpath(__file__))

def run_system():
  args = CN()
  args.station = station
  args.start = start
  args.end = end
  args.sun_activity = sun_activity
  args.min_before = min_before
  args.min_after = min_after
  args.data_root = "inference_results"
  args.just_render = False
  args.gif_vis = True
  args.aws_access_key_id=aws_access_key_id
  args.aws_secret_access_key=aws_secret_access_key
  args.canadian_data = True

  # detection model config
  DET_CFG = {
      "ckpt_path": f"{here}/../checkpoints/v3.pth",
      "imsize": 1200,
      "anchor_sizes": [[16, 18, 20, 22, 24, 26, 28, 30, 32],
              [32, 36, 40, 44, 48, 52, 56, 60, 64],
              [64, 72, 80, 88, 96, 104, 112, 120, 128],
              [128, 144, 160, 176, 192, 208, 224, 240, 256],
              [256, 288, 320, 352, 384, 416, 448, 480, 512]],
      "nms_thresh": 0.3,
      "score_thresh": 0.1,
      "config_file": "COCO-Detection/faster_rcnn_R_101_FPN_3x.yaml",
      "use_gpu": torch.cuda.is_available(),
      "version": "v3"
  }

  # postprocessing config
  PP_CFG = {
      "imsize": 600,
      "geosize": 300000,
      "sun_activity": args.sun_activity,
      "clean_windfarm": True,
      "clean_rain": True
  }

  # directories
  DIRS = {
      "scan_dir": os.path.join(args.data_root, 'scans'),  # raw scans downloaded from AWS
      "npz_dir": os.path.join(args.data_root, 'arrays'), # rendered npz file
      "log_root_dir": os.path.join(args.data_root, 'logs'),
      "vis_det_dir": os.path.join(args.data_root, 'vis_dets'), # vis of detections from detection model
      "vis_NMS_MERGE_track_dir": os.path.join(args.data_root, 'vis_NMS_MERGE_tracks'), # vis of tracks after NMS & merge
      "ui_img_dir": os.path.join(args.data_root, 'ui', 'img'),
      "scan_and_track_dir": os.path.join(args.data_root, 'ui', "scans_and_tracks"),
  }

  days = get_days_list(args.start, args.end)
  print("Total number of days: %d" % len(days), flush=True)

  roost_system = RoostSystem(args, DET_CFG, PP_CFG, DIRS)
  for day_idx, day in enumerate(days):
    process_start_time = time.time()

    date_string = day.strftime('%Y%m%d')  # yyyymmdd
    print(f"-------------------- Day {day_idx+1}: {date_string} --------------------\n", flush=True)

    sun_activity_time = get_sun_activity_time(args.station, day, sun_activity=args.sun_activity) # utc
    start_time = sun_activity_time - timedelta(minutes=args.min_before)
    end_time = sun_activity_time + timedelta(minutes=args.min_after)

    keys = []
    if args.canadian_data:
        keys = get_keys_canadian(start_time, end_time, args.station,)
    else:
        print('Downloading keys from azure')
        keys = get_station_day_scan_keys(start_time, end_time, args.station,
                        aws_access_key_id=args.aws_access_key_id,
                        aws_secret_access_key=args.aws_secret_access_key)
        print('Downloaded keys from azure')
    keys = sorted(list(set(keys)))  # aws keys

    roost_system.run_day_station(day, sun_activity_time, keys, process_start_time)

station = "CASET" # a single station name, eg. KDOX
start = "20230526" # the first day to process
end = "20230601" # the last day to process
sun_activity = "sunrise" # process scans in a time window around "sunrise" or "sunset"
min_before = 30 # process scans this many minutes before the sun activity
min_after = 60 # process scans this many minutes after the sun activity
aws_access_key_id = "AKIAVVKPMTBXCVXXJRNK"
aws_secret_access_key = "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"
run_system()