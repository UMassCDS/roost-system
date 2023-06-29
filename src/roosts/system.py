import copy
import logging
import os
import time
from roosts.data.downloader import Downloader
from roosts.data.renderer import Renderer
from roosts.detection.detector import Detector
from roosts.tracking.tracker import Tracker
from roosts.utils.visualizer import Visualizer
from roosts.utils.postprocess import Postprocess
from roosts.utils.file_util import delete_files
from roosts.utils.time_util import scan_key_to_local_time


class RoostSystem:

    def __init__(self, args, det_cfg, pp_cfg, dirs):
        self.args = args
        self.dirs = dirs
        self.downloader = Downloader(
            download_dir=dirs["scan_dir"], npz_dir=dirs["npz_dir"],
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            canadian_data = args.canadian_data
        )
        self.renderer = Renderer(dirs["scan_dir"], dirs["npz_dir"], dirs["ui_img_dir"], args["canadian_data"])
        if not args.just_render:
            self.detector = Detector(**det_cfg)
            self.tracker = Tracker()
            self.postprocess = Postprocess(**pp_cfg)
            self.visualizer = Visualizer(sun_activity=self.args.sun_activity)

    def run_day_station(
            self,
            day, # timestamps that indicate the beginning of dates, no time zone info
            sun_activity_time, # utc timestamp for the next local sun activity after the beginning of the local date
            keys, # aws keys which uses UTC time: yyyy/mm/dd/ssss/ssssyyyymmdd_hhmmss*
            process_start_time
    ):
        local_date_string = day.strftime('%Y%m%d')  # yyyymmdd
        local_date_station_prefix = os.path.join(
            day.strftime('%Y'),
            day.strftime('%m'),
            day.strftime('%d'),
            self.args.station
        )  # yyyy/mm/dd/ssss
        local_year, local_month = day.strftime('%Y'), day.strftime('%m')

        log_dir = os.path.join(self.dirs["log_root_dir"], self.args.station, day.strftime('%Y'))
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{self.args.station}_{local_date_string}.log")
        logger = logging.getLogger(local_date_station_prefix)
        filelog = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        formatter.converter = time.gmtime
        filelog.setFormatter(formatter)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(filelog)

        ######################### (1) Download data #########################
        keys = self.downloader.download_scans(keys, logger)

        ######################### (2) Render data #########################
        (
            npz_files,  # the list of arrays for the detector to load and process
            scan_names, # the list of all scans for the tracker to know
            img_files,  # the list of dz05 images for visualization
        ) = self.renderer.render(keys, logger)
        delete_files([os.path.join(self.dirs["scan_dir"], key) for key in keys])

        if len(npz_files) == 0:
            process_end_time = time.time()
            logger.info(
                f'[Passed] no successfully rendered scan for {self.args.station} local time {local_date_string}; '
                f'total time elapse: {process_end_time - process_start_time}'
            )
            print(
                f"No successfully rendered scan.\nTotal time elapse: {process_end_time - process_start_time}\n",
                flush=True
            )
            return

        os.makedirs(self.dirs["scan_and_track_dir"], exist_ok=True)
        scans_path = os.path.join(
            self.dirs["scan_and_track_dir"],
            f'scans_{self.args.station}_{self.args.start}_{self.args.end}.txt'
        )
        tracks_path = os.path.join(
            self.dirs["scan_and_track_dir"],
            f'tracks_{self.args.station}_{self.args.start}_{self.args.end}.txt'
        )
        if not os.path.exists(scans_path):
            with open(scans_path, "w") as f:
                f.write("filename,local_time\n")
        if not os.path.exists(tracks_path):
            with open(tracks_path, 'w') as f:
                f.write(f'track_id,filename,from_{self.args.sun_activity},det_score,x,y,r,lon,lat,radius,local_time\n')
        with open(scans_path, "a+") as f:
            f.writelines([f"{scan_name},{scan_key_to_local_time(scan_name)}\n" for scan_name in scan_names])

        if self.args.just_render:
            return

        ######################### (3) Run detection models on the data #########################
        detections = self.detector.run(npz_files)
        logger.info(f'[Detection Done] {len(detections)} detections')

        ######################### (4) Run tracking on the detections #########################
        """
            In some scans, the detector does not find any roosts;
            we use "scan_names" (a name list of all scans) to allow the tracker to find some.
            NMS over tracks is applied to remove duplicated tracks, not sure if it's useful with new detection model.
        """
        tracked_detections, tracks = self.tracker.tracking(scan_names, copy.deepcopy(detections))
        logger.info(f'[Tracking Done] {len(tracks)} tracks with {len(tracked_detections)} tracked detections')

        ######################### (5) Postprocessing  #########################
        """
            (1) convert image coordinates to geometric coordinates;
            (2) clean up the false positives due to windfarm and rain using auxiliary information
        """
        cleaned_detections, tracks = self.postprocess.annotate_detections(
            copy.deepcopy(tracked_detections), copy.deepcopy(tracks), npz_files, sun_activity_time
        )
        logger.info(f'[Postprocessing Done] {len(cleaned_detections)} cleaned detections')

        ######################### (6) Visualize the detection and tracking results #########################
        # generate gif visualization
        if self.args.gif_vis:
            """ visualize detections under multiple thresholds of detection score"""
            self.visualizer.draw_dets_multi_thresh(
                img_files, copy.deepcopy(detections),
                os.path.join(self.dirs["vis_det_dir"], self.args.station, local_year, local_month)
            )

            """ visualize results after NMS and merging on tracks"""
            self.visualizer.draw_tracks_multi_thresh(
                img_files, copy.deepcopy(tracked_detections), copy.deepcopy(tracks),
                os.path.join(self.dirs["vis_NMS_MERGE_track_dir"], self.args.station, local_year, local_month)
            )

        # save the list of tracks for UI
        self.visualizer.save_predicted_tracks(cleaned_detections, tracks, tracks_path)

        process_end_time = time.time()
        logger.info(
            f'[Finished] running the system for {self.args.station} local time {local_date_string}; '
            f'total time elapse: {process_end_time - process_start_time}'
        )
        print(f"Total time elapse: {process_end_time - process_start_time}\n", flush=True)
