import os
import matplotlib.figure as mplfigure
import matplotlib.pyplot as plt
import cv2
import imageio
import roosts.utils.file_util as fileUtil
from roosts.utils.time_util import scan_key_to_local_time
from tqdm import tqdm
import itertools


class Visualizer:

    """
    Visualize the detection and tracking results
    """

    def __init__(self, width=600, height=600, sun_activity=None):
        self.width = width
        self.height = height
        assert sun_activity in ["sunrise", "sunset"]
        self.sun_activity = sun_activity

    def draw_detections(
        self,
        image_paths,
        detections,
        outdir,
        score_thresh=0.005,
        save_gif=True,
        vis_track=False,
        vis_track_after_NMS=True,
    ):
        """
        Draws detections on the images

        Args:
            image_paths: absolute path of images, type: list
            detections:  the output of detector or tracker with the structure
                         {"scanname":xx, "im_bbox": xx, "det_ID": xx, 'det_score': xx}
                         type: list of dict
            outdir: path to save images
            score_thresh: only display bbox with score higher than threshold
            save_gif:   save image sequence as gif on a single station in daily basis

        Returns:
            image with bboxes
        """
        fileUtil.mkdir(outdir)
        outpaths = []

        if not vis_track:
            # if visualize track, some detections are predicted by Kalman filter which may not have det score
            detections = [det for det in detections if det["det_score"] >= score_thresh]

        if vis_track and vis_track_after_NMS:
            # the track is not suppressed by NMS
            detections = [
                det
                for det in detections
                if ("track_NMS" in det.keys()) and (not det["track_NMS"])
            ]

        for image_path in tqdm(image_paths, desc="Visualizing"):
            image = cv2.imread(image_path)
            image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
            scanname = os.path.splitext(os.path.basename(image_path))[0]
            dets = [det for det in detections if det["scanname"] in scanname]
            outname = os.path.join(outdir, os.path.basename(image_path))
            self.overlay_detections(image, dets, outname)
            outpaths.append(outname)

        if save_gif:
            gif_path = os.path.join(outdir, scanname.split("_")[0] + ".gif")
            self.save_gif(outpaths, gif_path)
            return gif_path

        return True

    def draw_dets_multi_thresh(
        self,
        image_paths,
        detections,
        outdir,
    ):
        """
        Draws detections on the images under different score thresholds

        Args:
            image_paths: absolute path of images, type: list
            detections:  the output of detector or tracker with the structure
                         {"scanname":xx, "im_bbox": xx, "det_ID": xx, 'det_score': xx}
                         type: list of dict
            outdir: path to save images

        Returns:
            image with bboxes
        """
        fileUtil.mkdir(outdir)
        outpaths = []

        dets_multi_thresh = {}
        for score_thresh in [0.0, 0.05, 0.1, 0.3, 0.5, 0.7]:
            dets_multi_thresh[score_thresh] = [
                det for det in detections if det["det_score"] >= score_thresh
            ]

        for image_path in tqdm(image_paths, desc="Visualizing"):
            image = cv2.imread(image_path)
            image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
            scanname = os.path.splitext(os.path.basename(image_path))[0]

            out_thre = []
            for score_thresh in [0.0, 0.05, 0.1, 0.3, 0.5, 0.7]:
                dets = [
                    det
                    for det in dets_multi_thresh[score_thresh]
                    if det["scanname"] in scanname
                ]
                outname_thre = os.path.join(
                    outdir, scanname + "%.2f.jpg" % score_thresh
                )
                self.overlay_detections(image, dets, outname_thre)
                out_thre.append(outname_thre)

            outname = os.path.join(outdir, os.path.basename(image_path))
            outpaths.append(outname)
            # merge files and delect useless files
            figs = []
            for out in out_thre:
                fig = cv2.imread(out)
                fig = cv2.resize(fig, (300, 300))
                figs.append(fig)
            fileUtil.delete_files(out_thre)
            fig_all = cv2.hconcat(figs)
            cv2.imwrite(outname, fig_all)

        gif_path = os.path.join(outdir, scanname.split("_")[0] + ".gif")
        self.save_gif(outpaths, gif_path)
        return gif_path

    def draw_tracks_multi_thresh(
        self,
        image_paths,
        detections,
        tracks,
        outdir,
        vis_track_after_NMS=True,
        vis_track_after_merge=True,
        ignore_rain=True,
    ):
        """
        Draws tracks on the images under different threholds

        Args:
            image_paths: absolute path of images, type: list
            detections:  the output of detector or tracker with the structure
                         {"scanname":xx, "im_bbox": xx, "det_ID": xx, 'det_score': xx}
                         type: list of dict
            outdir: path to save images
            ignore_rain: do not visualize the rain track

        Returns:
            image with bboxes
        """
        fileUtil.mkdir(outdir)
        outpaths = []

        # NOTE: vis_track_after_NMS is useless, because the tracks have been suppressed in-place by tracker
        if vis_track_after_NMS:
            tracks = [t for t in tracks if not t["NMS_suppressed"]]
            display_option = "track_ID"

        if vis_track_after_merge:
            display_option = "merge_track_ID"

        if ignore_rain:
            tracks = [
                t for t in tracks if ("is_rain" in t.keys() and (not t["is_rain"]))
            ]

        tracks_multi_thresh = {}
        for score_thresh in [
            1,
            2,
            3,
            4,
            5,
            6,
        ]:  # number of bbox from detector in a track
            # id_list = [t["det_IDs"] for t in tracks if sum(t["det_or_pred"]) >= score_thresh]
            id_list = []
            for track in tracks:
                if sum(track["det_or_pred"]) >= score_thresh:
                    for idx in range(len(track["det_or_pred"]) - 1, -1, -1):
                        if track["det_or_pred"][idx]:
                            last_pred_idx = idx
                            break
                    # do not viz the tail of tracks
                    id_list.append(track["det_IDs"][: last_pred_idx + 1])

            tracks_multi_thresh[score_thresh] = list(itertools.chain(*id_list))

        for image_path in tqdm(image_paths, desc="Visualizing"):
            image = cv2.imread(image_path)
            image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
            scanname = os.path.splitext(os.path.basename(image_path))[0]
            dets = [det for det in detections if det["scanname"] in scanname]

            out_thre = []
            for score_thresh in [1, 2, 3, 4, 5, 6]:
                dets_thre = [
                    det
                    for det in dets
                    if det["det_ID"] in tracks_multi_thresh[score_thresh]
                ]
                outname_thre = os.path.join(
                    outdir, scanname + "%.2f.jpg" % score_thresh
                )
                self.overlay_detections(image, dets_thre, outname_thre, display_option)
                out_thre.append(outname_thre)

            outname = os.path.join(outdir, os.path.basename(image_path))
            outpaths.append(outname)
            # merge files and delect useless files
            figs = []
            for out in out_thre:
                fig = cv2.imread(out)
                fig = cv2.resize(fig, (300, 300))
                figs.append(fig)
            fileUtil.delete_files(out_thre)
            fig_all = cv2.hconcat(figs)
            cv2.imwrite(outname, fig_all)

        gif_path = os.path.join(outdir, scanname.split("_")[0] + ".gif")
        self.save_gif(outpaths, gif_path)
        return gif_path

    def overlay_detections(
        self, image, detections, outname, display_option="det_score"
    ):
        """Overlay bounding boxes on images"""

        fig = mplfigure.Figure(frameon=False)
        dpi = fig.get_dpi()
        fig.set_size_inches(
            (self.width + 1e-2) / dpi,
            (self.height + 1e-2) / dpi,
        )
        ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
        ax.axis("off")
        ax.imshow(
            image, extent=(0, self.width, self.height, 0), interpolation="nearest"
        )

        for det in detections:
            x, y, r = det["im_bbox"]
            score = det["det_score"]

            ax.add_patch(
                plt.Rectangle(
                    (x - r, y - r),
                    2 * r,
                    2 * r,
                    fill=False,
                    edgecolor="#FF00FF",
                    linewidth=3,
                )
            )
            if display_option == "track_ID":
                if "track_ID" in det.keys():
                    ax.text(
                        x - r,
                        y - r - 2,
                        "{:d}".format(det["track_ID"]),
                        bbox=dict(facecolor="blue", alpha=0.7),
                        fontsize=14,
                        color="white",
                    )
            elif display_option == "merge_track_ID":
                if "merge_track_ID" in det.keys():
                    display_text = det["merge_track_ID"]
                elif "track_ID" in det.keys():
                    display_text = "{:d}".format(det["track_ID"])
                else:
                    continue
                ax.text(
                    x - r,
                    y - r - 2,
                    display_text,
                    bbox=dict(facecolor="blue", alpha=0.7),
                    fontsize=14,
                    color="white",
                )
            else:  # det_score
                ax.text(
                    x - r,
                    y - r - 2,
                    "{:.3f}".format(score),
                    bbox=dict(facecolor="#FF00FF", alpha=0.7),
                    fontsize=14,
                    color="white",
                )
        fig.savefig(outname)
        plt.close()

    def save_gif(self, image_paths, outpath):
        """
        imageio may load the image in a different format from matplotlib,
        so I just reload the images from local disk by imageio.imread
        """
        seq = []
        image_paths.sort()
        for image_path in image_paths:
            seq.append(imageio.imread(image_path))
        kargs = {"duration": 0.5}
        imageio.mimsave(outpath, seq, "GIF", **kargs)

    def save_predicted_tracks(self, detections, tracks, outpath):
        det_dict = {}
        for det in detections:
            det_dict[det["det_ID"]] = det

        with open(outpath, "a+") as f:
            n_tracks = 0
            for track in tqdm(tracks, desc="Write tracks into csv"):
                if ("is_windfarm" in track.keys() and track["is_windfarm"]) or (
                    "is_rain" in track.keys() and track["is_rain"]
                ):
                    continue

                saved_track = False
                # remove the tail of tracks (which are generated from Kalman filter instead of detector)
                for idx in range(len(track["det_or_pred"]) - 1, -1, -1):
                    if track["det_or_pred"][idx]:
                        last_pred_idx = idx
                        break
                # do not report the tail of tracks
                for idx, det_ID in enumerate(track["det_IDs"]):
                    if idx > last_pred_idx:
                        break
                    det = det_dict[det_ID]
                    f.write(
                        "{:d},{:s},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:.3f},{:s}\n".format(
                            det["track_ID"],
                            det["scanname"],
                            det[f"from_{self.sun_activity}"],
                            det["det_score"],
                            det["im_bbox"][0],
                            det["im_bbox"][1],
                            det["im_bbox"][2],
                            det["geo_bbox"][0],
                            det["geo_bbox"][1],
                            det["geo_bbox"][2],
                            scan_key_to_local_time(det["scanname"]),
                        )
                    )
                    saved_track = True
                if saved_track:
                    n_tracks += 1
