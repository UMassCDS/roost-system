from detectron2.config import get_cfg
from detectron2.engine import DefaultPredictor
from detectron2 import model_zoo
import matplotlib.colors as pltc
import numpy as np
import os
from tqdm import tqdm
from geotiff import GeoTiff


class Detector:
    def __init__(
        self,
        ckpt_path,  # path of pretrained detector
        imsize,  # input image size
        anchor_sizes,  # predefined anchor sizes
        nms_thresh,  # non-maximum suppression
        score_thresh,  # filter out detections with score lower than score_thresh
        config_file,  # define the detection model
        use_gpu,  # GPU or CPU
        version,  # detector version
    ):
        cfg = get_cfg()
        cfg.merge_from_file(model_zoo.get_config_file(config_file))
        cfg.INPUT.MIN_SIZE_TEST = imsize
        cfg.INPUT.MAX_SIZE_TEST = imsize
        cfg.INPUT.FORMAT = "BGR"  # just so scan channels are not altered
        cfg.MODEL.WEIGHTS = ckpt_path
        cfg.MODEL.ANCHOR_GENERATOR.SIZES = anchor_sizes
        cfg.MODEL.ANCHOR_GENERATOR.ASPECT_RATIOS = [[1.0]]
        cfg.MODEL.ROI_HEADS.NUM_CLASSES = 1
        cfg.MODEL.ROI_HEADS.NMS_THRESH_TEST = nms_thresh
        cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = score_thresh
        cfg.MODEL.DEVICE = "cuda" if use_gpu else "cpu"

        self.version = version
        if version == "v2":
            self.predictor = DefaultPredictor(cfg)
        elif version == "v3":
            import roosts.detection.adaptors_fpn as adaptors_fpn

            cfg.MODEL.BACKBONE.NAME = "build_adaptor_resnet_fpn_backbone"
            cfg.ADAPTOR_TYPE = "linear"
            cfg.ADAPTOR_IN_CHANNELS = 9
            cfg.MODEL.PIXEL_MEAN = []
            cfg.MODEL.PIXEL_STD = []
            for _ in range(cfg.ADAPTOR_IN_CHANNELS):
                cfg.MODEL.PIXEL_MEAN.append(127.5)
                cfg.MODEL.PIXEL_STD.append(1.0)
            self.predictor = adaptors_fpn.AdaptorPredictor(cfg)
        else:
            raise NotImplementedError

    def _preprocess_npz_file(self, npz_paths):
        # extract useful information from raw scan file, normalize the data and convert it to uint8
        CHANNELS = [("reflectivity", 0.5), ("reflectivity", 1.5), ("velocity", 0.5)]
        NORMALIZERS = {
            "reflectivity": pltc.Normalize(vmin=-5, vmax=35),
            "velocity": pltc.Normalize(vmin=-15, vmax=15),
            "spectrum_width": pltc.Normalize(vmin=0, vmax=10),
            "differential_reflectivity": pltc.Normalize(vmin=-4, vmax=8),
            "differential_phase": pltc.Normalize(vmin=0, vmax=250),
            "cross_correlation_ratio": pltc.Normalize(vmin=0, vmax=1.1),
        }
        attributes = ["reflectivity", "velocity", "spectrum_width"]
        elevations = [0.5, 1.5, 2.5, 3.5, 4.5]

        image_list = []
        for i in range(len(npz_paths)):
            array = np.load(npz_paths[i])["array"]
            image_list.append(
                np.stack(
                    [
                        NORMALIZERS[attr](
                            array[attributes.index(attr), elevations.index(elev), :, :]
                        )
                        for (attr, elev) in CHANNELS
                    ],
                    axis=-1,
                )
            )

        return np.concatenate(image_list, axis=2)

    def run(self, array_files, file_type="npz"):
        outputs = []
        count = 0
        for idx, file in enumerate(tqdm(array_files, desc="Detecting")):
            # extract scanname
            name = os.path.splitext(os.path.basename(file))[0]
            # preprocess data
            if file_type == "npz":
                if self.version == "v2":
                    file_list = [file]
                elif self.version == "v3":
                    if idx == 0:
                        file_list = [file, file, file]
                    elif idx == 1:
                        file_list = [array_files[0], array_files[0], file]
                    else:
                        file_list = [array_files[idx - 2], array_files[idx - 1], file]
                else:
                    raise NotImplementedError
                data = self._preprocess_npz_file(file_list)
            elif file_type == "tiff":
                data = np.array(GeoTiff(file, crs_code=4326).read())
            np.nan_to_num(data, copy=False, nan=0.0)
            data = (data * 255).astype(np.uint8)
            # detect roosts
            prediction = self.predictor(data)["instances"]
            scores = prediction.scores.cpu().numpy()
            if len(scores) == 0:  # no roost detected in this scan
                continue
            bbox = prediction.pred_boxes.tensor.cpu().numpy()
            H, W = prediction.image_size
            centers = prediction.pred_boxes.get_centers().cpu().numpy()
            if file_type == "npz":
                # flip the y axis, from geographical (big y means North) to image (big y means lower)
                centers[:, 1] = H - centers[:, 1]
            radius = ((bbox[:, 2] - bbox[:, 0]) + (bbox[:, 3] - bbox[:, 1])) / 4.0
            radius = radius[:, np.newaxis]
            bbox_xyr = np.hstack((centers, radius))
            # reformat the detections
            for kk in range(len(scores)):
                det = {
                    "scanname": name,
                    "det_ID": count,
                    "det_score": scores[kk],
                    "im_bbox": bbox_xyr[kk],
                }
                count += 1
                outputs.append(det)

        return outputs
