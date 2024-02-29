import torch

print("Cuda available:", torch.cuda.is_available())
import os
import json
import numpy as np
import matplotlib.colors as pltc
import copy
import cv2
import argparse
from detectron2 import model_zoo, engine
from detectron2.config import get_cfg
from detectron2.data import MetadataCatalog, DatasetCatalog
from detectron2.data import detection_utils, build_detection_test_loader
from detectron2.data import get_detection_dataset_dicts
import detectron2.data.transforms as T
from detectron2.structures import BoxMode
from detectron2.checkpoint import DetectionCheckpointer
from detectron2.evaluation import COCOEvaluator, inference_on_dataset
from detectron2.utils.visualizer import Visualizer, ColorMode
from detectron2.utils.logger import setup_logger

from adaptors_fpn import CustomResize

########## args ##########
parser = argparse.ArgumentParser(description="Roost detection")
parser.add_argument(
    "--test_dataset", required=True, type=int, help="testing dataset version"
)
parser.add_argument("--ckpt_path", type=str, help="pretrained predictor")
parser.add_argument(
    "--eval_strategy",
    default=1,
    type=int,
    help="1: ignore small objects < 15x15 out of 1200x1200",
)

parser.add_argument(
    "--adaptor", default="None", type=str, help="adaptor: linear, multi-layer"
)
parser.add_argument(
    "--input_channels", default=3, type=int, help="number of input channels for adaptor"
)

parser.add_argument(
    "--imsize", default=1200, type=int, help="reshape input image to this size"
)
parser.add_argument(
    "--network", required=True, type=str, help="detection model backbone"
)
parser.add_argument(
    "--anchor_strategy", required=True, type=int, help="anchor strategy"
)

parser.add_argument(
    "--visualize",
    action="store_true",
    help="visualize ground truth and inference results on all test data",
)
parser.add_argument(
    "--output_dir", required=True, type=str, help="experiment output dir"
)
args = parser.parse_args()
print(args)
setup_logger(
    output=os.path.join(args.output_dir, "eval.log")
)  # partly repetitive of slurm logs

if args.test_dataset == 1:
    DATASET = "roosts_v0.1.0"
    print("Testing with the roosts_v0.1.0 test split.")
elif args.test_dataset == 2:
    DATASET = "roosts_v0.2.0_standard_only_swallow"
    print(
        "Testing with swallow roost annotations in the roosts_v0.2.0 standard test split."
    )
elif args.test_dataset == 3:
    DATASET = "roosts_v0.2.0_standard"
    print("Testing with the roosts_v0.2.0 standard test split.")
elif args.test_dataset == 4:
    DATASET = "roosts_v0.2.0_dualpol"
    print("Testing with the roosts_v0.2.0 dualpol test split.")
else:
    print("Unknown testing dataset. Program ending.")
    exit()

if "v0.1.0" in DATASET:
    JSON_ROOT = "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.1.0"
    DATASET_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0.json")
elif "v0.2.0" in DATASET:
    JSON_ROOT = "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.2.0"
    DATASET_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0.json")

if "v0.1.0" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0_standard_splits.json")
elif "v0.2.0_standard" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0_standard_splits.json")
elif "v0.2.0_dualpol" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0_dualpol_splits.json")

SWALLOW_ROOST_DATASET_VERSIONS = [2]
NO_BAD_TRACK_DATASET_VERSIONS = [2, 3, 4]
NO_MISS_DAY_DATASET_VERSIONS = []

SPLITS = ["test"]  # "train", "val"
ARRAY_DIR = "/mnt/nfs/datasets/RadarNPZ"

if args.input_channels == 1:
    CHANNELS = [("reflectivity", 0.5)]
elif args.input_channels == 2:
    CHANNELS = [("reflectivity", 0.5), ("reflectivity", 1.5)]
elif args.input_channels == 3:
    CHANNELS = [("reflectivity", 0.5), ("reflectivity", 1.5), ("velocity", 0.5)]
elif args.input_channels == 4:
    CHANNELS = [
        ("reflectivity", 0.5),
        ("reflectivity", 1.5),
        ("velocity", 0.5),
        ("spectrum_width", 0.5),
    ]
elif args.input_channels == 6:
    CHANNELS = [
        ("reflectivity", 0.5),
        ("reflectivity", 1.5),
        ("velocity", 0.5),
        ("velocity", 1.5),
        ("spectrum_width", 0.5),
        ("spectrum_width", 1.5),
    ]
elif args.input_channels == 9:
    CHANNELS = [
        ("reflectivity", 0.5),
        ("reflectivity", 1.5),
        ("velocity", 0.5),
        ("velocity", 1.5),
        ("spectrum_width", 0.5),
        ("spectrum_width", 1.5),
        ("reflectivity", 2.5),
        ("velocity", 2.5),
        ("spectrum_width", 2.5),
    ]
elif args.input_channels == 12:
    CHANNELS = [
        ("reflectivity", 0.5),
        ("reflectivity", 1.5),
        ("velocity", 0.5),
        ("velocity", 1.5),
        ("spectrum_width", 0.5),
        ("spectrum_width", 1.5),
        ("reflectivity", 2.5),
        ("velocity", 2.5),
        ("spectrum_width", 2.5),
        ("reflectivity", 3.5),
        ("velocity", 3.5),
        ("spectrum_width", 3.5),
    ]
elif args.input_channels == 15:
    CHANNELS = [
        ("reflectivity", 0.5),
        ("reflectivity", 1.5),
        ("velocity", 0.5),
        ("velocity", 1.5),
        ("spectrum_width", 0.5),
        ("spectrum_width", 1.5),
        ("reflectivity", 2.5),
        ("reflectivity", 3.5),
        ("velocity", 2.5),
        ("velocity", 3.5),
        ("spectrum_width", 2.5),
        ("spectrum_width", 3.5),
        ("reflectivity", 4.5),
        ("velocity", 4.5),
        ("spectrum_width", 4.5),
    ]

NORMALIZERS = {
    "reflectivity": pltc.Normalize(vmin=-5, vmax=35),
    "velocity": pltc.Normalize(vmin=-15, vmax=15),
    "spectrum_width": pltc.Normalize(vmin=0, vmax=10),
    "differential_reflectivity": pltc.Normalize(vmin=-4, vmax=8),
    "differential_phase": pltc.Normalize(vmin=0, vmax=250),
    "cross_correlation_ratio": pltc.Normalize(vmin=0, vmax=1.1),
}


########## config ##########
# https://github.com/darkecology/detectron2/blob/master/detectron2/config/defaults.py

if args.network == "resnet50-FPN":
    CONFIG_FILE = "COCO-Detection/faster_rcnn_R_50_FPN_3x.yaml"
elif args.network == "resnet101-FPN":
    CONFIG_FILE = "COCO-Detection/faster_rcnn_R_101_FPN_3x.yaml"
elif args.network == "resnet50-c4":
    CONFIG_FILE = "COCO-Detection/faster_rcnn_R_50_C4_3x.yaml"
else:
    exit()


cfg = get_cfg()
cfg.merge_from_file(model_zoo.get_config_file(CONFIG_FILE))
cfg.INPUT.MIN_SIZE_TEST = args.imsize
cfg.INPUT.MAX_SIZE_TEST = args.imsize
cfg.DATASETS.TEST = (f"{DATASET}_test",)
cfg.DATALOADER.NUM_WORKERS = 2  # this affects the GPU memory
cfg.MODEL.WEIGHTS = args.ckpt_path  # path to the model we just trained
if args.anchor_strategy == 1:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [32],
        [64],
        [128],
        [256],
        [512],
    ]  # One size for each in feature map
elif args.anchor_strategy == 2:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [16],
        [32],
        [64],
        [128],
        [256],
    ]  # One size for each in feature map
elif args.anchor_strategy == 3:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [32],
        [64],
        [96],
        [128],
        [256],
    ]  # One size for each in feature map
elif args.anchor_strategy == 4:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [16, 32, 48, 64, 80, 96, 112, 128, 144],
        [16, 32, 48, 64, 80, 96, 112, 128, 144],
        [32, 48, 64, 80, 96, 112, 128, 144, 256],
        [48, 64, 80, 96, 112, 128, 144, 256, 512],
        [48, 64, 80, 96, 112, 128, 144, 256, 512],
    ]  # One size for each in feature map
elif args.anchor_strategy == 5:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [[16, 32, 48, 64, 80, 96, 112, 128, 144]]
elif args.anchor_strategy == 6:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [16, 20, 24, 28, 32],
        [32, 40, 48, 56, 64],
        [64, 80, 96, 112, 128],
        [128, 160, 192, 224, 256],
        [256, 320, 384, 448, 512],
    ]
elif args.anchor_strategy == 7:  # for c4
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512]
    ]
elif args.anchor_strategy == 8:  # for imsz 800
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [11, 14, 16, 18, 21],
        [22, 28, 32, 36, 42],
        [44, 56, 64, 72, 84],
        [88, 112, 128, 144, 168],
        [176, 224, 256, 288, 336],
    ]
elif args.anchor_strategy == 9:  # for imsz 800
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [11, 12, 14, 15, 16, 17, 18, 20, 21],
        [22, 24, 28, 30, 32, 34, 36, 40, 42],
        [44, 48, 56, 60, 64, 68, 72, 80, 84],
        [88, 96, 112, 120, 128, 136, 144, 160, 168],
        [176, 192, 224, 240, 256, 272, 288, 320, 336],
    ]
elif args.anchor_strategy == 10:
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [
        [16, 18, 20, 22, 24, 26, 28, 30, 32],
        [32, 36, 40, 44, 48, 52, 56, 60, 64],
        [64, 72, 80, 88, 96, 104, 112, 120, 128],
        [128, 144, 160, 176, 192, 208, 224, 240, 256],
        [256, 288, 320, 352, 384, 416, 448, 480, 512],
    ]
else:
    exit()  # not implement
cfg.MODEL.ANCHOR_GENERATOR.ASPECT_RATIOS = [
    [1.0]
]  # default [[0.5, 1.0, 2.0]], same for all in feature maps
cfg.MODEL.ROI_HEADS.NUM_CLASSES = 1  # only has one class (roost)
cfg.MODEL.ROI_HEADS.NMS_THRESH_TEST = 0.3  # default 0.5
cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = 0.005
cfg.OUTPUT_DIR = args.output_dir

####################################################
#### GPS: when using adaptors ######################
####################################################
if args.adaptor != "None":
    cfg.MODEL.BACKBONE.NAME = "custom_build_resnet_fpn_backbone"
    cfg.ADAPTOR_TYPE = args.adaptor
    cfg.ADAPTOR_IN_CHANNELS = len(CHANNELS) * 3
    cfg.MODEL.PIXEL_MEAN = []
    cfg.MODEL.PIXEL_STD = []
    for i in range(cfg.ADAPTOR_IN_CHANNELS):
        cfg.MODEL.PIXEL_MEAN.append(127.5)
        cfg.MODEL.PIXEL_STD.append(1.0)

########## data ##########
with open(DATASET_JSON) as f:
    dataset = json.load(f)
attributes = dataset["info"]["array_fields"]
elevations = dataset["info"]["array_elevations"]


# data registering
def get_roost_dicts(split):
    with open(SPLIT_JSON) as f:
        scan_list = json.load(f)[split]

    dataset_dicts = []
    scan_id_to_idx = {}
    idx = 0
    for scan_id in scan_list:
        # GPS: calculate neighbor frames indices
        neighbor_frame_id = []
        current_day = dataset["scans"][scan_id]["key"].split("_")[0][-2::]
        if scan_id == 0:  # GPS: repeat current frame
            neighbor_frame_id.append(idx)
            neighbor_frame_id.append(idx)
        elif scan_id == 1:  # GPS: repeat previous frame
            neighbor_frame_id.append(idx - 1)
            neighbor_frame_id.append(idx - 1)
        else:
            previous_day = dataset["scans"][scan_id - 1]["key"].split("_")[0][-2::]
            previous_day2 = dataset["scans"][scan_id - 2]["key"].split("_")[0][-2::]
            if current_day != previous_day:
                neighbor_frame_id.append(idx)
                neighbor_frame_id.append(idx)
            elif current_day != previous_day2:
                neighbor_frame_id.append(idx - 1)
                neighbor_frame_id.append(idx - 1)
            else:
                neighbor_frame_id.append(idx - 1)
                neighbor_frame_id.append(idx - 2)

        if "v0.1.0" in DATASET:
            array_path = os.path.join(
                ARRAY_DIR, "v0.1.0", dataset["scans"][scan_id]["array_path"]
            )
        elif "v0.2.0" in DATASET:
            array_path = os.path.join(
                ARRAY_DIR,
                dataset["scans"][scan_id]["dataset_version"],
                dataset["scans"][scan_id]["array_path"],
            )
        scan_id_to_idx[scan_id] = idx
        dataset_dicts.append(
            {
                "file_name": array_path,
                "image_id": scan_id,
                "neighbor_id": neighbor_frame_id,  # GPS: to track neighbor frame
                "split": split,  # GPS: to track the split
                "height": dataset["info"]["array_shape"][-1],
                "width": dataset["info"]["array_shape"][-2],
                "annotations": [],
            }
        )
        idx += 1

    scan_id_set = set(scan_id_to_idx)
    for anno in dataset["annotations"]:
        if (
            args.test_dataset in SWALLOW_ROOST_DATASET_VERSIONS
            and anno["subcategory"] != "swallow-roost"
        ):
            continue
        if (
            args.test_dataset in NO_BAD_TRACK_DATASET_VERSIONS
            and anno["subcategory"] == "bad-track"
        ):
            continue
        if (
            args.test_dataset in NO_MISS_DAY_DATASET_VERSIONS
            and anno["day_notes"] == "miss"
        ):
            continue

        if anno["scan_id"] in scan_id_set:
            if (
                anno["bbox"][2] + anno["bbox"][3]
            ) / 2.0 <= args.imsize * 15.0 / 1200 and args.eval_strategy == 1:
                continue
            dataset_dicts[scan_id_to_idx[anno["scan_id"]]]["annotations"].append(
                {
                    "bbox": [
                        anno["bbox"][0],
                        anno["bbox"][1],
                        anno["bbox"][2] + anno["bbox"][0],
                        anno["bbox"][3] + anno["bbox"][1],
                    ],
                    "bbox_mode": BoxMode.XYXY_ABS,
                    "segmentation": [],
                    "category_id": 0,
                }
            )
    return dataset_dicts


for d in SPLITS:
    DatasetCatalog.register(f"{DATASET}_{d}", lambda d=d: get_roost_dicts(d))
    MetadataCatalog.get(f"{DATASET}_{d}").set(thing_classes=["roost"])

# GPS: load datasets to load neighbor frames
split_dicts = {}
for d in SPLITS:
    split_dicts[d] = get_detection_dataset_dicts(f"{DATASET}_{d}", filter_empty=False)

if args.visualize:
    roost_metadata = MetadataCatalog.get(f"{DATASET}_test")
    dataset_dicts = get_detection_dataset_dicts(f"{DATASET}_test", filter_empty=False)

    predictor = engine.DefaultPredictor(
        cfg
    )  # default predictor takes care of data loading/model init etc.

    # for i, d in enumerate(random.sample(dataset_dicts, 10)):
    for i, d in enumerate(dataset_dicts):
        outname = os.path.basename(d["file_name"]).split(".")[0]
        print(i, outname)
        array = np.load(d["file_name"])["array"]
        image = np.stack(
            [
                NORMALIZERS[attr](
                    array[attributes.index(attr), elevations.index(elev), :, :]
                )
                for (attr, elev) in CHANNELS
            ],
            axis=-1,
        )
        np.nan_to_num(image, copy=False, nan=0.0)
        image = (image * 255).astype(np.uint8)
        outputs = predictor(image)
        v1 = Visualizer(
            image, metadata=roost_metadata, scale=0.5, instance_mode=ColorMode.IMAGE_BW
        )
        out1 = v1.draw_instance_predictions(outputs["instances"].to("cpu")).get_image()
        # use a new visualizer, otherwise, the prediction and gt bbox will overlayed
        v2 = Visualizer(
            image, metadata=roost_metadata, scale=0.5, instance_mode=ColorMode.IMAGE_BW
        )
        out2 = v2.draw_dataset_dict(d).get_image()
        out = cv2.hconcat([out1, out2])
        cv2.imwrite(f"{cfg.OUTPUT_DIR}/{outname}.jpg", out1)
        cv2.imwrite(f"{cfg.OUTPUT_DIR}/{outname}_gt.jpg", out2)


# data loader
def mapper(dataset_dict):
    """
    :param dataset_dict: Metadata of one image, in Detectron2 Dataset format.
    """
    dataset_dict = copy.deepcopy(dataset_dict)  # it will be modified by code below
    previous_dict = split_dicts[dataset_dict["split"]][
        dataset_dict["neighbor_id"][0]
    ]  # GPS: dict of previous frame
    previous_dict2 = split_dicts[dataset_dict["split"]][
        dataset_dict["neighbor_id"][1]
    ]  # GPS: dict of next frame

    array = np.load(dataset_dict["file_name"])["array"]
    image = np.stack(
        [
            NORMALIZERS[attr](
                array[attributes.index(attr), elevations.index(elev), :, :]
            )
            for (attr, elev) in CHANNELS
        ],
        axis=-1,
    )
    np.nan_to_num(image, copy=False, nan=0.0)
    image = (image * 255).astype(np.uint8)

    # GPS: do the same with neighbor array (t-1) and concatenate with current frame
    previous_array = np.load(previous_dict["file_name"])["array"]
    previous_image = np.stack(
        [
            NORMALIZERS[attr](
                previous_array[attributes.index(attr), elevations.index(elev), :, :]
            )
            for (attr, elev) in CHANNELS
        ],
        axis=-1,
    )
    np.nan_to_num(previous_image, copy=False, nan=0.0)
    previous_image = (previous_image * 255).astype(np.uint8)
    # GPS: same for t-2
    previous_array2 = np.load(previous_dict2["file_name"])["array"]
    previous_image2 = np.stack(
        [
            NORMALIZERS[attr](
                previous_array2[attributes.index(attr), elevations.index(elev), :, :]
            )
            for (attr, elev) in CHANNELS
        ],
        axis=-1,
    )
    np.nan_to_num(previous_image2, copy=False, nan=0.0)
    previous_image2 = (previous_image2 * 255).astype(np.uint8)

    # GPS: concatenate all time-frames
    image = np.concatenate((previous_image2, previous_image, image), axis=2)

    aug_input = T.AugInput(image)
    transform = CustomResize((args.imsize, args.imsize))(aug_input)  # GPS
    # transform = T.Resize((args.imsize, args.imsize))(aug_input)
    image = torch.from_numpy(np.array(aug_input.image.transpose(2, 0, 1)))

    annos = [
        detection_utils.transform_instance_annotations(
            annotation, [transform], image.shape[1:]
        )
        for annotation in dataset_dict.pop("annotations")
    ]
    instances = detection_utils.annotations_to_instances(annos, image.shape[1:])

    dataset_dict["image"] = image
    dataset_dict["instances"] = instances
    return dataset_dict


def main():
    model = engine.defaults.build_model(cfg)
    DetectionCheckpointer(model).load(args.ckpt_path)
    test_loader = build_detection_test_loader(cfg, f"{DATASET}_test", mapper=mapper)
    evaluator = COCOEvaluator(
        f"{DATASET}_test",
        tasks=("bbox",),
        distributed=False,
        use_fast_impl=True,
        output_dir=cfg.OUTPUT_DIR,
    )
    print(inference_on_dataset(model, test_loader, evaluator))


if __name__ == "__main__":
    main()
