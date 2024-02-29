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

DATASETS = {
    1: "v0.1.0_standard",
    2: "v0.2.0_no_dualpol",
    3: "v0.2.0_dualpol",
    4: "v0.2.0_standard",
    5: "v0.2.0_add_no_dualpol",
    6: "v0.2.0_add_dualpol",
    7: "v0.2.0_add_standard",
    8: "v0.2.0_station1",
    9: "v0.2.0_station2",
    10: "v0.2.0_station3",
    11: "v0.2.0_station4",
    12: "v0.2.1_add_no_dualpol",
    13: "v0.2.2_add_no_dualpol",
    14: "v0.2.3_add_no_dualpol",
    15: "v0.2.4_add_no_dualpol",
    16: "v0.2.5_add_no_dualpol",
}
assert args.test_dataset in DATASETS, "Unknown testing dataset. Program ending."
DATASET = DATASETS[args.test_dataset]  # e.g. v0.2.0_standard
DATASET_VERSION = DATASET.split("_")[0]  # e.g. v0.2.0
JSON_ROOT = f"/work/pi_drsheldon_umass_edu/roosts/datasets/roosts_{DATASET_VERSION}"
DATASET_JSON = os.path.join(JSON_ROOT, f"roosts_{DATASET_VERSION}.json")
SPLIT_JSON = os.path.join(JSON_ROOT, f"roosts_{DATASET}_splits.json")
print(f"Testing with {DATASET}.")

SPLITS = ["test"]  # "train", "val"
ARRAY_DIR = (
    "/gypsum/datasets/RadarNPZ"  # "/work/pi_drsheldon_umass_edu/roosts/RadarNPZ"
)
CHANNELS = [("reflectivity", 0.5), ("reflectivity", 1.5), ("velocity", 0.5)]
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
        if "v0.1.0" in DATASET:
            array_path = os.path.join(
                ARRAY_DIR, "v0.1.0", dataset["scans"][scan_id]["array_path"]
            )
        elif "v0.2" in DATASET:  # TODO: hard coded
            if dataset["scans"][scan_id]["dataset_version"] == "v0.1.0":
                array_path = os.path.join(
                    ARRAY_DIR, "v0.1.0", dataset["scans"][scan_id]["array_path"]
                )
            else:  # datasets v0.2.B all share the arrays v0.2.0
                array_path = os.path.join(
                    ARRAY_DIR, "v0.2.0", dataset["scans"][scan_id]["array_path"]
                )
        scan_id_to_idx[scan_id] = idx
        dataset_dicts.append(
            {
                "file_name": array_path,
                "image_id": scan_id,
                "height": dataset["info"]["array_shape"][-1],
                "width": dataset["info"]["array_shape"][-2],
                "annotations": [],
            }
        )
        idx += 1

    scan_id_set = set(scan_id_to_idx)
    for anno in dataset["annotations"]:
        if "v0.1.0" not in DATASET and anno["subcategory"] == "bad-track":
            continue  # skip bad tracks

        if anno["scan_id"] in scan_id_set:
            if (
                anno["bbox"][2] + anno["bbox"][3]
            ) / 2.0 <= args.imsize * 15.0 / 1200 and args.eval_strategy in [1, 91]:
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
                    # "segmentation": [],
                    "category_id": 0,
                }
            )
    return dataset_dicts


for d in SPLITS:
    DatasetCatalog.register(f"{DATASET}_{d}", lambda d=d: get_roost_dicts(d))
    MetadataCatalog.get(f"{DATASET}_{d}").set(thing_classes=["roost"])

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

    array = np.load(dataset_dict["file_name"])["array"]
    if args.eval_strategy == 91:  # center-corner
        tmp = array.copy()
        h, w = array.shape[-2], array.shape[-1]
        array[:, :, : int(h / 2), : int(w / 2)] = tmp[:, :, int(h / 2) :, int(w / 2) :]
        array[:, :, : int(h / 2), int(w / 2) :] = tmp[:, :, int(h / 2) :, : int(w / 2)]
        array[:, :, int(h / 2) :, : int(w / 2)] = tmp[:, :, : int(h / 2), int(w / 2) :]
        array[:, :, int(h / 2) :, int(w / 2) :] = tmp[:, :, : int(h / 2), : int(w / 2)]
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
    aug_input = T.AugInput(image)
    transform = T.Resize((args.imsize, args.imsize))(aug_input)
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
