import torch

print("Cuda available:", torch.cuda.is_available())
import random
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
from detectron2.data import (
    detection_utils,
    build_detection_train_loader,
    build_detection_test_loader,
)
from detectron2.data import get_detection_dataset_dicts
import detectron2.data.transforms as T
from detectron2.structures import BoxMode
from detectron2.utils.visualizer import Visualizer
from detectron2.utils.logger import setup_logger


########## args ##########
parser = argparse.ArgumentParser(description="Roost detection")
parser.add_argument(
    "--train_dataset", required=True, type=int, help="training dataset version"
)
parser.add_argument(
    "--imsize", default=1200, type=int, help="reshape input image to this size"
)
parser.add_argument("--flip", action="store_true", help="flip to augment")
parser.add_argument("--rotate", action="store_true", help="rotate to augment")
parser.add_argument(
    "--filter_empty",
    action="store_true",
    help="ignore scans without annotations during training",
)
parser.add_argument("--seed", default=1, type=int, help="random seed")

parser.add_argument(
    "--network", required=True, type=str, help="detection model backbone"
)
parser.add_argument(
    "--pretrain", default="det", type=str, help="init with no, cls, or det pretraining"
)
parser.add_argument(
    "--anchor_strategy", required=True, type=int, help="anchor strategy"
)
parser.add_argument(
    "--reg_loss",
    default="smooth_l1",
    type=str,
    help="smooth_l1 or giou loss for bbox regression",
)

parser.add_argument("--lr", default=1e-3, type=float, help="learning rate")
parser.add_argument("--max_iter", default=150000, type=int, help="max iteration")
parser.add_argument(
    "--eval_period",
    default=0,
    type=int,
    help="evaluate every xx iterations, 0 to disable",
)
parser.add_argument(
    "--checkpoint_period",
    default=10000,
    type=int,
    help="save checkpoint every xx iterations",
)

parser.add_argument(
    "--visualize", action="store_true", help="sample scans and visualize"
)
parser.add_argument(
    "--output_dir", required=True, type=str, help="experiment output dir"
)
args = parser.parse_args()
print(args)
setup_logger(
    output=os.path.join(args.output_dir, "train.log")
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
assert args.train_dataset in DATASETS, "Unknown training dataset. Program ending."
DATASET = DATASETS[args.train_dataset]  # e.g. v0.2.0_standard
DATASET_VERSION = DATASET.split("_")[0]  # e.g. v0.2.0
JSON_ROOT = f"/work/pi_drsheldon_umass_edu/roosts/datasets/roosts_{DATASET_VERSION}"
DATASET_JSON = os.path.join(JSON_ROOT, f"roosts_{DATASET_VERSION}.json")
SPLIT_JSON = os.path.join(JSON_ROOT, f"roosts_{DATASET}_splits.json")
print(f"Training with {DATASET}.")

SPLITS = ["train"]  # "val", "test"
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

# https://github.com/KaimingHe/deep-residual-networks
if "resnet50" in args.network:
    imgnet_pretrained_url = (
        "https://dl.fbaipublicfiles.com/detectron2/ImageNetPretrained/MSRA/R-50.pkl"
    )
elif "resnet101" in args.network:
    imgnet_pretrained_url = (
        "https://dl.fbaipublicfiles.com/detectron2/ImageNetPretrained/MSRA/R-101.pkl"
    )

cfg = get_cfg()
cfg.merge_from_file(model_zoo.get_config_file(CONFIG_FILE))
cfg.INPUT.MIN_SIZE_TRAIN = args.imsize
cfg.INPUT.MAX_SIZE_TRAIN = args.imsize
cfg.INPUT.MIN_SIZE_TEST = args.imsize
cfg.INPUT.MAX_SIZE_TEST = args.imsize
cfg.DATASETS.TRAIN = (f"{DATASET}_train",)
cfg.DATASETS.TEST = (f"{DATASET}_test",)  # not used actually, no eval during training
cfg.DATALOADER.NUM_WORKERS = 2  # this affects the GPU memory
cfg.DATALOADER.FILTER_EMPTY_ANNOTATIONS = args.filter_empty
if args.pretrain == "cls":
    cfg.MODEL.WEIGHTS = imgnet_pretrained_url
elif args.pretrain == "det":
    cfg.MODEL.WEIGHTS = model_zoo.get_checkpoint_url(
        CONFIG_FILE
    )  # initialize from model zoo
else:
    cfg.MODEL.WEIGHT = ""
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
cfg.MODEL.RPN.BBOX_REG_LOSS_TYPE = args.reg_loss
cfg.MODEL.ROI_BOX_HEAD.BBOX_REG_LOSS_TYPE = args.reg_loss
cfg.MODEL.ROI_HEADS.BATCH_SIZE_PER_IMAGE = (
    128  # number of proposals to sample for training, default 512
)
cfg.MODEL.ROI_HEADS.NUM_CLASSES = 1  # only has one class (roost)
cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = 0.5
cfg.SOLVER.IMS_PER_BATCH = 4
cfg.SOLVER.BASE_LR = args.lr
cfg.TEST.EVAL_PERIOD = args.eval_period
cfg.SOLVER.CHECKPOINT_PERIOD = args.checkpoint_period
cfg.SOLVER.MAX_ITER = args.max_iter  # default 40k, 1x 90k, 3x 270k
# cfg.SOLVER.STEPS = (int(.75 * args.max_iter), int(.90 * args.max_iter)) # default 30k, 1x (60k, 80k), 3x (210k, 250k)
cfg.OUTPUT_DIR = args.output_dir

random.seed(args.seed)
os.environ["PYTHONHASHSEED"] = str(args.seed)
np.random.seed(args.seed)
torch.manual_seed(args.seed)
torch.cuda.manual_seed(args.seed)


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

    for anno in dataset["annotations"]:
        if "v0.1.0" not in DATASET and anno["subcategory"] == "bad-track":
            continue  # skip bad tracks

        if anno["scan_id"] in scan_id_to_idx:
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
    roost_metadata = MetadataCatalog.get(f"{DATASET}_train")
    dataset_dicts = get_detection_dataset_dicts(
        f"{DATASET}_train", filter_empty=args.filter_empty
    )

    for i, d in enumerate(random.sample(dataset_dicts, 10)):
        print(d["file_name"])
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
        visualizer = Visualizer(image, metadata=roost_metadata, scale=0.5)
        # visualizer = Visualizer(np.repeat(image[:, :, :1], 3, axis=2), metadata=roost_metadata, scale=0.5)
        out = visualizer.draw_dataset_dict(d)
        cv2.imwrite(f"vis_train_{i}.png", out.get_image()[:, :, ::-1])


# data loader
def mapper(dataset_dict):
    """
    :param dataset_dict: Metadata of one image, in Detectron2 Dataset format.
    """
    dataset_dict = copy.deepcopy(dataset_dict)  # it will be modified by code below

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
    aug_input = T.AugInput(image)

    augs = [T.Resize((args.imsize, args.imsize))]
    if args.flip:
        augs.append(T.RandomFlip())
    if args.rotate:
        augs.append(
            T.RandomRotation(
                angle=[0, 90, 180, 270], expand=False, sample_style="choice"
            )
        )
    augs = T.AugmentationList(augs)
    transforms = augs(aug_input)

    image = torch.from_numpy(np.array(aug_input.image.transpose(2, 0, 1)))
    annos = [
        detection_utils.transform_instance_annotations(
            annotation, transforms, image.shape[1:]
        )
        for annotation in dataset_dict.pop("annotations")
    ]
    instances = detection_utils.annotations_to_instances(annos, image.shape[1:])

    dataset_dict["image"] = image
    dataset_dict["instances"] = instances
    return dataset_dict


########## trainer ##########
class CustomTrainer(engine.DefaultTrainer):
    @classmethod
    def build_train_loader(cls, cfg):
        return build_detection_train_loader(cfg, mapper=mapper)

    @classmethod
    def build_test_loader(cls, cfg, dataset_name=f"{DATASET}_test"):
        return build_detection_test_loader(cfg, dataset_name, mapper=mapper)


def main():
    trainer = CustomTrainer(cfg)
    trainer.resume_or_load(resume=False)
    trainer.train()


if __name__ == "__main__":
    main()
