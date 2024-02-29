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

from adaptors_fpn import CustomResize

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
    "--adaptor", default="None", type=str, help="adaptor: linear, multi-layer"
)
parser.add_argument(
    "--input_channels", default=3, type=int, help="number of input channels for adaptor"
)

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

if args.train_dataset == 1:
    DATASET = "roosts_v0.1.0"
    print("Training with the roosts_v0.1.0 train split.")
elif args.train_dataset == 2:
    DATASET = "roosts_v0.2.0_all_nmd_nbt"
    print("Training with the roosts_v0.2.0 all train split. No miss day, no bad track.")
elif args.train_dataset == 3:
    DATASET = "roosts_v0.2.0_all_md_nbt"
    print(
        "Training with the roosts_v0.2.0 all train split. Keeping miss day, no bad track."
    )
elif args.train_dataset == 4:
    DATASET = "roosts_v0.2.0_all_nmd_bt"
    print(
        "Training with the roosts_v0.2.0 all train split. No miss day, keeping bad track."
    )
elif args.train_dataset == 5:
    DATASET = "roosts_v0.2.0_all_md_bt"
    print(
        "Training with the roosts_v0.2.0 all train split. Keeping miss day, keeping bad track."
    )
elif args.train_dataset == 6:
    DATASET = "roosts_v0.2.0_dualpol"
    print(
        "Training with the roosts_v0.2.0 dualpol train split. Keeping miss day, no bad track."
    )
else:
    print("Unknown training dataset. Program ending.")

if "v0.1.0" in DATASET:
    JSON_ROOT = "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.1.0"
    DATASET_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0.json")
elif "v0.2.0" in DATASET:
    JSON_ROOT = "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.2.0"
    DATASET_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0.json")

if "v0.1.0" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0_standard_splits.json")
elif "v0.2.0_all" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0_all_splits.json")
elif "v0.2.0_dualpol" in DATASET:
    SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.2.0_dualpol_splits.json")

NO_BAD_TRACK_DATASET_VERSIONS = [2, 3, 6]
NO_MISS_DAY_DATASET_VERSIONS = [2, 4]

SPLITS = ["train"]  # "val", "test"
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
else:
    sys.exit("number of channels <%d> not in list" % (args.input_channels))

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
    # cfg.MODEL.BACKBONE.FREEZE_AT = 0 # GPS: to unfreeze all layers (default=2)

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

    for anno in dataset["annotations"]:
        if (
            args.train_dataset in NO_BAD_TRACK_DATASET_VERSIONS
            and anno["subcategory"] == "bad-track"
        ):
            continue
        if (
            args.train_dataset in NO_MISS_DAY_DATASET_VERSIONS
            and anno["day_notes"] == "miss"
        ):
            continue

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

# GPS: load datasets to load neighbor frames
split_dicts = {}
for d in SPLITS:
    split_dicts[d] = get_detection_dataset_dicts(
        f"{DATASET}_{d}", filter_empty=args.filter_empty
    )

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
    previous_dict = split_dicts[dataset_dict["split"]][
        dataset_dict["neighbor_id"][0]
    ]  # GPS: dict of previous frame
    previous_dict2 = split_dicts[dataset_dict["split"]][dataset_dict["neighbor_id"][1]]

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

    augs = [
        CustomResize((args.imsize, args.imsize))
    ]  # GPS: default interp=Image.BILINEAR
    # augs = [T.Resize((args.imsize, args.imsize))]
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
