import numpy as np


def calculate_box_overlap(box, boxes):
    if boxes.ndim == 1:
        boxes = boxes[np.newaxis, :]

    ixmin = np.maximum(boxes[:, 0], box[0])
    iymin = np.maximum(boxes[:, 1], box[1])
    ixmax = np.minimum(boxes[:, 2], box[2])
    iymax = np.minimum(boxes[:, 3], box[3])
    iw = np.maximum(ixmax - ixmin + 1.0, 0.0)
    ih = np.maximum(iymax - iymin + 1.0, 0.0)
    inters = iw * ih

    uni = (
        (box[2] - box[0] + 1.0) * (box[3] - box[1] + 1.0)
        + (boxes[:, 2] - boxes[:, 0] + 1.0) * (boxes[:, 3] - boxes[:, 1] + 1.0)
        - inters
    )

    overlaps = inters / uni
    return overlaps
