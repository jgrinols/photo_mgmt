"""Contains utility functions"""
import uuid, asyncio, logging
from io import BytesIO, FileIO
from typing import Tuple, IO, Dict
from json import JSONDecoder

import fs
from fs.mountfs import MountFS
from path import Path
from PIL import Image

from .constants import Constants

logger = logging.getLogger(Constants.LOGGER_NAME)

Dimension = Tuple[int, int]
Bounding = Dict[str, float]

def get_pwgo_fs(phys_fs=None):
    """Gets a filesystem object that can be used to map relative paths from Piwigo"""
    phys_fs = phys_fs or fs.open_fs(Constants.PWGO_GALLERIES_HOST_PATH)
    pgfs = MountFS()
    pgfs.mount(str(Path.joinpath(Constants.PWGO_GALLERY_VIRT_PATH, "galleries")), phys_fs)
    return pgfs

def map_pwgo_path(pwgo_rel_path):
    """Maps a relative path from the Piwigo database to a path that's accessible from the current environment"""
    return Path.joinpath(Constants.PWGO_GALLERY_VIRT_PATH, pwgo_rel_path).normpath()

def convert_pct_bounding_box(img_dimen: Dimension, bounding_box: Bounding) -> Bounding:
    """Converts the image dimension percentage based bounding box format used by Rekognition
    into the pixel coordinate form used by PIL/Pillow"""
    invalid_checks = [
        lambda b: any(v < 0 or v > 1 for v in b.values()),
        lambda b: b["Left"] + b["Width"] > 1,
        lambda b: b["Top"] + b["Height"] > 1
    ]
    for check in invalid_checks:
        if check(bounding_box):
            raise ValueError("Invalid values for percentage bounding box")

    left = img_dimen[0] * bounding_box["Left"]
    top = img_dimen[1] * bounding_box["Top"]
    right = img_dimen[0] * (bounding_box["Left"] + bounding_box["Width"])
    bottom = img_dimen[1] * (bounding_box["Top"] + bounding_box["Height"])

    return (int(round(left)),int(round(top)),int(round(right)),int(round(bottom)))

def get_cropped_image(file: IO, box: Bounding) -> IO:
    """generates a cropped image file from an exisiting image file using the specified
    bounding box--the bounding box is expected as a rekognition (left, top, width, height) box"""
    with Image.open(file) as img:
        px_box = convert_pct_bounding_box(img.size, box)
        cropped = img.crop(px_box)

    if Constants.IMG_CROP_PATH:
        cropped_file = FileIO(Constants.IMG_CROP_PATH.joinpath(f"{uuid.uuid4()}.JPEG"), mode='wb+')
    else:
        cropped_file = BytesIO()

    cropped.save(cropped_file, format="JPEG")
    cropped_file.seek(0)

    return cropped_file

def delayed_task_generator(coro, *args, delay=0, **kwargs):
    """generator function which accepts a coroutine and yields back a
    sleep task with given <delay>."""
    sleep_task = asyncio.create_task(asyncio.sleep(delay))
    cancel = yield sleep_task
    if cancel and not sleep_task._must_cancel:
        sleep_task.cancel()
    if not (sleep_task.cancelled() or sleep_task._must_cancel):
        yield asyncio.create_task(coro(*args, **kwargs))

def extract_json_objects(text, decoder=JSONDecoder()):
    """Find JSON objects in text, and yield the decoded JSON data

    Does not attempt to look for JSON arrays, text, or other JSON types outside
    of a parent JSON object.

    """
    pos = 0
    while True:
        match = text.find('{', pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1

def parse_sql(filename):
    data = open(filename, 'r').readlines()
    stmts = []
    delim = ';'
    stmt = ''

    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        if line.startswith('--'):
            continue

        if 'DELIMITER' in line:
            delim = line.split()[1]
            continue

        if delim not in line:
            stmt += line.replace(delim, ';')
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip().rstrip(delim))
            stmt = ''
        else:
            stmts.append(line.strip().rstrip(delim))
    return stmts

class NoopTask:
    """Dummy task"""
    def done(self):
        """Returns true"""
        return True
