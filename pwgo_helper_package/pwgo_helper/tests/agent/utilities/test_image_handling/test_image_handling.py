"""container for TestImageHandling"""
import os

import pytest
from PIL import Image
import imagehash

from .....agent import utilities

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

class TestImageHandling:
    """Tests for image handling related functions in the utilities module"""
    def test_convert_pct_bounding_box_simple(self):
        """basic functional test of convert_pct_bounding_box"""
        test_data = {
            "ImageDimensions": { "Width": 3024, "Height": 4032 },
            "BoundingBox": {
                "Width": 0.22200915217399598,
                "Height": 0.20713073015213014,
                "Left": 0.44219455122947695,
                "Top": 0.1630047857761383
            }
        }

        img_dimen = (test_data["ImageDimensions"]["Width"],
            test_data["ImageDimensions"]["Height"])
        left,top,right,bottom = utilities.convert_pct_bounding_box(img_dimen
            , test_data["BoundingBox"])
        assert left == 1337
        assert top == 657
        assert right == 2009
        assert bottom == 1492

    def test_convert_pct_bounding_box_full_width(self):
        """Test bounding box that encompasses full width of image"""
        test_data = {
            "ImageDimensions": { "Width": 3024, "Height": 4032 },
            "BoundingBox": {
                "Width": 1,
                "Height": 0.20713073015213014,
                "Left": 0,
                "Top": 0.1630047857761383
            }
        }

        img_dimen = (test_data["ImageDimensions"]["Width"],
            test_data["ImageDimensions"]["Height"])
        left,top,right,bottom = utilities.convert_pct_bounding_box(img_dimen
            , test_data["BoundingBox"])
        assert left == 0
        assert top == 657
        assert right == img_dimen[0]
        assert bottom == 1492

    def test_get_cropped_image_simple(self):
        """basic functional test of get_cropped_image"""
        crop_bounding = {
            "Left": 0.35,
            "Top": 0.25,
            "Width": 0.25,
            "Height": 0.45
        }
        with open(os.path.join(MODULE_PATH, "test_image.JPG"), mode='rb') as test_image:
            test_crop_result = utilities.get_cropped_image(test_image, crop_bounding)

        test_img_crop_path = os.path.join(MODULE_PATH, "test_image_crop.JPG")
        with Image.open(test_img_crop_path) as expected_crop_img, Image.open(test_crop_result) as result_crop_img:
            expected_hash = imagehash.average_hash(expected_crop_img)
            result_hash = imagehash.average_hash(result_crop_img)

        assert expected_hash - result_hash <= 1

    def test_get_scaled_image(self):
        """tests the basic functionality of the get_scaled_image utility function"""
        max_size = (250,200)
        with open(os.path.join(MODULE_PATH, "test_image.JPG"), mode='rb') as test_image:
            test_scale_result = utilities.get_scaled_image(test_image, max_size)

        with Image.open(test_scale_result) as scaled_test_img:
            assert scaled_test_img.size[0] and scaled_test_img.size[0] <= max_size[0]
            assert scaled_test_img.size[1] and scaled_test_img.size[1] <= max_size[1]
