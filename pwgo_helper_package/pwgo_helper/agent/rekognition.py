"""Container module for Rekognize class"""
from __future__ import annotations

import json
from typing import Dict, List, IO

from py_linq import Enumerable
import aiobotocore

from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig

class RekognitionClient():
    """A wrapper class with static methods for exposing the Rekognition client api"""
    @staticmethod
    def get_logger():
        """gets a logger..."""
        return ProgramConfig.get().create_logger(__name__)

    def __init__(self):
        self.logger = RekognitionClient.get_logger()
        with open(AgentConfig.get().rek_db_config) as rek_cfg:
            self._config = json.load(rek_cfg)
        self._rek_client = None

    async def __aenter__(self):
        session = aiobotocore.session.AioSession()
        client_ctx = session.create_client(
            'rekognition',
            aws_access_key_id=self._config["aws_access_key_id"],
            aws_secret_access_key=self._config["aws_secret_access_key"],
            region_name=self._config["region_name"]
        )
        self._rek_client = await client_ctx.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._rek_client.__aexit__(exc_type, exc_val, exc_tb)

    async def detect_labels(self, img_file: IO) -> List[Dict]:
        """Finds contextual labels in the image such as objects and setting"""
        resp = await self._rek_client.detect_labels(
            Image={"Bytes": img_file.read()},
            MinConfidence=80
        )

        return resp["Labels"]

    async def detect_faces(self, img_file: IO) -> List[Dict]:
        """Finds the faces in the given image and returns location and details"""
        resp = await self._rek_client.detect_faces(Image={"Bytes": img_file.read()})
        return resp["FaceDetails"]

    async def create_face_collection(self, collection_id: str):
        """Creates a new face collection (index) with the given name"""
        resp = await self._rek_client.create_collection(CollectionId=collection_id)
        self.logger.info("Created collection %s with arn %s", collection_id, resp['CollectionArn'])

    async def index_faces_from_image(self, img_file: IO, **kwargs) -> List[Dict]:
        """Adds faces from the given image into the default face collection/index"""
        if "external_image_id" not in kwargs:
            raise RuntimeError("must supply an external_image_id arg")

        resp = await self._rek_client.index_faces(
            CollectionId = self._config["collection_id"],
            Image = {"Bytes": img_file.read()},
            ExternalImageId = kwargs["external_image_id"],
            DetectionAttributes = ["ALL"]
        )

        return resp["FaceRecords"]

    async def get_indexed_faces(self, **kwargs):
        """Gets all the faces that are currently in the default face collection/index"""
        request_args = { "CollectionId": self._config["collection_id"] }
        if "next_token" in kwargs:
            request_args["NextToken"] = kwargs["next_token"]

        resp = await self._rek_client.list_faces(**request_args)
        result = Enumerable(resp["Faces"])
        if "NextToken" in resp:
            result = result.union(Enumerable(await self._rek_client.get_indexed_faces(next_token=resp["NextToken"])))

        return result.to_list()

    async def remove_indexed_faces(self, face_ids):
        """Removes the given face ids from the default face collection/index"""
        if not isinstance(face_ids, list):
            raise TypeError("face_ids must be a list")

        if not face_ids:
            return

        resp = await self._rek_client.delete_faces(
            CollectionId = self._config["collection_id"],
            FaceIds = face_ids
        )

        return resp["DeletedFaces"]

    async def remove_all_indexed_faces(self):
        """Removes all faces from the default face collection/index"""
        idx_face_ids = Enumerable(await self._rek_client.get_indexed_faces()) \
            .select(func=lambda f: f["FaceId"]) \
            .to_list()
        if idx_face_ids:
            return self.remove_indexed_faces(idx_face_ids)

    async def match_face_from_image(self, img_file: IO) -> Dict:
        """Attempt to match the provided image to the existing rekognition face index.
        Returns the single highest ranked match or an empty face match list if the image
        was not matched. There are cases when the detectfaces call returns a low quality
        face that is then not picked up by the search_faces_by_image call. In this case
        the latter responds with the InvalidParameterException--this is caught and "None"
        is returned"""
        try:
            resp = await self._rek_client.search_faces_by_image(
                CollectionId = self._config["collection_id"],
                Image = {"Bytes": img_file.read()},
                MaxFaces = 1
            )

        except self._rek_client.exceptions.InvalidParameterException:
            self.logger.info("No faces detected")
            return None

        if resp["FaceMatches"]:
            return resp["FaceMatches"][0]

    async def describe_collection(self):
        """Gets metadata for the default face collection/index"""
        return await self._rek_client.describe_collection(CollectionId = self._config["collection_id"])
