"""container module for PiwigoImage"""
from __future__ import annotations

import json, datetime, logging
from io import IOBase
from contextlib import contextmanager
from typing import Dict

from . import utilities
from .constants import Constants
from .db_connection_pool import DbConnectionPool

logger = logging.getLogger(Constants.LOGGER_NAME)

class PiwigoImage:
    """Class which encapsulates the core attributes of an image in the Piwigo db."""
    def __init__(self, **kwargs):
        self.id = int(kwargs["id"])
        self.file = kwargs["file"]
        self._path = kwargs["path"]
        if "metadata" in kwargs:
            self.metadata = kwargs["metadata"]
        else:
            self.metadata = None

    @staticmethod
    async def create(img_id: int, load_metadata: bool = False) -> PiwigoImage:
        """Creates an instance from the given image id by looking up details in database"""
        logger.debug("looking up image details from db")
        async with DbConnectionPool.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,_):
            sql = """
                SELECT file, path
                FROM images
                WHERE id = %s
            """
            await cur.execute(sql, (img_id))
            result = await cur.fetchone()
            if not result:
                raise RuntimeError(f"could not resolve image details for id {img_id}")

            return_args = {
                "id": img_id,
                "file": result["file"],
                "path": result["path"]
            }

        if load_metadata:
            async with DbConnectionPool.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,_):
                sql = """
                    SELECT image_metadata
                    FROM image_metadata
                    WHERE id = %s
                """
                await cur.execute(sql, (img_id))
                result = await cur.fetchone()
                if not result:
                    raise RuntimeError(f"could not resolve image metadata for id {img_id}")

                metadata = json.loads(result["image_metadata"])
                return_args["metadata"] = PiwigoImageMetadata(metadata)

        return PiwigoImage(**return_args)

    @contextmanager
    def open_file(self, mode: str='r') -> IOBase:
        """opens the PiwigoImage file. Usage: with piwigo_img.open_file(mode='w+') as img_file:"""
        pgfs = utilities.get_pwgo_fs()
        from_path = utilities.map_pwgo_path(self._path)
        img_file = pgfs.openbin(from_path, mode=mode)
        try:
            yield img_file

        finally:
            img_file.close()
            pgfs.close()

class PiwigoImageMetadata:
    """DTO to encapsulate the metadata fields that we're interested in"""
    def __init__(self, raw: Dict):
        required_fields = ["name", "comment", "author", "date_creation", "tags"]
        for field in required_fields:
            if not field in raw:
                raise AttributeError(f"Required attribute {field} missing")

        self.name = raw[required_fields[0]]
        self.comment = raw[required_fields[1]]
        self.author = raw[required_fields[2]]
        self.create_date = datetime.datetime.strptime(
            raw[required_fields[3]],
            '%Y-%m-%d %H:%M:%S'
        )
        self._tags = list(dict.fromkeys(raw[required_fields[4]]))

    @property
    def tags(self) -> list[str]:
        """returns a list of image tags"""
        return self._tags

    @tags.setter
    def tags(self, value):
        """sets the tags list from the deduplictad value provided."""
        self._tags = list(dict.fromkeys(value))

    def get_iptc_dict(self):
        """returns the metadata as a dictonary with values mapped to iptc keys"""
        iptc_dict = {}
        if self.name:
            iptc_dict["Iptc.Application2.ObjectName"] = self.name[ 0 : 64 ]
        if self.comment:
            iptc_dict["Iptc.Application2.Caption"] = self.comment[ 0 : 2000 ]
        if self.author:
            iptc_dict["Iptc.Application2.Byline"] = self.author[ 0 : 32 ]
        if self.tags:
            iptc_dict["Iptc.Application2.Keywords"] = list(self.tags)

        return iptc_dict
