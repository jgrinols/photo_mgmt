"""container module for AutoTagger class"""
from __future__ import annotations

import json, asyncio
from typing import List, Dict, Optional
from contextlib import asynccontextmanager, AsyncExitStack

from py_linq import Enumerable

from . import strings
from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig
from ..db_connection_pool import DbConnectionPool
from .pwgo_image import PiwigoImage
from .rekognition import RekognitionClient
from . import utilities

class AutoTagger():
    """Manages the autotagging process for a given PiwigoImage. Provides static methods that
    are involved with initializing/resyncing the autotagging functionality"""
    EXT_REFS_SEP = ":"

    @staticmethod
    def get_logger():
        """gets a logger..."""
        return ProgramConfig.get().get_logger(__name__)

    def __init__(self, img, stack):
        self.logger = AutoTagger.get_logger()
        self.image: PiwigoImage = img
        self._rek_client = None
        self._exit_stack = stack

    @staticmethod
    @asynccontextmanager
    async def create(img_ref) -> AutoTagger:
        """Creates an autotagger instance with the given PiwigoImage instance or Piwigo image id"""
        if isinstance(img_ref, PiwigoImage):
            image = img_ref
        else:
            image_id = int(img_ref)
            image = await PiwigoImage.create(image_id)

        async with AsyncExitStack() as stack:
            # there's nothing to cleanup in the autotagger itself
            # but a rek client may get added to the exit stack if needed
            # which will get cleaned up when the exit stack goes out of scope
            yield AutoTagger(image, stack)

    async def autotag_image(self) -> None:
        """initiates the autotagging process for the current image"""
        self.logger.debug("beginning autotagging of %s", self.image.file)
        cfg = ProgramConfig.get()
        async with DbConnectionPool.get().acquire_dict_cursor(db=cfg.pwgo_db_name) as (cur,_):
            sql = """
                SELECT 1
                FROM piwigo.image_category
                WHERE image_id = %s AND category_id = %s
            """
            await cur.execute(sql, (self.image.id, AgentConfig.get().auto_tag_proc_alb))
            existing_proc = await cur.fetchone()

        if existing_proc:
            self.logger.warning("image_id %s already exists in the processed auto-tag album. skipping autotagging."
                , self.image.id)
            await self._move_image_to_processed(True)
        else:
            face_images = await self._get_face_image_files()
            tag_coros = []
            for img, index in face_images:
                tag_coros.append(self._get_tags_for_face_image(img, index))

            tag_coros.append(self._get_label_tags())
            results = await asyncio.gather(*tag_coros)
            tags = set().union(*results)
            for tag in tags:
                if not isinstance(tag, int):
                    raise TypeError()
            await asyncio.gather(self.add_tags(tags), self._move_image_to_processed())

    async def add_implicit_tags(self) -> None:
        """adds any tags that should be added based on implicit tag configuration."""
        cfg = ProgramConfig.get()
        self.logger.debug("checking if any implicit tags should be added for %s", self.image.file)
        async with DbConnectionPool.get().acquire_dict_cursor(db=cfg.pwgo_db_name) as (cnt_cur,_):
            sql = """
                SELECT COUNT(*) AS cnt
                FROM image_tag it
                JOIN expanded_implicit_tags imp
                ON imp.triggered_by_tag_id = it.tag_id
                LEFT JOIN image_tag it2
                ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
                WHERE it.image_id = %s AND it2.image_id IS NULL
            """

            await cnt_cur.execute(sql, (self.image.id))
            cnt = (await cnt_cur.fetchone())["cnt"]

        if cnt:
            self.logger.info("adding %s implicit tags for %s", cnt, self.image.file)
            if not ProgramConfig.get().dry_run:
                async with DbConnectionPool.get().acquire_dict_cursor(db=cfg.pwgo_db_name) as (ins_cur,conn):
                    sql = """
                        INSERT INTO image_tag (image_id, tag_id)
                        SELECT DISTINCT it.image_id, imp.implied_tag_id
                        FROM image_tag it
                        JOIN expanded_implicit_tags imp
                        ON imp.triggered_by_tag_id = it.tag_id
                        LEFT JOIN image_tag it2
                        ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
                        WHERE it.image_id = %s AND it2.image_id IS NULL
                    """
                    await ins_cur.execute(sql, (self.image.id))
                    await conn.commit()

    async def add_tags(self, tags: List[int]) -> None:
        """accepts a list of tag ids and applies them to the image.
        Skips tags that already exist on the image."""
        if tags:
            self.logger.info("Adding %s tags to %s", tags, self.image.file)
            if not ProgramConfig.get().dry_run:
                async with DbConnectionPool.get().acquire_dict_cursor(db=ProgramConfig.get().pwgo_db_name) as (cur,conn):
                    sql = """
                        INSERT INTO image_tag (image_id, tag_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE tag_id = tag_id
                    """
                    for tag in tags:
                        await cur.execute(sql, (self.image.id, tag))

                    await conn.commit()

    async def _get_rek_client(self) -> RekognitionClient:
        if not self._rek_client:
            self._rek_client = await self._exit_stack.enter_async_context(RekognitionClient())
        return self._rek_client

    async def add_indexed_image(self, index_cat_id):
        """adds the current image to the Rekognition face index"""
        self.logger.info(strings.LOG_ADD_IMG_FACES(self.image.file))

        if not ProgramConfig.get().dry_run:
            with self.image.open_file() as img_file:
                client = await self._get_rek_client()
                faces = await client.index_faces_from_image(
                    img_file,
                    external_image_id = f"{index_cat_id}{AutoTagger.EXT_REFS_SEP}{self.image.id}"
                )

            sql = """
                INSERT INTO indexed_faces ( face_id, image_id, piwigo_image_id, piwigo_category_id, face_confidence, face_details )
                VALUES ( '%s', '%s', %s, %s, %s, '%s')
            """
            if faces:
                async with DbConnectionPool.get().acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,conn):
                    for frec in faces:
                        face = frec["Face"]
                        refs = face["ExternalImageId"].split(AutoTagger.EXT_REFS_SEP)

                        await cur.execute(sql % (
                            face["FaceId"],
                            face["ImageId"],
                            refs[1],
                            refs[0],
                            face["Confidence"],
                            json.dumps(frec["FaceDetail"])
                        ))

                    await conn.commit()

    async def _get_face_image_files(self):
        """gets the location of faces detected in the image from Rekognition
        and generates cropped images for those locations"""
        self.logger.debug(strings.LOG_DETECT_IMG_FACES(self.image.file))
        results = []
        db_cn_ctx = DbConnectionPool.get()
        if not ProgramConfig.get().dry_run:
            async with db_cn_ctx.acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,conn):
                sql = """
                    SELECT face_details
                    FROM rekognition.processed_faces
                    WHERE piwigo_image_id = %s
                """

                await cur.execute(sql, self.image.id)
                face_details = [json.loads(d["face_details"]) for d in await cur.fetchall()]
                existing = len(face_details) > 0

                if not existing:
                    with self.image.open_file() as img_file:
                        client = await self._get_rek_client()
                        face_details = await client.detect_faces(img_file)

                if face_details:
                    with self.image.open_file() as img_file:
                        if not existing:
                            for index, detail in enumerate(face_details):
                                detail["index"] = index

                                sql = """
                                    INSERT INTO processed_faces ( piwigo_image_id, face_index, face_details )
                                    VALUES ( %s, %s, '%s' )
                                """

                                await cur.execute(sql % (
                                    self.image.id,
                                    detail["index"],
                                    json.dumps(detail)
                                ))

                            await conn.commit()

                        img_file.seek(0)
                        results = [
                            (utilities.get_cropped_image(img_file, f["BoundingBox"]), f["index"]) for f in face_details
                        ]

        return results

    async def _get_tags_for_face_image(self, img, index: int) -> List[int]:
        matched_face = await self._get_matched_face(img, index)
        tags = []
        if matched_face:
            tags = await AutoTagger._get_tags_for_match(matched_face)
        return tags

    async def _get_matched_face(self, img, index: int) -> Optional[Dict]:
        """gets the top ranked matched face (or None) for the given image"""
        self.logger.debug("attempting to match face in image")
        result = None
        if not ProgramConfig.get().dry_run:
            client = await self._get_rek_client()
            result = await client.match_face_from_image(img)

        if result and "Face" in result:
            async with DbConnectionPool.get().acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,conn):
                sql = """
                    UPDATE processed_faces SET matched_to_face_id = '%s'
                    WHERE piwigo_image_id = %s AND face_index = %s
                """

                await cur.execute(sql % (
                        result["Face"]["FaceId"],
                        self.image.id,
                        index
                    ))
                await conn.commit()

            return result["Face"]
        return None

    async def _get_label_tags(self) -> List[int]:
        labels = await self._fetch_image_labels()
        return await AutoTagger._get_tag_ids_for_labels(labels)

    async def _fetch_image_labels(self):
        labels = []
        existing = False
        if not ProgramConfig.get().dry_run:
            db_cn_ctx = DbConnectionPool.get()
            async with db_cn_ctx.acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,conn):
                sql = """
                    SELECT label
                    FROM rekognition.image_labels
                    WHERE piwigo_image_id = %s and confidence >= %s
                """

                await cur.execute(sql, (self.image.id, AgentConfig.get().min_tag_confidence))
                existing_labels = [l["label"] for l in await cur.fetchall()]
                existing = len(existing_labels) > 0

                if not existing:
                    with self.image.open_file() as img_file:
                        client = await self._get_rek_client()
                        labels = await client.detect_labels(img_file)

                    if labels:
                        sql = """
                            INSERT INTO image_labels (piwigo_image_id, label, confidence, parents)
                            VALUES (%s, '%s', %s, '%s')
                        """

                        for label in labels:
                            await cur.execute(sql % (
                                    self.image.id,
                                    label["Name"],
                                    label["Confidence"],
                                    json.dumps(label["Parents"])
                                ))
                        await conn.commit()

        if existing:
            return existing_labels
        else:
            return [l["Name"] for l in labels]

    async def _move_image_to_processed(self, already_processed: bool = False) -> None:
        """removes the image from the autotag virtual album and adds it to the
        processed virtual album"""
        self.logger.info(strings.LOG_MOVE_IMG(self.image.file))
        if not ProgramConfig.get().dry_run:
            async with DbConnectionPool.get().acquire_dict_cursor(db=ProgramConfig.get().pwgo_db_name) as (cur,conn):
                if not already_processed:
                    ins_sql = """
                        INSERT INTO image_category (image_id, category_id)
                        VALUES (%s, %s)
                    """
                    await cur.execute(ins_sql, (self.image.id, AgentConfig.get().auto_tag_proc_alb))
                    await conn.commit()

                del_sql = """
                    DELETE FROM image_category
                    WHERE image_id = %s AND category_id = %s
                """
                await cur.execute(del_sql, (self.image.id, AgentConfig.get().auto_tag_alb))

                await conn.commit()

    @classmethod
    async def remove_indexed_faces(cls, face_ids):
        """removes a face from the Rekognition face index"""
        cls.get_logger().info("Removing %s faces from face index", len(face_ids))

        if not ProgramConfig.get().dry_run:
            async with AsyncExitStack() as stack:
                db_cn_ctx = DbConnectionPool.get()
                cur,conn = await stack.enter_async_context(db_cn_ctx.acquire_dict_cursor(db=AgentConfig.get().rek_db_name))
                rek_client = await stack.enter_async_context(RekognitionClient())

                faces = await rek_client.remove_indexed_faces(face_ids)

                sql = """
                    DELETE FROM rekognition.indexed_faces
                    WHERE face_id IN (%s)
                """
                fmt_strings = ','.join(['%s'] * len(face_ids))
                await cur.execute(sql % fmt_strings, tuple(face_ids))

                await conn.commit()

        return faces

    @classmethod
    async def sync_face_index(cls):
        """Adds and/or removes images from the Rekognition face index as neccessary to sync it
        up with the Piwigo face index album"""
        cls.get_logger().info("beginning face index sync")
        cls.get_logger().debug("getting list of currently indexed images")
        existing = {}
        if not ProgramConfig.get().dry_run:
            async with RekognitionClient() as rek_client:
                for face in await rek_client.get_indexed_faces():
                    pwgo_image_id = face["ExternalImageId"].split(AutoTagger.EXT_REFS_SEP)[1]
                    if pwgo_image_id in existing:
                        existing[pwgo_image_id].append(face["FaceId"])
                    else:
                        existing[pwgo_image_id] = [face["FaceId"]]

        cls.get_logger().info("pulling the expected face index collection from db")
        async with DbConnectionPool.get().acquire_dict_cursor(db=ProgramConfig.get().pwgo_db_name) as (cur,_):
            sql = """
                SELECT i.id image_id
                    , c.id category_id
                    , i.file
                    , i.`path`
                FROM piwigo.images i
                JOIN piwigo.image_category ic
                ON ic.image_id = i.id
                JOIN piwigo.categories c
                ON c.id = ic.category_id
                WHERE c.id IN (%s)
            """
            format_strings = ','.join(['%s'] * len(AgentConfig.get().face_idx_albs))

            await cur.execute(sql % format_strings, tuple(AgentConfig.get().face_idx_albs))
            wanted = []
            for row in await cur.fetchall():
                wanted.append({
                    "img": PiwigoImage(id=row["image_id"],file=row["file"],path=row["path"]),
                    "cat_id": row["category_id"]
                })

        remove_keys = Enumerable(existing.keys()) \
                .except_(Enumerable(wanted).select(func=lambda i: str(i["img"].id))) \
                .to_list()
        # the irony of an impossible to comprehend list comprehension
        # I apologize for potentially inflicting this on my future self, but it's a gd work of art
        remove_face_ids = [id for sub in [v for k, v in existing.items() if k in remove_keys] for id in sub]

        if remove_face_ids:
            await AutoTagger.remove_indexed_faces(remove_face_ids)

        add = Enumerable(wanted) \
            .where(lambda i: str(i["img"].id) not in existing) \
            .to_list()
        for rec in add:
            async with AutoTagger.create(rec["img"]) as tagger:
                await tagger.add_indexed_image(rec["cat_id"])

        cls.get_logger().info("finished face index sync")

    @classmethod
    async def process_new_tag(cls, tag_id: int) -> None:
        """Checks if the new tag associated with the given id should be added to any
        previously autotagged images"""
        cls.get_logger().debug("checking if new tag needs to be applied to any existing autotagged images")

        async with AsyncExitStack() as stack:
            db_cn_ctx = DbConnectionPool.get()
            cur,_ = await stack.enter_async_context(db_cn_ctx.acquire_dict_cursor(db=AgentConfig.get().rek_db_name))
            sql = """
                SELECT il.piwigo_image_id
                FROM image_labels il
                JOIN piwigo.tags t
                ON t.name = il.label
                WHERE t.id = %s AND il.confidence >= %s
            """
            await cur.execute(sql, (tag_id, AgentConfig.get().min_tag_confidence))
            for img in await cur.fetchall():
                tagger = await stack.enter_async_context(AutoTagger.create(img["piwigo_image_id"]))
                await tagger.add_tags([tag_id])

    @classmethod
    async def _get_tags_for_match(cls, face: Dict) -> List[int]:
        """attempts to resolve tag(s) for the image based on the matched face:
        1) get description for album using matched ExternalImageId
        2) find json object(s) in description
        3) look for a "tags" array in json object
        """
        refs = face["ExternalImageId"].split(AutoTagger.EXT_REFS_SEP)
        tags = set()

        async with DbConnectionPool.get().acquire_dict_cursor(db=ProgramConfig.get().pwgo_db_name) as (cur,_):
            sql = "SELECT comment FROM categories WHERE id = %s"
            await cur.execute(sql, (refs[0]))
            result = await cur.fetchone()
            if not result:
                raise RuntimeError(f"could not find album with id {refs[0]}")

            if result["comment"]:
                for obj in utilities.extract_json_objects(result["comment"]):
                    if "tags" in obj and isinstance(obj["tags"], list):
                        for tag in obj["tags"]:
                            if isinstance(tag,int):
                                tags.add(tag)

        return list(tags)

    @classmethod
    async def _get_tag_ids_for_labels(cls, labels: List[str]) -> List[int]:
        """Gets ids from the piwigo tags table for records that match the
        given labels by name"""

        cls.get_logger().debug("Getting tag ids for detected labels")
        tag_ids = []
        if labels:
            async with DbConnectionPool.get().acquire_dict_cursor(db=ProgramConfig.get().pwgo_db_name) as (cur,_):
                sql = """
                    SELECT id
                    FROM tags
                    WHERE name IN (%s)
                """
                fmt_strings = ",".join(['%s'] * len(labels))
                await cur.execute(sql % fmt_strings, tuple(labels))
                tag_ids = [row["id"] for row in await cur.fetchall()]

        return tag_ids
