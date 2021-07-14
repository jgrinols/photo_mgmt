"""container module for TestAutotagger"""
from io import IOBase
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from py_linq import Enumerable

from ...agent import utilities
from ...agent.autotagger import AutoTagger
from ...agent.pwgo_image import PiwigoImage
from ...agent.config import Configuration as AgentConfig
from ...config import Configuration as ProgramConfig
from ...agent.rekognition import RekognitionClient

class TestAutotagger:
    """tests for the AutoTagger class"""
    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_create(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests the create method when passing a pwgo image id"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        async with AutoTagger.create(22) as tagger:
            assert isinstance(tagger.image, PiwigoImage)
            assert tagger.image.file == "IMG_0958.JPG"

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_get_tag_ids_for_label(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests basic functioning of the _get_tag_ids_for_labels method"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        tag_ids = await AutoTagger._get_tag_ids_for_labels(labels=["snow","car","baby"])
        assert tag_ids == [31,32,34]

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_get_tags_for_match(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests basic functioning of the _get_tags_for_match method"""
        mck_face_match = { "ExternalImageId": "133:1" }
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        tag_ids = await AutoTagger._get_tags_for_match(face=mck_face_match)
        assert tag_ids == [22]

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_add_tags(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests basic functioning of the add_tags method
        including testing that already existing tags are not duplicated"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,_):
            async with AutoTagger.create(367) as tagger:
                await tagger.add_tags([16,20,28])
                sql = """
                    SELECT tag_id
                    FROM image_tag
                    WHERE image_id = 367
                """
                await cur.execute(sql)
                res_tags = Enumerable([r["tag_id"] for r in await cur.fetchall()])

        # these calls act as asserts since they will raise an error if the values
        # appear not exactly once each
        res_tags.single(lambda t: t == 16)
        res_tags.single(lambda t: t == 20)
        res_tags.single(lambda t: t == 28)

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_add_implicit_tags(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """test basic functioning of the add_implicit_tags method.
        adds 'christmas' tag to an image then calls the add_implit_tags method.
        verify that 'holidays' tag was added"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        img_id = 22
        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,conn):
            sql = """
                INSERT INTO image_tag (image_id, tag_id)
                VALUES (%s, 28)
            """
            await cur.execute(sql, (img_id))
            await conn.commit()
        async with AutoTagger.create(img_id) as tagger:
            await tagger.add_implicit_tags()

        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,_):
            sql = """
                SELECT 1
                FROM image_tag
                WHERE image_id = %s AND tag_id = %s
            """
            await cur.execute(sql, (img_id, 24))
            assert cur.rowcount == 1

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(AutoTagger, "_get_rek_client")
    @patch.object(ProgramConfig, "get")
    async def test_add_indexed_image(self, m_get_pcfg, m_rek, m_get_acfg, test_db, db_cfg):
        """test basic functioning of the add_indexed_image method.
        mocks the rekognition client and checks that expected db entry
        is created"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        img_id = 242
        img_cat_id = 129
        img_face_bounding = {"Width": 0.15, "Height": 0.20, "Left": 0.25, "Top": 0.17}
        mck_idx_faces = [{
            "Face": {
                "FaceId": "1b758c12-2a91-40d6-9a3b-ca52aee171d2"
                , "ImageId": "02e670c6-dbaa-3d1a-83e6-2945aefbffe7"
                , "Confidence": 99.99846
                , "ExternalImageId": f"{img_cat_id}:{img_id}"
            }
            , "FaceDetail": { "BoundingBox": img_face_bounding }
        }]
        m_rek.return_value = AsyncMock(spec=RekognitionClient)
        m_rek.return_value.index_faces_from_image.return_value = mck_idx_faces
        img = await PiwigoImage.create(img_id)
        with patch.object(PiwigoImage, "open_file") as _:
            async with AutoTagger.create(img) as tagger:
                await tagger.add_indexed_image(img_cat_id)

        async with test_db.acquire_dict_cursor(db="rekognition") as (cur,_):
            sql = """
                SELECT face_id, image_id, piwigo_image_id, piwigo_category_id, face_confidence, face_details
                FROM indexed_faces
                WHERE face_id = %s
            """
            await cur.execute(sql, (mck_idx_faces[0]["Face"]["FaceId"]))
            result = await cur.fetchall()
            assert len(result) == 1
            assert result[0]["image_id"] == mck_idx_faces[0]["Face"]["ImageId"]
            assert result[0]["piwigo_image_id"] == img_id
            assert result[0]["piwigo_category_id"] == 129
            assert result[0]["face_confidence"] == 99.99846
            detail = json.loads(result[0]["face_details"])
            assert mck_idx_faces[0]["FaceDetail"] == detail

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(utilities, "get_cropped_image")
    @patch.object(AutoTagger, "_get_rek_client")
    @patch.object(ProgramConfig, "get")
    async def test_get_face_image_files(self, m_get_pcfg, m_rek, m_crp_img, m_get_acfg, test_db, db_cfg):
        """test basic functioning of the _get_face_image_files method.
        mocks the rekognition client and checks that expected db entry
        is created"""
        img_id = 543
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        mck_faces = [{ "BoundingBox": { "Width": 0.18, "Height": 0.27, "Left": 0.73, "Top": 0.37 } }
            , { "BoundingBox": {"Width": 0.16, "Height": 0.24, "Left": 0.33, "Top": 0.06 } }]
        m_rek.return_value = AsyncMock(spec=RekognitionClient)
        m_rek.return_value.detect_faces.return_value = mck_faces
        img = await PiwigoImage.create(img_id)
        with patch.object(PiwigoImage, "open_file") as mck_open:
            mck_open.return_value = MagicMock(spec=IOBase)
            async with AutoTagger.create(img) as tagger:
                await tagger._get_face_image_files()

        mck_open.call_count == 2
        assert m_crp_img.call_count == 2

        async with test_db.acquire_dict_cursor(db="rekognition") as (cur,_):
            sql = """
                SELECT face_index, face_details
                FROM processed_faces
                WHERE piwigo_image_id = %s
            """
            await cur.execute(sql, (img_id))
            result = await cur.fetchall()
            assert len(result) == 2
            for face in result:
                db_detail = json.loads(face["face_details"])
                mck_detail = mck_faces[face["face_index"]]
                assert db_detail == mck_detail

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(AutoTagger, "_get_rek_client")
    @patch.object(ProgramConfig, "get")
    async def test_get_matched_face(self, m_get_pcfg, m_rek, m_get_acfg, test_db, db_cfg):
        """test basic functionality of _get_matched_face method.
        inserts a stub processed_image record and checks that the stub is
        updated with the correct face id"""
        mck_matched_face = { "Face": { "FaceId": "e2f5c37f-3ef5-4a06-bfe2-7251384f7873" } }
        m_rek.return_value = AsyncMock(spec=RekognitionClient)
        m_rek.return_value.match_face_from_image.return_value = mck_matched_face
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        img_id = 826
        img_face_idx = 0
        matched_img_id = 582
        async with test_db.acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,conn):
            # insert indexed face to satisy fk in processed_faces
            sql = """
                INSERT INTO indexed_faces ( face_id, image_id, piwigo_image_id, piwigo_category_id, face_confidence, face_details)
                VALUES ( '%s', '%s', %s, %s, %s, '%s' )
            """
            await cur.execute(sql % (mck_matched_face["Face"]["FaceId"], '30d1e2fe-bb3a-3fd8-abd5-a1917c706ae8',
                matched_img_id, 129, 99.99, '{}'))
            # insert processed face record
            sql = """
                INSERT INTO processed_faces ( piwigo_image_id, face_index, face_details )
                VALUES ( %s, %s, '%s' )
            """
            await cur.execute(sql % (img_id, img_face_idx, '{}'))
            await conn.commit()

            img = await PiwigoImage.create(img_id)
            with patch.object(PiwigoImage, "open_file") as mck_open:
                mck_open.return_value = MagicMock(spec=IOBase)
                async with AutoTagger.create(img) as tagger:
                    matched_face = await tagger._get_matched_face(img, img_face_idx)

            assert matched_face == mck_matched_face["Face"]
            sql = """
                SELECT matched_to_face_id
                FROM processed_faces
                WHERE piwigo_image_id = %s AND face_index = %s
            """
            await cur.execute(sql % (img_id, img_face_idx))
            result = await cur.fetchall()
            assert len(result) == 1
            assert result[0]["matched_to_face_id"] == mck_matched_face["Face"]["FaceId"]

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(AutoTagger, "_get_rek_client")
    @patch.object(ProgramConfig, "get")
    async def test_fetch_image_labels(self, m_get_pcfg, m_rek, m_get_acfg, test_db, db_cfg):
        """tests basic functionality of the _fetch_image_labels method.
        verify that expected record is inserted into image_labels table"""
        img_id = 543
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        mck_matched_labels = [
            { "Name": "Beverage", "Confidence": 99, "Parents": [] }
            ,{ "Name": "Alcohol", "Confidence": 81, "Parents": [{ "Name": "Beverage" }] }
            ,{ "Name": "Beer", "Confidence": 75, "Parents": [{ "Name": "Beverage" }, { "Name": "Alcohol" }] }
        ]
        m_rek.return_value = AsyncMock(spec=RekognitionClient)
        m_rek.return_value.detect_labels.return_value = mck_matched_labels

        img = await PiwigoImage.create(img_id)
        with patch.object(PiwigoImage, "open_file") as mck_open:
            mck_open.return_value = MagicMock(spec=IOBase)
            async with AutoTagger.create(img) as tagger:
                labels = await tagger._fetch_image_labels()

        assert labels == ["Beverage","Alcohol","Beer"]
        async with test_db.acquire_dict_cursor(db=AgentConfig.get().rek_db_name) as (cur,_):
            sql = """
                SELECT label
                FROM image_labels
                WHERE piwigo_image_id = %s
            """
            await cur.execute(sql % (img_id))
            result = await cur.fetchall()

        assert len(result) == 3
        db_labels = [l["label"] for l in result]
        assert labels.sort() == db_labels.sort()

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_move_image_to_processed(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """test the basic functioning of the _move_image_to_processed method"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        img_id = 22
        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            await cur.execute(sql, (img_id, AgentConfig.get().auto_tag_alb))
            await conn.commit()

            img = await PiwigoImage.create(img_id)
            async with AutoTagger.create(img) as tagger:
                await tagger._move_image_to_processed()

            sql = """
                SELECT image_id
                FROM image_category
                WHERE image_id = %s AND category_id = %s
            """
            await cur.execute(sql % (img_id, AgentConfig.get().auto_tag_alb))
            result = await cur.fetchall()
            assert len(result) == 0

            await cur.execute(sql % (img_id, AgentConfig.get().auto_tag_proc_alb))
            result = await cur.fetchall()
            assert len(result) == 1

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch('pwgo_helper.agent.autotagger.RekognitionClient')
    @patch.object(ProgramConfig, "get")
    async def test_remove_indexed_faces(self, m_get_pcfg, m_rek, m_get_acfg, test_db, db_cfg):
        """test the basic functioning of the remove_indexed_faces method.
        inserts 3 test faces in indexed_faces table then passes two of the ids
        to the remove method. verifies that the 2 have been removed and the
        the third is still there."""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        m_rek.return_value = AsyncMock(spec=RekognitionClient)
        mck_inx_faces = [
            ('80f5f9fe-34fc-4643-ad9d-78cc5f73c7e5','279901c8-258e-4816-82ea-f038560a7f98',9564,129,99,'{}')
            ,('af8c34c1-8f43-491b-9a1d-f5e4fd0168d5','a01e4ed1-bf1b-452e-a88e-daa0fcfe48b1',5864,129,99,'{}')
            ,('cee89487-b727-4f3e-926d-ebcf717e3b37','0ea8a5aa-34cf-4030-b6fb-f1480cf71cbf',4568,129,99,'{}')
        ]
        async with test_db.acquire_dict_cursor(db="rekognition") as (cur,conn):
            sql = """
                INSERT INTO indexed_faces (face_id, image_id, piwigo_image_id, piwigo_category_id, face_confidence, face_details)
                VALUES ('%s','%s',%s,%s,%s,'%s')
            """
            for face in mck_inx_faces:
                await cur.execute(sql % face)
            await conn.commit()

            await AutoTagger.remove_indexed_faces([mck_inx_faces[0][0],mck_inx_faces[1][0]])

            sql = """
                SELECT face_id
                FROM indexed_faces
                WHERE face_id = '%s'
            """
            await cur.execute(sql % (mck_inx_faces[0][0]))
            result = await cur.fetchall()
            assert len(result) == 0

            await cur.execute(sql % (mck_inx_faces[1][0]))
            result = await cur.fetchall()
            assert len(result) == 0

            await cur.execute(sql % (mck_inx_faces[2][0]))
            result = await cur.fetchall()
            assert len(result) == 1

    @pytest.mark.asyncio
    @patch.object(AutoTagger, "add_indexed_image")
    @patch.object(AutoTagger, "remove_indexed_faces")
    @patch.object(AutoTagger, "create")
    @patch('pwgo_helper.agent.autotagger.RekognitionClient')
    @patch.object(ProgramConfig, "get")
    @patch.object(AgentConfig, "get")
    async def test_sync_face_index(self, m_get_acfg, m_get_pcfg, rek, at_create, at_rem_idx, _, test_db, db_cfg):
        """tests the basic functioning of the sync_face_index static method."""
        acfg = AgentConfig()
        acfg.face_idx_albs = [129,130,131]
        pcfg = ProgramConfig()
        pcfg.db_config = db_cfg
        pcfg.dry_run = False
        m_get_acfg.return_value = acfg
        m_get_pcfg.return_value = pcfg
        mck_rek_client = MagicMock(spec=RekognitionClient)
        mck_curr_faces = [
            (129,584,'560a7c6b-4d29-40e1-b2cc-0224c0b25bf9'),
            (130,9688,'9d0fdcb4-4b9d-4138-b4d7-f6a124a6a385')
        ]
        mck_rek_client.get_indexed_faces = AsyncMock(
            return_value=[{"ExternalImageId": f"{f[0]}:{f[1]}", "FaceId": f[2]} for f in mck_curr_faces]
        )
        rek.return_value.__aenter__.return_value = mck_rek_client
        mck_tagger = AsyncMock(spec=AutoTagger)
        at_create.return_value.__aenter__.return_value = mck_tagger

        await AutoTagger.sync_face_index()

        at_rem_idx.assert_called_once_with([mck_curr_faces[1][2]])
        assert mck_tagger.add_indexed_image.await_count == 9

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    async def test_process_new_tag(self, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests the basic functionality of the process_new_tag method"""
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        a_cfg.rek_db_name = "rekognition"
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg
        new_tag = (999,'test_auto_label')
        img_id = 367
        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,conn):
            sql = """
                INSERT INTO rekognition.image_labels (piwigo_image_id,label,confidence,parents)
                VALUES (%s,'%s',%s,'%s')
            """
            await cur.execute(sql % (img_id,'test_auto_label',99,'[]'))
            sql = """
                INSERT INTO piwigo.tags (id,name,url_name,lastmodified)
                VALUES (%s,'%s','%s','%s')
            """
            await cur.execute(sql % (new_tag[0], new_tag[1], new_tag[1], '2020-01-01 00:00:00'))
            await conn.commit()

            await AutoTagger.process_new_tag(new_tag[0])

            sql = """
                SELECT 1
                FROM piwigo.image_tag
                WHERE image_id = %s AND tag_id = %s
            """
            await cur.execute(sql % (img_id, new_tag[0]))
            assert len(await cur.fetchall()) == 1

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(ProgramConfig, "get")
    @patch.object(AutoTagger, "_move_image_to_processed")
    @patch.object(AutoTagger, "add_tags")
    @patch.object(AutoTagger, "_get_face_image_files")
    @patch.object(AutoTagger, "_get_label_tags")
    @patch.object(AutoTagger, "_get_tags_for_face_image")
    async def test_autotag_image(self, m_i_tags, m_l_tags, m_files, m_add, m_mv, m_get_pcfg, m_get_acfg, test_db, db_cfg):
        """tests the basic functioning of the main autotag_image method"""
        mck_img_file = MagicMock(spec=IOBase)
        m_files.return_value = [(mck_img_file, 0)]
        m_i_tags.return_value = [45]
        m_l_tags.return_value = [20,21]
        a_cfg = AgentConfig()
        p_cfg = ProgramConfig()
        p_cfg.db_config = db_cfg
        m_get_acfg.return_value = a_cfg
        m_get_pcfg.return_value = p_cfg

        img = await PiwigoImage.create(110)
        with patch.object(PiwigoImage, "open_file") as _:
            async with AutoTagger.create(img) as tagger:
                await tagger.autotag_image()

        m_i_tags.assert_awaited_once_with(mck_img_file, 0)
        m_l_tags.assert_awaited_once()
        m_add.assert_awaited_once_with({45,20,21})
        m_mv.assert_awaited_once()
