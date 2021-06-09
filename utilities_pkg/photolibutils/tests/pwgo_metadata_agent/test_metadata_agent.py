"""container module for TestMetadataAgent"""
# pylint: disable=protected-access
import json, asyncio, os, subprocess, re, signal
from unittest.mock import MagicMock, patch, mock_open

import pytest

from ...pwgo_metadata_agent.metadata_agent import MetadataAgent
from ...pwgo_metadata_agent.constants import Constants
from ...pwgo_metadata_agent import strings
from ...pwgo_metadata_agent.autotagger import AutoTagger

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
MYSQL_CONF_PATH = os.path.join(MODULE_PATH, "mysql.json")
REK_CONF_PATH = os.path.join(MODULE_PATH, "rekognition.json")

class TestMetadataAgent:
    """Tests for the MetadataAgent"""

    @pytest.mark.asyncio
    @patch("photolibutils.pwgo_metadata_agent.metadata_agent.AutoTagger")
    async def test_unforced_stop(self, mck_atag, test_db, test_db_cfg):
        """tests proper handling unforced stopping of agent. queue should be cleared before the agent stops.
        this is behavior for sigquit"""
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(2)

        mck_atag.sync_face_index = mck_sync_face_idx
        Constants.WORKERS_CNT = 2
        with patch("builtins.open", mock_open(read_data=json.dumps(test_db_cfg))):
            agent = MetadataAgent()
        with patch.object(agent, "process_autotag_backlog"):
            await agent.start()

        proc_evt_tgt = "photolibutils.pwgo_metadata_agent.event_dispatcher.EventDispatcher.process_event"
        with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
            async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,conn):
                sql = """
                    INSERT INTO image_category (image_id, category_id)
                    VALUES (%s, %s)
                """
                ids = [60,61,62,63,64]
                for img_id in ids:
                    await cur.execute(sql % (img_id, 125))
                await conn.commit()

            await asyncio.sleep(1)
            await agent.stop(False)

        assert mck_proc_evt.await_count == len(ids)

    @pytest.mark.asyncio
    @patch("photolibutils.pwgo_metadata_agent.metadata_agent.AutoTagger")
    async def test_forced_stop(self, mck_atag, test_db, test_db_cfg):
        """tests proper handling forced stopping of agent. queue should NOT be cleared before the agent stops.
        this is behavior for sigterm and sigint"""
        proc_time = 2
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(proc_time)

        mck_atag.sync_face_index = mck_sync_face_idx
        Constants.WORKERS_CNT = 2
        with patch("builtins.open", mock_open(read_data=json.dumps(test_db_cfg))):
            agent = MetadataAgent()
        with patch.object(agent, "process_autotag_backlog"):
            await agent.start()

        proc_evt_tgt = "photolibutils.pwgo_metadata_agent.event_dispatcher.EventDispatcher.process_event"
        with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
            async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,conn):
                sql = """
                    INSERT INTO image_category (image_id, category_id)
                    VALUES (%s, %s)
                """
                ids = [90,91,92,93,94]
                cat_id = 125
                for img_id in ids:
                    await cur.execute(sql % (img_id, cat_id))
                await conn.commit()

                await asyncio.sleep(proc_time * .5)
                await agent.stop(True)

        assert mck_proc_evt.await_count == Constants.WORKERS_CNT

    @pytest.mark.asyncio
    @patch.object(AutoTagger, "sync_face_index")
    @patch.object(AutoTagger, "create")
    async def test_autotag_backlog(self, mck_atag_create, _, test_db, test_db_cfg):
        """test proper functioning of the processing of backlog items when agent starts"""
        mck_atagger = MagicMock(spec=AutoTagger)
        mck_atag_create.return_value.__aenter__.return_value = mck_atagger
        Constants.WORKERS_CNT = 2
        with patch("builtins.open", mock_open(read_data=json.dumps(test_db_cfg))):
            agent = MetadataAgent()

        async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            ids = [155,156,157]
            for img_id in ids:
                await cur.execute(sql % (img_id, 125))
            await conn.commit()

            await agent.start()
            await asyncio.sleep(1)
            await agent.stop()

        assert mck_atagger.autotag_image.await_count == len(ids)

    @pytest.mark.asyncio
    async def test_out_of_process(self, capfd, test_db):
        """runs the metadata agent in a new process, inserts records into database,
        and verifies proper handling"""
        face_idx_images = [
            "IMG_0135.JPG","IMG_0242.JPG","IMG_0283.JPG","IMG_0326.JPG","IMG_0343.JPG","IMG_0600.JPG",
            "IMG_0042.JPG","IMG_0575.JPG","IMG_0632.JPG","IMG_0299.JPG","IMG_0133.JPG"
        ]
        atag_bload_images = [185,199]
        async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            for img_id in atag_bload_images:
                await cur.execute(sql, (img_id, 125))
            await conn.commit()
        add_face_idx_re = re.compile('^.*('+strings.LOG_ADD_IMG_FACES('.*')+')')
        detect_faces_re = re.compile('^.*('+strings.LOG_DETECT_IMG_FACES('.*')+')')
        move_img_re = re.compile('^.*('+strings.LOG_MOVE_IMG('.*')+')')
        queue_evt_re = re.compile('^.*('+strings.LOG_QUEUE_EVT+')')
        handle_sig_re = re.compile('^.*('+strings.LOG_HANDLE_SIG('.*')+')')
        agent_proc = subprocess.Popen([
            "pwgo-metadata-agent",
            "--piwigo-galleries-host-path",
            "/workspace",
            "--image-crop-save-path",
            "/workspace",
            "-db",
            MYSQL_CONF_PATH,
            "--rekognition-config",
            REK_CONF_PATH,
            "-v",
            "DEBUG",
            "--workers",
            "10",
            "--dry-run"
        ])
        await asyncio.sleep(3)
        try:
            # verify program is still running
            assert not agent_proc.returncode
            captured = capfd.readouterr().err.splitlines()

            # verification of initial face sync
            add_face_idx_logs = [ add_face_idx_re.match(s) for s in captured ]
            add_face_idx_logs = [ m.group(1) for m in add_face_idx_logs if m ]
            assert len(face_idx_images) == len(add_face_idx_logs)
            for img in face_idx_images:
                assert strings.LOG_ADD_IMG_FACES(img) in add_face_idx_logs

            # verification of autotag backlog handling
            detect_faces_logs = [ detect_faces_re.match(s) for s in captured ]
            detect_faces_logs = [ m.group(1) for m in detect_faces_logs if m ]
            move_img_logs = [ move_img_re.match(s) for s in captured ]
            move_img_logs = [ m.group(1) for m in move_img_logs if m ]
            assert len(detect_faces_logs) == len(atag_bload_images)
            assert len(move_img_logs) == len(atag_bload_images)
            async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,_):
                sql_ids_placeholders = ','.join(['%s' for x in atag_bload_images])
                sql = f"""
                    SELECT id, file
                    FROM images
                    WHERE id IN ({sql_ids_placeholders})
                """
                await cur.execute(sql, tuple(atag_bload_images))
                atag_fnames = await cur.fetchall()

            for img in atag_fnames:
                assert strings.LOG_DETECT_IMG_FACES(img["file"]) in detect_faces_logs
                assert strings.LOG_MOVE_IMG(img["file"]) in move_img_logs

            # test handling of new face index image
            img_id = 207
            async with test_db.acquire_dict_cursor(db=Constants.PWGO_DB) as (cur,conn):
                sql = """
                    INSERT INTO image_category (image_id, category_id)
                    VALUES (%s, %s)
                """
                await cur.execute(sql, (img_id, 129))
                await conn.commit()

                sql = """
                    SELECT file
                    FROM images
                    WHERE id = %s
                """
                await cur.execute(sql, (img_id))
                img = await cur.fetchone()

            await asyncio.sleep(3)
            captured = capfd.readouterr().err.splitlines()
            evt_queue_logs = [ queue_evt_re.match(s) for s in captured ]
            evt_queue_logs = [ m.group(1) for m in evt_queue_logs if m ]
            assert len(evt_queue_logs) == 1
            add_faces_logs = [ add_face_idx_re.match(s) for s in captured ]
            add_faces_logs = [ m.group(1) for m in add_faces_logs if m ]
            assert strings.LOG_ADD_IMG_FACES(img["file"]) in add_faces_logs

            agent_proc.send_signal(signal.SIGQUIT)
            await asyncio.sleep(3)
            captured = capfd.readouterr().err.splitlines()
            handle_sig_logs = [ handle_sig_re.match(s) for s in captured ]
            handle_sig_logs = [ m.group(1) for m in handle_sig_logs if m ]
            assert len(handle_sig_logs) == 1
            # pylint: disable=no-member
            assert strings.LOG_HANDLE_SIG(signal.SIGQUIT.name) in handle_sig_logs

        finally:
            agent_proc.terminate()
