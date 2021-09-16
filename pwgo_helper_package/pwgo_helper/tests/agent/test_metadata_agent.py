"""container module for TestMetadataAgent"""
# pylint: disable=protected-access
import json, asyncio, os, subprocess, re, signal, logging, tempfile
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from contextlib import ExitStack

import pytest
from path import Path
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent

from ...agent.metadata_agent import MetadataAgent
from ...config import Configuration as ProgramConfig
from ...agent.config import Configuration as AgentConfig
from ...agent import strings as AgentStrings
from ... import strings as ProgramStrings
from ...agent.autotagger import AutoTagger
from ...agent.image_virtual_path_event_task import ImageVirtualPathEventTask
from ...agent.image_metadata_event_task import ImageMetadataEventTask
from .conftest import TestDbResult

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

class TestMetadataAgent:
    """Tests for the MetadataAgent"""

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch("pwgo_helper.agent.metadata_agent.AutoTagger")
    async def test_unforced_stop(self, mck_atag, m_get_acfg, test_db: TestDbResult):
        """tests proper handling unforced stopping of agent. queue should be cleared before the agent stops.
        this is behavior for sigquit"""
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(2)

        mck_atag.sync_face_index = mck_sync_face_idx
        acfg = AgentConfig()
        acfg.workers = 2
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db
        }
        ProgramConfig.initialize(**pcfg_params)
        m_get_acfg.return_value = acfg
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            m_get_acfg.return_value.virtualfs_root = str(vfs_root_path)
            m_get_acfg.return_value.piwigo_galleries_host_path = tmp_dir
            with patch("builtins.open", mock_open(read_data=json.dumps(test_db.db_host))):
                agent = MetadataAgent(logging.getLogger(__name__))
            with patch.object(agent, "process_autotag_backlog"):
                await agent.start()

            proc_evt_tgt = "pwgo_helper.agent.event_dispatcher.EventDispatcher.process_event"
            with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
                async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,conn):
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

        # should see 2 events processed for each image_category value--one for image_cateogry message
        # and another for the subsequent image_virtual_path message
        assert mck_proc_evt.await_count == len(ids * 2)

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch("pwgo_helper.agent.metadata_agent.AutoTagger")
    async def test_forced_stop(self, m_atag, m_get_acfg, test_db: TestDbResult):
        """tests proper handling forced stopping of agent. queue should NOT be cleared before the agent stops.
        this is behavior for sigterm and sigint"""
        proc_time = 2
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(proc_time)

        m_atag.sync_face_index = mck_sync_face_idx
        acfg = AgentConfig()
        acfg.workers = 2
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db
        }
        ProgramConfig.initialize(**pcfg_params)
        m_get_acfg.return_value = acfg
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            m_get_acfg.return_value.virtualfs_root = str(vfs_root_path)
            m_get_acfg.return_value.piwigo_galleries_host_path = tmp_dir
            with patch("builtins.open", mock_open(read_data=json.dumps(test_db.db_host))):
                agent = MetadataAgent(logging.getLogger(__name__))
            with patch.object(agent, "process_autotag_backlog"):
                await agent.start()

            proc_evt_tgt = "pwgo_helper.agent.event_dispatcher.EventDispatcher.process_event"
            with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
                async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,conn):
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

        assert mck_proc_evt.await_count == AgentConfig.get().workers

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch.object(AutoTagger, "sync_face_index")
    @patch.object(AutoTagger, "create")
    async def test_autotag_backlog(self, m_atag_create, _, m_get_acfg, test_db: TestDbResult):
        """test proper functioning of the processing of backlog items when agent starts"""
        mck_atagger = MagicMock(spec=AutoTagger)
        m_atag_create.return_value.__aenter__.return_value = mck_atagger
        acfg = AgentConfig()
        acfg.workers = 2
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db
        }
        ProgramConfig.initialize(**pcfg_params)
        m_get_acfg.return_value = acfg
        with patch("builtins.open", mock_open(read_data=json.dumps(test_db.db_host))):
            agent = MetadataAgent(logging.getLogger(__name__))

        async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            ids = [155,156,157]
            for img_id in ids:
                await cur.execute(sql % (img_id, 125))
            await conn.commit()

            # patching the vfs rebuild so we don't have to bother setting up temp paths for it
            with patch.object(ImageVirtualPathEventTask, "rebuild_virtualfs"):
                await agent.start()
                await asyncio.sleep(1)
                await agent.stop()

        assert mck_atagger.autotag_image.await_count == len(ids)

    @pytest.mark.asyncio
    async def test_out_of_process(self, capfd, test_db: TestDbResult):
        """runs the metadata agent in a new process, inserts records into database,
        and verifies proper handling"""
        face_idx_images = [
            "IMG_0135.JPG","IMG_0242.JPG","IMG_0283.JPG","IMG_0326.JPG","IMG_0343.JPG","IMG_0600.JPG",
            "IMG_0042.JPG","IMG_0575.JPG","IMG_0632.JPG","IMG_0299.JPG","IMG_0133.JPG"
        ]
        atag_bload_images = [185,199]
        async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            for img_id in atag_bload_images:
                await cur.execute(sql, (img_id, 125))
            await conn.commit()
        add_face_idx_re = re.compile('^.*('+AgentStrings.LOG_ADD_IMG_FACES('.*')+')')
        vfs_remove_re = re.compile('^.*('+AgentStrings.LOG_VFS_REBUILD_REMOVE('.*')+')')
        vfs_create_re = re.compile('^.*('+AgentStrings.LOG_VFS_REBUILD_CREATE('.*')+')')
        detect_faces_re = re.compile('^.*('+AgentStrings.LOG_DETECT_IMG_FACES('.*')+')')
        move_img_re = re.compile('^.*('+AgentStrings.LOG_MOVE_IMG('.*')+')')
        queue_evt_re = re.compile('^.*('+AgentStrings.LOG_QUEUE_EVT+')')
        handle_sig_re = re.compile('^.*('+AgentStrings.LOG_HANDLE_SIG('.*')+')')

        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            test_str = "test"
            agent_proc = subprocess.Popen([
                "pwgo-helper",
                "--log-level",
                "DEBUG",
                "--lib-log-level",
                "ERROR",
                "--dry-run",
                "--db-conn-json",
                json.dumps(test_db.db_host),
                "--pwgo-db-name",
                test_db.piwigo_db,
                "--msg-db-name",
                test_db.messaging_db,
                "--rek-db-name",
                test_db.rekognition_db,
                "agent",
                "--piwigo-galleries-host-path",
                "/workspace",
                "--image-crop-save-path",
                "/workspace",
                "--virtualfs-root",
                str(vfs_root_path),
                "--rek-access-key",
                test_str,
                "--rek-secret-access-key",
                test_str,
                "--rek-collection-arn",
                test_str,
                "--rek-collection-id",
                test_str,
                "--workers",
                "10"
            ])
            await asyncio.sleep(3)
            try:
                try:
                    # verify program is still running
                    assert not agent_proc.returncode
                    captured = capfd.readouterr().err.splitlines()

                    # verification of initial face sync
                    add_face_idx_logs = [ add_face_idx_re.match(s) for s in captured ]
                    add_face_idx_logs = [ m.group(1) for m in add_face_idx_logs if m ]
                    assert len(face_idx_images) == len(add_face_idx_logs)
                    for img in face_idx_images:
                        assert AgentStrings.LOG_ADD_IMG_FACES(img) in add_face_idx_logs

                    # verification of virtualfs rebuild
                    vfs_remove_logs = [ vfs_remove_re.match(s) for s in captured ]
                    vfs_remove_logs = [ m.group(1) for m in vfs_remove_logs if m ]
                    assert len(vfs_remove_logs) == 1
                    vfs_create_logs = [ vfs_create_re.match(s) for s in captured ]
                    vfs_create_logs = [ m.group(1) for m in vfs_create_logs if m ]
                    assert len(vfs_create_logs) == 1

                    # verification of autotag backlog handling
                    detect_faces_logs = [ detect_faces_re.match(s) for s in captured ]
                    detect_faces_logs = [ m.group(1) for m in detect_faces_logs if m ]
                    move_img_logs = [ move_img_re.match(s) for s in captured ]
                    move_img_logs = [ m.group(1) for m in move_img_logs if m ]
                    assert len(detect_faces_logs) == len(atag_bload_images)
                    assert len(move_img_logs) == len(atag_bload_images)
                    async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,_):
                        sql_ids_placeholders = ','.join(['%s' for x in atag_bload_images])
                        sql = f"""
                            SELECT id, file
                            FROM images
                            WHERE id IN ({sql_ids_placeholders})
                        """
                        await cur.execute(sql, tuple(atag_bload_images))
                        atag_fnames = await cur.fetchall()

                    for img in atag_fnames:
                        assert AgentStrings.LOG_DETECT_IMG_FACES(img["file"]) in detect_faces_logs
                        assert AgentStrings.LOG_MOVE_IMG(img["file"]) in move_img_logs

                    # test handling of new face index image
                    img_id = 207
                    async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,conn):
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
                except Exception:
                    print("external process output:\n")
                    print(*captured, sep="\n")
                    raise

                try:
                    await asyncio.sleep(3)
                    # verify program is still running
                    assert not agent_proc.returncode
                    captured = capfd.readouterr().err.splitlines()
                    evt_queue_logs = [ queue_evt_re.match(s) for s in captured ]
                    evt_queue_logs = [ m.group(1) for m in evt_queue_logs if m ]
                    # one event for the image_category insert and another for the triggered
                    # image_virtual_path insert
                    assert len(evt_queue_logs) == 2
                    add_faces_logs = [ add_face_idx_re.match(s) for s in captured ]
                    add_faces_logs = [ m.group(1) for m in add_faces_logs if m ]
                    assert AgentStrings.LOG_ADD_IMG_FACES(img["file"]) in add_faces_logs

                except Exception:
                    print("external process output:\n")
                    print(*captured, sep="\n")
                    raise

                try:
                    agent_proc.send_signal(signal.SIGQUIT)
                    await asyncio.sleep(3)
                    captured = capfd.readouterr().err.splitlines()
                    handle_sig_logs = [ handle_sig_re.match(s) for s in captured ]
                    handle_sig_logs = [ m.group(1) for m in handle_sig_logs if m ]
                    assert len(handle_sig_logs) == 1
                    # pylint: disable=no-member
                    assert AgentStrings.LOG_HANDLE_SIG(signal.SIGQUIT.name) in handle_sig_logs

                except Exception:
                    print("external process output:\n")
                    print(*captured, sep="\n")
                    raise

            finally:
                agent_proc.terminate()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", [{ "run_db_mods": False }], indirect=True)
    async def test_db_init(self, capfd, test_db: TestDbResult):
        """tests running db initialization at startup. this is just a bare
        bones test confirming that the init did indeed run"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            async with test_db.db_connection_pool.acquire_dict_cursor(db=test_db.piwigo_db) as (cur,_):
                vfs_root_path = Path(tmp_dir).joinpath("vfs")
                vfs_root_path.mkdir()

                # verify table created by init doesn't exist
                sql = """
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.TABLES
                    WHERE table_schema = 'piwigo' AND table_name = 'category_paths' 
                """
                await cur.execute(sql)
                res = await cur.fetchone()
                assert not res["cnt"]

                test_str = "test"
                agent_proc = subprocess.Popen([
                    "pwgo-helper",
                    "--log-level",
                    "DEBUG",
                    "--dry-run",
                    "--db-conn-json",
                    json.dumps(test_db.db_host),
                    "--pwgo-db-name",
                    test_db.piwigo_db,
                    "--msg-db-name",
                    test_db.messaging_db,
                    "--rek-db-name",
                    test_db.rekognition_db,
                    "agent",
                    "--piwigo-galleries-host-path",
                    "/workspace",
                    "--image-crop-save-path",
                    "/workspace",
                    "--virtualfs-root",
                    str(vfs_root_path),
                    "--rek-access-key",
                    test_str,
                    "--rek-secret-access-key",
                    test_str,
                    "--rek-collection-arn",
                    test_str,
                    "--rek-collection-id",
                    test_str,
                    "--workers",
                    "10",
                    "--initialize-db"
                ])
                await asyncio.sleep(3)
                try:
                    # verify program is still running
                    assert not agent_proc.returncode
                    captured = capfd.readouterr().err.splitlines()
                    db_init_re = re.compile('^.*('+AgentStrings.LOG_INITIALIZE_DB+')')
                    db_init_logs = [ db_init_re.match(s) for s in captured ]
                    db_init_logs = [ m.group(1) for m in db_init_logs if m ]
                    assert len(db_init_logs) == 1

                    # verify table created by init now exists
                    sql = f"""
                        SELECT COUNT(*) AS cnt
                        FROM information_schema.TABLES
                        WHERE table_schema = '{test_db.piwigo_db}' AND table_name = 'category_paths' 
                    """
                    await cur.execute(sql)
                    res = await cur.fetchone()
                    assert res["cnt"]

                except Exception:
                    print("external process output:\n")
                    print(*captured, sep="\n")
                    raise

                finally:
                    agent_proc.terminate()

    @pytest.mark.asyncio
    async def test_env_opts(self, capfd, test_db: TestDbResult):
        """tests that options are properly settable from environment"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()

            test_str = "test"
            os.environ["PWGO_HLPR_LOG_LEVEL"] = "DEBUG"
            os.environ["PWGO_HLPR_AGENT_WORKERS"] = "17"
            os.environ["PWGO_HLPR_AGENT_INITIALIZE_DB"] = "True"
            os.environ["PWGO_HLPR_DB_CONN_JSON"] = json.dumps(test_db.db_host)
            os.environ["PWGO_HLPR_PWGO_DB_NAME"] = test_db.piwigo_db
            os.environ["PWGO_HLPR_MSG_DB_NAME"] = test_db.messaging_db
            os.environ["PWGO_HLPR_REK_DB_NAME"] = test_db.rekognition_db
            os.environ["PWGO_HLPR_AGENT_REK_ACCESS_KEY"] = test_str
            os.environ["PWGO_HLPR_AGENT_REK_SECRET_ACCESS_KEY"] = test_str
            os.environ["PWGO_HLPR_AGENT_REK_COLLECTION_ARN"] = test_str
            os.environ["PWGO_HLPR_AGENT_REK_COLLECTION_ID"] = test_str
            agent_proc = subprocess.Popen([
                    "pwgo-helper",
                    "--dry-run",
                    "agent",
                    "--piwigo-galleries-host-path",
                    "/workspace",
                    "--image-crop-save-path",
                    "/workspace",
                    "--virtualfs-root",
                    str(vfs_root_path)
                ])
            await asyncio.sleep(3)

            try:
                captured = capfd.readouterr().err.splitlines()
                assert not agent_proc.returncode
                re_str = '^.*('+ProgramStrings.LOG_PRG_OPT('log_level', os.environ["PWGO_HLPR_LOG_LEVEL"])+')'
                prg_verb_opt_re = re.compile(re_str)
                prg_verb_opt_logs = [ prg_verb_opt_re.match(s) for s in captured ]
                prg_verb_opt_logs = [ m.group(1) for m in prg_verb_opt_logs if m ]
                assert len(prg_verb_opt_logs) == 1

                re_str = '^.*('+AgentStrings.LOG_AGNT_OPT('workers', os.environ["PWGO_HLPR_AGENT_WORKERS"])+')'
                agnt_wkrs_opt_re = re.compile(re_str)
                agnt_wkrs_opt_logs = [ agnt_wkrs_opt_re.match(s) for s in captured ]
                agnt_wkrs_opt_logs = [ m.group(1) for m in agnt_wkrs_opt_logs if m ]
                assert len(agnt_wkrs_opt_logs) == 1

                agnt_initdb_opt_re = re.compile('^.*('+AgentStrings.LOG_AGNT_OPT('initialize_db', True)+')')
                agnt_initdb_opt_logs = [ agnt_initdb_opt_re.match(s) for s in captured ]
                agnt_initdb_opt_logs = [ m.group(1) for m in agnt_initdb_opt_logs if m ]
                assert len(agnt_initdb_opt_logs) == 1
            except Exception:
                print("external process output:\n")
                print(*captured, sep="\n")
                raise

    @pytest.mark.asyncio
    async def test_multi_event_multi_row(self, mocker):
        """this tests a particular scenario that was causing problems with properly
        reusing an existing image metadata task"""
        mck_write_evt1 = MagicMock(spec=WriteRowsEvent)
        setattr(mck_write_evt1, "table", "pwgo_message")
        evt1_row1_msg = { "image_id": 999, "table_name": "image_tag", "table_primary_key": [999, 50], "operation": "DELETE"}
        evt1_row2_msg = evt1_row1_msg.copy()
        evt1_row2_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 51]
        evt1_row3_msg = evt1_row1_msg.copy()
        evt1_row3_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 52]
        evt1_row4_msg = evt1_row1_msg.copy()
        evt1_row4_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 53]
        mck_write_evt1.rows = [
            { "values": { "id": 100, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt1_row1_msg) }},
            { "values": { "id": 101, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt1_row2_msg) }},
            { "values": { "id": 102, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt1_row3_msg) }},
            { "values": { "id": 109, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt1_row4_msg) }}
        ]
        mck_write_evt2 = MagicMock(spec=WriteRowsEvent)
        setattr(mck_write_evt2, "table", "pwgo_message")
        evt2_row1_msg = { "image_id": 999, "table_name": "image_tag", "table_primary_key": [999, 50], "operation": "INSERT"}
        evt2_row2_msg = evt1_row1_msg.copy()
        evt2_row2_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 51]
        evt2_row3_msg = evt1_row1_msg.copy()
        evt2_row3_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 52]
        mck_write_evt2.rows = [
            { "values": { "id": 103, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt2_row1_msg) }},
            { "values": { "id": 104, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt2_row2_msg) }},
            { "values": { "id": 105, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt2_row3_msg) }}
        ]
        mck_write_evt3 = MagicMock(spec=WriteRowsEvent)
        setattr(mck_write_evt3, "table", "pwgo_message")
        evt3_row1_msg = { "image_id": 999, "table_name": "image_tag", "table_primary_key": [999, 50], "operation": "INSERT"}
        evt3_row2_msg = evt1_row1_msg.copy()
        evt3_row2_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 51]
        evt3_row3_msg = evt1_row1_msg.copy()
        evt3_row3_msg["table_primary_key"] = [evt1_row1_msg["image_id"], 52]
        mck_write_evt3.rows = [
            { "values": { "id": 106, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt3_row1_msg) }},
            { "values": { "id": 107, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt3_row2_msg) }},
            { "values": { "id": 108, "message_timestamp": datetime.fromisoformat("2021-01-01 10:05:36")
                , "message_type": "IMG_METADATA", "message": json.dumps(evt3_row3_msg) }}
        ]

        agent = MetadataAgent(logging.getLogger(__name__))

        async def mck_start_evt_mon():
            mon_task = asyncio.create_task(agent._event_monitor())
            mon_task.set_name("agent-event-monitor")
            mon_task.request_cancel = False
            await asyncio.sleep(0)
            return mon_task

        async def handle_evts(_):
            await asyncio.sleep(.1)

        evts = [mck_write_evt1, mck_write_evt2, mck_write_evt3]
        def bstrm_iter(_):
            nonlocal evts
            try:
                return iter([evts.pop(0)])
            except IndexError:
                return iter(())

        with ExitStack() as stack:
            mck_bstrm = stack.enter_context(patch.object(agent, "_binlog_stream", spec=BinLogStreamReader))
            mck_bstrm.__iter__ = bstrm_iter
            _ = stack.enter_context(patch.object(agent, "_start_event_monitor", new=mck_start_evt_mon))
            _ = stack.enter_context(
                patch.object(ImageMetadataEventTask, "_handle_events", new=handle_evts))
            mck_handle_evts = mocker.spy(ImageMetadataEventTask, "_handle_events")
            mck_at = stack.enter_context(patch("pwgo_helper.agent.metadata_agent.AutoTagger"))
            mck_vfs_task = stack.enter_context(patch("pwgo_helper.agent.metadata_agent.ImageVirtualPathEventTask"))
            mck_pcfg_get = stack.enter_context(patch.object(ProgramConfig, "get"))
            mck_pcfg_get.return_value = ProgramConfig()
            mck_pcfg_get.return_value.log_level = "DEBUG"
            mck_pcfg_get.return_value.dry_run = True
            mck_acfg_get = stack.enter_context(patch.object(AgentConfig, "get"))
            mck_acfg_get.return_value = AgentConfig()
            mck_acfg_get.return_value.workers = 10
            mck_acfg_get.return_value.worker_error_limit = 0
            # wait secs may need to be set higher when debugging test with breakpoints to work properly
            mck_acfg_get.return_value.img_tag_wait_secs = 1
            mck_acfg_get.return_value.stop_timeout = 999
            _ = stack.enter_context(patch.object(agent, "process_autotag_backlog"))
            mck_at.sync_face_index = AsyncMock()
            mck_vfs_task.rebuild_virtualfs = AsyncMock()

            await agent.start()
            while evts:
                await asyncio.sleep(1)
            await agent.stop()

            mck_handle_evts.assert_called_once()
