"""container module for TestMetadataAgent"""
# pylint: disable=protected-access
import json, asyncio, os, subprocess, re, signal, logging, tempfile
from unittest.mock import MagicMock, patch, mock_open

import pytest
from path import Path

from ...agent.metadata_agent import MetadataAgent
from ...agent.config import Configuration as AgentConfig
from ...agent import strings
from ...agent.autotagger import AutoTagger
from ...agent.image_virtual_path_event_task import ImageVirtualPathEventTask

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

class TestMetadataAgent:
    """Tests for the MetadataAgent"""

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    @patch("pwgo_helper.agent.metadata_agent.AutoTagger")
    async def test_unforced_stop(self, mck_atag, mck_get_cfg, test_db, db_cfg):
        """tests proper handling unforced stopping of agent. queue should be cleared before the agent stops.
        this is behavior for sigquit"""
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(2)

        mck_atag.sync_face_index = mck_sync_face_idx
        cfg = AgentConfig()
        cfg.workers = 2
        cfg.pwgo_db_config = db_cfg
        mck_get_cfg.return_value = cfg
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            mck_get_cfg.return_value.virtualfs_root = str(vfs_root_path)
            mck_get_cfg.return_value.piwigo_galleries_host_path = tmp_dir
            with patch("builtins.open", mock_open(read_data=json.dumps(db_cfg))):
                agent = MetadataAgent(logging.getLogger(__name__))
            with patch.object(agent, "process_autotag_backlog"):
                await agent.start()

            proc_evt_tgt = "pwgo_helper.agent.event_dispatcher.EventDispatcher.process_event"
            with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
                async with test_db.acquire_dict_cursor(db=AgentConfig.get().pwgo_db_config["name"]) as (cur,conn):
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
    async def test_forced_stop(self, mck_atag, mck_get_cfg, test_db, db_cfg):
        """tests proper handling forced stopping of agent. queue should NOT be cleared before the agent stops.
        this is behavior for sigterm and sigint"""
        proc_time = 2
        async def mck_sync_face_idx():
            await asyncio.sleep(2)
        async def proc_evt(_):
            await asyncio.sleep(proc_time)

        mck_atag.sync_face_index = mck_sync_face_idx
        cfg = AgentConfig()
        cfg.workers = 2
        cfg.pwgo_db_config = db_cfg
        mck_get_cfg.return_value = cfg
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            mck_get_cfg.return_value.virtualfs_root = str(vfs_root_path)
            mck_get_cfg.return_value.piwigo_galleries_host_path = tmp_dir
            with patch("builtins.open", mock_open(read_data=json.dumps(db_cfg))):
                agent = MetadataAgent(logging.getLogger(__name__))
            with patch.object(agent, "process_autotag_backlog"):
                await agent.start()

            proc_evt_tgt = "pwgo_helper.agent.event_dispatcher.EventDispatcher.process_event"
            with patch(proc_evt_tgt, wraps=proc_evt) as mck_proc_evt:
                async with test_db.acquire_dict_cursor(db=AgentConfig.get().pwgo_db_config["name"]) as (cur,conn):
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
    async def test_autotag_backlog(self, mck_atag_create, _, mck_get_cfg, test_db, db_cfg):
        """test proper functioning of the processing of backlog items when agent starts"""
        mck_atagger = MagicMock(spec=AutoTagger)
        mck_atag_create.return_value.__aenter__.return_value = mck_atagger
        cfg = AgentConfig()
        cfg.workers = 2
        cfg.pwgo_db_config = db_cfg
        mck_get_cfg.return_value = cfg
        with patch("builtins.open", mock_open(read_data=json.dumps(db_cfg))):
            agent = MetadataAgent(logging.getLogger(__name__))

        async with test_db.acquire_dict_cursor(db=AgentConfig.get().pwgo_db_config["name"]) as (cur,conn):
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
    async def test_out_of_process(self, capfd, test_db, db_cfg):
        """runs the metadata agent in a new process, inserts records into database,
        and verifies proper handling"""
        face_idx_images = [
            "IMG_0135.JPG","IMG_0242.JPG","IMG_0283.JPG","IMG_0326.JPG","IMG_0343.JPG","IMG_0600.JPG",
            "IMG_0042.JPG","IMG_0575.JPG","IMG_0632.JPG","IMG_0299.JPG","IMG_0133.JPG"
        ]
        atag_bload_images = [185,199]
        async with test_db.acquire_dict_cursor(db="piwigo") as (cur,conn):
            sql = """
                INSERT INTO image_category (image_id, category_id)
                VALUES (%s, %s)
            """
            for img_id in atag_bload_images:
                await cur.execute(sql, (img_id, 125))
            await conn.commit()
        add_face_idx_re = re.compile('^.*('+strings.LOG_ADD_IMG_FACES('.*')+')')
        vfs_remove_re = re.compile('^.*('+strings.LOG_VFS_REBUILD_REMOVE('.*')+')')
        vfs_create_re = re.compile('^.*('+strings.LOG_VFS_REBUILD_CREATE('.*')+')')
        detect_faces_re = re.compile('^.*('+strings.LOG_DETECT_IMG_FACES('.*')+')')
        move_img_re = re.compile('^.*('+strings.LOG_MOVE_IMG('.*')+')')
        queue_evt_re = re.compile('^.*('+strings.LOG_QUEUE_EVT+')')
        handle_sig_re = re.compile('^.*('+strings.LOG_HANDLE_SIG('.*')+')')

        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()
            agent_proc = subprocess.Popen([
                "pwgo-helper",
                "-v",
                "DEBUG",
                "--dry-run",
                "agent",
                "--piwigo-galleries-host-path",
                "/workspace",
                "--image-crop-save-path",
                "/workspace",
                "--virtualfs-root",
                str(vfs_root_path),
                "--pwgo-db-host",
                db_cfg["host"],
                "--pwgo-db-port",
                str(db_cfg["port"]),
                "--pwgo-db-user",
                db_cfg["user"],
                "--pwgo-db-pw",
                db_cfg["passwd"],
                "--workers",
                "10"
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
                async with test_db.acquire_dict_cursor(db="piwigo") as (cur,_):
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
                async with test_db.acquire_dict_cursor(db="piwigo") as (cur,conn):
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", [{ "run_db_mods": False }], indirect=True)
    async def test_db_init(self, capfd, test_db, db_cfg):
        """tests running db initialization at startup. this is just a bare
        bones test confirming that the init did indeed run"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            async with test_db.acquire_dict_cursor(db="piwigo") as (cur,_):
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

                agent_proc = subprocess.Popen([
                    "pwgo-helper",
                    "-v",
                    "DEBUG",
                    "--dry-run",
                    "agent",
                    "--piwigo-galleries-host-path",
                    "/workspace",
                    "--image-crop-save-path",
                    "/workspace",
                    "--virtualfs-root",
                    str(vfs_root_path),
                    "--pwgo-db-host",
                    db_cfg["host"],
                    "--pwgo-db-port",
                    str(db_cfg["port"]),
                    "--pwgo-db-user",
                    db_cfg["user"],
                    "--pwgo-db-pw",
                    db_cfg["passwd"],
                    "--workers",
                    "10",
                    "--initialize-db"
                ])
                await asyncio.sleep(3)
                try:
                    # verify program is still running
                    assert not agent_proc.returncode
                    captured = capfd.readouterr().err.splitlines()
                    db_init_re = re.compile('^.*('+strings.LOG_INITIALIZE_DB+')')
                    db_init_logs = [ db_init_re.match(s) for s in captured ]
                    db_init_logs = [ m.group(1) for m in db_init_logs if m ]
                    assert len(db_init_logs) == 1

                    # verify table created by init now exists
                    sql = """
                        SELECT COUNT(*) AS cnt
                        FROM information_schema.TABLES
                        WHERE table_schema = 'piwigo' AND table_name = 'category_paths' 
                    """
                    await cur.execute(sql)
                    res = await cur.fetchone()
                    assert res["cnt"]

                finally:
                    agent_proc.terminate()

    @pytest.mark.asyncio
    async def test_env_opts(self, capfd, test_db, db_cfg):
        """tests that options are properly settable from environment"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            vfs_root_path = Path(tmp_dir).joinpath("vfs")
            vfs_root_path.mkdir()

            os.environ["PWGO_HLPR_VERBOSITY"] = "DEBUG"
            os.environ["PWGO_HLPR_AGENT_WORKERS"] = "17"
            os.environ["PWGO_HLPR_AGENT_INITIALIZE_DB"] = "True"
            os.environ["PWGO_HLPR_AGENT_PWGO_DB_HOST"] = db_cfg["host"]
            os.environ["PWGO_HLPR_AGENT_PWGO_DB_PORT"] = str(db_cfg["port"])
            os.environ["PWGO_HLPR_AGENT_PWGO_DB_USER"] = db_cfg["user"]
            os.environ["PWGO_HLPR_AGENT_PWGO_DB_PW"] = db_cfg["passwd"]
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
            assert not agent_proc.returncode
            captured = capfd.readouterr().err.splitlines()
            prg_verb_opt_re = re.compile('^.*('+strings.LOG_PRG_OPT('verbosity', os.environ["PWGO_HLPR_VERBOSITY"])+')')
            prg_verb_opt_logs = [ prg_verb_opt_re.match(s) for s in captured ]
            prg_verb_opt_logs = [ m.group(1) for m in prg_verb_opt_logs if m ]
            assert len(prg_verb_opt_logs) == 1

            agnt_wkrs_opt_re = re.compile('^.*('+strings.LOG_AGNT_OPT('workers', os.environ["PWGO_HLPR_AGENT_WORKERS"])+')')
            agnt_wkrs_opt_logs = [ agnt_wkrs_opt_re.match(s) for s in captured ]
            agnt_wkrs_opt_logs = [ m.group(1) for m in agnt_wkrs_opt_logs if m ]
            assert len(agnt_wkrs_opt_logs) == 1

            agnt_initdb_opt_re = re.compile('^.*('+strings.LOG_AGNT_OPT('initialize_db', True)+')')
            agnt_initdb_opt_logs = [ agnt_initdb_opt_re.match(s) for s in captured ]
            agnt_initdb_opt_logs = [ m.group(1) for m in agnt_initdb_opt_logs if m ]
            assert len(agnt_initdb_opt_logs) == 1
