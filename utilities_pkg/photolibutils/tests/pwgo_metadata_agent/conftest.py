"""test setup utilities for pwgo_metadata_agent tests"""
import tempfile,json,asyncio,os
from unittest.mock import MagicMock,patch

import pytest
from aiomysql.cursors import DictCursor

from ...pwgo_metadata_agent.db_connection_pool import DbConnectionPool
from ...pwgo_metadata_agent.utilities import parse_sql

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_SCRIPTS_PATH = os.path.join(MODULE_PATH, "test_db_scripts")
DB_MOD_SCRIPTS_PATH = "/workspace/piwigo/db_init"
REK_SCRIPTS_PATH = "/workspace/rekognition_db"

def _handle_exception(_loop, ctx):
    msg = ctx.get("exception", ctx["message"])
    raise RuntimeError(msg)

@pytest.fixture(scope="session")
def event_loop():
    """redefine built in event_loop fixture to be session scoped
    otherwise, can't use it in the below fixtures"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    #attach an exception handler here and in the actual app code
    loop.set_exception_handler(_handle_exception)
    yield loop
    loop.close()

@pytest.fixture(scope="session")
def mck_dict_cursor():
    """provides a reference to the mocked DictCursor which will be return by the MYSQL_CONN_POOL
    which can then be used to mock return values from the fetch* method(s)"""

    with patch.object(DbConnectionPool, "get") as mck_conn_pool_get:
        mck_conn_pool_get.return_value = MagicMock(DbConnectionPool)
        mck_curs = MagicMock(DictCursor)
        mck_conn_pool_get.return_value.acquire_dict_cursor.return_value.__aenter__.return_value = (mck_curs,None)

        yield mck_curs

@pytest.fixture(scope="session")
def test_db_cfg():
    """provides config object for local test db"""
    return {"host": "localhost", "port": 3306, "user": "root", "passwd": "vscode", "temp_db": True}
    #return {"host": "192.168.1.100","port": 3306,"user": "root","passwd": "s!0F5X9XsYFY","temp_db": False}

# making the test_db function scoped so we don't have to worry about db cleanup in the tests
# building and tearing down the db is surprisingly quick, so whatever
@pytest.fixture(scope="function")
async def test_db(test_db_cfg):
    """sets up test database and configures the DbConnectionPool instance"""
    db_mod_scripts = [
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_category_paths.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_implicit_tags.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_image_metadata.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_image_virtual_paths.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_image_category_triggers.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_tags_triggers.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_image_tag_triggers.sql"),
        os.path.join(DB_MOD_SCRIPTS_PATH, "create_pwgo_message.sql"),
        os.path.join(REK_SCRIPTS_PATH, "create_image_labels.sql"),
        os.path.join(REK_SCRIPTS_PATH, "create_index_faces.sql"),
        os.path.join(REK_SCRIPTS_PATH, "create_processed_faces.sql")
    ]

    with tempfile.NamedTemporaryFile(mode='w+') as cfg_file:
        json.dump(test_db_cfg, cfg_file)
        cfg_file.flush()
        await DbConnectionPool.initialize(cfg_file.name)
        conn_pool = DbConnectionPool.get()
        if test_db_cfg["temp_db"]:
            async with conn_pool.acquire_connection() as conn:
                async with conn.cursor() as cur:
                    with open(os.path.join(DB_SCRIPTS_PATH, "db_create.sql")) as sql:
                        await cur.execute(sql.read())
                    await cur.execute('USE `piwigo`;')
                    stmts = parse_sql(os.path.join(DB_SCRIPTS_PATH, "build_db.sql"))
                    for stmt in stmts:
                        await cur.execute(stmt)

                    for sql_path in db_mod_scripts:
                        stmts = parse_sql(sql_path)
                        for stmt in stmts:
                            await cur.execute(stmt)
                await conn.commit()

        yield conn_pool

        if test_db_cfg["temp_db"]:
            async with conn_pool.acquire_connection() as conn:
                async with conn.cursor() as cur:
                    with open(os.path.join(DB_SCRIPTS_PATH, "db_drop.sql")) as sql_file:
                        sql = sql_file.read()
                        await conn_pool.clear()
                        await cur.execute(sql)
                        await conn.commit()

        conn_pool.terminate()
