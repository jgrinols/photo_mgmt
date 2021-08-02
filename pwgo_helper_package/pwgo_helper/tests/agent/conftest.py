"""test setup utilities for pwgo_metadata_agent tests"""
import asyncio,os
from unittest.mock import MagicMock,patch

import pytest
from aiomysql.cursors import DictCursor

from ...db_connection_pool import DbConnectionPool
from ...agent.utilities import parse_sql
from ...config import Configuration as ProgramConfig

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_SCRIPTS_PATH = os.path.join(MODULE_PATH, "test_db_scripts")

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
def db_cfg():
    """provides config object for local test db"""
    return {
        "host": "mariadb",
        "port": 3306,
        "user": "root",
        "password": "vscode"
    }

# making the test_db function scoped so we don't have to worry about db cleanup in the tests
# building and tearing down the db is surprisingly quick, so whatever
@pytest.fixture(scope="function", params=[{ "run_db_mods": True }])
async def test_db(request, db_cfg):
    """sets up test database and configures the DbConnectionPool instance"""
    cfg = ProgramConfig.get()
    # fixture can accept a boolean param indicating that initialization should be skipped
    if request.param["run_db_mods"]:
        db_mod_scripts = [
            cfg.piwigo_db_scripts.create_category_paths,
            cfg.piwigo_db_scripts.create_implicit_tags,
            cfg.piwigo_db_scripts.create_image_metadata,
            cfg.piwigo_db_scripts.create_image_virtual_paths,
            cfg.piwigo_db_scripts.create_image_category_triggers,
            cfg.piwigo_db_scripts.create_tags_triggers,
            cfg.piwigo_db_scripts.create_image_tag_triggers,
            cfg.piwigo_db_scripts.create_pwgo_message,
            cfg.rekognition_db_scripts.create_rekognition_db,
            cfg.rekognition_db_scripts.create_image_labels,
            cfg.rekognition_db_scripts.create_index_faces,
            cfg.rekognition_db_scripts.create_processed_faces
        ]
    else:
        db_mod_scripts = []

    async with DbConnectionPool.initialize(**db_cfg) as conn_pool:
        async with conn_pool.acquire_connection() as conn:
            async with conn.cursor() as cur:
                with open(os.path.join(DB_SCRIPTS_PATH, "db_create.sql")) as sql:
                    await cur.execute(sql.read())
                await cur.execute('USE `piwigo`;')
                with open(os.path.join(DB_SCRIPTS_PATH, "build_db.sql"), 'r') as script:
                    stmts = parse_sql(script.read())
                for stmt in stmts:
                    await cur.execute(stmt)

                for sql in db_mod_scripts:
                    stmts = parse_sql(sql)
                    for stmt in stmts:
                        await cur.execute(stmt)
            await conn.commit()

        yield conn_pool

        async with conn_pool.acquire_connection() as conn:
            async with conn.cursor() as cur:
                with open(os.path.join(DB_SCRIPTS_PATH, "db_drop.sql")) as sql_file:
                    sql = sql_file.read()
                    await conn_pool.clear()
                    await cur.execute(sql)
                    await conn.commit()
