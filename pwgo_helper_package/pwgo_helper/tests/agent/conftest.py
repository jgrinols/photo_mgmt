"""test setup utilities for pwgo_metadata_agent tests"""
import asyncio,os,string,random
from unittest.mock import MagicMock,patch

import pytest
from asyncmy.cursors import DictCursor

from ...db_connection_pool import DbConnectionPool
from ...agent.utilities import parse_sql
from ...config import Configuration as ProgramConfig, PiwigoScripts, RekognitionScripts

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

# making the test_db function scoped so we don't have to worry about db cleanup in the tests
# building and tearing down the db is surprisingly quick, so whatever
@pytest.fixture(scope="function", params=[{ "run_db_mods": True }])
async def test_db(request):
    """sets up test database and configures the DbConnectionPool instance"""
    db_cfg = {
        "host": "mariadb",
        "port": 3306,
        "user": "root",
        "password": "vscode"
    }
    pwgo_db_name = ''.join(random.choice(string.ascii_lowercase+string.digits) for i in range(8))
    msg_db_name = ''.join(random.choice(string.ascii_lowercase+string.digits) for i in range(8))
    rek_db_name = ''.join(random.choice(string.ascii_lowercase+string.digits) for i in range(8))
    # fixture can accept a boolean param indicating that initialization should be skipped
    if request.param["run_db_mods"]:
        pwgo_scripts = PiwigoScripts(pwgo_db_name=pwgo_db_name,msg_db_name=msg_db_name)
        rek_scripts = RekognitionScripts(rek_db_name=rek_db_name)
        db_mod_scripts = [
            pwgo_scripts.create_category_paths,
            pwgo_scripts.create_implicit_tags,
            pwgo_scripts.create_image_metadata,
            pwgo_scripts.create_image_virtual_paths,
            pwgo_scripts.create_image_category_triggers,
            pwgo_scripts.create_tags_triggers,
            pwgo_scripts.create_image_tag_triggers,
            pwgo_scripts.create_pwgo_message,
            rek_scripts.create_rekognition_db,
            rek_scripts.create_image_labels,
            rek_scripts.create_index_faces,
            rek_scripts.create_processed_faces
        ]
    else:
        db_mod_scripts = []

    async with DbConnectionPool.initialize(**db_cfg) as conn_pool:
        async with conn_pool.acquire_connection() as conn:
            async with conn.cursor() as cur:
                with open(os.path.join(DB_SCRIPTS_PATH, "db_create.sql")) as sql:
                    await cur.execute(sql.read().replace("{{pwgo_db}}", pwgo_db_name).replace("{{msg_db}}", msg_db_name))
                await cur.execute(f'USE `{pwgo_db_name}`;')
                with open(os.path.join(DB_SCRIPTS_PATH, "build_db.sql"), 'r') as script:
                    stmts = parse_sql(script.read())
                for stmt in stmts:
                    await cur.execute(stmt)

                for sql in db_mod_scripts:
                    stmts = parse_sql(sql)
                    for stmt in stmts:
                        await cur.execute(stmt)
            await conn.commit()

        yield TestDbResult(db_cfg, conn_pool, pwgo_db_name, msg_db_name, rek_db_name)

        async with conn_pool.acquire_connection() as conn:
            async with conn.cursor() as cur:
                with open(os.path.join(DB_SCRIPTS_PATH, "db_drop.sql")) as sql_file:
                    sql = sql_file.read().replace("{{pwgo_db}}", pwgo_db_name) \
                        .replace("{{msg_db}}", msg_db_name) \
                        .replace("{{rek_db}}", rek_db_name)
                    await conn_pool.clear()
                    await cur.execute(sql)
                    await conn.commit()

class TestDbResult:
    """class to encapsulate test database parameters needed by tests"""
    def __init__(self, db_host, conn_pool, pwgo_db, msg_db, rek_db) -> None:
        self.db_host = db_host
        self.db_connection_pool = conn_pool
        self.piwigo_db = pwgo_db
        self.messaging_db = msg_db
        self.rekognition_db = rek_db
