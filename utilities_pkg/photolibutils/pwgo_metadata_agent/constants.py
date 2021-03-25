"""container module for Constants"""
import contextvars

from path import Path

class Constants():
    """Contains program constants, read-only, and configuration values"""
    PWGO_GALLERY_VIRT_PATH = Path("/config/www/gallery")
    PWGO_DB = "piwigo"
    EVENT_TABLES = { "pwgo_message": {} }
    AUTO_TAG_ALB = 125
    AUTO_TAG_PROC_ALB = 126
    FACE_IDX_PARENT_ALB = 128
    FACE_IDX_ALBS = []

    IMG_TAG_WAIT_SECS = 1

    REKOGNITION_DB = "rekognition"
    MSG_DB = "messaging"

    STOP_MSG = {
        "target": "metadata_agent",
        "text": "stop"
    }

    # set from command parameter--so not quite "constants"
    PWGO_GALLERIES_HOST_PATH = None
    REKOGNITION_CFG_FILE = None
    MYSQL_CFG_FILE = None
    MYSQL_CONN_POOL = None
    Q_JOBS = False
    IMG_CROP_PATH = None
    DEBUG = False
    WORKERS_CNT = None

    @staticmethod
    async def initialize_program_configs(**kwargs):
        """Initialize program configuration values"""
        Constants.PWGO_GALLERIES_HOST_PATH = kwargs["pwgo_galleries_host_path"]
        Constants.REKOGNITION_CFG_FILE = kwargs["rekognition_cfg_file"]
        Constants.MYSQL_CFG_FILE = kwargs["mysql_cfg_file"]
        Constants.IMG_CROP_PATH = kwargs["img_crop_path"]
        Constants.WORKERS_CNT = kwargs["worker_count"]
        Constants.DEBUG = kwargs["debug"]
        # need to import here otherwise we create a circular dependency
        #pylint: disable=import-outside-toplevel
        from photolibutils.pwgo_metadata_agent.utilities import DbConnectionPool
        Constants.MYSQL_CONN_POOL = contextvars.ContextVar("DB Connection Pool")
        Constants.MYSQL_CONN_POOL.set(await DbConnectionPool.create(Constants.MYSQL_CFG_FILE))
        await Constants.__set_face_index_categories()

    @staticmethod
    async def __set_face_index_categories():
        async with Constants.MYSQL_CONN_POOL.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur, _):
            sql = """
                SELECT c.id
                FROM piwigo.categories c
                WHERE c.id_uppercat = %s
                    AND c.name NOT LIKE '%s'
            """

            await cur.execute(sql % (Constants.FACE_IDX_PARENT_ALB, ".%"))
            cats = []
            for row in await cur.fetchall():
                cats.append(row["id"])

        Constants.FACE_IDX_ALBS = cats
