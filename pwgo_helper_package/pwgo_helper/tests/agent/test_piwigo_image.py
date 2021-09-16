"""container module for TestPiwigoImageMetadata and TestPiwigoImage"""
# pylint: disable=protected-access
import datetime, json
from unittest.mock import AsyncMock, patch

import pytest

from ...agent.pwgo_image import PiwigoImageMetadata,PiwigoImage
from ...agent.config import Configuration as AgentConfig
from ...config import Configuration as ProgramConfig
from .conftest import TestDbResult

class TestPiwigoImageMetadata:
    """tests for the PiwigoImageMetadata class"""
    def test_init(self):
        """tests the basic functioning of PiwigoImageMetadata.
        Included test of tag deduplicatiohn"""
        mdata = {
            "name": "test_name",
            "comment": "test_comment",
            "author": "test_author",
            "date_creation": "2021-01-01 08:00:00",
            "tags": ["tag1","tag2","tag1"]
        }
        pwgo_mdata = PiwigoImageMetadata(mdata)
        assert pwgo_mdata.name == mdata["name"]
        assert pwgo_mdata.comment == mdata["comment"]
        assert pwgo_mdata.author == mdata["author"]
        assert pwgo_mdata.create_date == datetime.datetime(2021,1,1,8,0,0)
        assert pwgo_mdata.tags == ["tag1","tag2"]

        pwgo_iptc = pwgo_mdata.get_iptc_dict()
        assert pwgo_iptc["Iptc.Application2.ObjectName"] == mdata["name"]
        assert pwgo_iptc["Iptc.Application2.Caption"] == mdata["comment"]
        assert pwgo_iptc["Iptc.Application2.Byline"] == mdata["author"]
        assert pwgo_iptc["Iptc.Application2.Keywords"] == ["tag1","tag2"]

    def test_missing_field(self):
        """tests that proper error is raised if required field is missing"""
        mdata = {
            "name": "test_name",
            "author": "test_author",
            "date_creation": "2021-01-01 08:00:00",
            "tags": ["tag1","tag2"]
        }
        with pytest.raises(AttributeError, match="Required attribute comment missing"):
            _ = PiwigoImageMetadata(mdata)

    def test_null_fields(self):
        """tests that we gracefully handle receiving null values from db"""
        mdata = {
            "name": None,
            "author": None,
            "date_creation": None,
            "comment": None,
            "tags": None
        }
        pwgo_mdata = PiwigoImageMetadata(mdata)

    def test_long_metadata_fields(self):
        """tests that metadata values that exceed iptc specs are properly truncated"""
        mdata = {
            "name": "n" * 70,
            "comment": "c" * 2001,
            "author": "a" * 50,
            "date_creation": "2021-01-01 08:00:00",
            "tags": ["tag1","tag2"]
        }
        pwgo_mdata = PiwigoImageMetadata(mdata)
        assert pwgo_mdata.name == mdata["name"]
        assert pwgo_mdata.comment == mdata["comment"]
        assert pwgo_mdata.author == mdata["author"]

        pwgo_iptc = pwgo_mdata.get_iptc_dict()
        assert pwgo_iptc["Iptc.Application2.ObjectName"] == "n" * 64
        assert pwgo_iptc["Iptc.Application2.Caption"] == "c" * 2000
        assert pwgo_iptc["Iptc.Application2.Byline"] == "a" * 32

class TestPiwigoImage:
    """tests for the PiwigoImage class"""
    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    async def test_create(self, m_get_acfg, mck_dict_cursor, test_db: TestDbResult):
        """basic functional test of create static method"""
        test_file = { "file": "test_file.JPG", "path": "/photos/test_file.JPG" }
        test_mdata = { "image_metadata": '''{
            "name": "test_name",
            "comment": "test_comment",
            "author": "test_author",
            "date_creation": "2021-01-01 08:00:00",
            "tags": ["tag1","tag2"]
        }'''}
        acfg = AgentConfig()
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db,
            "dry_run": False
        }
        ProgramConfig.initialize(**pcfg_params)
        m_get_acfg.return_value = acfg
        mck_fetch = AsyncMock(side_effect=[test_file,test_mdata])
        mck_dict_cursor.fetchone = mck_fetch
        pwgo_img = await PiwigoImage.create(1, load_metadata=True)
        assert pwgo_img.id == 1
        assert pwgo_img.file == test_file["file"]
        assert pwgo_img._path == test_file["path"]
        assert pwgo_img.metadata
