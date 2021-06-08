'''container module for TestDatabaseEvent'''
import json

from ...pwgo_metadata_agent.database_event_row import DatabaseEventRow,ImageMetadataEventRow,TagEventRow

def _build_evt_msg(**kwargs):
    msg = {}
    msg[kwargs["id_nm"]] = kwargs["id_val"]
    msg["table_name"] = kwargs["t_nm"]
    msg["table_primary_key"] = kwargs["pk_vals"]
    msg["operation"] = kwargs["oper"]
    if "before" in kwargs:
        msg["before"] = kwargs["before"]
    if "after" in kwargs:
        msg["after"] = kwargs["after"]
    return msg

def _build_evt_row(**kwargs):
    row = {"values": {}}
    row["values"]["message_type"] = kwargs["m_type"]
    row["values"]["message"] = json.dumps(_build_evt_msg(**kwargs))
    return row

class TestDatabaseEventRow:
    '''tests for database event dtos in database_event module'''
    def test_image_metadata_event_from_json(self):
        '''tests the basic functioning of parsing generic metadata event from json string'''
        evt_msg = _build_evt_row(m_type="IMG_METADATA",id_nm="image_id",id_val=1
                ,t_nm="image_tag",pk_vals=[1,1],oper="INSERT")
        evt_dto = DatabaseEventRow.from_json(evt_msg["values"]["message_type"], evt_msg["values"]["message"])

        assert isinstance(evt_dto, ImageMetadataEventRow)
        assert not evt_dto.db_event_data
        assert evt_dto.record_id == 1
        assert evt_dto.table_name == "image_tag"
        assert evt_dto.table_primary_key == [1,1]
        assert evt_dto.db_event_type == "INSERT"
        assert evt_dto.image_id == evt_dto.record_id

    def test_tag_event_from_json(self):
        '''tests the basic functioning of parsing tag event from json string'''
        evt_msg = _build_evt_row(m_type="TAGS",id_nm="tag_id",id_val=1
                ,t_nm="tags",pk_vals=[1],oper="INSERT")
        evt_dto = DatabaseEventRow.from_json(evt_msg["values"]["message_type"], evt_msg["values"]["message"])

        assert isinstance(evt_dto, TagEventRow)
        assert not evt_dto.db_event_data
        assert evt_dto.record_id == 1
        assert evt_dto.table_name == "tags"
        assert evt_dto.table_primary_key == [1]
        assert evt_dto.db_event_type == "INSERT"
        assert evt_dto.tag_id == evt_dto.record_id

    def test_tag_update_from_json(self):
        '''tests the functioning of parsing an update event on a tag'''
        before_data={"name": "before_nm"}
        after_data={"name": "after_nm"}
        evt_msg = _build_evt_row(m_type="TAGS",id_nm="tag_id",id_val=1
            ,t_nm="tags",pk_vals=[1],oper="UPDATE",before=before_data,after=after_data)
        evt_dto = DatabaseEventRow.from_json(evt_msg["values"]["message_type"], evt_msg["values"]["message"])

        assert isinstance(evt_dto, TagEventRow)
        assert evt_dto.record_id == 1
        assert evt_dto.table_name == "tags"
        assert evt_dto.table_primary_key == [1]
        assert evt_dto.db_event_type == "UPDATE"
        assert evt_dto.tag_id == evt_dto.record_id
        assert evt_dto.db_event_data["UPDATE"]["before"]["name"] == "before_nm"
        assert evt_dto.db_event_data["UPDATE"]["after"]["name"] == "after_nm"
