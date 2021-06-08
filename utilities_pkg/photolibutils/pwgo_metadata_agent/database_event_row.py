'''container module for database event dtos'''
from __future__ import annotations
import json

class DatabaseEventRow:
    '''encapsulates the attributes of a generic db event'''
    def __init__(self, **kwargs):
        self.record_id = kwargs["record_id"]
        self.table_name = kwargs["table_name"]
        self.table_primary_key = kwargs["table_primary_key"]
        self.db_event_type = kwargs["operation"]
        self.db_event_data = {}

    @staticmethod
    def from_json(msg_type: str, json_str: str) -> DatabaseEventRow:
        '''constructs a db event row object from json string'''
        if not isinstance(json_str, str):
            raise TypeError("json_str must be a valid json string")

        mdata = json.loads(json_str)
        if msg_type == "IMG_METADATA":
            result = ImageMetadataEventRow(**mdata)
        elif msg_type == "TAGS":
            result = TagEventRow(**mdata)
        else:
            raise NotImplementedError(f"unknown msg type {msg_type}")

        if mdata["operation"] == "UPDATE":
            result.db_event_data["UPDATE"] = {
                "before": mdata["before"],
                "after": mdata["after"]
            }

        return result

class ImageMetadataEventRow(DatabaseEventRow):
    '''encapsulates the attributes of a generic metadata db event row'''
    def __init__(self, **kwargs):
        self.image_id = kwargs["image_id"]
        super().__init__(record_id=self.image_id, **kwargs)

class TagEventRow(DatabaseEventRow):
    '''encapsulates the attributes of a tag db event row'''
    def __init__(self, **kwargs):
        self.tag_id = kwargs["tag_id"]
        super().__init__(record_id=self.tag_id, **kwargs)
