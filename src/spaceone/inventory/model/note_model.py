from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Note(MongoModel):
    note_id = StringField(max_length=40, generate_id="note", unique=True)
    note = StringField()
    created_by = StringField(max_length=40)
    record_id = StringField(max_length=40)
    cloud_service_id = StringField(max_length=40)
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        "updatable_fields": ["note"],
        "minimal_fields": [
            "note_id",
            "note",
            "created_by",
            "record_id",
            "cloud_service_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["-created_at"],
        "indexes": [
            {
                "fields": ["domain_id", "workspace_id", "project_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1",
            },
            {
                "fields": ["domain_id", "cloud_service_id"],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2",
            },
            {
                "fields": ["domain_id", "note_id"],
                "name": "COMPOUND_INDEX_FOR_GET",
            },
            "record_id",
            "cloud_service_id",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
