from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CollectionState(MongoModel):
    collector_id = StringField(max_length=40)
    job_task_id = StringField(max_length=40)
    secret_id = StringField(max_length=40)
    cloud_service_id = StringField(max_length=40)
    disconnected_count = IntField(default=0)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": ["job_task_id", "disconnected_count", "updated_at"],
        "indexes": [
            {
                "fields": [
                    "domain_id",
                    "cloud_service_id",
                    "collector_id",
                    "secret_id",
                ],
                "name": "COMPOUND_INDEX_FOR_GET",
            },
            {
                "fields": ["domain_id", "collector_id", "-disconnected_count"],
                "name": "COMPOUND_INDEX_FOR_DELETE_1",
            },
            {
                "fields": ["domain_id", "cloud_service_id"],
                "name": "COMPOUND_INDEX_FOR_DELETE_2",
            },
            {
                "fields": [
                    "domain_id",
                    "collector_id",
                    "job_task_id",
                    "secret_id",
                    "updated_at",
                ],
                "name": "COMPOUND_INDEX_FOR_DELETE_3",
            },
            "cloud_service_id",
        ],
    }
