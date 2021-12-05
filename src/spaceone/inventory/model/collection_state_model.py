from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CollectionState(MongoModel):
    collector_id = StringField(max_length=40)
    job_task_id = StringField(max_length=40)
    secret_id = StringField(max_length=40)
    resource_id = StringField(max_length=40)
    resource_type = StringField(max_length=255)
    disconnected_count = IntField(default=0)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'job_task_id',
            'disconnected_count',
            'updated_at'
        ],
        'indexes': [
            'collector_id',
            'job_task_id',
            'secret_id',
            'resource_id',
            'resource_type',
            'disconnected_count',
            'domain_id'
        ]
    }

