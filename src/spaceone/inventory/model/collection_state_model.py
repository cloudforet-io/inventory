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
            {
                "fields": ['domain_id', 'collector_id', 'resource_id', 'secret_id'],
                "name": "COMPOUND_INDEX_FOR_GET"
            },
            {
                "fields": ['domain_id', 'collector_id', '-disconnected_count'],
                "name": "COMPOUND_INDEX_FOR_DELETE_1"
            },
            {
                "fields": ['resource_id', 'domain_id'],
                "name": "COMPOUND_INDEX_FOR_DELETE_2"
            },
            {
                "fields": ['domain_id', 'collector_id', 'job_task_id', 'secret_id', 'updated_at'],
                "name": "COMPOUND_INDEX_FOR_DELETE_3"
            },
        ]
    }

