from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceTag(MongoModel):
    cloud_service_id = StringField(max_length=40)
    key = StringField(max_length=255)
    value = StringField(max_length=255)
    type = StringField(max_length=255, choices=('CUSTOM', 'MANAGED'))
    provider = StringField(max_length=255, default=None, null=True)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'key',
            'value',
            'project_id'
        ],
        'minimal_fields': [
            'cloud_service_id',
            'key',
            'value',
            'type',
            'provider',
            'project_id',
        ],
        'indexes': [
            'cloud_service_id',
            'key',
            'value',
            'type',
            'provider',
            'project_id',
            'domain_id',
            'created_at'
        ]
    }
