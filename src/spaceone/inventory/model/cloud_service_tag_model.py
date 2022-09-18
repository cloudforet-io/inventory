from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceTag(MongoModel):
    cloud_service_id = StringField(max_length=40)
    k = StringField(max_length=255)
    v = StringField(max_length=255)
    provider = StringField(max_length=255)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'k',
            'v',
            'project_id'
        ],
        'minimal_fields': [
            'cloud_service_id',
            'k',
            'v',
            'provider',
            'project_id',
        ],
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'cloud_service_id',
            'k',
            'v',
            'provider',
            'project_id',
            'created_at'
        ]
    }
