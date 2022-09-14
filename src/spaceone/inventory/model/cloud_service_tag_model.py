from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceTag(MongoModel):
    tag_id = StringField(max_length=40, generate_id='tag', unique=True)
    cloud_service_id = StringField(max_length=40)
    key = StringField(max_length=255)
    value = DynamicField()
    provider = StringField(max_length=255)
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
            'tag_id',
            'cloud_service_id',
            'key',
            'value',
            'provider',
            'project_id',
        ],
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'cloud_service_id',
            'key',
            'value',
            'provider'
        ]
    }
