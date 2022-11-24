from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceTag(MongoModel):
    cloud_service_id = StringField(max_length=40)
    key = StringField(max_length=255)
    value = StringField(max_length=255)
    type = StringField(max_length=255, choices=('CUSTOM', 'MANAGED'))
    provider = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'key',
            'value'
        ],
        'minimal_fields': [
            'cloud_service_id',
            'key',
            'value',
            'type',
            'provider'
        ],
        'indexes': [
            {
                "fields": ['domain_id', 'cloud_service_id', 'key', 'value'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"   # Cloud Service Tag Search
            },
            {
                "fields": ['domain_id', 'key', 'value', 'cloud_service_id'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"   # Cloud Service Search
            },
            {
                "fields": ['domain_id', 'cloud_service_id', 'type'],
                "name": "COMPOUND_INDEX_FOR_UPDATE"
            }
        ]
    }
