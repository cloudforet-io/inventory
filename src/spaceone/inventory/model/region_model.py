from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id='region', unique=True)
    name = StringField(max_length=255)
    region_key = StringField(max_length=255)
    region_code = StringField(max_length=255, unique_with=['provider', 'domain_id'])
    provider = StringField(max_length=255)
    ref_region = StringField(max_length=255)
    tags = DictField()
    domain_id = StringField(max_length=255)
    updated_by = StringField(default=None, null=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'region_key',
            'tags',
            'updated_by',
            'updated_at'
        ],
        'minimal_fields': [
            'region_id',
            'name',
            'region_code',
            'provider'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            # 'region_id',
            'region_key',
            'region_code',
            'provider',
            'ref_region',
            'domain_id',
        ]
    }
