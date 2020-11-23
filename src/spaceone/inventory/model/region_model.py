from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id='region', unique=True)
    name = StringField(max_length=255)
    region_code = StringField(max_length=255, unique_with=['provider', 'domain_id'])
    provider = StringField(max_length=255)
    ref_region = StringField(max_length=255)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'tags',
            'updated_at'
        ],
        'exact_fields': [
            'region_id',
            'region_code',
            'provider',
            'ref_region',
            'domain_id'
        ],
        'minimal_fields': [
            'region_id',
            'name',
            'region_code',
            'provider'
        ],
        # 'ordering': [
        #     'name'
        # ],
        'indexes': [
            'region_id',
            'region_code',
            'provider',
            'ref_region',
            'domain_id'
        ]
    }
