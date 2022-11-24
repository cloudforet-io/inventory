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
            {
                "fields": ['domain_id', 'region_id'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"
            },
            {
                "fields": ['domain_id', 'region_code', 'provider'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"
            },
            {
                "fields": ['domain_id', 'region_key'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_3"
            },
            {
                "fields": ['region_id', 'ref_region'],
                "name": "COMPOUND_INDEX_FOR_REF_1"
            },
            {
                "fields": ['region_code', 'provider', 'ref_region'],
                "name": "COMPOUND_INDEX_FOR_REF_2"
            },
        ]
    }
