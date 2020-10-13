from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id='region', unique=True)
    state = StringField(max_length=20, default='ACTIVE')
    name = StringField(max_length=255)
    region_code = StringField(max_length=255, unique_with=['region_type', 'domain_id'])
    region_type = StringField(max_length=255, choices=('AWS', 'GOOGLE_CLOUD', 'AZURE', 'DATACENTER'))
    ref_region = StringField(max_length=255)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'tags'
        ],
        'exact_fields': [
            'region_id',
            'state',
            'region_code',
            'region_type',
            'ref_region',
            'domain_id'
        ],
        'minimal_fields': [
            'region_id',
            'name',
            'state',
            'region_code',
            'region_type'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            'region_id',
            'state',
            'region_code',
            'region_type',
            'ref_region',
            'domain_id'
        ]
    }
