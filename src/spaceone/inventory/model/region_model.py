from mongoengine import *
from datetime import datetime
from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id='region', unique=True)
    state = StringField(max_length=20, default='ACTIVE')
    name = StringField(max_length=255)
    region_code = StringField(max_length=255, unique_with=['region_type', 'domain_id'])
    region_type = StringField(max_length=255, choices=('AWS', 'GOOGLE_CLOUD', 'AZURE', 'DATACENTER'))
    region_ref = StringField(max_length=255)
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
            'region_type'
        ],
        'minimal_fields': [
            'region_id',
            'name',
            'state',
            'region_code',
            'region_type'
        ],
        'change_query_keys': {},
        'ordering': [
            'name'
        ],
        'indexes': [
            'region_id',
            'state',
            'domain_id',
            'region_code',
            'region_type'
        ]
    }

    def __str__(self):
        return self.region_id

    @queryset_manager
    def objects(doc_cls, queryset):
        return queryset.filter(state__ne='DELETED')

    def delete(self):
        self.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })
