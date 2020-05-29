from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel


class Region(MongoModel):
    region_id = StringField(max_length=40, generate_id='region', unique=True)
    state = StringField(max_length=20, default='ACTIVE')
    name = StringField(max_length=255)
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
            'state'
        ],
        'minimal_fields': [
            'region_id',
            'name',
            'state'
        ],
        'change_query_keys': {},
        'ordering': [
            'name'
        ],
        'indexes': [
            'region_id',
            'state',
            'domain_id'
        ]
    }

    @queryset_manager
    def objects(doc_cls, queryset):
        return queryset.filter(state__ne='DELETED')

    def delete(self):
        self.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })

    def append(self, key, data):
        if key == 'members':
            data.update({
                'region': self
            })

            RegionMemberMap.create(data)
        else:
            super().append(key, data)

        return self

    def remove(self, key, data):
        if key == 'members':
            query = {
                'filter': [{
                    'k': 'region',
                    'v': self,
                    'o': 'eq'
                }, {
                    'k': 'user_id',
                    'v': data,
                    'o': 'eq'
                }]
            }
            member_map_vos, map_count = RegionMemberMap.query(**query)
            member_map_vos.delete()
        else:
            super().remove(key, data)

        return self


class RegionMemberMap(MongoModel):
    region = ReferenceField('Region', reverse_delete_rule=CASCADE)
    user_id = StringField(max_length=40)
    labels = ListField(StringField(max_length=255))

    meta = {
        'reference_query_keys': {
            'region': Region,
        },
        'change_query_keys': {
            'region_id': 'region.region_id'
        },
        'indexes': [
            'region',
            'user_id'
        ]
    }