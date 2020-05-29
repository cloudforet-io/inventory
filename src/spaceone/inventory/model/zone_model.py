from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.region_model import Region


class Zone(MongoModel):
    zone_id = StringField(max_length=40, generate_id='zone', unique=True)
    name = StringField(max_length=255)
    state = StringField(max_length=20, default='ACTIVE')
    tags = DictField()
    region = ReferenceField('Region', reverse_delete_rule=DENY)
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
            'zone_id',
            'state'
        ],
        'minimal_fields': [
            'zone_id',
            'name',
            'state'
        ],
        'change_query_keys': {
            'region_id': 'region.region_id'
        },
        'reference_query_keys': {
            'region': Region
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'zone_id',
            'state',
            'region',
            'domain_id'
        ],
        'aggregate': {
            'lookup': {
                'region': {
                    'from': 'region'
                }
            }
        }
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
                'zone': self
            })

            ZoneMemberMap.create(data)
        else:
            super().append(key, data)

        return self

    def remove(self, key, data):
        if key == 'members':
            query = {
                'filter': [{
                    'k': 'zone',
                    'v': self,
                    'o': 'eq'
                }, {
                    'k': 'user_id',
                    'v': data,
                    'o': 'eq'
                }]
            }

            member_map_vos, map_count = ZoneMemberMap.query(**query)
            member_map_vos.delete()
        else:
            super().remove(key, data)

        return self


class ZoneMemberMap(MongoModel):
    zone = ReferenceField('Zone', reverse_delete_rule=CASCADE)
    user_id = StringField(max_length=40)
    labels = ListField(StringField(max_length=255))

    meta = {
        'reference_query_keys': {
            'zone': Zone,
        },
        'change_query_keys': {
            'zone_id': 'zone.zone_id'
        },
        'indexes': [
            'zone',
            'user_id'
        ]
    }
