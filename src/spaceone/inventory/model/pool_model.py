from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.model.region_model import Region


class Pool(MongoModel):
    pool_id = StringField(max_length=40, generate_id='pool', unique=True)
    name = StringField(max_length=255)
    state = StringField(max_length=20, default='ACTIVE')
    tags = DictField()
    zone = ReferenceField('Zone', reverse_delete_rule=DENY)
    region = ReferenceField('Region', reverse_delete_rule=DENY)
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'tags',
            'state'
        ],
        'exact_fields': [
            'pool_id',
            'state'
        ],
        'minimal_fields': [
            'pool_id',
            'name',
            'state'
        ],
        'change_query_keys': {
            'zone_id': 'zone.zone_id',
            'region_id': 'region.region_id'
        },
        'reference_query_keys': {
            'zone': Zone,
            'region': Region
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'pool_id',
            'state',
            'zone',
            'region',
            'domain_id'
        ],
        'aggregate': {
            'lookup': {
                'region': {
                    'from': 'region'
                },
                'zone': {
                    'from': 'zone'
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
                'pool': self
            })

            PoolMemberMap.create(data)
        else:
            super().append(key, data)

        return self

    def remove(self, key, data):
        if key == 'members':
            query = {
                'filter': [{
                    'k': 'pool',
                    'v': self,
                    'o': 'eq'
                }, {
                    'k': 'user_id',
                    'v': data,
                    'o': 'eq'
                }]
            }

            member_map_vos, map_count = PoolMemberMap.query(**query)
            member_map_vos.delete()
        else:
            super().remove(key, data)

        return self


class PoolMemberMap(MongoModel):
    pool = ReferenceField('Pool', reverse_delete_rule=CASCADE)
    user_id = StringField(max_length=40)
    labels = ListField(StringField(max_length=255))

    meta = {
        'reference_query_keys': {
            'pool': Pool,
        },
        'change_query_keys': {
            'pool_id': 'pool.pool_id'
        },
        'indexes': [
            'pool',
            'user_id'
        ]
    }
