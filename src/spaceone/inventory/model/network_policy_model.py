from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource


class RoutingTable(EmbeddedDocument):
    cidr = StringField(max_length=40)
    destination = StringField(max_length=40)
    interface = StringField(max_length=40, null=True, default=None)


class NetworkPolicy(MongoModel):
    network_policy_id = StringField(max_length=40, generate_id='npolicy', unique=True)
    name = StringField(max_length=255)
    routing_tables = ListField(EmbeddedDocumentField(RoutingTable))
    dns = ListField(StringField(max_length=40))
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = DictField()
    zone = ReferenceField('Zone', reverse_delete_rule=DENY)
    region = ReferenceField('Region', reverse_delete_rule=DENY)
    domain_id = StringField(max_length=255)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'routing_tables',
            'dns',
            'data',
            'metadata',
            'reference',
            'tags',
            'collection_info'
        ],
        'exact_fields': [
            'network_policy_id',
            'collection_info.state'
        ],
        'minimal_fields': [
            'network_policy_id',
            'name',
            'reference',
            'collection_info.state'
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
            'network_policy_id',
            'zone',
            'region',
            'domain_id',
            'reference.resource_id',
            'collection_info.state'
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
