from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.model.network_model import Network
from spaceone.inventory.model.network_policy_model import NetworkPolicy
from spaceone.inventory.model.network_type_model import NetworkType
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource


class IPRange(EmbeddedDocument):
    start = StringField(max_length=40)
    end = StringField(max_length=40)


class Subnet(MongoModel):
    subnet_id = StringField(max_length=40, generate_id='subnet', unique=True)
    name = StringField(max_length=255)
    cidr = StringField(max_length=40)
    ip_ranges = ListField(EmbeddedDocumentField(IPRange))
    gateway = StringField(max_length=40)
    vlan = IntField()
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = DictField()
    network = ReferenceField('Network', reverse_delete_rule=DENY)
    network_type = ReferenceField('NetworkType', reverse_delete_rule=DENY)
    network_policy = ReferenceField('NetworkPolicy', null=True, default=None, reverse_delete_rule=NULLIFY)
    project_id = StringField(max_length=40, null=True, default=None)
    zone = ReferenceField('Zone', reverse_delete_rule=DENY)
    region = ReferenceField('Region', reverse_delete_rule=DENY)
    domain_id = StringField(max_length=255)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'state',
            'name',
            'ip_ranges',
            'gateway',
            'vlan',
            'data',
            'metadata',
            'tags',
            'network_type',
            'network_policy',
            'project_id',
            'reference',
            'collection_info'
        ],
        'exact_fields': [
            'subnet_id',
            'cidr',
            'gateway',
            'collection_info.state'
        ],
        'minimal_fields': [
            'subnet_id',
            'name',
            'cidr',
            'gateway',
            'reference',
            'collection_info.state'
        ],
        'change_query_keys': {
            'zone_id': 'zone.zone_id',
            'region_id': 'region.region_id',
            'network_id': 'network.network_id',
            'network_type_id': 'network_type.network_type_id',
            'network_policy_id': 'network_policy.network_policy_id'
        },
        'reference_query_keys': {
            'zone': Zone,
            'region': Region,
            'network_type': NetworkType,
            'network_policy': NetworkPolicy,
            'network': Network
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'subnet_id',
            'cidr',
            'network',
            'network_type',
            'network_policy',
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
                },
                'network': {
                    'from': 'network'
                },
                'network_type': {
                    'from': 'network_type'
                },
                'network_policy': {
                    'from': 'network_policy'
                }
            }
        }
    }
