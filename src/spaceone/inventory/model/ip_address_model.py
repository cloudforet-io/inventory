from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.subnet_model import Subnet
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.model.network_model import Network
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource


class Resource(EmbeddedDocument):
    type = StringField(max_length=40)
    id = StringField(max_length=40)


class IPAddress(MongoModel):
    ip_address = StringField(max_length=40)
    ip_int = IntField(default=None, null=True)
    state = StringField(max_length=40)
    resource = EmbeddedDocumentField(Resource)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = DictField()
    subnet = ReferenceField('Subnet', reverse_delete_rule=CASCADE)
    network = ReferenceField('Network', reverse_delete_rule=CASCADE)
    zone = ReferenceField('Zone', reverse_delete_rule=CASCADE)
    domain_id = StringField(max_length=255)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    updated_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'state',
            'resource',
            'data',
            'metadata',
            'reference',
            'tags',
            'collection_info'
        ],
        'exact_fields': [
            'ip_address',
            'state',
            'collection_info.state'
        ],
        'minimal_fields': [
            'ip_address',
            'state',
            'reference',
            'collection_info.state'
        ],
        'change_query_keys': {
            'zone_id': 'zone.zone_id',
            'network_id': 'network.network_id',
            'subnet_id': 'subnet.subnet_id'
        },
        'reference_query_keys': {
            'zone': Zone,
            'network': Network,
            'subnet': Subnet
        },
        'ordering': [
            'ip_address'
        ],
        'indexes': [
            'ip_address',
            'state',
            'subnet',
            'network',
            'zone',
            'domain_id',
            'reference.resource_id',
            'collection_info.state'
        ],
        'aggregate': {
            'lookup': {
                'zone': {
                    'from': 'zone'
                },
                'network': {
                    'from': 'network'
                },
                'subnet': {
                    'from': 'subnet'
                }
            }
        }
    }
