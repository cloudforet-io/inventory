from datetime import datetime
from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.model.pool_model import Pool
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource


class IPAddress(EmbeddedDocument):
    ip_address = StringField()
    cidr = StringField(default='')
    subnet_id = StringField(default='')
    tags = DictField()


class NIC(EmbeddedDocument):
    device_index = IntField(default=0)
    device = StringField(max_length=50, default='')
    nic_type = StringField(max_length=20, default='PHYSICAL')
    ip_addresses = ListField(EmbeddedDocumentField(IPAddress))
    mac_address = StringField(default='')
    public_ip_address = StringField(default='', max_length=100)
    tags = DictField()

    def to_dict(self):
        return self.to_mongo()


class Disk(EmbeddedDocument):
    device_index = IntField(default=0)
    device = StringField(max_length=50, default='')
    size = FloatField(default=0)
    disk_type = StringField(max_length=20, default='LOCAL')
    disk_id = StringField(default='')
    volume_id = StringField(default='')
    storage_id = StringField(default='')
    tags = DictField()

    def to_dict(self):
        return self.to_mongo()


class Server(MongoModel):
    server_id = StringField(max_length=40, generate_id='server', unique=True)
    name = StringField(max_length=255, default='')
    state = StringField(max_length=20,
                        choices=('PENDING', 'INSERVICE', 'MAINTENANCE', 'CLOSED', 'DELETED'))
    primary_ip_address = StringField(max_length=100)
    ip_addresses = ListField(StringField())
    server_type = StringField(max_length=20, default='UNKNOWN',
                              choices=('UNKNOWN', 'BAREMETAL', 'VM', 'HYPERVISOR'))
    os_type = StringField(max_length=20, choices=('LINUX', 'WINDOWS'))
    provider = StringField(max_length=40)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    nics = ListField(EmbeddedDocumentField(NIC))
    disks = ListField(EmbeddedDocumentField(Disk))
    tags = DictField()
    pool = ReferenceField('Pool', default=None, null=True, reverse_delete_rule=NULLIFY)
    zone = ReferenceField('Zone', default=None, null=True, reverse_delete_rule=NULLIFY)
    region = ReferenceField('Region', default=None, null=True, reverse_delete_rule=NULLIFY)
    project_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'primary_ip_address',
            'ip_addresses',
            'server_type',
            'os_type',
            'provider',
            'data',
            'metadata',
            'reference',
            'nics',
            'disks',
            'asset',
            'pool',
            'zone',
            'region',
            'project_id',
            'domain_id',
            'tags',
            'collection_info',
            'deleted_at'
        ],
        'exact_fields': [
            'server_id',
            'state',
            'primary_ip_address'
            'server_type',
            'os_type',
            'project_id',
            'domain_id',
            'collection_info.state'
        ],
        'minimal_fields': [
            'server_id',
            'name',
            "state",
            "primary_ip_address",
            "server_type",
            "os_type",
            'provider',
            'reference',
            'collection_info.state'

        ],
        'change_query_keys': {
            'asset_id': 'asset.asset_id',
            'pool_id': 'pool.pool_id',
            'zone_id': 'zone.zone_id',
            'region_id': 'region.region_id'
        },
        'reference_query_keys': {
            'pool': Pool,
            'zone': Zone,
            'region': Region
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'server_id',
            'state',
            'primary_ip_address',
            'server_type',
            'os_type',
            'provider',
            'pool',
            'zone',
            'region',
            'project_id',
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
                'pool': {
                    'from': 'pool'
                }
            }
        }
    }

    """
    @queryset_manager
    def objects(doc_cls, queryset):
        return queryset.filter(state__ne='DELETED')

    # def update(self, data):
    #     if self.state == 'DELETED':
    #         raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='Server', resource_id=self.server_id)
    #
    #     return super().update(data)

    def delete(self):
        # if self.state == 'DELETED':
        #     raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='Server', resource_id=self.server_id)

        self.update({
            'state': 'DELETED',
            #'asset': None,
            'pool': None,
            'zone': None,
            'region': None,
            'deleted_at': datetime.utcnow()
        })
    """