from datetime import datetime
from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.reference_resource_model import ReferenceResource
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.error import *


class CollectionInfo(EmbeddedDocument):
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))

    def to_dict(self):
        return dict(self.to_mongo())


class NIC(EmbeddedDocument):
    device_index = IntField(default=0)
    device = StringField(max_length=50, default=None)
    nic_type = StringField(max_length=20, default=None)
    ip_addresses = ListField(StringField(max_length=100))
    cidr = StringField(default=None)
    mac_address = StringField(default=None)
    public_ip_address = StringField(default=None, max_length=100)
    tags = DictField()

    def to_dict(self):
        return self.to_mongo()


class Disk(EmbeddedDocument):
    device_index = IntField(default=0)
    device = StringField(max_length=50, default=None)
    disk_type = StringField(max_length=20, default=None)
    size = FloatField(default=None)
    tags = DictField()

    def to_dict(self):
        return self.to_mongo()


class Server(MongoModel):
    server_id = StringField(max_length=40, generate_id='server', unique=True)
    name = StringField(max_length=255, default='')
    state = StringField(max_length=20, choices=('ACTIVE', 'DELETED'), default='ACTIVE')
    primary_ip_address = StringField(max_length=100, default=None, null=True)
    ip_addresses = ListField(StringField())
    os_type = StringField(max_length=20, choices=('LINUX', 'WINDOWS'))
    account = StringField(max_length=255, default=None, null=True)
    instance_type = StringField(max_length=255, default=None, null=True)
    provider = StringField(max_length=40)
    cloud_service_group = StringField(max_length=255, default=None, null=True)
    cloud_service_type = StringField(max_length=255, default=None, null=True)
    ref_cloud_service_type = StringField(max_length=255, default=None, null=True)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    nics = ListField(EmbeddedDocumentField(NIC))
    disks = ListField(EmbeddedDocumentField(Disk))
    tags = DictField()
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
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
            'os_type',
            'account',
            'instance_type',
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'data',
            'metadata',
            'reference',
            'nics',
            'disks',
            'project_id',
            'region_code',
            'tags',
            'collection_info',
            'updated_at',
            'deleted_at',
        ],
        'minimal_fields': [
            'server_id',
            'name',
            "state",
            "primary_ip_address",
            "os_type",
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'region_code',
            'reference.resource_id',
            'project_id',
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'reference_query_keys': {
            'ref_cloud_service_type': {
                'model': CloudServiceType,
                'foreign_key': 'ref_cloud_service_type'
            },
            'ref_region': {
                'model': Region,
                'foreign_key': 'ref_region'
            }
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            # 'server_id',
            'state',
            'primary_ip_address',
            'os_type',
            'account',
            'instance_type',
            'reference.resource_id',
            'data.power_state.status',
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'ref_cloud_service_type',
            'region_code',
            'ref_region',
            'project_id',
            'domain_id',
            'collection_info.collectors',
            'collection_info.service_accounts',
            'collection_info.secrets',
            'created_at',
            'updated_at',
            {
                "fields": ['domain_id', 'provider', 'region_code', 'state', 'project_id',
                           'cloud_service_group', 'cloud_service_type', 'ref_cloud_service_type'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
        ]
    }

    def update(self, data):
        if self.state == 'DELETED':
            raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='Server', resource_id=self.server_id)

        return super().update(data)

    def delete(self):
        if self.state == 'DELETED':
            raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='Server', resource_id=self.server_id)

        self.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })
