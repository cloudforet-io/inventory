from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.reference_resource_model import ReferenceResource
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.error import *


class Tag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)
    type = StringField(max_length=255, choices=('CUSTOM', 'MANAGED'))
    provider = StringField(max_length=255, default=None, null=True)


class CollectionInfo(EmbeddedDocument):
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))

    def to_dict(self):
        return dict(self.to_mongo())


class CloudService(MongoModel):
    cloud_service_id = StringField(max_length=40, generate_id='cloud-svc', unique=True)
    name = StringField(max_length=255, default=None, null=True)
    state = StringField(max_length=20, choices=('ACTIVE', 'DISCONNECTED', 'DELETED'), default='ACTIVE')
    account = StringField(max_length=255, default=None, null=True)
    instance_type = StringField(max_length=255, default=None, null=True)
    instance_size = FloatField(max_length=255, default=None, null=True)
    ip_addresses = ListField(StringField(max_length=255), default=[])
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    provider = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = ListField(EmbeddedDocumentField(Tag, required=True), default=[])
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'name',
            'data',
            'state',
            'account',
            'instance_type',
            'instance_size',
            'ip_addresses',
            'metadata',
            'reference',
            'tags',
            'project_id',
            'region_code',
            'cloud_service_group',
            'cloud_service_type',
            'collection_info',
            'updated_at',
            'deleted_at',
        ],
        'minimal_fields': [
            'cloud_service_id',
            'name',
            'cloud_service_group',
            'cloud_service_type',
            'provider',
            'reference.resource_id',
            'region_code',
            'project_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id',
            'ip_address': 'ip_addresses'
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
        'indexes': [
            'reference.resource_id',
            'state',
            {
                "fields": ['domain_id', 'state', 'cloud_service_id'],
                "name": "COMPOUND_INDEX_FOR_GC_1"
            },
            {
                "fields": ['domain_id', 'state', 'updated_at'],
                "name": "COMPOUND_INDEX_FOR_GC_2"
            },
            {
                "fields": ['domain_id', 'state', '-deleted_at'],
                "name": "COMPOUND_INDEX_FOR_GC_3"
            },
            {
                "fields": ['domain_id', 'state', 'provider', 'cloud_service_group', 'cloud_service_type',
                           'reference.resource_id', 'account', 'cloud_service_id'],
                "name": "COMPOUND_INDEX_FOR_COLLECTOR"
            },
            {
                "fields": ['domain_id', 'state', 'provider', 'cloud_service_group', 'cloud_service_type',
                           'project_id', 'region_code'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"
            },
            {
                "fields": ['domain_id', 'state', 'ref_cloud_service_type', 'project_id', 'region_code'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"
            },
            {
                "fields": ['domain_id', 'state', '-created_at', 'project_id'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_3"
            },
            {
                "fields": ['domain_id', 'state', '-deleted_at', 'project_id'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_4"
            },
            {
                "fields": ['domain_id', 'cloud_service_id', 'project_id'],
                "name": "COMPOUND_INDEX_FOR_GET"
            }
        ]
    }

    def update(self, data):
        if self.state == 'DELETED':
            raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='CloudService', resource_id=self.cloud_service_id)

        return super().update(data)

    def delete(self):
        if self.state == 'DELETED':
            raise ERROR_RESOURCE_ALREADY_DELETED(resource_type='CloudService', resource_id=self.cloud_service_id)

        self.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })
