from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.error import *


class CloudServiceTag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)


class CloudService(MongoModel):
    cloud_service_id = StringField(max_length=40, generate_id='cloud-svc', unique=True)
    state = StringField(max_length=20, choices=('INSERVICE', 'DELETED'), default='INSERVICE')
    provider = StringField(max_length=255)
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = ListField(EmbeddedDocumentField(CloudServiceTag))
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    garbage_collection = DictField(default={})
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'data',
            'state',
            'metadata',
            'reference',
            'tags',
            'project_id',
            'region_code',
            'cloud_service_group',
            'cloud_service_type',
            'ref_cloud_service_type',
            'ref_region',
            'collection_info',
            'garbage_collection',
            'updated_at',
            'deleted_at'
        ],
        'minimal_fields': [
            'cloud_service_id',
            'cloud_service_group',
            'cloud_service_type',
            'provider',
            'reference.resource_id',
            'region_code',
            'project_id'
        ],
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
            'provider',
            'cloud_service_group',
            'cloud_service_type'
        ],
        'indexes': [
            'cloud_service_id',
            'state',
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
            'collection_info.state',
            'collection_info.collectors',
            'collection_info.service_accounts',
            'collection_info.secrets',
            'garbage_collection',
            'created_at',
            'updated_at',
            {
                "fields": ['domain_id', 'provider', 'region_code', 'state', 'project_id'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
            ('tags.key', 'tags.value')
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
