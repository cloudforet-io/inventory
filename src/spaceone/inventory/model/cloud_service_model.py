from mongoengine import *
from datetime import datetime

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource
from spaceone.inventory.error import *


class CloudService(MongoModel):
    cloud_service_id = StringField(max_length=40, generate_id='cloud-svc', unique=True)
    cloud_service_type = StringField(max_length=255, default='')
    state = StringField(max_length=20, choices=('INSERVICE', 'DELETED'), default='INSERVICE')
    provider = StringField(max_length=255, default='')
    cloud_service_group = StringField(max_length=255, default=None, null=True)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = DictField()
    region_code = StringField(max_length=255)
    region_type = StringField(max_length=255, choices=('AWS', 'GOOGLE_CLOUD', 'AZURE', 'DATACENTER'))
    region_ref = StringField(max_length=255)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    deleted_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'data',
            'metadata',
            'reference',
            'tags',
            'project_id',
            'collection_info'
        ],
        'exact_fields': [
            'cloud_service_id',
            'project_id',
            'domain_id',
            'collection_info.state'
        ],
        'minimal_fields': [
            'cloud_service_id',
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'reference.resource_id',
            'project_id'
        ],
        'ordering': [
            'provider',
            'cloud_service_group',
            'cloud_service_type'
        ],
        'indexes': [
            'cloud_service_id',
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'region_code',
            'region_type',
            'domain_id',
            'reference.resource_id',
            'collection_info.state'
        ],
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
            'region': None,
            'deleted_at': datetime.utcnow()
        })
