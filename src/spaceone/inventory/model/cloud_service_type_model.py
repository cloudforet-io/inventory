from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collection_info_model import CollectionInfo


class CloudServiceType(MongoModel):
    cloud_service_type_id = StringField(max_length=40, generate_id='cloud-svc-type', unique=True)
    name = StringField(max_length=255, unique_with=['provider', 'group', 'domain_id'])
    provider = StringField(max_length=255)
    group = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    service_code = StringField(max_length=255, default=None, null=True)
    is_primary = BooleanField(default=False)
    is_major = BooleanField(default=False)
    resource_type = StringField(max_length=255)
    labels = ListField(StringField(max_length=255))
    metadata = DictField()
    tags = DictField()
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'service_code',
            'is_primary',
            'is_major',
            'resource_type',
            'metadata',
            'labels',
            'ref_cloud_service_type',
            'tags',
            'collection_info',
            'updated_at'
        ],
        'exact_fields': [
            'cloud_service_type_id',
            'is_primary',
            'is_major',
            'domain_id',
            'collection_info.state'
        ],
        'minimal_fields': [
            'cloud_service_type_id',
            'name',
            'provider',
            'group',
            'service_code',
            'is_primary',
            'is_major',
            'resource_type',
            'collection_info.state'
        ],
        # 'ordering': [
        #     'provider',
        #     'group',
        #     'name'
        # ],
        'indexes': [
            'cloud_service_type_id',
            'name',
            'provider',
            'group',
            'ref_cloud_service_type',
            'service_code',
            'is_primary',
            'is_major',
            'resource_type',
            'labels',
            'domain_id',
            'collection_info.state',
            'collection_info.collectors',
            'collection_info.service_accounts',
            'collection_info.secrets',
            'created_at',
            'updated_at'
        ]
    }
