from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CloudServiceType(MongoModel):
    cloud_service_type_id = StringField(max_length=40, generate_id='cloud-svc-type', unique=True)
    name = StringField(max_length=255, unique_with=['provider', 'group', 'domain_id'])
    provider = StringField(max_length=255)
    group = StringField(max_length=255)
    cloud_service_type_key = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    service_code = StringField(max_length=255, default=None, null=True)
    is_primary = BooleanField(default=False)
    is_major = BooleanField(default=False)
    resource_type = StringField(max_length=255)
    labels = ListField(StringField(max_length=255))
    metadata = DictField()
    tags = DictField()
    domain_id = StringField(max_length=40)
    updated_by = StringField(default=None, null=True)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'cloud_service_type_key',
            'service_code',
            'is_primary',
            'is_major',
            'resource_type',
            'metadata',
            'labels',
            'tags',
            'updated_by',
            'updated_at'
        ],
        'minimal_fields': [
            'cloud_service_type_id',
            'name',
            'provider',
            'group',
            'service_code',
            'is_primary',
            'is_major',
            'resource_type'
        ],
        'ordering': [
            'provider',
            'group',
            'name'
        ],
        'indexes': [
            # 'cloud_service_type_id',
            'name',
            'provider',
            'group',
            'cloud_service_type_key',
            'ref_cloud_service_type',
            'service_code',
            'is_primary',
            'is_major',
            'resource_type',
            'labels',
            'domain_id',
            'created_at',
            'updated_at',
        ]
    }
