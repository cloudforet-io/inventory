from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.region_model import Region
from spaceone.inventory.model.collection_info_model import CollectionInfo
from spaceone.inventory.model.reference_resource_model import ReferenceResource


class CloudService(MongoModel):
    cloud_service_id = StringField(max_length=40, generate_id='cloud-svc', unique=True)
    cloud_service_type = StringField(max_length=255, default='')
    provider = StringField(max_length=255, default='')
    cloud_service_group = StringField(max_length=255, default=None, null=True)
    data = DictField()
    metadata = DictField()
    reference = EmbeddedDocumentField(ReferenceResource, default=ReferenceResource)
    tags = DictField()
    region = ReferenceField('Region', default=None, null=True, reverse_delete_rule=NULLIFY)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    collection_info = EmbeddedDocumentField(CollectionInfo, default=CollectionInfo)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'data',
            'metadata',
            'reference',
            'tags',
            'region',
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
            'reference',
            'collection_info.state'
        ],
        'change_query_keys': {
            'region_id': 'region.region_id'
        },
        'reference_query_keys': {
            'region': Region
        },
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
            'region',
            'domain_id',
            'reference.resource_id',
            'collection_info.state'
        ],
        'aggregate': {
            'lookup': {
                'region': {
                    'from': 'region'
                }
            }
        }
    }
